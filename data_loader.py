"""
data_loader.py - Единый загрузчик данных по BTC (1H баров)

Все функции принимают start_date в формате "YYYY-MM-DD".
Все функции возвращают DataFrame с индексом DatetimeIndex, tz-naive, name="datetime".

Источники данных:
    - Bybit API         : BTC OHLCV + taker_ratio + taker_buy/sell vol (1H и 1D)
    - yfinance          : BTC + макро-индикаторы (1D -> ffill 1H)
    - alternative.me    : Fear & Greed Index (1D -> ffill 1H)
    - Bybit Futures API : Open Interest (1H)
    - Bybit Futures API : Funding Rate (8H -> ffill 1H)
    - Bybit Futures API : Long/Short Ratio (1H)
    - Blockchain.com    : On-chain метрики BTC (1D -> ffill 1H)
    - yfinance          : MOVE Index - облигационный VIX (1D -> ffill 1H)

Использование:
    from data_loader import load_all
    data = load_all()
    data["candles_1h"]    - 1H свечи + taker_ratio + taker volumes (мастер-индекс)
    data["bybit_daily"]   - дневные свечи Bybit (ffill на 1H)
    data["macro"]         - макро-индикаторы (1D, ffill на 1H)
    data["fear_greed"]    - Fear & Greed (1D, ffill на 1H)
    data["open_interest"] - OI 1H
    data["funding_rate"]  - Funding Rate (8H, ffill на 1H)
    data["long_short"]    - Long/Short Ratio (1H)
    data["onchain"]       - On-chain метрики BTC (1D, ffill на 1H)

Изменения v2:
    - Убран warmup-эффект SMA200: все rolling() используют min_periods=1
    - _bybit_kline теперь использует _get_session() вместо прямого requests.get
    - Удалён дублирующий import requests
    - Логирование переведено с f-строк на %s (PEP8 + производительность)
    - Исправлены аннотации типов для совместимости с Python < 3.10
"""

import logging
import os
import time
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

DOWNLOAD_DAYS = 720  # сколько дней истории скачивать

_INTERVAL_1H_MS = 3_600_000
_INTERVAL_1D_MS = 86_400_000
_FR_CHUNK_MS = 5_184_000_000    # 60 дней
_LS_CHUNK_MS = 6_912_000_000    # 80 дней

# ---------------------------------------------------------------------------
# HTTP-сессия с retry
# ---------------------------------------------------------------------------

_SESSION = None


def _get_session():
    """Возвращает переиспользуемую HTTP-сессию с автоматическими retry."""
    global _SESSION
    if _SESSION is None:
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": "Mozilla/5.0 DataLoader/1.0"})
        _SESSION = session
    return _SESSION


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _to_dt(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")


def _to_ms(date_str):
    return int(_to_dt(date_str).timestamp() * 1000)


def _normalize_index(df, name="datetime"):
    df.index = pd.to_datetime(df.index)
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df.index.name = name
    return df.sort_index()


def _ms_col_to_index(df, col="timestamp"):
    df = df.copy()
    df.index = pd.to_datetime(df[col], unit="ms")
    df.index = df.index.tz_localize(None)
    df.index.name = "datetime"
    return df.drop(columns=[col]).sort_index()


def _end_date_default():
    return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def _ffill_to_master(df, master_index):
    combined = df.reindex(df.index.union(master_index)).sort_index()
    return combined.ffill().reindex(master_index)


def _bybit_paginate(endpoint, params):
    """Итерирует по страницам Bybit API с cursor-пагинацией."""
    all_records = []
    cursor = None
    session = _get_session()
    while True:
        if cursor:
            params["cursor"] = cursor
        try:
            resp = session.get(endpoint, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data["retCode"] != 0:
                logger.error("Bybit API ошибка: %s", data["retMsg"])
                break
            records = data["result"]["list"]
            next_cursor = data["result"].get("nextPageCursor", "")
            if not records:
                break
            all_records.extend(records)
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
            time.sleep(0.12)
        except Exception as exc:
            logger.error("Ошибка запроса %s: %s", endpoint, exc)
            break
    return all_records


def _bybit_kline(start_date, end_date, symbol, interval_str, interval_ms):
    """
    Скачивает OHLCV свечи с Bybit через сессию с retry.

    ИСПРАВЛЕНО: используем _get_session() вместо requests.get напрямую,
    что обеспечивает retry и переиспользование соединений.
    """
    base_url = "https://api.bybit.com/v5/market/kline"
    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_rows = []
    cur_start = start_ms
    session = _get_session()

    while cur_start < end_ms:
        cur_end = min(cur_start + 200 * interval_ms, end_ms)
        try:
            resp = session.get(
                base_url,
                params={
                    "category": "linear",
                    "symbol": symbol,
                    "interval": interval_str,
                    "start": cur_start,
                    "end": cur_end,
                    "limit": 200,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if data["retCode"] != 0:
                logger.error("Bybit kline API: %s", data["retMsg"])
                break
            rows = data["result"]["list"]
            if rows:
                all_rows.extend(rows)
        except Exception as exc:
            logger.error("Ошибка запроса Bybit kline: %s", exc)
        cur_start = cur_end
        time.sleep(0.15)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        all_rows,
        columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
    )
    df["timestamp"] = df["timestamp"].astype(int)
    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        df[col] = df[col].astype(float)

    df = _ms_col_to_index(df, col="timestamp")
    return df[~df.index.duplicated(keep="last")]


# ---------------------------------------------------------------------------
# Загрузчики данных
# ---------------------------------------------------------------------------

def load_candles_1h(start_date, end_date=None, symbol="BTCUSDT"):
    """
    1H OHLCV свечи BTC с Bybit.

    Колонки:
        taker_ratio     - позиция close внутри свечи (0=low, 1=high)
        taker_buy_vol   - оценочный объём агрессивных покупок в USDT
        taker_sell_vol  - оценочный объём агрессивных продаж в USDT
        taker_vol_ratio - дисбаланс покупок/продаж: >1 давление покупателей
    """
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Bybit 1H candles | %s -> %s", start_date, end_date)

    df = _bybit_kline(
        start_date, end_date,
        symbol=symbol,
        interval_str="60",
        interval_ms=_INTERVAL_1H_MS,
    )
    if df.empty:
        logger.warning("Bybit 1H: нет данных")
        return pd.DataFrame()

    hl = (df["high"] - df["low"]).replace(0, np.nan)
    df["taker_ratio"] = ((df["close"] - df["low"]) / hl).clip(0, 1)
    df["taker_buy_vol"] = df["turnover"] * df["taker_ratio"]
    df["taker_sell_vol"] = df["turnover"] * (1 - df["taker_ratio"])
    df["taker_vol_ratio"] = (
        df["taker_buy_vol"] / df["taker_sell_vol"].replace(0, np.nan)
    )

    logger.info(
        "Bybit 1H готово: %d свечей | %s - %s",
        len(df), df.index[0], df.index[-1],
    )
    return df


def load_bybit_daily(start_date, end_date=None, symbol="BTCUSDT"):
    """Дневные OHLCV свечи BTC с Bybit."""
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Bybit Daily | %s -> %s", start_date, end_date)

    df = _bybit_kline(
        start_date, end_date,
        symbol=symbol,
        interval_str="D",
        interval_ms=_INTERVAL_1D_MS,
    )
    if df.empty:
        logger.warning("Bybit Daily: нет данных")
        return pd.DataFrame()

    logger.info(
        "Bybit Daily готово: %d свечей | %s - %s",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df


MACRO_TICKERS = {
    "BTC-USD":  "Bitcoin",
    "SPY":      "S&P 500 ETF",
    "DX-Y.NYB": "Dollar Index (DXY)",
    "^VIX":     "CBOE VIX Index",
    "TLT":      "20+ Year Treasury",
    "IEF":      "7-10 Year Treasury",
    "GLD":      "Gold ETF",
    "USO":      "Oil ETF (WTI)",
    "^TNX":     "10Y Treasury Yield",
    "^MOVE":    "ICE BofA MOVE Index (Bond VIX)",
}


def _safe_yf_download(ticker, start, end, retries=3, delay=1.0):
    yf_logger = logging.getLogger("yfinance")
    yf_logger.setLevel(logging.CRITICAL)
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(
                ticker, start=start, end=end,
                interval="1d", progress=False,
                auto_adjust=True, threads=False,
            )
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            if attempt < retries:
                time.sleep(delay * attempt)
            else:
                logger.warning("[%s] ошибка после %d попыток: %s", ticker, retries, exc)
    return pd.DataFrame()


def _flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join(filter(None, map(str, c))).strip() for c in df.columns
        ]
    return df


def _get_close(df, ticker):
    df = _flatten_columns(df.copy())
    for col in [f"Close_{ticker}", "Close", ticker]:
        if col in df.columns:
            return df[col].rename(ticker)
    return None


def load_macro(start_date, end_date=None):
    """
    Дневные макро-индикаторы (ffill на 1H мастер-индекс в load_all).

    ИСПРАВЛЕНО: все rolling() используют min_periods=1, чтобы не создавать
    NaN-warmup в начале датасета. Это позволяет предсказывать на будущие
    числа без потери актуальных данных.

    Ранее rolling(200) без min_periods обрезал 199 первых значений,
    что при ограниченном окне данных приводило к потере обучающих примеров.
    """
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Макро-индикаторы | %s -> %s", start_date, end_date)

    data_dict = {}
    for ticker, name in MACRO_TICKERS.items():
        logger.info("  %-12s (%s)", ticker, name)
        df = _safe_yf_download(ticker, start=start_date, end=end_date)
        series = _get_close(df, ticker) if not df.empty else None
        if series is not None and not series.empty:
            data_dict[ticker] = series
        time.sleep(0.3)

    if not data_dict:
        logger.error("Не удалось загрузить ни одного тикера.")
        return pd.DataFrame()

    df = pd.concat(data_dict.values(), axis=1)
    df = _normalize_index(df)
    df = df.dropna(how="all")

    result = pd.DataFrame(index=df.index)

    if "BTC-USD" in df:
        btc = df["BTC-USD"]
        result["btc_close"] = btc
        result["btc_pct_change"] = btc.pct_change() * 100
        result["btc_log_return"] = np.log(btc / btc.shift(1))

    if "SPY" in df:
        result["sp500_close"] = df["SPY"]
        result["sp500_pct"] = df["SPY"].pct_change() * 100

    if "DX-Y.NYB" in df:
        result["dxy_close"] = df["DX-Y.NYB"]
        result["dxy_pct"] = df["DX-Y.NYB"].pct_change() * 100

    if "^VIX" in df:
        result["vix_close"] = df["^VIX"]
        logger.info(
            "VIX загружен: range [%.1f, %.1f]",
            df["^VIX"].min(), df["^VIX"].max(),
        )

    if "GLD" in df:
        result["gold_close"] = df["GLD"]
        if "btc_close" in result:
            result["btc_gold_ratio"] = result["btc_close"] / result["gold_close"]

    if "TLT" in df:
        result["tlt_close"] = df["TLT"]
        result["tlt_pct"] = df["TLT"].pct_change() * 100

    if "IEF" in df:
        result["ief_close"] = df["IEF"]

    if "USO" in df:
        result["oil_close"] = df["USO"]
        result["oil_pct"] = df["USO"].pct_change() * 100

    if "^TNX" in df:
        result["treasury_yield_10y"] = df["^TNX"]

    if "tlt_close" in result and "ief_close" in result:
        result["tlt_ief_spread"] = result["tlt_close"] - result["ief_close"]

    if "btc_pct_change" in result and "sp500_pct" in result:
        for w in (30, 90):
            # min_periods=max(w//2, 1) — допускаем расчёт при наличии хотя бы
            # половины окна, чтобы не терять первые недели датасета
            result[f"btc_spy_corr_{w}d"] = (
                result["btc_pct_change"].rolling(w, min_periods=max(w // 2, 1))
                .corr(result["sp500_pct"])
            )

    if "btc_pct_change" in result and "dxy_pct" in result:
        for w in (30, 90):
            result[f"btc_dxy_corr_{w}d"] = (
                result["btc_pct_change"].rolling(w, min_periods=max(w // 2, 1))
                .corr(result["dxy_pct"])
            )

    if "btc_close" in result and "sp500_close" in result:
        result["btc_spy_ratio"] = result["btc_close"] / result["sp500_close"]

    if "btc_close" in result:
        # КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: min_periods=1 вместо дефолтного min_periods=window.
        # Ранее rolling(200) без min_periods давал NaN на первых 199 строках,
        # что при скачивании ровно нужного периода "съедало" начало датасета.
        # С min_periods=1 значения вычисляются по доступным данным с первого бара.
        # Для SMA200 первые ~50 баров будут неточными, но не NaN — модель сама
        # научится дисконтировать ранние оценки через признак history_bars_available.
        for w in (7, 30, 90, 200):
            result[f"btc_sma_{w}d"] = result["btc_close"].rolling(w, min_periods=1).mean()
        result["btc_volatility_30d"] = (
            result["btc_pct_change"].rolling(30, min_periods=1).std()
        )

    if "^MOVE" in df:
        move = df["^MOVE"]
        result["move_index"] = move
        result["move_pct_change"] = move.pct_change() * 100

        move_mean = move.rolling(30, min_periods=10).mean()
        move_std = move.rolling(30, min_periods=10).std().replace(0, np.nan)
        result["move_z_score"] = (move - move_mean) / move_std

        if "vix_close" in result:
            vix = result["vix_close"]
            vix_mean = vix.rolling(30, min_periods=10).mean()
            vix_std = vix.rolling(30, min_periods=10).std().replace(0, np.nan)
            vix_z = (vix - vix_mean) / vix_std
            result["move_vix_spread"] = result["move_z_score"] - vix_z

        logger.info("MOVE Index добавлен")
    else:
        logger.warning("^MOVE не загружен. Признаки move_* будут пропущены.")

    logger.info(
        "Макро готово: %d дней, %d колонок", len(result), len(result.columns)
    )
    return result


def load_fear_greed(start_date):
    """Crypto Fear & Greed Index (alternative.me). Колонки: fear_greed_index."""
    start_ms = _to_ms(start_date)
    logger.info("Fear & Greed Index | %s -> сегодня", start_date)
    try:
        resp = _get_session().get(
            "https://api.alternative.me/fng/?limit=0", timeout=30
        )
        resp.raise_for_status()
        raw = resp.json().get("data", [])
    except Exception as exc:
        logger.error("Fear & Greed API ошибка: %s", exc)
        return pd.DataFrame()

    if not raw:
        logger.warning("Fear & Greed: пустой ответ")
        return pd.DataFrame()

    df = pd.DataFrame(raw)
    df["timestamp_ms"] = df["timestamp"].astype(int) * 1000
    df = df[df["timestamp_ms"] >= start_ms]
    df = df[["timestamp_ms", "value"]].rename(columns={"value": "fear_greed_index"})
    df["fear_greed_index"] = df["fear_greed_index"].astype(int)
    df = _ms_col_to_index(df, col="timestamp_ms")
    df = df[~df.index.duplicated(keep="last")]
    logger.info("Fear & Greed готово: %d дней", len(df))
    return df


def load_open_interest(start_date, end_date=None, symbol="BTCUSDT", interval="1h"):
    """Open Interest с Bybit (1H). Колонки: open_interest."""
    endpoint = "https://api.bybit.com/v5/market/open-interest"
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Open Interest (%s) | %s -> %s", interval, start_date, end_date)

    records = _bybit_paginate(endpoint, {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": interval,
        "startTime": _to_ms(start_date),
        "endTime": _to_ms(end_date),
        "limit": 200,
    })
    if not records:
        logger.warning("Open Interest: нет данных")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["timestamp"] = df["timestamp"].astype(int)
    df["open_interest"] = df["openInterest"].astype(float)
    df = df[["timestamp", "open_interest"]]
    df = _ms_col_to_index(df, col="timestamp")
    df = df[~df.index.duplicated(keep="last")]
    logger.info("Open Interest готово: %d записей", len(df))
    return df


def load_funding_rate(start_date, end_date=None, symbol="BTCUSDT"):
    """Funding Rate с Bybit (каждые 8 часов). Колонки: funding_rate."""
    endpoint = "https://api.bybit.com/v5/market/funding/history"
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Funding Rate | %s -> %s", start_date, end_date)

    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_records = []
    cur_start = start_ms

    while cur_start < end_ms:
        cur_end = min(cur_start + _FR_CHUNK_MS, end_ms)
        records = _bybit_paginate(endpoint, {
            "category": "linear",
            "symbol": symbol,
            "startTime": cur_start,
            "endTime": cur_end,
            "limit": 200,
        })
        if records:
            all_records.extend(records)
            logger.info("  +%d записей FR", len(records))
        cur_start = cur_end
        time.sleep(0.15)

    if not all_records:
        logger.warning("Funding Rate: нет данных")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["timestamp"] = df["fundingRateTimestamp"].astype(int)
    df["funding_rate"] = df["fundingRate"].astype(float)
    df = df[["timestamp", "funding_rate"]]
    df = _ms_col_to_index(df, col="timestamp")
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    logger.info(
        "Funding Rate готово: %d записей | %s - %s",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df


def load_long_short_ratio(start_date, end_date=None, symbol="BTCUSDT", period="1h"):
    """Long/Short Ratio трейдеров с Bybit (1H). Колонки: buy_ratio, sell_ratio, ls_ratio."""
    endpoint = "https://api.bybit.com/v5/market/account-ratio"
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Long/Short Ratio (%s) | %s -> %s", period, start_date, end_date)

    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_records = []
    cur_start = start_ms
    session = _get_session()

    while cur_start < end_ms:
        cur_end = min(cur_start + _LS_CHUNK_MS, end_ms)
        try:
            resp = session.get(
                endpoint,
                params={
                    "category": "linear",
                    "symbol": symbol,
                    "period": period,
                    "limit": 500,
                    "startTime": cur_start,
                    "endTime": cur_end,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if data["retCode"] != 0:
                logger.error("L/S Ratio API: %s", data["retMsg"])
                break
            records = data["result"]["list"]
            if records:
                all_records.extend(records)
                logger.info("  +%d записей L/S", len(records))
        except Exception as exc:
            logger.error("Ошибка L/S Ratio: %s", exc)
        cur_start = cur_end
        time.sleep(0.20)

    if not all_records:
        logger.warning("Long/Short Ratio: нет данных")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["timestamp"] = df["timestamp"].astype(int)
    df["buy_ratio"] = df["buyRatio"].astype(float)
    df["sell_ratio"] = df["sellRatio"].astype(float)
    df["ls_ratio"] = df["buy_ratio"] / df["sell_ratio"].replace(0, np.nan)
    df = df[["timestamp", "buy_ratio", "sell_ratio", "ls_ratio"]]
    df = _ms_col_to_index(df, col="timestamp")
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    logger.info(
        "Long/Short Ratio готово: %d записей | %s - %s",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df


# Чарты Blockchain.com и соответствующие названия колонок
_ONCHAIN_CHARTS = {
    "n-transactions":                  "onchain_tx_count",
    "mempool-count":                   "onchain_mempool_count",
    "mempool-size":                    "onchain_mempool_bytes",
    "hash-rate":                       "onchain_hash_rate",
    "transaction-fees-usd":            "onchain_fees_usd",
    "estimated-transaction-volume-usd": "onchain_tx_volume_usd",
}


def load_onchain(start_date, end_date=None):
    """
    On-chain метрики Bitcoin - Blockchain.com Charts API (бесплатно, без ключей).

    Эндпоинт: https://api.blockchain.info/charts/$chart?start=YYYY-MM-DD&sampled=false&format=json
    Гранулярность: 1D -> ffill на 1H мастер-индекс в load_all.

    Колонки:
        onchain_tx_count      - подтверждённых транзакций в день
        onchain_mempool_count - транзакций в мемпуле (моментальный снимок)
        onchain_mempool_bytes - размер мемпула в байтах
        onchain_hash_rate     - хешрейт сети (TH/s)
        onchain_fees_usd      - суммарные комиссии майнерам в USD
        onchain_tx_volume_usd - оценочный объём переводов в USD

    Производные признаки:
        onchain_fees_per_tx   - средняя комиссия на транзакцию (USD)
        onchain_tx_count_pct  - дневное изменение числа транзакций (%)
        onchain_hash_rate_pct - дневное изменение хешрейта (%)
        onchain_mempool_pct   - дневное изменение мемпула (%)
    """
    if end_date is None:
        end_date = _end_date_default()
    logger.info("On-chain метрики | %s -> %s", start_date, end_date)

    base_url = "https://api.blockchain.info/charts/{chart}"
    series_dict = {}
    session = _get_session()

    for chart, col_name in _ONCHAIN_CHARTS.items():
        try:
            resp = session.get(
                base_url.format(chart=chart),
                params={
                    "start": start_date,
                    "format": "json",
                    "sampled": "false",
                },
                timeout=20,
            )
            resp.raise_for_status()
            raw = resp.json()

            if raw.get("status") != "ok" or not raw.get("values"):
                logger.warning("  %s: пустой ответ", chart)
                continue

            records = raw["values"]
            ts = [r["x"] for r in records]
            val = [r["y"] for r in records]

            s = pd.Series(val, index=pd.to_datetime(ts, unit="s"), name=col_name)
            s.index = s.index.tz_localize(None)
            s.index.name = "datetime"
            series_dict[col_name] = s
            logger.info("  %s: %d точек", chart, len(s))

        except Exception as exc:
            logger.warning("  %s: ошибка - %s", chart, exc)

        time.sleep(0.3)

    if not series_dict:
        logger.warning("On-chain: не удалось загрузить ни одного чарта")
        return pd.DataFrame()

    df = pd.concat(series_dict.values(), axis=1).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[
        (df.index >= pd.Timestamp(start_date))
        & (df.index < pd.Timestamp(end_date))
    ]

    if "onchain_fees_usd" in df and "onchain_tx_count" in df:
        df["onchain_fees_per_tx"] = (
            df["onchain_fees_usd"] / df["onchain_tx_count"].replace(0, np.nan)
        )

    if "onchain_tx_count" in df:
        df["onchain_tx_count_pct"] = df["onchain_tx_count"].pct_change() * 100

    if "onchain_hash_rate" in df:
        df["onchain_hash_rate_pct"] = df["onchain_hash_rate"].pct_change() * 100

    if "onchain_mempool_count" in df:
        df["onchain_mempool_pct"] = df["onchain_mempool_count"].pct_change() * 100

    logger.info(
        "On-chain готово: %d дней, %d колонок | %s - %s",
        len(df), len(df.columns), df.index[0].date(), df.index[-1].date(),
    )
    return df


def load_all(start_date=None, end_date=None, days=DOWNLOAD_DAYS):
    """
    Загружает все источники и приводит к единому 1H мастер-индексу.

    Аргументы:
        start_date : начальная дата "YYYY-MM-DD" (игнорируется, если указан days)
        end_date   : конечная дата "YYYY-MM-DD" (по умолчанию: завтра)
        days       : сколько дней назад от сегодня скачать (приоритет над start_date)

    Возвращает dict:
        "candles_1h"    - 1H свечи + taker_ratio + taker volumes (мастер-индекс)
        "bybit_daily"   - дневные свечи (ffill на 1H)
        "macro"         - макро (1D, ffill на 1H)
        "fear_greed"    - Fear & Greed (1D, ffill на 1H)
        "open_interest" - OI (1H)
        "funding_rate"  - Funding Rate (8H, ffill на 1H)
        "long_short"    - Long/Short Ratio (1H)
        "onchain"       - On-chain метрики BTC (1D, ffill на 1H)
    """
    if end_date is None:
        end_date = _end_date_default()
    if days is not None:
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        logger.info("Режим days=%d: скачиваем с %s по %s", days, start_date, end_date)
    elif start_date is None:
        raise ValueError("Укажите start_date или days.")

    logger.info("load_all | %s -> %s", start_date, end_date)

    result = {}
    result["candles_1h"] = load_candles_1h(start_date, end_date)
    if result["candles_1h"].empty:
        logger.error("candles_1h пуст - загрузка прервана")
        return result

    master_idx = result["candles_1h"].index

    def _align(df):
        return _ffill_to_master(df, master_idx) if not df.empty else df

    result["bybit_daily"] = _align(load_bybit_daily(start_date, end_date))
    result["macro"] = _align(load_macro(start_date, end_date))
    result["fear_greed"] = _align(load_fear_greed(start_date))
    result["open_interest"] = _align(
        load_open_interest(start_date, end_date, interval="1h")
    )
    result["funding_rate"] = _align(load_funding_rate(start_date, end_date))
    result["long_short"] = _align(
        load_long_short_ratio(start_date, end_date, period="1h")
    )
    result["onchain"] = _align(load_onchain(start_date, end_date))

    logger.info("Итого загружено:")
    for k, v in result.items():
        if isinstance(v, pd.DataFrame) and not v.empty:
            logger.info(
                "  %-20s %7d строк | %s - %s",
                k, len(v), v.index[0].date(), v.index[-1].date(),
            )
        else:
            logger.info("  %-20s пусто", k)

    return result


def save_all(data, prefix=""):
    """Сохраняет все DataFrame из словаря data в CSV-файлы."""
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    p = f"{prefix}_" if prefix else ""
    for name, df in data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            fname = f"{p}{name}_{stamp}.csv"
            df.to_csv(fname)
            logger.info("Сохранено: %s", fname)


if __name__ == "__main__":
    data = load_all()
    save_all(data, prefix="btc")
