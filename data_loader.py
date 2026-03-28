"""
data_loader.py - Единый загрузчик данных по BTC

Все функции принимают start_date в формате "YYYY-MM-DD".
Все функции возвращают DataFrame с индексом типа DatetimeIndex,
tz-naive (без timezone), именованным "datetime".

Источники данных:
    - Bybit API         : BTC OHLCV + taker_ratio (4H и 1D)
    - yfinance          : BTC + макро-индикаторы (1D)
    - alternative.me    : Fear & Greed Index (1D -> ffill 4H)
    - Bybit Futures API : Open Interest (4H)
    - Bybit Futures API : Funding Rate (8H -> ffill 4H)
    - Bybit Futures API : Long/Short Ratio (4H)
    - Coinglass API     : Ликвидации long/short BTC в USD (4H)
    - yfinance          : MOVE Index — облигационный VIX (1D -> ffill 4H)

Использование:
    from data_loader import load_all
    data = load_all(start_date="2023-10-01")
    data["candles_4h"]    - 4H свечи + taker_ratio (мастер-индекс)
    data["bybit_daily"]   - дневные свечи Bybit (ffill на 4H)
    data["macro"]         - макро-индикаторы (1D, ffill на 4H), включая MOVE Index
    data["fear_greed"]    - Fear & Greed (1D, ffill на 4H)
    data["open_interest"] - OI 4H
    data["funding_rate"]  - Funding Rate (8H, ffill на 4H)
    data["long_short"]    - Long/Short Ratio (4H)
    data["liquidations"]  - Ликвидации BTC (4H, если задан API-ключ Coinglass)

Coinglass API (бесплатно):
    Зарегистрироваться на coinglass.com -> Settings -> API Key
    load_all(coinglass_api_key="api_ключ")
    Или через переменную окружения: COINGLASS_API_KEY

Примечание по Taker Ratio:
    Bybit kline не возвращает taker_buy_vol в стандартном ответе.
    Используем классическую формулу buying pressure из OHLCV:
        taker_ratio = (close - low) / (high - low)
    0.5 = нейтраль, >0.6 = давление покупателей, <0.4 = давление продавцов.
"""

import os
import time
import logging
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

START_DATE = "2023-03-12"

# Coinglass: max 200 записей / запрос на free tier, чанки по 33 дня (33*6=198 4H-баров)
_COINGLASS_CHUNK_DAYS = 33
_COINGLASS_LIMIT = 200


# ОБЩИЕ УТИЛИТЫ

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
    df = df.drop(columns=[col])
    return df.sort_index()


def _end_date_default():
    return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def _ffill_to_master(df, master_index):
    combined = df.reindex(df.index.union(master_index)).sort_index()
    combined = combined.ffill()
    return combined.reindex(master_index)


def _period_to_start(period):
    period = period.lower().strip()
    now = pd.Timestamp.now().normalize()
    if period.endswith("d"):
        start = now - pd.Timedelta(days=int(period[:-1]))
    elif period.endswith("mo"):
        start = now - pd.DateOffset(months=int(period[:-2]))
    elif period.endswith("y"):
        start = now - pd.DateOffset(years=int(period[:-1]))
    else:
        raise ValueError(f"Неверный период: {period}. Примеры: '30d', '6mo', '2y'")
    return start.strftime("%Y-%m-%d")


# ПАГИНАЦИЯ

def _bybit_paginate(endpoint, params):
    all_records = []
    cursor = None

    while True:
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(endpoint, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data["retCode"] != 0:
                logger.error(f"Bybit API ошибка: {data['retMsg']}")
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

        except Exception as e:
            logger.error(f"Ошибка запроса {endpoint}: {e}")
            break

    return all_records


# 1 BYBIT KLINE (4H и 1D свечи)

def _bybit_kline(start_date, end_date, symbol, interval_str, interval_ms):
    base_url = "https://api.bybit.com/v5/market/kline"
    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_rows = []
    cur_start = start_ms

    while cur_start < end_ms:
        cur_end = min(cur_start + 200 * interval_ms, end_ms)

        try:
            resp = requests.get(base_url, params={
                "category": "linear",
                "symbol": symbol,
                "interval": interval_str,
                "start": cur_start,
                "end": cur_end,
                "limit": 200,
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data["retCode"] != 0:
                logger.error(f"Bybit kline API: {data['retMsg']}")
                break

            rows = data["result"]["list"]
            if rows:
                all_rows.extend(rows)

        except Exception as e:
            logger.error(f"Ошибка запроса Bybit kline: {e}")

        cur_start = cur_end
        time.sleep(0.15)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=[
        "timestamp", "open", "high", "low", "close", "volume", "turnover"
    ])
    df["timestamp"] = df["timestamp"].astype(int)
    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        df[col] = df[col].astype(float)

    df = _ms_col_to_index(df, col="timestamp")
    df = df[~df.index.duplicated(keep="last")]
    return df


def load_candles_4h(start_date, end_date=None, symbol="BTCUSDT"):
    """
    4H OHLCV свечи BTC с Bybit + taker_ratio.

    taker_ratio = (close - low) / (high - low)
        ~0.5 = нейтраль
        >0.6 = давление покупателей
        <0.4 = давление продавцов
    """
    if end_date is None:
        end_date = _end_date_default()

    logger.info(f"Bybit 4H candles | {start_date} -> {end_date}")

    df = _bybit_kline(
        start_date, end_date,
        symbol=symbol,
        interval_str="240",
        interval_ms=4 * 60 * 60 * 1000,
    )

    if df.empty:
        logger.warning("Bybit 4H: нет данных")
        return pd.DataFrame()

    hl = (df["high"] - df["low"]).replace(0, np.nan)
    df["taker_ratio"] = ((df["close"] - df["low"]) / hl).clip(0, 1)

    logger.info(f"Bybit 4H готово: {len(df)} свечей | {df.index[0]} - {df.index[-1]}")
    return df


def load_bybit_daily(start_date, end_date=None, symbol="BTCUSDT"):
    """Дневные OHLCV свечи BTC с Bybit."""
    if end_date is None:
        end_date = _end_date_default()

    logger.info(f"Bybit Daily | {start_date} -> {end_date}")

    df = _bybit_kline(
        start_date, end_date,
        symbol=symbol,
        interval_str="D",
        interval_ms=24 * 60 * 60 * 1000,
    )

    if df.empty:
        logger.warning("Bybit Daily: нет данных")
        return pd.DataFrame()

    logger.info(f"Bybit Daily готово: {len(df)} свечей | {df.index[0].date()} - {df.index[-1].date()}")
    return df


# 2 МАКРО-ИНДИКАТОРЫ

# ^VIX  : CBOE VIX spot (не ETF). Порог > 30 = рыночный стресс.
# ^MOVE : ICE BofA MOVE Index — облигационный VIX.
#         Нормальный уровень ~80-100 bp. Рост -> BTC обычно падает.

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


def _safe_yf_download(ticker, start, end, interval="1d", retries=3, delay=1.0):
    yf_logger = logging.getLogger("yfinance")
    yf_logger.setLevel(logging.CRITICAL)

    for attempt in range(1, retries + 1):
        try:
            df = yf.download(
                ticker, start=start, end=end,
                interval=interval, progress=False,
                auto_adjust=True, threads=False,
            )
            if df is not None and not df.empty:
                return df
        except Exception as e:
            if attempt < retries:
                time.sleep(delay * attempt)
            else:
                logger.warning(f"[{ticker}] ошибка после {retries} попыток: {e}")
    return pd.DataFrame()


def _flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(filter(None, map(str, c))).strip() for c in df.columns]
    return df


def _get_close(df, ticker):
    df = _flatten_columns(df.copy())
    for col in [f"Close_{ticker}", "Close", ticker]:
        if col in df.columns:
            return df[col].rename(ticker)
    return None


def load_macro(start_date, end_date=None):
    """
    Дневные макро-индикаторы: цены закрытия + производные метрики.

    VIX : используется ^VIX (spot), порог > 30 = рыночный стресс (~10-15% времени).
          VIX ETF не используется — имеет другие ценовые уровни и decay.

    MOVE Index признаки:
        move_index       — абсолютное значение MOVE (bp)
        move_pct_change  — дневное изменение MOVE (%)
        move_z_score     — z-score MOVE относительно 30-дневной нормы
        move_vix_spread  — z-score MOVE минус z-score VIX:
                           > 0 — облигационный рынок нервознее акционного
                           < 0 — стандартный equity-риск
    """
    if end_date is None:
        end_date = _end_date_default()

    logger.info(f"Макро-индикаторы | {start_date} -> {end_date}")
    data_dict = {}

    for ticker, name in MACRO_TICKERS.items():
        logger.info(f"  {ticker:<12} ({name})")
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
        logger.info(f"VIX загружен: range [{df['^VIX'].min():.1f}, {df['^VIX'].max():.1f}]")

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
            result[f"btc_spy_corr_{w}d"] = (
                result["btc_pct_change"].rolling(w).corr(result["sp500_pct"])
            )

    if "btc_pct_change" in result and "dxy_pct" in result:
        for w in (30, 90):
            result[f"btc_dxy_corr_{w}d"] = (
                result["btc_pct_change"].rolling(w).corr(result["dxy_pct"])
            )

    if "btc_close" in result and "sp500_close" in result:
        result["btc_spy_ratio"] = result["btc_close"] / result["sp500_close"]

    if "btc_close" in result:
        for w in (7, 30, 90, 200):
            result[f"btc_sma_{w}d"] = result["btc_close"].rolling(w).mean()
        result["btc_volatility_30d"] = result["btc_pct_change"].rolling(30).std()

    if "^MOVE" in df:
        move = df["^MOVE"]
        result["move_index"]      = move
        result["move_pct_change"] = move.pct_change() * 100

        move_mean = move.rolling(30, min_periods=20).mean()
        move_std  = move.rolling(30, min_periods=20).std().replace(0, np.nan)
        result["move_z_score"] = (move - move_mean) / move_std

        if "vix_close" in result:
            vix      = result["vix_close"]
            vix_mean = vix.rolling(30, min_periods=20).mean()
            vix_std  = vix.rolling(30, min_periods=20).std().replace(0, np.nan)
            vix_z    = (vix - vix_mean) / vix_std
            result["move_vix_spread"] = result["move_z_score"] - vix_z

        logger.info("MOVE Index добавлен")
    else:
        logger.warning("^MOVE не загружен (yfinance). Признаки move_* будут пропущены.")

    logger.info(f"Макро готово: {len(result)} дней, {len(result.columns)} колонок")
    return result


# 3 FEAR & GREED INDEX

def load_fear_greed(start_date):
    """Crypto Fear & Greed Index (alternative.me). Колонки: fear_greed_index."""
    api_url = "https://api.alternative.me/fng/?limit=0"
    start_ms = _to_ms(start_date)

    logger.info(f"Fear & Greed Index | {start_date} -> сегодня")

    try:
        resp = requests.get(api_url, timeout=30)
        resp.raise_for_status()
        raw = resp.json().get("data", [])
    except Exception as e:
        logger.error(f"Fear & Greed API ошибка: {e}")
        return pd.DataFrame()

    if not raw:
        logger.warning("Fear & Greed: API вернул пустой ответ")
        return pd.DataFrame()

    df = pd.DataFrame(raw)
    df["timestamp_ms"] = df["timestamp"].astype(int) * 1000
    df = df[df["timestamp_ms"] >= start_ms]
    df = df[["timestamp_ms", "value"]].rename(columns={"value": "fear_greed_index"})
    df["fear_greed_index"] = df["fear_greed_index"].astype(int)
    df = _ms_col_to_index(df, col="timestamp_ms")
    df = df[~df.index.duplicated(keep="last")]

    logger.info(f"Fear & Greed готово: {len(df)} дней")
    return df


# 4 OPEN INTEREST

def load_open_interest(start_date, end_date=None, symbol="BTCUSDT", interval="4h"):
    """Open Interest с Bybit. Колонки: open_interest."""
    endpoint = "https://api.bybit.com/v5/market/open-interest"
    interval_map = {"1h": "1h", "4h": "4h", "1d": "1d"}

    if end_date is None:
        end_date = _end_date_default()

    logger.info(f"Open Interest ({interval}) | {start_date} -> {end_date}")

    records = _bybit_paginate(endpoint, {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": interval_map.get(interval, "4h"),
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

    logger.info(f"Open Interest готово: {len(df)} записей")
    return df


# 5 FUNDING RATE

def load_funding_rate(start_date, end_date=None, symbol="BTCUSDT"):
    """
    Funding Rate с Bybit (каждые 8 часов).
    Чанки по 60 дней — API лимит ~200 записей / запрос.
    Колонки: funding_rate
    """
    endpoint = "https://api.bybit.com/v5/market/funding/history"
    chunk_ms = 60 * 24 * 60 * 60 * 1000

    if end_date is None:
        end_date = _end_date_default()

    logger.info(f"Funding Rate | {start_date} -> {end_date}")

    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_records = []
    cur_start = start_ms

    while cur_start < end_ms:
        cur_end = min(cur_start + chunk_ms, end_ms)
        records = _bybit_paginate(endpoint, {
            "category": "linear",
            "symbol": symbol,
            "startTime": cur_start,
            "endTime": cur_end,
            "limit": 200,
        })
        if records:
            all_records.extend(records)
            logger.info(f"  +{len(records)} записей FR")
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
        f"Funding Rate готово: {len(df)} записей | "
        f"{df.index[0].date()} - {df.index[-1].date()}"
    )
    return df


# 6 LONG / SHORT RATIO

def load_long_short_ratio(start_date, end_date=None, symbol="BTCUSDT", period="4h"):
    """
    Long/Short Ratio трейдеров с Bybit.

    Contrarian сигнал:
        buy_ratio > 0.70 — все в лонге -> возможный разворот вниз
        buy_ratio < 0.30 — все в шорте -> возможный short squeeze

    Колонки: buy_ratio, sell_ratio, ls_ratio (buy/sell)
    """
    endpoint = "https://api.bybit.com/v5/market/account-ratio"
    chunk_ms = 80 * 24 * 60 * 60 * 1000

    if end_date is None:
        end_date = _end_date_default()

    logger.info(f"Long/Short Ratio ({period}) | {start_date} -> {end_date}")

    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_records = []
    cur_start = start_ms

    while cur_start < end_ms:
        cur_end = min(cur_start + chunk_ms, end_ms)

        try:
            resp = requests.get(endpoint, params={
                "category": "linear",
                "symbol": symbol,
                "period": period,
                "limit": 500,
                "startTime": cur_start,
                "endTime": cur_end,
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data["retCode"] != 0:
                logger.error(f"L/S Ratio API: {data['retMsg']}")
                break

            records = data["result"]["list"]
            if records:
                all_records.extend(records)
                logger.info(f"  +{len(records)} записей L/S")

        except Exception as e:
            logger.error(f"Ошибка L/S Ratio: {e}")

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
        f"Long/Short Ratio готово: {len(df)} записей | "
        f"{df.index[0].date()} - {df.index[-1].date()}"
    )
    return df


# 7 ЛИКВИДАЦИИ (Coinglass API)

def load_liquidations(start_date, end_date=None, symbol="BTC",
                      api_key=None, interval="4h"):
    """
    Агрегированные ликвидации BTC по всем биржам — Coinglass API.

    liq_ratio = long_liq / short_liq:
        >> 1 — ликвидируются преимущественно лонги (давление вниз)
        << 1 — ликвидируются преимущественно шорты (давление вверх)

    Колонки: long_liq_usd, short_liq_usd

    Free tier ограничен 200 записями / запрос.
    Чанки по _COINGLASS_CHUNK_DAYS дней, с задержкой между запросами.
    """
    key = api_key or os.environ.get("COINGLASS_API_KEY", "")
    if not key:
        logger.warning(
            "Coinglass API ключ не задан. Загрузка ликвидаций пропускается.\n"
            "  Задайте: load_all(coinglass_api_key='ваш_ключ')\n"
        )
        return pd.DataFrame()

    if end_date is None:
        end_date = _end_date_default()

    endpoint = "https://open-api.coinglass.com/api/pro/v1/futures/liquidation/chart"
    interval_map = {"1h": "1h", "4h": "4h", "12h": "12h", "1d": "1d"}
    chunk_ms = _COINGLASS_CHUNK_DAYS * 24 * 60 * 60 * 1000

    logger.info(f"Coinglass Liquidations ({interval}) | {start_date} -> {end_date}")

    start_ms = _to_ms(start_date)
    end_ms   = _to_ms(end_date)
    all_rows = []
    cur_start = start_ms

    while cur_start < end_ms:
        cur_end = min(cur_start + chunk_ms, end_ms)

        try:
            resp = requests.get(
                endpoint,
                headers={"coinglassSecret": key},
                params={
                    "symbol":    symbol,
                    "interval":  interval_map.get(interval, "4h"),
                    "limit":     _COINGLASS_LIMIT,
                    "startTime": cur_start,
                    "endTime":   cur_end,
                },
                timeout=30,
            )
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            logger.error(f"Coinglass API ошибка: {e}")
            break

        if raw.get("code") not in ("0", 0) and raw.get("success") is not True:
            logger.error(f"Coinglass API вернул ошибку: {raw.get('msg', raw)}")
            break

        data      = raw.get("data", {})
        date_list = data.get("dateList",  [])
        long_list = data.get("longList",  [])
        short_list = data.get("shortList", [])

        if date_list:
            for ts, lng, sht in zip(date_list, long_list, short_list):
                all_rows.append({
                    "timestamp":     int(ts),
                    "long_liq_usd":  float(lng),
                    "short_liq_usd": float(sht),
                })
            logger.info(f"  +{len(date_list)} записей LIQ")

        cur_start = cur_end
        time.sleep(0.5)

    if not all_rows:
        logger.warning("Coinglass: нет данных в запрошенном диапазоне")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df = _ms_col_to_index(df, col="timestamp")
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()

    logger.info(
        f"Liquidations готово: {len(df)} записей | "
        f"{df.index[0].date()} - {df.index[-1].date()} | "
        f"long_liq_usd sum=${df['long_liq_usd'].sum()/1e9:.2f}B"
    )
    return df


# МАСТЕР-ЗАГРУЗЧИК

def load_all(start_date=START_DATE, end_date=None, coinglass_api_key=None):
    """
    Загружает все источники и приводит к единому 4H мастер-индексу.

    Аргументы:
        start_date        : начальная дата "YYYY-MM-DD"
        end_date          : конечная дата "YYYY-MM-DD" или None (сегодня)
        coinglass_api_key : ключ API Coinglass для ликвидаций (опционально)
                            Альтернативно — задайте env COINGLASS_API_KEY

    Возвращает dict:
        "candles_4h"    - 4H свечи + taker_ratio (мастер-индекс)
        "bybit_daily"   - дневные свечи (ffill на 4H)
        "macro"         - макро (1D, ffill на 4H), включая MOVE Index
        "fear_greed"    - Fear & Greed (1D, ffill на 4H)
        "open_interest" - OI (4H)
        "funding_rate"  - Funding Rate (8H, ffill на 4H)
        "long_short"    - Long/Short Ratio (4H)
        "liquidations"  - Ликвидации BTC long/short в USD (4H)
    """
    if end_date is None:
        end_date = _end_date_default()

    logger.info(f"load_all | {start_date} -> {end_date}")

    result = {}

    result["candles_4h"] = load_candles_4h(start_date, end_date)
    if result["candles_4h"].empty:
        logger.error("candles_4h пуст — загрузка прервана")
        return result
    master_idx = result["candles_4h"].index

    def _align(df):
        if df.empty:
            return df
        return _ffill_to_master(df, master_idx)

    result["bybit_daily"]   = _align(load_bybit_daily(start_date, end_date))
    result["macro"]         = _align(load_macro(start_date, end_date))
    result["fear_greed"]    = _align(load_fear_greed(start_date))
    result["open_interest"] = _align(load_open_interest(start_date, end_date, interval="4h"))
    result["funding_rate"]  = _align(load_funding_rate(start_date, end_date))
    result["long_short"]    = _align(load_long_short_ratio(start_date, end_date, period="4h"))
    result["liquidations"]  = _align(
        load_liquidations(start_date, end_date, api_key=coinglass_api_key)
    )

    logger.info("Итого загружено:")
    for k, v in result.items():
        if isinstance(v, pd.DataFrame) and not v.empty:
            logger.info(
                f"  {k:<20} {len(v):>6} строк | "
                f"{v.index[0].date()} - {v.index[-1].date()}"
            )
        else:
            logger.info(f"  {k:<20} пусто (нет данных или ограничение API)")

    return result


def save_all(data, prefix=""):
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    p = f"{prefix}_" if prefix else ""
    for name, df in data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            fname = f"{p}{name}_{stamp}.csv"
            df.to_csv(fname)
            logger.info(f"Сохранено: {fname}")


if __name__ == "__main__":
    data = load_all(start_date=START_DATE)
    save_all(data, prefix="btc")