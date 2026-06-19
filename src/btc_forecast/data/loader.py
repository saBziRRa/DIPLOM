"""
data_loader.py — Единый загрузчик данных по BTC (1H и 6H бары).

Все функции принимают start_date в формате "YYYY-MM-DD".
Все функции возвращают DataFrame с индексом DatetimeIndex, tz-naive,
name="datetime", время — московское (MSK, UTC+3).

Источники данных:
    - Bybit API         : BTC OHLCV 1H/6H + taker_ratio + taker volumes
    - yfinance          : BTC + макро-индикаторы (1D → ffill 1H)
    - alternative.me    : Fear & Greed Index (1D → ffill 1H)
    - Bybit Futures API : Open Interest (1H)
    - Bybit Futures API : Funding Rate (8H → ffill 1H)
    - Bybit Futures API : Long/Short Ratio (1H)
    - CryptoCompare News: новостной поток BTC (скоринг CryptoBERT/VADER)
    - CryptoPanic News  : агрегатор крипто-новостей (нужен auth_token)


Использование:
    from data_loader import load_all
    data = load_all(days=365)
"""

import logging
import os
import time
import warnings
from datetime import datetime, timedelta, timezone

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

# Константы


DOWNLOAD_DAYS = 720

_INTERVAL_1H_MS = 3_600_000
_INTERVAL_6H_MS = 21_600_000
_INTERVAL_1D_MS = 86_400_000

_FR_CHUNK_MS = 5_184_000_000      # ~60 дней
_LS_CHUNK_MS = 6_912_000_000      # ~80 дней

# Московское время (UTC+3, без DST)
_MSK = timezone(timedelta(hours=3))

# Endpoint для Funding Rate
_FR_ENDPOINT = "https://api.bybit.com/v5/market/funding/history"

# Эндпоинты новостных источников
_CC_NEWS_URL = "https://min-api.cryptocompare.com/data/v2/news/"
_CP_NEWS_URL = "https://cryptopanic.com/api/v1/posts/"

# Лаги публикации для разных типов источников (в часах MSK).
# Дневной close UTC = 03:00 MSK следующего дня, плюс берём запас в 21 час,
# чтобы значение точно стало доступно с 00:00 MSK дня D+1. Итого 24 ч
# даёт прозрачную семантику «вчерашний день» в любой момент сегодняшнего.
_DAILY_PUBLICATION_LAG_H = 24

# Funding rate Bybit публикует в моменты 00:00 / 08:00 / 16:00 UTC,
# но фактическое значение фиксируется ровно в этот момент. На 1H мастер
# даём сдвиг в 1 час для запаса (значение в баре T становится фичей в T+1).
_FUNDING_PUBLICATION_LAG_H = 1

# OI и L/S Ratio Bybit публикует на конце бара. Чтобы исключить
# одновременное использование (значение и таргет с одного и того же бара),
# даём минимальный сдвиг в 1 бар.
_INTRABAR_PUBLICATION_LAG_H = 1

# Новость бара T становится фичей только в баре T+1.
_NEWS_PUBLICATION_LAG_H = 1


# HTTP-сессия с retry


_SESSION = None


def _get_session() -> requests.Session:
    """Возвращает переиспользуемую HTTP-сессию с retry (для Bybit)."""
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
        session.headers.update(
            {"User-Agent": "Mozilla/5.0 DataLoader/3.0"}
        )
        _SESSION = session
    return _SESSION



# Вспомогательные функции


def _to_dt(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def _to_ms(date_str: str) -> int:
    return int(_to_dt(date_str).timestamp() * 1000)


def _utc_to_msk(dt: datetime) -> datetime:
    """Конвертирует UTC datetime в MSK (UTC+3), tz-naive."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_MSK).replace(tzinfo=None)


def _unix_to_msk_naive(unix_seconds: pd.Series) -> pd.Series:
    """Unix-секунды → tz-naive MSK Series."""
    ts = pd.to_datetime(unix_seconds.astype(int), unit="s", utc=True)
    return ts.dt.tz_convert(_MSK).dt.tz_localize(None)


def _normalize_index(df: pd.DataFrame, name: str = "datetime") -> pd.DataFrame:
    df.index = pd.to_datetime(df.index)
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df.index.name = name
    return df.sort_index()


def _normalize_dtindex_precision(
    index: pd.Index,
    name: str = "datetime",
) -> pd.DatetimeIndex:
    """
    Приводит DatetimeIndex к tz-naive datetime64[ns].

    Нужно для pandas.merge_asof: ключи merge должны иметь одинаковый dtype.
    На Python 3.14 / pandas 2.x иногда всплывает расхождение ms/us precision.
    """
    idx = pd.DatetimeIndex(pd.to_datetime(index))
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    idx = idx.astype("datetime64[ns]")
    idx.name = name
    return idx.sort_values()


def _ms_col_to_index(df: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    """Преобразует колонку с Unix-ms в DatetimeIndex (MSK, tz-naive)."""
    df = df.copy()
    ts = pd.to_datetime(df[col], unit="ms", utc=True)
    ts = ts.dt.tz_convert(_MSK).dt.tz_localize(None)
    df.index = ts.rename("datetime")
    df = df.drop(columns=[col])
    df = df[~df.index.duplicated(keep="last")]
    return df.sort_index()


def _end_date_default() -> str:
    """Завтра в MSK, формат YYYY-MM-DD."""
    return (datetime.now(_MSK) + timedelta(days=1)).strftime("%Y-%m-%d")

# Выравнивание источников на мастер-индекс (anti-leakage)

def _align_with_publication_lag(
    df: pd.DataFrame,
    master_index: pd.DatetimeIndex,
    lag_hours: int,
) -> pd.DataFrame:
    """
    Выравнивает df на master_index через merge_asof с обязательным
    сдвигом, имитирующим задержку публикации.

    Логика: значение источника со штампом T становится доступным только
    в баре T + lag_hours. Для каждого бара мастер-индекса берём последнее
    значение источника, чей штамп + lag <= штамп бара.

    Это математически эквивалентно: target_idx <- df.index + lag_hours,
    затем merge_asof(direction="backward") без tolerance.
    """
    if df.empty:
        return df

    df = df.copy()
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df.index = _normalize_dtindex_precision(df.index)
    df.index = df.index + pd.Timedelta(hours=lag_hours)
    df.index.name = "datetime"

    master_index_norm = _normalize_dtindex_precision(master_index)
    target = pd.DataFrame(index=master_index_norm)
    target.index.name = "datetime"

    merged = pd.merge_asof(
        target.reset_index(),
        df.reset_index(),
        on="datetime",
        direction="backward",
    )
    merged = merged.set_index("datetime")
    merged = merged.reindex(master_index_norm)
    merged.index = master_index
    return merged


def _align_daily(
    df: pd.DataFrame,
    master_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    """Дневные источники: macro, fear & greed, bybit_daily."""
    return _align_with_publication_lag(
        df, master_index, _DAILY_PUBLICATION_LAG_H,
    )


def _align_funding(
    df: pd.DataFrame,
    master_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    """Funding rate (8H): значение фиксируется в момент funding."""
    return _align_with_publication_lag(
        df, master_index, _FUNDING_PUBLICATION_LAG_H,
    )


def _align_intrabar(
    df: pd.DataFrame,
    master_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    """OI, L/S Ratio (1H): значение бара T доступно в баре T+1."""
    return _align_with_publication_lag(
        df, master_index, _INTRABAR_PUBLICATION_LAG_H,
    )


# Bybit kline

def _bybit_paginate(endpoint: str, params: dict) -> list:
    """Итерирует по страницам Bybit API с cursor-пагинацией."""
    all_records = []
    cursor = None
    session = _get_session()
    params = params.copy()

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


_KLINE_LIMIT = 1000
_KLINE_SLEEP = 0.4
_KLINE_RATELIMIT_SLEEP = 5


def _bybit_kline(
    start_date: str,
    end_date: str,
    symbol: str,
    interval_str: str,
    interval_ms: int,
) -> pd.DataFrame:
    """Скачивает OHLCV свечи с Bybit v5 API."""
    base_url = "https://api.bybit.com/v5/market/kline"
    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_rows = []
    cur_start = start_ms
    session = _get_session()

    while cur_start < end_ms:
        cur_end = min(cur_start + _KLINE_LIMIT * interval_ms, end_ms)
        try:
            resp = session.get(
                base_url,
                params={
                    "category": "linear",
                    "symbol": symbol,
                    "interval": interval_str,
                    "start": cur_start,
                    "end": cur_end,
                    "limit": _KLINE_LIMIT,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            ret_code = data.get("retCode", -1)
            if ret_code == 10006:
                logger.warning(
                    "Bybit rate limit, пауза %d сек...",
                    _KLINE_RATELIMIT_SLEEP,
                )
                time.sleep(_KLINE_RATELIMIT_SLEEP)
                continue
            if ret_code != 0:
                logger.error("Bybit kline API: %s", data.get("retMsg"))
                break
            rows = data["result"]["list"]
            if rows:
                all_rows.extend(rows)
        except Exception as exc:
            logger.error("Ошибка запроса Bybit kline: %s", exc)
        cur_start = cur_end
        time.sleep(_KLINE_SLEEP)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        all_rows,
        columns=[
            "timestamp", "open", "high", "low",
            "close", "volume", "turnover",
        ],
    )
    df["timestamp"] = df["timestamp"].astype(int)
    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        df[col] = df[col].astype(float)

    return _ms_col_to_index(df, col="timestamp")


# Загрузчики данных — свечи


def load_candles_1h(
    start_date: str,
    end_date: str = None,
    symbol: str = "BTCUSDT",
) -> pd.DataFrame:
    """1H OHLCV свечи BTC с Bybit (MSK)."""
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Bybit 1H candles | %s → %s", start_date, end_date)

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
        "Bybit 1H готово: %d свечей | %s — %s",
        len(df), df.index[0], df.index[-1],
    )
    return df


def load_candles_6h(
    start_date: str,
    end_date: str = None,
    symbol: str = "BTCUSDT",
) -> pd.DataFrame:
    """6H OHLCV свечи BTC с Bybit (MSK)."""
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Bybit 6H candles | %s → %s", start_date, end_date)

    df = _bybit_kline(
        start_date, end_date,
        symbol=symbol,
        interval_str="360",
        interval_ms=_INTERVAL_6H_MS,
    )
    if df.empty:
        logger.warning("Bybit 6H: нет данных")
        return pd.DataFrame()

    logger.info(
        "Bybit 6H готово: %d свечей | %s — %s",
        len(df), df.index[0], df.index[-1],
    )
    return df


def load_bybit_daily(
    start_date: str,
    end_date: str = None,
    symbol: str = "BTCUSDT",
) -> pd.DataFrame:
    """Дневные OHLCV свечи BTC с Bybit (MSK)."""
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Bybit Daily | %s → %s", start_date, end_date)

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
        "Bybit Daily готово: %d свечей | %s — %s",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df


# Макро-индикаторы


MACRO_TICKERS = {
    "BTC-USD": "Bitcoin",
    "SPY": "S&P 500 ETF",
    "DX-Y.NYB": "Dollar Index (DXY)",
    "^VIX": "CBOE VIX Index",
    "TLT": "20+ Year Treasury",
    "IEF": "7-10 Year Treasury",
    "GLD": "Gold ETF",
    "USO": "Oil ETF (WTI)",
    "^TNX": "10Y Treasury Yield",
    "^MOVE": "ICE BofA MOVE Index (Bond VIX)",
}


def _safe_yf_download(
    ticker: str,
    start: str,
    end: str,
    retries: int = 3,
    delay: float = 1.0,
) -> pd.DataFrame:
    """Скачивает 1D данные с yfinance с повторными попытками."""
    yf_logger = logging.getLogger("yfinance")
    yf_logger.setLevel(logging.CRITICAL)
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(
                ticker, start=start, end=end, interval="1d",
                progress=False, auto_adjust=True, threads=False,
                timeout=10,
            )
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            if attempt < retries:
                time.sleep(delay * attempt)
            else:
                logger.warning(
                    "[%s] ошибка после %d попыток: %s",
                    ticker, retries, exc,
                )
    return pd.DataFrame()


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Разворачивает MultiIndex колонки в плоские строки."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join(filter(None, map(str, c))).strip()
            for c in df.columns
        ]
    return df


def _get_close(df: pd.DataFrame, ticker: str):
    """Извлекает колонку Close из DataFrame yfinance."""
    df = _flatten_columns(df.copy())
    for col in [f"Close_{ticker}", "Close", ticker]:
        if col in df.columns:
            return df[col].rename(ticker)
    return None


def load_macro(start_date: str, end_date: str = None) -> pd.DataFrame:
    """Макро-индикаторы (1D, далее выравниваются через _align_daily)."""
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Макро-индикаторы | %s → %s", start_date, end_date)

    data_dict = {}
    for ticker, name in MACRO_TICKERS.items():
        logger.info("  %-12s (%s)", ticker, name)
        raw = _safe_yf_download(ticker, start=start_date, end=end_date)
        series = _get_close(raw, ticker) if not raw.empty else None
        if series is not None and not series.empty:
            data_dict[ticker] = series
        time.sleep(0.3)

    if not data_dict:
        logger.error("Не удалось загрузить ни одного тикера.")
        return pd.DataFrame()

    df = pd.concat(list(data_dict.values()), axis=1)
    df = _normalize_index(df)
    df = df.dropna(how="all")

    df.index = df.index.round("1s")
    if not df.index.is_unique:
        df = df.groupby(level=0).last()

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

    if "GLD" in df:
        result["gold_close"] = df["GLD"]
        if "btc_close" in result:
            result["btc_gold_ratio"] = (
                result["btc_close"] / result["gold_close"]
            )

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
                result["btc_pct_change"]
                .rolling(w, min_periods=max(w // 2, 1))
                .corr(result["sp500_pct"])
            )

    if "btc_pct_change" in result and "dxy_pct" in result:
        for w in (30, 90):
            result[f"btc_dxy_corr_{w}d"] = (
                result["btc_pct_change"]
                .rolling(w, min_periods=max(w // 2, 1))
                .corr(result["dxy_pct"])
            )

    if "btc_close" in result and "sp500_close" in result:
        result["btc_spy_ratio"] = (
            result["btc_close"] / result["sp500_close"]
        )

    if "btc_close" in result:
        for w in (7, 30, 90, 200):
            result[f"btc_sma_{w}d"] = (
                result["btc_close"].rolling(w, min_periods=1).mean()
            )
        result["btc_volatility_30d"] = (
            result["btc_pct_change"].rolling(30, min_periods=1).std()
        )

    if "^MOVE" in df:
        move = df["^MOVE"]
        result["move_index"] = move
        result["move_pct_change"] = move.pct_change() * 100
        move_mean = move.rolling(30, min_periods=10).mean()
        move_std = (
            move.rolling(30, min_periods=10).std().replace(0, np.nan)
        )
        result["move_z_score"] = (move - move_mean) / move_std
        if "vix_close" in result:
            vix = result["vix_close"]
            vix_mean = vix.rolling(30, min_periods=10).mean()
            vix_std = (
                vix.rolling(30, min_periods=10).std().replace(0, np.nan)
            )
            vix_z = (vix - vix_mean) / vix_std
            result["move_vix_spread"] = result["move_z_score"] - vix_z
    else:
        logger.warning("^MOVE не загружен. Признаки move_* пропущены.")

    if not result.index.is_unique:
        result = result.groupby(level=0).last()

    logger.info(
        "Макро готово: %d дней, %d колонок", len(result), len(result.columns),
    )
    return result

# Fear & Greed


def load_fear_greed(start_date: str) -> pd.DataFrame:
    """Crypto Fear & Greed Index (alternative.me)."""
    start_ms = _to_ms(start_date)
    logger.info("Fear & Greed Index | %s → сегодня", start_date)
    try:
        resp = _get_session().get(
            "https://api.alternative.me/fng/?limit=0", timeout=30,
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
    df = df[["timestamp_ms", "value"]].rename(
        columns={"value": "fear_greed_index"},
    )
    df["fear_greed_index"] = df["fear_greed_index"].astype(int)
    df = _ms_col_to_index(df, col="timestamp_ms")
    logger.info("Fear & Greed готово: %d дней", len(df))
    return df


# ---------------------------------------------------------------------------
# Open Interest — 1H
# ---------------------------------------------------------------------------

def load_open_interest(
    start_date: str,
    end_date: str = None,
    symbol: str = "BTCUSDT",
) -> pd.DataFrame:
    """Open Interest с Bybit (только 1H)."""
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Open Interest (1h) | %s → %s", start_date, end_date)

    endpoint = "https://api.bybit.com/v5/market/open-interest"
    records = _bybit_paginate(
        endpoint,
        {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "1h",
            "startTime": _to_ms(start_date),
            "endTime": _to_ms(end_date),
            "limit": 200,
        },
    )
    if not records:
        logger.warning("Open Interest: нет данных")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["timestamp"] = df["timestamp"].astype(int)
    df["open_interest"] = df["openInterest"].astype(float)
    df = df[["timestamp", "open_interest"]]
    df = _ms_col_to_index(df, col="timestamp")
    logger.info("Open Interest готово: %d записей", len(df))
    return df


# ---------------------------------------------------------------------------
# Funding Rate — 8H
# ---------------------------------------------------------------------------

def load_funding_rate(
    start_date: str,
    end_date: str = None,
    symbol: str = "BTCUSDT",
) -> pd.DataFrame:
    """Funding Rate с Bybit (каждые 8 часов)."""
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Funding Rate | %s → %s", start_date, end_date)

    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_records = []
    cur_start = start_ms

    while cur_start < end_ms:
        cur_end = min(cur_start + _FR_CHUNK_MS, end_ms)
        records = _bybit_paginate(
            _FR_ENDPOINT,
            {
                "category": "linear",
                "symbol": symbol,
                "startTime": cur_start,
                "endTime": cur_end,
                "limit": 200,
            },
        )
        if records:
            all_records.extend(records)
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
    logger.info(
        "Funding Rate готово: %d записей | %s — %s",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df


# ---------------------------------------------------------------------------
# Long/Short Ratio — 1H
# ---------------------------------------------------------------------------

def load_long_short_ratio(
    start_date: str,
    end_date: str = None,
    symbol: str = "BTCUSDT",
) -> pd.DataFrame:
    """Long/Short Ratio трейдеров с Bybit (только 1H)."""
    if end_date is None:
        end_date = _end_date_default()
    logger.info("Long/Short Ratio (1h) | %s → %s", start_date, end_date)

    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    all_records = []
    cur_start = start_ms
    session = _get_session()

    while cur_start < end_ms:
        cur_end = min(cur_start + _LS_CHUNK_MS, end_ms)
        try:
            resp = session.get(
                "https://api.bybit.com/v5/market/account-ratio",
                params={
                    "category": "linear",
                    "symbol": symbol,
                    "period": "1h",
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
    logger.info(
        "Long/Short Ratio готово: %d записей | %s — %s",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df


# ---------------------------------------------------------------------------
# Новостной сентимент
# ---------------------------------------------------------------------------

def fetch_cryptocompare_news(
    start_date: str,
    end_date: str = None,
    categories: str = "BTC",
    pause: float = 0.25,
    max_pages: int = 600,
) -> pd.DataFrame:
    """Качает новости CryptoCompare назад по времени до start_date."""
    # Границы окна считаем явно в UTC: CC отдаёт published_on в UTC-секундах,
    # а naive datetime.timestamp() зависит от таймзоны ОС и даёт плавающий
    # сдвиг (на машине в MSK — ровно 3 часа), из-за чего край окна разъезжается.
    start_dt = _to_dt(start_date)
    start_utc = start_dt.replace(tzinfo=timezone.utc)
    if end_date is None:
        end_ts = int(datetime.now(timezone.utc).timestamp())
    else:
        end_ts = int(_to_dt(end_date).replace(tzinfo=timezone.utc).timestamp())
    stop_ts = int(start_utc.timestamp())

    rows = []
    cursor = end_ts
    session = _get_session()
    for _page in range(max_pages):
        params = {"lang": "EN", "lTs": cursor}
        if categories:
            params["categories"] = categories
        try:
            resp = session.get(_CC_NEWS_URL, params=params, timeout=20)
            resp.raise_for_status()
            items = resp.json().get("Data", [])
        except Exception as exc:
            # Ошибка запроса в середине пагинации не должна молча обнулять
            # весь источник: сохраняем уже собранные страницы и выходим.
            logger.warning(
                "CryptoCompare News: ошибка запроса на стр. %d (%s) — "
                "оставляем %d уже собранных новостей",
                _page, exc, len(rows),
            )
            break
        if not items:
            logger.info("CryptoCompare News: достигнут конец потока")
            break
        for it in items:
            rows.append({
                "published_on": it["published_on"],
                "title": it.get("title", ""),
                "body": it.get("body", ""),
                "source": it.get("source", ""),
            })
        oldest = min(it["published_on"] for it in items)
        if oldest <= stop_ts:
            break
        cursor = oldest - 1
        time.sleep(pause)

    if not rows:
        logger.warning("CryptoCompare News: пусто")
        return pd.DataFrame(columns=["title", "body", "source"])

    df = pd.DataFrame(rows).drop_duplicates(subset=["published_on", "title"])
    # Нижнюю границу режем по UTC-секундам (published_on), а не по уже
    # сдвинутому в MSK индексу — иначе теряем/добираем ~3 часа на краю.
    df = df[df["published_on"] >= stop_ts]
    df.index = _unix_to_msk_naive(df["published_on"]).rename("datetime")
    df = df.drop(columns=["published_on"]).sort_index()
    if df.empty:
        logger.warning("CryptoCompare News: после фильтра по дате пусто")
        return pd.DataFrame(columns=["title", "body", "source"])
    logger.info(
        "CryptoCompare News готово: %d новостей | %s — %s",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df


def fetch_cryptopanic_news(
    start_date: str,
    end_date: str = None,
    currencies: str = "BTC",
    auth_token: str = None,
    pause: float = 0.5,
    max_pages: int = 200,
) -> pd.DataFrame:
    """Качает новости CryptoPanic назад по времени до start_date.

    Токен берётся из аргумента или переменной окружения
    CRYPTOPANIC_TOKEN. Без токена источник пропускается.
    """
    token = auth_token or os.environ.get("CRYPTOPANIC_TOKEN")
    if not token:
        logger.warning("CryptoPanic: токен не задан — источник пропущен")
        return pd.DataFrame(columns=["title", "body", "source"])

    start_naive = pd.Timestamp(_to_dt(start_date))
    if end_date is None:
        end_naive = pd.Timestamp(datetime.now(_MSK).replace(tzinfo=None))
    else:
        end_naive = pd.Timestamp(_to_dt(end_date))

    rows = []
    session = _get_session()
    url = _CP_NEWS_URL
    params = {
        "auth_token": token,
        "currencies": currencies,
        "kind": "news",
        "public": "true",
    }
    for _ in range(max_pages):
        try:
            resp = session.get(url, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.warning("CryptoPanic: ошибка запроса (%s)", exc)
            break

        items = payload.get("results", [])
        if not items:
            break
        for it in items:
            published = it.get("published_at") or it.get("created_at")
            src = it.get("source") or {}
            rows.append({
                "published_at": published,
                "title": it.get("title", ""),
                "body": it.get("title", ""),
                "source": src.get("title") or src.get("domain", ""),
            })

        next_url = payload.get("next")
        if not next_url:
            break
        oldest_raw = min((it.get("published_at") or "") for it in items)
        oldest_ts = pd.to_datetime(oldest_raw, utc=True, errors="coerce")
        if pd.notna(oldest_ts):
            oldest_msk = oldest_ts.tz_convert(_MSK).tz_localize(None)
            if oldest_msk <= start_naive:
                break

        url = next_url
        params = None  # ссылка next уже содержит все параметры запроса
        time.sleep(pause)

    if not rows:
        logger.warning("CryptoPanic News: пусто")
        return pd.DataFrame(columns=["title", "body", "source"])

    df = pd.DataFrame(rows)
    idx = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    idx = idx.dt.tz_convert(_MSK).dt.tz_localize(None)
    df.index = idx.rename("datetime")
    df = df.drop(columns=["published_at"])
    df = df[df.index.notna()].sort_index()
    df = df[(df.index >= start_naive) & (df.index <= end_naive)]
    df = df.drop_duplicates(subset=["title"])
    if df.empty:
        logger.warning("CryptoPanic News: нет новостей в окне")
        return pd.DataFrame(columns=["title", "body", "source"])

    logger.info(
        "CryptoPanic News готово: %d новостей | %s — %s",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df[["title", "body", "source"]]


def _combine_news(*frames: pd.DataFrame) -> pd.DataFrame:
    """Объединяет новости источников, убирая дубли по часу и заголовку."""
    parts = [f for f in frames if f is not None and not f.empty]
    if not parts:
        return pd.DataFrame(columns=["title", "body", "source"])

    combined = pd.concat(parts).sort_index()
    combined = combined[combined.index.notna()]
    combined["_floor"] = combined.index.floor("h")
    combined = combined.drop_duplicates(subset=["_floor", "title"])
    combined = combined.drop(columns=["_floor"])
    return combined.sort_index()


def _vader_scorer(texts: list) -> np.ndarray:
    """Скоринг текстов через VADER (fallback)."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    return np.array(
        [analyzer.polarity_scores(t or "")["compound"] for t in texts],
        dtype=float,
    )


def _cryptobert_scorer(
    texts: list,
    model: str = "ElKulako/cryptobert",
    batch_size: int = 64,
) -> np.ndarray:
    """Скоринг текстов через CryptoBERT."""
    from transformers import pipeline
    clf = pipeline(
        "text-classification", model=model,
        truncation=True, max_length=128, top_k=None,
    )
    weight = {
        "bearish": -1.0, "neutral": 0.0, "bullish": 1.0,
        "negative": -1.0, "positive": 1.0,
    }
    scores = np.zeros(len(texts), dtype=float)
    for start in range(0, len(texts), batch_size):
        chunk = [t or "" for t in texts[start:start + batch_size]]
        for i, preds in enumerate(clf(chunk)):
            scores[start + i] = sum(
                weight.get(p["label"].lower(), 0.0) * p["score"]
                for p in preds
            )
    return scores


def score_news(news: pd.DataFrame, use_cryptobert: bool = True) -> pd.DataFrame:
    """Добавляет колонку sentiment в [-1, 1]."""
    if news.empty:
        news = news.copy()
        news["sentiment"] = pd.Series(dtype=float)
        return news
    texts = (
        news["title"].fillna("") + ". " + news["body"].fillna("")
    ).tolist()
    scores = None
    if use_cryptobert:
        try:
            scores = _cryptobert_scorer(texts)
        except Exception as exc:
            logger.warning("CryptoBERT недоступен (%s) — VADER", exc)
    if scores is None:
        scores = _vader_scorer(texts)
    news = news.copy()
    news["sentiment"] = scores
    return news


def build_sentiment_features(
    news: pd.DataFrame,
    master_index: pd.DatetimeIndex,
    lag_hours: int = _NEWS_PUBLICATION_LAG_H,
    flow_window: int = 168,
) -> pd.DataFrame:
    """Агрегирует новости в часовые фичи на мастер-индексе."""
    cols = [
        "news_count", "news_sent_mean", "news_sent_std",
        "news_pos_share", "news_neg_share", "news_count_z",
        "news_sent_ewma",
    ]
    if news.empty or "sentiment" not in news.columns:
        return pd.DataFrame(0.0, index=master_index, columns=cols)

    bucket = news.copy()
    bucket["hour"] = bucket.index.floor("h")
    grouped = bucket.groupby("hour")["sentiment"]

    hourly = pd.DataFrame({
        "news_count": grouped.size(),
        "news_sent_mean": grouped.mean(),
        "news_sent_std": grouped.std(),
        "news_pos_share": grouped.apply(lambda s: (s > 0.2).mean()),
        "news_neg_share": grouped.apply(lambda s: (s < -0.2).mean()),
    })

    hourly = hourly.reindex(master_index)
    hourly["news_count"] = hourly["news_count"].fillna(0.0)
    for c in ["news_sent_mean", "news_sent_std",
              "news_pos_share", "news_neg_share"]:
        hourly[c] = hourly[c].fillna(0.0)

    mean = hourly["news_count"].rolling(flow_window, min_periods=24).mean()
    std = hourly["news_count"].rolling(flow_window, min_periods=24).std()
    hourly["news_count_z"] = (
        (hourly["news_count"] - mean) / std.replace(0, np.nan)
    ).fillna(0.0)
    hourly["news_sent_ewma"] = hourly["news_sent_mean"].ewm(span=24).mean()

    hourly = hourly.shift(lag_hours).fillna(0.0)
    return hourly[cols]


def load_sentiment(
    master_index: pd.DatetimeIndex,
    start_date: str,
    end_date: str = None,
    use_cryptobert: bool = True,
    cryptopanic_token: str = None,
) -> pd.DataFrame:
    """Сквозной хелпер: качает из источников → скорит → агрегирует."""
    cc = fetch_cryptocompare_news(start_date, end_date)
    cp = fetch_cryptopanic_news(
        start_date, end_date, auth_token=cryptopanic_token,
    )
    news = _combine_news(cc, cp)
    news = score_news(news, use_cryptobert=use_cryptobert)

    # Диагностика пересечения: главная причина нулевых news_* —
    # рассинхрон диапазона/таймзоны между новостями и мастер-индексом.
    if not news.empty:
        n_lo, n_hi = news.index.min(), news.index.max()
        m_lo, m_hi = master_index.min(), master_index.max()
        overlap = (n_hi >= m_lo) and (n_lo <= m_hi)
        logger.info(
            "Sentiment: новостей=%d [%s — %s], мастер [%s — %s], "
            "пересечение=%s",
            len(news), n_lo, n_hi, m_lo, m_hi,
            "есть" if overlap else "НЕТ",
        )
        if not overlap:
            logger.error(
                "Sentiment: окно новостей не пересекается с мастер-индексом "
                "— все news_* будут нулевыми. Проверьте таймзону/диапазон.",
            )

    return build_sentiment_features(news, master_index)


# ---------------------------------------------------------------------------
# Главная функция — load_all
# ---------------------------------------------------------------------------

def load_all(
    start_date: str = None,
    end_date: str = None,
    days: int = DOWNLOAD_DAYS,
    cryptopanic_token: str = None,
) -> dict:
    if end_date is None:
        end_date = _end_date_default()
    if days is not None:
        start_date = (
            datetime.now(_MSK) - timedelta(days=days)
        ).strftime("%Y-%m-%d")

    _now_msk = datetime.now(_MSK)
    _end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    _start_dt = datetime.strptime(start_date, "%Y-%m-%d")

    if _end_dt.year > _now_msk.year + 1:
        raise RuntimeError(
            f"Подозрительная конечная дата: {end_date}. "
            f"Проверьте системные часы! Текущее MSK: "
            f"{_now_msk:%Y-%m-%d %H:%M:%S}"
        )
    if _start_dt > _end_dt:
        raise ValueError(
            f"start_date ({start_date}) позже end_date ({end_date})"
        )

    logger.info("load_all | %s → %s", start_date, end_date)

    result = {}

    # 1H candles — мастер-индекс. Никакого сдвига, реальные timestamps.
    result["candles_1h"] = load_candles_1h(start_date, end_date)
    if result["candles_1h"].empty:
        logger.error("candles_1h пуст — загрузка прервана")
        return result

    master_idx = result["candles_1h"].index

    # 6H candles (не выравниваем на 1H мастер, отдельный таймфрейм)
    result["candles_6h"] = load_candles_6h(start_date, end_date)

    # ── Дневные источники → lag 24h (значение публикуется на close,
    #    становится доступным с 00:00 MSK следующего дня) ──
    result["bybit_daily"] = _align_daily(
        load_bybit_daily(start_date, end_date), master_idx,
    )
    result["macro"] = _align_daily(
        load_macro(start_date, end_date), master_idx,
    )
    result["fear_greed"] = _align_daily(
        load_fear_greed(start_date), master_idx,
    )

    # ── Internal sources (1H) → lag 1h (значение бара T → фича бара T+1) ──
    result["open_interest"] = _align_intrabar(
        load_open_interest(start_date, end_date), master_idx,
    )
    result["long_short"] = _align_intrabar(
        load_long_short_ratio(start_date, end_date), master_idx,
    )

    # ── Funding rate (8H) → lag 1h (значение фиксируется в момент funding) ──
    result["funding_rate"] = _align_funding(
        load_funding_rate(start_date, end_date), master_idx,
    )

    # ── Новостной сентимент (1H) → сдвиг 1 бар внутри build_sentiment ──
    result["sentiment"] = load_sentiment(
        master_idx, start_date, end_date,
        cryptopanic_token=cryptopanic_token,
    )

    logger.info("Итого загружено:")
    for k, v in result.items():
        if isinstance(v, pd.DataFrame) and not v.empty:
            logger.info(
                "  %-22s %7d строк | %s — %s",
                k, len(v), v.index[0].date(), v.index[-1].date(),
            )
        else:
            logger.info("  %-22s пусто", k)

    return result


# ---------------------------------------------------------------------------
# Сохранение
# ---------------------------------------------------------------------------

def save_all(
    data: dict,
    prefix: str = "",
    directory: str | None = None,
) -> None:
    """Сохраняет все DataFrame из data в CSV-файлы."""
    import os

    out_dir = directory or "."
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now(_MSK).strftime("%Y%m%d_%H%M")
    p = f"{prefix}_" if prefix else ""
    for name, df in data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            fname = os.path.join(out_dir, f"{p}{name}_{stamp}.csv")
            df.to_csv(fname)
            logger.info("Сохранено: %s", fname)


if __name__ == "__main__":
    data = load_all()
    save_all(data, prefix="btc")