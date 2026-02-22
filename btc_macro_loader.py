import time
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# Вспомогательные функции

def period_to_start_date(period: str) -> str:
    period = period.lower().strip()
    now = pd.Timestamp.now().normalize()

    if period.endswith("d"):
        start = now - pd.Timedelta(days=int(period[:-1]))
    elif period.endswith("mo"):
        start = now - pd.DateOffset(months=int(period[:-2]))
    elif period.endswith("y"):
        start = now - pd.DateOffset(years=int(period[:-1]))
    else:
        raise ValueError("Период должен быть вида: '30d', '3mo', '1y', '2y' ...")

    return start.strftime("%Y-%m-%d")


def _safe_download(ticker, start, end, interval="1d", retries=3, delay=1.0):
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end,
                interval=interval,
                progress=False,
                auto_adjust=True,
                threads=False,
            )
            if df is not None and not df.empty:
                return df
        except Exception as e:
            if attempt < retries:
                time.sleep(delay * attempt)
            else:
                print(f"    [{ticker}] ошибка после {retries} попыток: {str(e)[:80]}")
    return pd.DataFrame()


def _flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(filter(None, map(str, col))).strip() for col in df.columns]
    return df


def _get_close(df, ticker):
    df = _flatten_columns(df.copy())
    for col in [f"Close_{ticker}", "Close", ticker]:
        if col in df.columns:
            return df[col].rename(ticker)
    return None


def _normalize_ohlcv(df):
    ohlcv_cols = ["Open", "High", "Low", "Close", "Volume"]
    df = _flatten_columns(df)
    col_map = {}
    for ohlcv in ohlcv_cols:
        for c in df.columns:
            if c.lower().startswith(ohlcv.lower()) and c not in col_map.values():
                col_map[c] = ohlcv
                break
    df = df.rename(columns=col_map)
    return df[[c for c in ohlcv_cols if c in df.columns]]


# Макро-индикаторы

MACRO_TICKERS = {
    "BTC-USD":  "Bitcoin",
    "SPY":      "S&P 500 ETF",
    "QQQ":      "Nasdaq-100 ETF",
    "DX-Y.NYB": "Dollar Index (DXY)",
    "^VIX":     "CBOE Volatility Index",
    "TLT":      "20+ Year Treasury",
    "IEF":      "7-10 Year Treasury",
    "GLD":      "Gold ETF",
    "USO":      "Oil ETF (WTI)",
    "^TNX":     "10Y Treasury Yield",
}

def download_btc_macro_data(start_date="2020-01-01", end_date=None):
    if end_date is None:
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"  Макро-индикаторы | {start_date} -> {end_date}")

    data_dict = {}

    for ticker, name in MACRO_TICKERS.items():
        print(f"  {ticker:<12} ({name})...", end=" ", flush=True)
        df = _safe_download(ticker, start=start_date, end=end_date, interval="1d")
        series = _get_close(df, ticker) if not df.empty else None

        if series is not None and not series.empty:
            data_dict[ticker] = series
            print(f"{len(series)} дней")
        else:
            print("нет данных")
        time.sleep(0.3)

    if not data_dict:
        print("Не удалось загрузить ни одного тикера.")
        return pd.DataFrame()

    df = pd.concat(data_dict.values(), axis=1)
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df.index.name = "date"
    df = df.dropna(how="all").sort_index()

    print(f"\n  Итого: {len(df)} дней | {df.index[0].date()} - {df.index[-1].date()}")
    print(f"  Тикеры: {list(df.columns)}")

    result = pd.DataFrame(index=df.index)

    if "BTC-USD" in df:
        btc = df["BTC-USD"]
        result["btc_close"]      = btc
        result["btc_pct_change"] = btc.pct_change() * 100
        result["btc_log_return"] = np.log(btc / btc.shift(1))

    if "SPY" in df:
        result["sp500_close"] = df["SPY"]
        result["sp500_pct"]   = df["SPY"].pct_change() * 100

    if "QQQ" in df:
        result["nasdaq_close"] = df["QQQ"]
        result["nasdaq_pct"]   = df["QQQ"].pct_change() * 100

    if "DX-Y.NYB" in df:
        result["dxy_close"] = df["DX-Y.NYB"]
        result["dxy_pct"]   = df["DX-Y.NYB"].pct_change() * 100

    if "^VIX" in df:
        result["vix_close"] = df["^VIX"]

    if "GLD" in df:
        result["gold_close"] = df["GLD"]
        if "btc_close" in result:
            result["btc_gold_ratio"] = result["btc_close"] / result["gold_close"]

    if "TLT" in df:
        result["tlt_close"] = df["TLT"]
        result["tlt_pct"]   = df["TLT"].pct_change() * 100

    if "IEF" in df:
        result["ief_close"] = df["IEF"]

    if "USO" in df:
        result["oil_close"] = df["USO"]
        result["oil_pct"]   = df["USO"].pct_change() * 100

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
            result[f"btc_sma_{w}"] = result["btc_close"].rolling(w).mean()
        result["btc_volatility_30d"] = result["btc_pct_change"].rolling(30).std()

    if "vix_close" in result and "sp500_pct" in result:
        result["risk_regime"] = np.where(
            (result["vix_close"] > 25) & (result["sp500_pct"].rolling(5).mean() < -1),
            "stress",
            "normal",
        )

    return result

# Свечи BTC (1D и 4H)

def download_btc_candles(start_date="2023-01-01", end_date=None):
    if end_date is None:
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    result = {}

    # Дневные свечи
    print(f"\n  BTC Свечи 1D | {start_date} -> {end_date}")

    df_1d = _safe_download("BTC-USD", start=start_date, end=end_date, interval="1d")
    if not df_1d.empty:
        df_1d = _normalize_ohlcv(df_1d)
        df_1d.index = pd.to_datetime(df_1d.index).tz_localize(None)
        df_1d.index.name = "datetime"
        df_1d = df_1d.sort_index()
        result["1d"] = df_1d
        print(f"  OK 1D: {len(df_1d)} свечей | {df_1d.index[0].date()} - {df_1d.index[-1].date()}")
    else:
        print("  1D: нет данных")

    # 4-часовые свечи (1h -> resample 4h)
    print(f"\n  BTC Свечи 4H | {start_date} -> {end_date}")

    hourly_start = pd.Timestamp(start_date)
    hourly_end   = pd.Timestamp(end_date)
    chunk_size   = pd.Timedelta(days=180)
    chunks       = []
    chunk_start  = hourly_start

    while chunk_start < hourly_end:
        chunk_end = min(chunk_start + chunk_size, hourly_end)
        s = chunk_start.strftime("%Y-%m-%d")
        e = chunk_end.strftime("%Y-%m-%d")

        print(f"  Чанк 1H: {s} -> {e} ...", end=" ", flush=True)
        df_h = _safe_download("BTC-USD", start=s, end=e, interval="1h", delay=1.5)

        if not df_h.empty:
            df_h = _normalize_ohlcv(df_h)
            df_h.index = pd.to_datetime(df_h.index).tz_localize(None)
            chunks.append(df_h)
            print(f"OK {len(df_h)} баров")
        else:
            print("нет данных")

        chunk_start = chunk_end
        time.sleep(0.5)

    if chunks:
        df_1h = pd.concat(chunks).sort_index()
        df_1h = df_1h[~df_1h.index.duplicated(keep="last")]
        df_1h.index.name = "datetime"

        df_4h = df_1h.resample("4h", closed="left", label="left").agg({
            "Open":   "first",
            "High":   "max",
            "Low":    "min",
            "Close":  "last",
            "Volume": "sum",
        }).dropna(how="all")

        result["4h"] = df_4h
        print(f"\n  OK 4H: {len(df_4h)} свечей | {df_4h.index[0]} - {df_4h.index[-1]}")
    else:
        print("\n  4H: нет данных")

    return result

# Удобные обёртки

def quick_btc_macro(period="1y"):
    return download_btc_macro_data(start_date=period_to_start_date(period))

def quick_btc_candles(period="1y"):
    return download_btc_candles(start_date=period_to_start_date(period))

# Точка входа

if __name__ == "__main__":
    PERIOD = "2y"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    df_macro = quick_btc_macro(PERIOD)
    if not df_macro.empty:
        fname = f"btc_macro_{timestamp}.csv"
        df_macro.to_csv(fname)
        print(f"\nМакро сохранено -> {fname}")
        print(df_macro.tail(3).to_string())

    candles = quick_btc_candles(PERIOD)

    if "1d" in candles:
        fname = f"btc_candles_1d_{timestamp}.csv"
        candles["1d"].to_csv(fname)
        print(f"\nСвечи 1D сохранено -> {fname}")
        print(candles["1d"].tail(3).to_string())

    if "4h" in candles:
        fname = f"btc_candles_4h_{timestamp}.csv"
        candles["4h"].to_csv(fname)
        print(f"\nСвечи 4H сохранено -> {fname}")
        print(candles["4h"].tail(3).to_string())
