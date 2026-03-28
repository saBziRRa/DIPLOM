"""
feature_engineering.py - Построение признаков для классификации скачков BTC

Принимает на вход dict из data_loader.load_all().
Возвращает готовый DataFrame с признаками и таргетами.

Таргеты:
    target_4h : скачок цены через 1 бар (4H) на +/-X%  -> (-1, 0, 1)
    target_1d : скачок цены через 6 баров (24H) на +/-Y% -> (-1, 0, 1)
    Пороги по умолчанию: 2% (4H) и 3.5% (1D).

Использование (вариант 1 - через ipynb):
    вставить сначала блок data_loader.py, а потом этот блок future_engeneering.py,
    затем запустить поочередно, начиная с data_loader.

Использование (вариант 2 - через data_loader):
    from data_loader import load_all, START_DATE
    from feature_engineering import build_features

    data = load_all(start_date=START_DATE, coinglass_api_key="api_ключ")
    df, feature_cols = build_features(data, threshold_4h=0.02, threshold_1d=0.035)

Использование (вариант 3 - из сохранённых CSV, без API):
    from feature_engineering import load_from_csv, build_features

    data = load_from_csv(csv_dir=".", prefix="btc")
    df, feature_cols = build_features(data, threshold_4h=0.02, threshold_1d=0.035)
"""

import logging
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# УТИЛИТЫ

def _rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def _atr(high, low, close, period=14):
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


# 1 ОБЪЕДИНЕНИЕ ИСТОЧНИКОВ

def _merge_sources(data):
    candles = data.get("candles_4h", pd.DataFrame())

    if candles.empty:
        raise ValueError("candles_4h пуст — нет мастер-индекса")

    df = candles.copy()
    df.columns = [f"c_{c.lower()}" for c in df.columns]

    sources = {
        "d":   data.get("bybit_daily",   pd.DataFrame()),
        "m":   data.get("macro",         pd.DataFrame()),
        "fg":  data.get("fear_greed",    pd.DataFrame()),
        "oi":  data.get("open_interest", pd.DataFrame()),
        "fr":  data.get("funding_rate",  pd.DataFrame()),
        "ls":  data.get("long_short",    pd.DataFrame()),
        "liq": data.get("liquidations",  pd.DataFrame()),
    }

    for prefix, src in sources.items():
        if src.empty:
            logger.warning(f"Источник '{prefix}' пуст, пропускаем")
            continue
        src = src.copy()
        src.columns = [f"{prefix}_{c}" for c in src.columns]
        df = df.join(src, how="left")

    logger.info(f"Объединено: {len(df)} строк, {len(df.columns)} исходных колонок")
    return df


# 2 ОБРАБОТКА ПРОПУСКОВ

def _handle_missing(df):
    price_patterns = [
        "close", "open", "high", "low", "sma", "ratio",
        "vix", "yield", "spread", "gold", "oil", "tlt",
        "ief", "sp500", "nasdaq", "dxy", "btc_spy",
        "open_interest", "fear_greed", "turnover", "volume",
        "move_index",
    ]

    pct_patterns = [
        "pct", "log_return", "volatility", "funding_rate",
        "risk_regime", "ls_ratio", "ls_buy", "ls_sell",
        "liq_usd", "liq_ratio", "move_pct",
    ]

    corr_patterns = ["corr"]

    for col in df.columns:
        col_l = col.lower()
        if any(p in col_l for p in corr_patterns):
            df[col] = df[col].ffill().fillna(0)
        elif any(p in col_l for p in pct_patterns):
            df[col] = df[col].fillna(0)
        elif any(p in col_l for p in price_patterns):
            df[col] = df[col].ffill()

    df = df.dropna(subset=["c_close"])

    missing_pct = df.isnull().mean() * 100
    high_missing = missing_pct[missing_pct > 5]
    if not high_missing.empty:
        logger.warning(
            f"Колонки с >5% пропусков после fillna: "
            f"{high_missing.round(1).to_dict()}"
        )
    else:
        logger.info("Пропуски обработаны, критических колонок нет")

    logger.info(f"После обработки пропусков: {len(df)} строк")
    return df


# 3 ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ

def _add_technical(df):
    close  = df["c_close"]
    high   = df["c_high"]
    low    = df["c_low"]

    for p in [6, 14, 24]:
        df[f"rsi_{p}"] = _rsi(close, p)

    macd_line   = _ema(close, 12) - _ema(close, 26)
    macd_signal = _ema(macd_line, 9)
    df["macd"]      = macd_line
    df["macd_hist"] = macd_line - macd_signal

    sma20    = close.rolling(20).mean()
    std20    = close.rolling(20).std()
    bb_upper = sma20 + 2 * std20
    bb_lower = sma20 - 2 * std20
    bb_width = (bb_upper - bb_lower) / sma20
    df["bb_pos"]     = (close - bb_lower) / (bb_upper - bb_lower).replace(0, np.nan)
    df["bb_width"]   = bb_width
    df["bb_squeeze"] = (bb_width < bb_width.rolling(50).mean()).astype(int)

    atr14 = _atr(high, low, close, 14)
    df["atr_pct"]   = atr14 / close
    df["atr_ratio"] = atr14 / atr14.rolling(50).mean()

    for w, label in [(6, "1d"), (24, "4d"), (42, "7d"), (168, "28d")]:
        df[f"sma_{label}"] = close.rolling(w).mean()

    df["price_to_sma_1d"]  = close / df["sma_1d"]
    df["price_to_sma_4d"]  = close / df["sma_4d"]
    df["price_to_sma_7d"]  = close / df["sma_7d"]
    df["price_to_sma_28d"] = close / df["sma_28d"]
    df["sma_7d_to_28d"]    = df["sma_7d"] / df["sma_28d"]

    rsi14    = df["rsi_14"]
    rsi_min  = rsi14.rolling(14).min()
    rsi_max  = rsi14.rolling(14).max()
    df["stoch_rsi"] = (rsi14 - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan)

    log_ret = np.log(close / close.shift(1))
    df["volatility_1d"]     = log_ret.rolling(6).std()
    df["volatility_7d"]     = log_ret.rolling(42).std()
    df["volatility_28d"]    = log_ret.rolling(168).std()
    df["volatility_regime"] = df["volatility_7d"] / df["volatility_28d"]

    df = df.drop(columns=["sma_1d", "sma_4d", "sma_7d", "sma_28d"])

    logger.info("Технические индикаторы добавлены")
    return df


# 4 ПРИЗНАКИ ОБЪЁМА И TURNOVER

def _add_volume_features(df):
    vol   = df["c_volume"]
    close = df["c_close"]

    vol_mean = vol.rolling(168).mean()
    df["volume_ratio"] = vol / vol_mean.replace(0, np.nan)
    df["volume_spike"] = (df["volume_ratio"] > 2.0).astype(int)
    vwap = (close * vol).rolling(24).sum() / vol.rolling(24).sum()
    _vwap_dist_tmp = (close - vwap) / vwap.replace(0, np.nan)

    price_dir = np.sign(close.pct_change())
    vol_dir   = np.sign(vol.pct_change())
    df["vol_price_divergence"] = (price_dir != vol_dir).astype(int)

    if "d_turnover" in df.columns:
        d_turn_mean            = df["d_turnover"].rolling(30).mean()
        df["d_turnover_ratio"] = df["d_turnover"] / d_turn_mean.replace(0, np.nan)
        df["d_turnover_ratio_lag_1"]   = df["d_turnover_ratio"].shift(1)
        df["d_turnover_ratio_rolling3"] = df["d_turnover_ratio"].rolling(3).mean()

    logger.info("Признаки объёма добавлены")
    return df


# 5 СВЕЧНЫЕ ПАТТЕРНЫ

def _add_candle_features(df):
    open_  = df["c_open"]
    high   = df["c_high"]
    low    = df["c_low"]
    close  = df["c_close"]

    body         = close - open_
    body_abs     = body.abs()
    candle_range = (high - low).replace(0, np.nan)

    df["body_ratio"]   = body_abs / candle_range
    df["body_dir"]     = np.sign(body)

    upper_wick = high - pd.concat([open_, close], axis=1).max(axis=1)
    lower_wick = pd.concat([open_, close], axis=1).min(axis=1) - low
    df["upper_wick_ratio"] = upper_wick / candle_range
    df["lower_wick_ratio"] = lower_wick / candle_range

    df["doji"]   = (df["body_ratio"] < 0.1).astype(int)
    df["hl_pct"] = (high - low) / close

    logger.info("Свечные паттерны добавлены")
    return df


# 6 ПРОИЗВОДНЫЕ ОТ МАКРО

def _add_macro_features(df):
    close = df["c_close"]

    if "m_btc_sma_7d" in df.columns and "m_btc_close" in df.columns:
        df["btc_to_macro_sma90"] = close / df["m_btc_sma_90d"]

    sma200_4h = close.rolling(1200).mean()
    df["btc_to_sma200_4h"] = close / sma200_4h

    # vix_high: ^VIX spot > 30 = рыночный стресс (~10-15% времени)
    if "m_vix_close" in df.columns:
        df["vix_high"] = (df["m_vix_close"] > 30).astype(int)

    if "fg_fear_greed_index" in df.columns:
        fg = df["fg_fear_greed_index"]
        df["fg_extreme_fear"] = (fg < 25).astype(int)
        df["fg_fear"]         = ((fg >= 25) & (fg < 45)).astype(int)
        df["fg_greed"]        = ((fg >= 55) & (fg < 75)).astype(int)

    if "m_tlt_ief_spread" in df.columns:
        df["yield_spread_change"] = df["m_tlt_ief_spread"].diff()

    if "m_treasury_yield_10y" in df.columns:
        y = df["m_treasury_yield_10y"]
        df["yield_vs_mean"] = y - y.rolling(60).mean()

    m_pct_cols = [
        c for c in df.columns
        if c.startswith("m_") and any(
            c.endswith(s) for s in ["_pct", "_pct_change", "_log_return"]
        )
    ]
    derived_daily_cols = [
        c for c in [
            "yield_spread_change", "yield_vs_mean",
            "m_move_z_score", "m_move_vix_spread",
        ]
        if c in df.columns
    ]
    daily_pct_cols = m_pct_cols + derived_daily_cols
    for col in daily_pct_cols:
        df[col] = df[col].shift(6)

    logger.info(f"Макро признаки добавлены (pct сдвинуты на 1 день: {len(daily_pct_cols)} колонок)")
    return df


# 7 ПРИЗНАКИ OI, FUNDING RATE, L/S RATIO

def _add_futures_features(df):
    if "oi_open_interest" in df.columns:
        oi = df["oi_open_interest"]
        df["oi_change_pct"]  = oi.pct_change() * 100
        df["oi_change_4bar"] = oi.pct_change(4) * 100
        oi_mean              = oi.rolling(168).mean()
        df["oi_ratio"]       = oi / oi_mean.replace(0, np.nan)

        price_dir = np.sign(df["c_close"].pct_change())
        oi_dir    = np.sign(df["oi_change_pct"])
        df["oi_price_divergence"] = (price_dir != oi_dir).astype(int)

    if "fr_funding_rate" in df.columns:
        fr = df["fr_funding_rate"].fillna(0)
        df["funding_rate_abs"] = fr.abs()
        df["funding_change"]   = fr.diff()

    if "ls_buy_ratio" in df.columns:
        br = df["ls_buy_ratio"].fillna(0.5)
        df["ls_buy_ratio"]     = br
        df["ls_long_dominant"] = (br > 0.65).astype(int)
        df["ls_ratio_change"]  = df["ls_ls_ratio"].fillna(1).diff()
        df["ls_ratio_vs_mean"] = (
            df["ls_ls_ratio"].fillna(1)
            / df["ls_ls_ratio"].fillna(1).rolling(42).mean()
        )
    else:
        logger.warning("Long/Short Ratio: данные отсутствуют (ls_buy_ratio не найден)")

    logger.info("Фьючерсные признаки добавлены (OI + FR + L/S)")
    return df


# 8 ПРИЗНАКИ ЛИКВИДАЦИЙ

def _add_liquidation_features(df):
    """
    liq_ratio     : > 1 — ликвидируются лонги, < 1 — шорты (short squeeze)
    liq_imbalance : (long - short) / total, нормализован в [-1, 1]
    liq_spike_*   : объём > 3σ выше нормы — экстремальный момент
    liq_cumsum_4bar: накопленный net дисбаланс за 16 часов
    """
    long_col  = "liq_long_liq_usd"
    short_col = "liq_short_liq_usd"

    if long_col not in df.columns or short_col not in df.columns:
        logger.warning(
            "Ликвидации: данные отсутствуют (liq_long_liq_usd / liq_short_liq_usd не найдены). "
            "Задайте coinglass_api_key в load_all()."
        )
        return df

    long_liq  = df[long_col].fillna(0)
    short_liq = df[short_col].fillna(0)
    total_liq = long_liq + short_liq

    liq_mean = total_liq.rolling(168).mean()
    liq_std  = total_liq.rolling(168).std().replace(0, np.nan)

    df["liq_long_norm"]  = long_liq  / liq_mean.replace(0, np.nan)
    df["liq_short_norm"] = short_liq / liq_mean.replace(0, np.nan)
    df["liq_total_norm"] = total_liq / liq_mean.replace(0, np.nan)

    df["liq_ratio"]     = long_liq / short_liq.replace(0, np.nan)
    df["liq_imbalance"] = (long_liq - short_liq) / total_liq.replace(0, np.nan)

    df["liq_spike_long"]  = ((long_liq  - liq_mean) / liq_std > 3).astype(int)
    df["liq_spike_short"] = ((short_liq - liq_mean) / liq_std > 3).astype(int)

    net_liq = long_liq - short_liq
    df["liq_net_cumsum_4bar"] = net_liq.rolling(4).sum()

    logger.info(
        f"Признаки ликвидаций добавлены: "
        f"long sum={long_liq.sum()/1e9:.1f}B USD, "
        f"short sum={short_liq.sum()/1e9:.1f}B USD"
    )
    return df


# 9 ЛАГОВЫЕ ПРИЗНАКИ

def _add_lags(df):
    log_ret = np.log(df["c_close"] / df["c_close"].shift(1))

    for lag in [1, 2, 3, 6]:
        df[f"ret_lag_{lag}"] = log_ret.shift(lag)

    for lag in [1, 2, 3]:
        if "fr_funding_rate" in df.columns:
            df[f"funding_lag_{lag}"] = df["fr_funding_rate"].fillna(0).shift(lag)
        if "rsi_14" in df.columns:
            df[f"rsi_lag_{lag}"] = df["rsi_14"].shift(lag)

    df["cum_ret_1d"] = log_ret.rolling(6).sum().shift(1)
    df["cum_ret_7d"] = log_ret.rolling(42).sum().shift(1)

    logger.info("Лаговые признаки добавлены")
    return df


# 10 ВРЕМЕННЫЕ ПРИЗНАКИ

def _add_time_features(df):
    idx = df.index

    df["hour"]       = idx.hour
    df["dayofweek"]  = idx.dayofweek
    df["is_weekend"] = (idx.dayofweek >= 5).astype(int)
    df["month"]      = idx.month

    df["session_asia"] = ((idx.hour >= 0) & (idx.hour < 8)).astype(int)

    logger.info("Временные признаки добавлены")
    return df


# 11 ПРОИЗВОДНЫЕ МОМЕНТУМ-ПРИЗНАКИ

def _add_derived_momentum(df):
    """
    rsi_velocity      : скорость изменения RSI за 3 бара
    rsi_acceleration  : ускорение RSI (производная velocity)
    funding_oi_combo  : fr_funding_rate × oi_change_pct
    vol_z_score       : (volatility_1d - volatility_28d) / volatility_28d
    trend_strength    : |price_to_sma_7d - 1| / volatility_7d
    fg_reversal_signal: fg_fear_greed_index.diff(6)
    vol_price_confirm : volume_ratio × |ret_lag_1|
    ls_acceleration   : скорость изменения ls_buy_ratio за 3 бара
    bars_since_big_move: число баров с последнего движения >2σ (макс. 168)
    """
    if "rsi_14" in df.columns:
        df["rsi_velocity"]     = df["rsi_14"].diff(3)
        df["rsi_acceleration"] = df["rsi_velocity"].diff(3)

    if "fr_funding_rate" in df.columns and "oi_change_pct" in df.columns:
        fr = df["fr_funding_rate"].fillna(0)
        oi = df["oi_change_pct"].fillna(0)
        df["funding_oi_combo"] = fr * oi

    if "volatility_1d" in df.columns and "volatility_28d" in df.columns:
        vol_norm = df["volatility_28d"].replace(0, np.nan)
        df["vol_z_score"] = (df["volatility_1d"] - df["volatility_28d"]) / vol_norm

    if "price_to_sma_7d" in df.columns and "volatility_7d" in df.columns:
        vol7 = df["volatility_7d"].replace(0, np.nan)
        df["trend_strength"] = (df["price_to_sma_7d"] - 1).abs() / vol7

    if "fg_fear_greed_index" in df.columns:
        df["fg_reversal_signal"] = df["fg_fear_greed_index"].diff(6)

    if "volume_ratio" in df.columns and "ret_lag_1" in df.columns:
        df["vol_price_confirm"] = df["volume_ratio"] * df["ret_lag_1"].abs()

    if "ls_buy_ratio" in df.columns:
        df["ls_acceleration"] = df["ls_buy_ratio"].diff(3)

    if "ret_lag_1" in df.columns:
        log_ret  = df["ret_lag_1"]
        ret_std  = log_ret.rolling(168).std()
        big_move = (log_ret.abs() > 2 * ret_std).astype(int)
        counter  = []
        c = 0
        for v in big_move:
            if v == 1:
                c = 0
            else:
                c += 1
            counter.append(c)
        df["bars_since_big_move"] = counter
        df["bars_since_big_move"] = df["bars_since_big_move"].clip(upper=168)

    logger.info("Производные моментум-признаки добавлены")
    return df


# 12 ЦЕЛЕВЫЕ ПЕРЕМЕННЫЕ

def _add_targets(df, threshold_4h=0.0125, threshold_1d=0.02):
    close = df["c_close"]

    future_ret_4h = close.shift(-1) / close - 1
    future_ret_1d = close.shift(-6) / close - 1

    def classify(ret, threshold):
        labels = pd.Series(0, index=ret.index, dtype=int)
        labels[ret >  threshold] =  1
        labels[ret < -threshold] = -1
        return labels

    df["target_4h"] = classify(future_ret_4h, threshold_4h)
    df["target_1d"] = classify(future_ret_1d, threshold_1d)

    df = df.dropna(subset=["target_4h", "target_1d"])

    logger.info(f"Таргеты 4H: {df['target_4h'].value_counts().to_dict()}")
    logger.info(f"Таргеты 1D: {df['target_1d'].value_counts().to_dict()}")
    return df


# 13 УДАЛЕНИЕ АБСОЛЮТНЫХ ЦЕН, УТЕЧЕК И ДУБЛИКАТОВ

def _drop_leakage(df):
    absolute_price_cols = [
        "c_open", "c_high", "c_low", "c_close",
        "c_volume", "c_turnover",
        "m_btc_close", "m_sp500_close", "m_nasdaq_close",
        "m_dxy_close", "m_gold_close", "m_tlt_close",
        "m_ief_close", "m_oil_close",
        "m_btc_sma_7d", "m_btc_sma_30d",
        "m_btc_sma_90d", "m_btc_sma_200d",
        "m_btc_spy_ratio",
        "d_open", "d_high", "d_low", "d_close",
        "d_volume",
        "oi_open_interest",
        "ls_sell_ratio", "ls_ls_ratio",
        "liq_long_liq_usd", "liq_short_liq_usd",
    ]

    confirmed_duplicates = [
        "candle_strength",      # r=1.000 с body_ratio
        "m_btc_log_return",     # r=1.000 с m_btc_pct_change
        "m_nasdaq_pct",         # r=0.957 с m_sp500_pct
        "btc_to_macro_sma30",   # r=0.999 с price_to_sma_28d
        "btc_to_macro_sma7",    # r=0.985 с price_to_sma_7d
        "sma7_to_sma200_4h",    # r=0.982 с btc_to_sma200_4h
        "vwap_dist",            # r=0.975 с price_to_sma_4d
        "turnover_ratio",       # r=0.991 с volume_ratio
        "vix_medium",           # r=-1.000 с vix_high
        "macd_signal",          # r=0.955 с macd
        "sma_1d_to_7d",         # r=0.945 с macd
    ]

    legacy_cols = [
        "move_z_score",
        "move_vix_spread",
        "ls_short_dominant",
    ]

    redundant_flags = [
        "rsi_14_overbought",
        "rsi_14_oversold",
    ]

    zero_signal_cols = [
        "fg_lag_1", "fg_lag_2", "fg_lag_3",
        "ls_buy_ratio_lag_1", "ls_buy_ratio_lag_2", "ls_buy_ratio_lag_3",
        "volume_lag_1", "volume_lag_2", "volume_lag_3",
        "volume_lag_6", "volume_lag_12",
        "ret_lag_12",
        "cum_ret_3d",
        "m_tlt_pct",
        "m_btc_spy_corr_30d", "m_btc_spy_corr_90d",
        "m_btc_dxy_corr_30d", "m_btc_dxy_corr_90d",
        "m_btc_volatility_30d",
        "macd_cross",
        "vix_change",
        "fg_extreme_greed",
        "fg_change",
        "funding_positive",
        "funding_extreme",
        "funding_cumsum_6bar",
        "session_europe", "session_ny", "session_overlap",
    ]

    # Признаки с низким сигналом по результатам корреляционного анализа.
    # Финальный набор: 38 признаков.
    weak_signal_cols = [
        "atr_pct",
        "atr_ratio",
        "bars_since_big_move",
        "bb_squeeze",
        "body_dir",
        "body_ratio",
        "btc_to_macro_sma90",
        "btc_to_sma200_4h",
        "cum_ret_1d",
        "d_turnover",
        "doji",
        "fg_extreme_fear",
        "fg_greed",
        "fg_reversal_signal",
        "funding_lag_1",
        "funding_lag_2",
        "funding_lag_3",
        "funding_oi_combo",
        "funding_rate_abs",
        "hour",
        "is_weekend",
        "lower_wick_ratio",
        "m_btc_gold_ratio",
        "m_move_pct_change",
        "m_move_z_score",
        "m_oil_pct",
        "m_treasury_yield_10y",
        "macd",
        "macd_hist",
        "month",
        "oi_change_pct",
        "oi_price_divergence",
        "oi_ratio",
        "price_to_sma_4d",
        "price_to_sma_7d",
        "ret_lag_3",
        "rsi_6",
        "rsi_acceleration",
        "rsi_lag_1",
        "rsi_lag_2",
        "session_asia",
        "sma_7d_to_28d",
        "upper_wick_ratio",
        "vol_price_divergence",
        "vol_z_score",
        "volatility_28d",
        "yield_spread_change",
        "yield_vs_mean",
    ]

    all_to_drop = (
        absolute_price_cols
        + confirmed_duplicates
        + legacy_cols
        + redundant_flags
        + zero_signal_cols
        + weak_signal_cols
    )

    to_drop = [c for c in all_to_drop if c in df.columns]
    df = df.drop(columns=to_drop)

    n_price  = len([c for c in absolute_price_cols if c in to_drop])
    n_dupes  = len([c for c in confirmed_duplicates + legacy_cols + redundant_flags if c in to_drop])
    n_zero   = len([c for c in zero_signal_cols if c in to_drop])
    n_weak   = len([c for c in weak_signal_cols if c in to_drop])
    logger.info(
        f"Удалено {len(to_drop)} колонок: "
        f"{n_price} абсолютных цен, "
        f"{n_dupes} дубликатов/устаревших, "
        f"{n_zero} нулевого сигнала, "
        f"{n_weak} слабого сигнала"
    )
    return df


# ЗАГРУЗКА ИЗ CSV

def load_from_csv(csv_dir=".", prefix="btc"):
    """
    Загружает ранее сохранённые CSV из data_loader.save_all() и восстанавливает
    dict совместимый с build_features(), без повторного обращения к API.
    """
    import glob
    import os

    source_keys = [
        "candles_4h", "bybit_daily", "macro", "fear_greed",
        "open_interest", "funding_rate", "long_short",
        "liquidations",
    ]
    result = {}

    for key in source_keys:
        pattern = os.path.join(csv_dir, f"{prefix}_{key}_*.csv")
        matches = sorted(glob.glob(pattern))

        if not matches:
            logger.warning(f"load_from_csv: файл для '{key}' не найден ({pattern})")
            result[key] = pd.DataFrame()
            continue

        path = matches[-1]
        df   = pd.read_csv(path, index_col=0, parse_dates=True)

        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df.index.name = "datetime"

        result[key] = df
        logger.info(
            f"  {key:<20} {len(df):>6} строк | "
            f"{df.index[0].date()} - {df.index[-1].date()} <- {os.path.basename(path)}"
        )

    if result.get("candles_4h", pd.DataFrame()).empty:
        raise FileNotFoundError(
            f"Файл candles_4h не найден в '{csv_dir}'. "
            "Сначала запустите data_loader.py и сохраните данные через save_all()."
        )

    logger.info("load_from_csv: все источники загружены")
    return result


# ГЛАВНАЯ ФУНКЦИЯ

def build_features(data, threshold_4h=0.0125, threshold_1d=0.02):
    """
    Строит полный датасет признаков из выхода data_loader.load_all().

    Аргументы:
        data         : dict из load_all()
        threshold_4h : порог для 4H таргета (по умолчанию 2%)
        threshold_1d : порог для 1D таргета (по умолчанию 3.5%)

    Возвращает:
        df           : DataFrame с признаками и таргетами
        feature_cols : список колонок-признаков (без таргетов)

    Пайплайн:
        1.  merge_sources            — объединение источников
        2.  handle_missing           — заполнение пропусков
        3.  add_technical            — RSI, MACD, BB, ATR, SMA, волатильность
        4.  add_volume_features      — volume_ratio, spike, divergence
        5.  add_candle_features      — body, wicks, doji, hl_pct
        6.  add_macro_features       — макро производные + MOVE z-score/spread сдвиг
        7.  add_futures_features     — OI + Funding + L/S Ratio
        7b. add_liquidation_features — ликвидации long/short
        8.  add_lags                 — ret_lag, rsi_lag, funding_lag
        9.  add_time_features        — hour, dayofweek, session
        10. add_derived_momentum     — RSI velocity, funding×OI, vol_z_score и др.
        11. add_targets              — target_4h, target_1d
        12. drop_leakage             — удаление цен, дубликатов, устаревших
    """
    logger.info("Построение признаков")

    df = _merge_sources(data)
    df = _handle_missing(df)
    df = _add_technical(df)
    df = _add_volume_features(df)
    df = _add_candle_features(df)
    df = _add_macro_features(df)
    df = _add_futures_features(df)
    df = _add_liquidation_features(df)
    df = _add_lags(df)
    df = _add_time_features(df)
    df = _add_derived_momentum(df)
    df = _add_targets(df, threshold_4h, threshold_1d)
    df = _drop_leakage(df)

    target_cols  = ["target_4h", "target_1d"]
    feature_cols = [c for c in df.columns if c not in target_cols]

    before = len(df)
    nan_per_col = df[feature_cols].isnull().sum()
    nan_per_col = nan_per_col[nan_per_col > 0].sort_values(ascending=False)
    if not nan_per_col.empty:
        logger.warning(
            f"Признаки с NaN перед dropna (топ-10): {nan_per_col.head(10).to_dict()}"
        )
        rows_with_nan = df[feature_cols].isnull().any(axis=1).sum()
        logger.warning(f"Строк будет удалено dropna: {rows_with_nan} из {before}")

    df = df.dropna(subset=feature_cols)
    logger.info(f"Удалено {before - len(df)} строк с NaN (rolling warmup)")

    if df.empty:
        logger.warning(
            "DataFrame пуст после dropna. Вероятная причина: недостаточно данных "
            "для rolling-окон (btc_to_sma200_4h требует >= 1200 4H-баров = ~200 дней). "
            "Минимальный рекомендуемый start_date: за 2+ лет до текущей даты."
        )
        return df, feature_cols

    logger.info(f"Итого: {len(df)} строк, {len(feature_cols)} признаков")
    logger.info(f"Период: {df.index[0].date()} - {df.index[-1].date()}")
    logger.info(f"Баланс target_4h : {df['target_4h'].value_counts().to_dict()}")
    logger.info(f"Баланс target_1d : {df['target_1d'].value_counts().to_dict()}")

    return df, feature_cols


def train_test_split_time(df, feature_cols, target_col, test_size=0.2):
    """
    Временное разбиение train/test без random split.
    Нормализацию делать fit только на train, transform на test.
    """
    split_idx  = int(len(df) * (1 - test_size))
    split_date = df.index[split_idx]

    train = df[df.index <  split_date]
    test  = df[df.index >= split_date]

    X_train = train[feature_cols]
    y_train = train[target_col]
    X_test  = test[feature_cols]
    y_test  = test[target_col]

    logger.info(f"Train: {len(train)} строк | {train.index[0].date()} - {train.index[-1].date()}")
    logger.info(f"Test:  {len(test)} строк  | {test.index[0].date()} - {test.index[-1].date()}")

    return X_train, X_test, y_train, y_test


def walk_forward_splits(df, n_folds=5, test_months=3):
    """
    Walk-forward splits с расширяющимся окном трейна.
    Возвращает список кортежей (train_idx, test_idx).
    """
    splits    = []
    total     = len(df)
    test_size = int(total * test_months / ((df.index[-1] - df.index[0]).days / 30))
    min_train = int(total * 0.60)

    for fold in range(n_folds):
        test_end   = total - fold * test_size
        test_start = test_end - test_size
        if test_start < min_train:
            break
        train_idx = df.index[:test_start]
        test_idx  = df.index[test_start:test_end]
        splits.append((train_idx, test_idx))
        logger.info(
            f"Fold {n_folds - fold}: "
            f"train до {train_idx[-1].date()} | "
            f"test {test_idx[0].date()} — {test_idx[-1].date()} "
            f"({len(test_idx)} баров)"
        )

    return list(reversed(splits))


if __name__ == "__main__":
    import glob

    csv_files = sorted(glob.glob("btc_*.csv"))
    if csv_files:
        logger.info(f"Найдены CSV: {csv_files}")
        data = load_from_csv()
    else:
        logger.info("CSV не найдены, запускаем data_loader")
        from data_loader import load_all, START_DATE
        data = load_all(start_date=START_DATE)

    df, feature_cols = build_features(data, threshold_4h=0.0125, threshold_1d=0.02)

    df.to_csv("features_dataset.csv")
    logger.info("Сохранено в features_dataset.csv")

    print(f"\nИтого признаков: {len(feature_cols)}")
    print("\nСписок признаков:")
    for i, f in enumerate(feature_cols, 1):
        print(f"  {i:3d}. {f}")