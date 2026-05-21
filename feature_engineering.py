"""
feature_engineering.py - Построение признаков для классификации скачков BTC

Принимает на вход dict из data_loader.load_all().
Мастер-индекс: 1H свечи (candles_1h).
Возвращает готовый DataFrame с признаками и таргетами.

Таргеты:
    target_1h : скачок цены через 1 бар (1H) на +/-0.8%  -> (-1, 0, 1)
    target_6h : скачок цены через 6 баров (6H) на +/-2% -> (-1, 0, 1)
    Пороги по умолчанию: 1% (1H) и 2% (6H).

Пересчёт rolling-окон относительно 4H версии (×4 баров):
    Период   4H-баров   1H-баров
    ──────   ────────   ────────
    1 день       6         24
    4 дня       24         96
    7 дней      42        168
    28 дней    168        672
    200 дней  1200       4800

Использование (вариант 1 - через data_loader):
    from data_loader import load_all, START_DATE
    from feature_engineering import build_features

    data = load_all(start_date=START_DATE)
    df, feature_cols = build_features(data, threshold_1h=0.008, threshold_6h=0.02)

Использование (вариант 2 - из сохранённых CSV, без API):
    from feature_engineering import load_from_csv, build_features

    data = load_from_csv(csv_dir=".", prefix="btc")
    df, feature_cols = build_features(data, threshold_1h=0.008, threshold_6h=0.02)
"""

import glob
import logging
import os
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


def _rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def _atr(high, low, close, period=14):
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def _zscore(series, window, min_periods=1):
    mean = series.rolling(window, min_periods=min_periods).mean()
    std = series.rolling(window, min_periods=min_periods).std().replace(0, np.nan)
    return (series - mean) / std


def _merge_sources(data):
    """
    Собирает все источники в единый DataFrame на мастер-индексе candles_1h.

    Префиксы колонок:
        c_   - 1H свечи (candles_1h)
        d_   - дневные свечи (bybit_daily)
        m_   - макро (macro)
        fg_  - Fear & Greed (fear_greed)
        oi_  - Open Interest
        fr_  - Funding Rate
        ls_  - Long/Short Ratio
        oc_  - On-chain метрики (onchain, strip "onchain_" из имён колонок)
    """
    candles = data.get("candles_1h", pd.DataFrame())
    if candles.empty:
        raise ValueError("candles_1h пуст — нет мастер-индекса")

    df = candles.copy()
    df.columns = [f"c_{c.lower()}" for c in df.columns]


    onchain_raw = data.get("onchain", pd.DataFrame())
    if not onchain_raw.empty:
        onchain_raw = onchain_raw.copy()
        onchain_raw.columns = [
            c.replace("onchain_", "") for c in onchain_raw.columns
        ]

    sources = {
        "d":  data.get("bybit_daily",   pd.DataFrame()),
        "m":  data.get("macro",         pd.DataFrame()),
        "fg": data.get("fear_greed",    pd.DataFrame()),
        "oi": data.get("open_interest", pd.DataFrame()),
        "fr": data.get("funding_rate",  pd.DataFrame()),
        "ls": data.get("long_short",    pd.DataFrame()),
        "oc": onchain_raw,
    }

    for prefix, src in sources.items():
        if src.empty:
            logger.warning(f"Источник '{prefix}' пуст, пропускаем")
            continue
        src = src.copy()
        src.columns = [f"{prefix}_{c}" for c in src.columns]
        df = df.join(src, how="left")

    logger.info(
        f"Объединено: {len(df)} строк, "
        f"{len(df.columns)} исходных колонок"
    )
    return df


def _handle_missing(df):
    price_patterns = [
        "close","open","high","low","sma","ratio","vix","yield","spread",
        "gold","oil","tlt","ief","sp500","nasdaq","dxy","btc_spy","open_interest",
        "fear_greed","turnover","volume","move_index","hash_rate","tx_volume",
        "oc_"
    ]
    pct_patterns = [
        "pct","log_return","volatility","funding_rate","risk_regime","ls_ratio",
        "ls_buy","ls_sell","move_pct","tx_count_pct","hash_rate_pct","mempool_pct",
        "fees_usd", "oc_"
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


def _add_technical(df):
    """
    RSI: периоды 14H, 24H (1 день), 48H (2 дня).
    MACD: 12/26/9 (стандартные, таймфрейм-независимые).
    BB:  24-барная (= 1 день на 1H).
    ATR: 14H.
    SMA: 24(1d), 96(4d), 168(7d), 672(28d) баров.
    Волатильность: 24H, 168H (7d), 672H (28d).
    """
    close = df["c_close"]
    high = df["c_high"]
    low = df["c_low"]

    # RSI
    for p in [14, 24, 48]:
        df[f"rsi_{p}"] = _rsi(close, p)

    # MACD
    macd_line = _ema(close, 12) - _ema(close, 26)
    macd_signal = _ema(macd_line, 9)
    df["macd"] = macd_line
    df["macd_hist"] = macd_line - macd_signal

    # Bollinger Bands (24 бара = 1 день)
    sma24 = close.rolling(24).mean()
    std24 = close.rolling(24).std()
    bb_upper = sma24 + 2 * std24
    bb_lower = sma24 - 2 * std24
    bb_width = (bb_upper - bb_lower) / sma24.replace(0, np.nan)
    df["bb_pos"] = (
        (close - bb_lower) / (bb_upper - bb_lower).replace(0, np.nan)
    )
    df["bb_width"] = bb_width
    # bb_squeeze: ширина полос ниже 7-дневной нормы
    df["bb_squeeze"] = (
        bb_width < bb_width.rolling(168).mean()
    ).astype(int)

    # ATR (14H)
    atr14 = _atr(high, low, close, 14)
    df["atr_pct"] = atr14 / close
    df["atr_ratio"] = atr14 / atr14.rolling(168).mean()

    # SMA — 1d/4d/7d/28d (в барах 1H)
    bars = {"1d": 24, "4d": 96, "7d": 168, "28d": 672}
    for label, w in bars.items():
        df[f"sma_{label}"] = close.rolling(w).mean()

    df["price_to_sma_1d"]  = close / df["sma_1d"]
    df["price_to_sma_4d"]  = close / df["sma_4d"]
    df["price_to_sma_7d"]  = close / df["sma_7d"]
    df["price_to_sma_28d"] = close / df["sma_28d"]
    df["sma_7d_to_28d"]    = df["sma_7d"] / df["sma_28d"]

    # Stochastic RSI (по RSI-24, окно 24)
    rsi24 = df["rsi_24"]
    rsi_min = rsi24.rolling(24).min()
    rsi_max = rsi24.rolling(24).max()
    df["stoch_rsi"] = (
        (rsi24 - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan)
    )

    # Волатильность логарифмических доходностей
    log_ret = np.log(close / close.shift(1))
    df["volatility_1d"]  = log_ret.rolling(24).std()
    df["volatility_7d"]  = log_ret.rolling(168).std()
    df["volatility_28d"] = log_ret.rolling(672).std()
    df["volatility_regime"] = (
        df["volatility_7d"] / df["volatility_28d"]
    )

    # Удаляем промежуточные SMA-колонки
    df = df.drop(columns=["sma_1d", "sma_4d", "sma_7d", "sma_28d"])

    logger.info("Технические индикаторы добавлены")
    return df


def _add_volume_features(df):
    """
    volume_ratio     : объём vs 7-дневная норма (168 баров).
    volume_spike     : флаг объёма > 2× нормы.
    vwap_dist        : отклонение цены от 1-дневного VWAP.
    vol_price_divergence: направление объёма vs цены.
    d_turnover_ratio : дневной оборот vs 30-дневная норма.

    Taker volume (новые признаки из candles_1h):
    taker_buy_zscore  : z-score taker_buy_vol vs 168-барная норма.
    taker_sell_zscore : z-score taker_sell_vol vs 168-барная норма.
    taker_vol_ratio_ma: скользящее среднее taker_vol_ratio за 24 бара.
    taker_vol_imbalance: (buy - sell) / (buy + sell) — нормализованный дисбаланс.
    """
    vol = df["c_volume"]
    close = df["c_close"]

    vol_mean = vol.rolling(168).mean()
    df["volume_ratio"] = vol / vol_mean.replace(0, np.nan)
    df["volume_spike"] = (df["volume_ratio"] > 2.0).astype(int)

    vwap = (close * vol).rolling(24).sum() / vol.rolling(24).sum()
    df["vwap_dist"] = (close - vwap) / vwap.replace(0, np.nan)

    price_dir = np.sign(close.pct_change())
    vol_dir = np.sign(vol.pct_change())
    df["vol_price_divergence"] = (price_dir != vol_dir).astype(int)

    if "d_turnover" in df.columns:
        d_turn_mean = df["d_turnover"].rolling(30).mean()
        df["d_turnover_ratio"] = (
            df["d_turnover"] / d_turn_mean.replace(0, np.nan)
        )
        df["d_turnover_ratio_lag1"] = df["d_turnover_ratio"].shift(1)
        df["d_turnover_ratio_ma3"]  = (
            df["d_turnover_ratio"].rolling(3).mean()
        )

    # Taker volume признаки
    if "c_taker_buy_vol" in df.columns and "c_taker_sell_vol" in df.columns:
        buy = df["c_taker_buy_vol"]
        sell = df["c_taker_sell_vol"]

        df["taker_buy_zscore"]  = _zscore(buy, 168)
        df["taker_sell_zscore"] = _zscore(sell, 168)

        total_taker = buy + sell
        df["taker_vol_imbalance"] = (
            (buy - sell) / total_taker.replace(0, np.nan)
        )

    if "c_taker_vol_ratio" in df.columns:
        df["taker_vol_ratio_ma"] = (
            df["c_taker_vol_ratio"].rolling(24).mean()
        )
        df["taker_vol_ratio_change"] = df["c_taker_vol_ratio"].diff(6)

    logger.info("Признаки объёма и taker volume добавлены")
    return df


def _add_candle_features(df):
    open_ = df["c_open"]
    high = df["c_high"]
    low = df["c_low"]
    close = df["c_close"]

    body = close - open_
    body_abs = body.abs()
    candle_range = (high - low).replace(0, np.nan)

    df["body_ratio"] = body_abs / candle_range
    df["body_dir"] = np.sign(body)

    upper_wick = high - pd.concat([open_, close], axis=1).max(axis=1)
    lower_wick = (
        pd.concat([open_, close], axis=1).min(axis=1) - low
    )
    df["upper_wick_ratio"] = upper_wick / candle_range
    df["lower_wick_ratio"] = lower_wick / candle_range

    df["doji"]   = (df["body_ratio"] < 0.1).astype(int)
    df["hl_pct"] = (high - low) / close

    logger.info("Свечные паттерны добавлены")
    return df


def _add_macro_features(df):
    """
    Дневные макро-данные уже выровнены на 1H через ffill в data_loader.
    Сдвиг pct-колонок на shift(24) = 1 день на 1H-индексе,
    чтобы исключить lookahead (данные за день T доступны только с T+1).
    """
    close = df["c_close"]

    if "m_btc_sma_90d" in df.columns:
        df["btc_to_macro_sma90"] = close / df["m_btc_sma_90d"]

    # 200-дневная SMA на 1H: 200 × 24 = 4800 баров
    sma200_1h = close.rolling(4800).mean()
    df["btc_to_sma200_1h"] = close / sma200_1h

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

    # Сдвиг всех дневных pct/return/derived колонок на 1 день вперёд (24 бара)
    m_pct_cols = [
        c for c in df.columns
        if c.startswith("m_") and any(
            c.endswith(s)
            for s in ["_pct", "_pct_change", "_log_return"]
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
        df[col] = df[col].shift(24)

    logger.info(
        f"Макро признаки добавлены "
        f"(pct сдвинуты на 24 бара: {len(daily_pct_cols)} колонок)"
    )
    return df


def _add_futures_features(df):
    """
    Open Interest: oi_change_pct (1H), oi_change_6bar (6H),
                   oi_ratio (vs 7d норма), oi_price_divergence.
    Funding Rate:  abs, change.
    L/S Ratio:     buy_ratio, доминирование, скользящее среднее.
    """
    if "oi_open_interest" in df.columns:
        oi = df["oi_open_interest"]
        df["oi_change_pct"]  = oi.pct_change() * 100
        df["oi_change_6bar"] = oi.pct_change(6) * 100
        oi_mean = oi.rolling(168).mean()
        df["oi_ratio"] = oi / oi_mean.replace(0, np.nan)

        price_dir = np.sign(df["c_close"].pct_change())
        oi_dir = np.sign(df["oi_change_pct"])
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
        logger.warning(
            "Long/Short Ratio: данные отсутствуют (ls_buy_ratio не найден)"
        )

    logger.info("Фьючерсные признаки добавлены (OI + FR + L/S)")
    return df


def _add_onchain_features(df):
    """
    On-chain метрики из Blockchain.com (дневные, ffill на 1H).

    Исходные колонки после merge (префикс oc_):
        oc_tx_count, oc_mempool_count, oc_mempool_bytes,
        oc_hash_rate, oc_fees_usd, oc_tx_volume_usd,
        oc_fees_per_tx, oc_tx_count_pct, oc_hash_rate_pct,
        oc_mempool_pct

    Производные признаки:
        oc_tx_count_zscore    : z-score числа транзакций vs 30d (720 баров)
        oc_hash_rate_zscore   : z-score хешрейта vs 30d
        oc_mempool_zscore     : z-score мемпула vs 30d
        oc_fees_per_tx_zscore : z-score комиссии на транзакцию vs 30d
        oc_tx_volume_zscore   : z-score объёма переводов vs 30d
        oc_fees_tx_ratio      : fees_usd / tx_volume_usd — доля комиссий в обороте
        oc_network_stress     : mempool_zscore × fees_per_tx_zscore — стресс сети

    Сдвиг pct_change-колонок на 24 бара (данные дня T доступны с T+1).
    """
    _OC_COLS = {
        "tx_count":    "oc_tx_count",
        "mempool":     "oc_mempool_count",
        "hash_rate":   "oc_hash_rate",
        "fees_per_tx": "oc_fees_per_tx",
        "tx_volume":   "oc_tx_volume_usd",
        "fees_usd":    "oc_fees_usd",
    }

    present = {k: v for k, v in _OC_COLS.items() if v in df.columns}
    if not present:
        logger.warning(
            "On-chain: данные отсутствуют (oc_* колонки не найдены)"
        )
        return df

    # Z-score за 30 дней = 720 баров (дневные данные, ffill  каждый бар одинаков в пределах дня, поэтому используем min_periods=20)
    _W = 720

    for key, col in present.items():
        zscore_col = f"oc_{key}_zscore"
        df[zscore_col] = _zscore(df[col], _W, min_periods=20)

    # Доля комиссий в общем объёме переводов
    if "oc_fees_usd" in df.columns and "oc_tx_volume_usd" in df.columns:
        df["oc_fees_tx_ratio"] = (
            df["oc_fees_usd"]
            / df["oc_tx_volume_usd"].replace(0, np.nan)
        )

    # Индикатор перегрузки сети: высокий мемпул + высокие комиссии
    if "oc_mempool_zscore" in df.columns and "oc_fees_per_tx_zscore" in df.columns:
        df["oc_network_stress"] = (
            df["oc_mempool_zscore"] * df["oc_fees_per_tx_zscore"]
        )

    # Сдвиг pct_change колонок на 1 день (24 бара) — антилуткхед
    pct_cols = [
        c for c in df.columns
        if c.startswith("oc_") and c.endswith("_pct")
    ]
    for col in pct_cols:
        df[col] = df[col].shift(24)

    logger.info(
        f"On-chain признаки добавлены: "
        f"{len(present)} базовых метрик, "
        f"pct сдвинуты на 24 бара"
    )
    return df


def _add_lags(df):
    """
    Лаги логарифмических доходностей: 1H, 2H, 3H, 6H, 12H, 24H.
    Лаги Funding Rate и RSI: 1H, 2H, 3H.
    Накопленная доходность: 1 день (24H) и 7 дней (168H).
    """
    log_ret = np.log(df["c_close"] / df["c_close"].shift(1))

    for lag in [1, 2, 3, 6, 12, 24]:
        df[f"ret_lag_{lag}"] = log_ret.shift(lag)

    for lag in [1, 2, 3]:
        if "fr_funding_rate" in df.columns:
            df[f"funding_lag_{lag}"] = (
                df["fr_funding_rate"].fillna(0).shift(lag)
            )
        if "rsi_24" in df.columns:
            df[f"rsi_lag_{lag}"] = df["rsi_24"].shift(lag)

    df["cum_ret_1d"] = log_ret.rolling(24).sum().shift(1)
    df["cum_ret_7d"] = log_ret.rolling(168).sum().shift(1)

    logger.info("Лаговые признаки добавлены")
    return df


def _add_time_features(df):
    """
    На 1H данных временные признаки намного информативнее, чем на 4H.
    hour даёт 24 уникальных значения вместо 6.
    """
    idx = df.index

    df["hour"]       = idx.hour
    df["dayofweek"]  = idx.dayofweek
    df["is_weekend"] = (idx.dayofweek >= 5).astype(int)
    df["month"]      = idx.month

    # Торговые сессии (UTC)
    df["session_asia"]    = ((idx.hour >= 0)  & (idx.hour < 8)).astype(int)
    df["session_europe"]  = ((idx.hour >= 7)  & (idx.hour < 16)).astype(int)
    df["session_ny"]      = ((idx.hour >= 13) & (idx.hour < 22)).astype(int)
    # Пересечение EU + NY: наибольший объём
    df["session_overlap"] = ((idx.hour >= 13) & (idx.hour < 16)).astype(int)

    logger.info("Временные признаки добавлены")
    return df


def _add_derived_momentum(df):
    """
    rsi_velocity       : скорость изменения RSI за 6 баров (6H).
    rsi_acceleration   : ускорение RSI (производная velocity за 6 баров).
    funding_oi_combo   : fr_funding_rate × oi_change_pct.
    vol_z_score        : (volatility_1d - volatility_28d) / volatility_28d.
    trend_strength     : |price_to_sma_7d - 1| / volatility_7d.
    fg_reversal_signal : diff за 24 бара (1 день) по Fear & Greed.
    vol_price_confirm  : volume_ratio × |ret_lag_1|.
    ls_acceleration    : скорость изменения ls_buy_ratio за 6 баров.
    bars_since_big_move: число баров с последнего движения >2σ (макс. 672).
    """
    if "rsi_24" in df.columns:
        df["rsi_velocity"]     = df["rsi_24"].diff(6)
        df["rsi_acceleration"] = df["rsi_velocity"].diff(6)

    if "fr_funding_rate" in df.columns and "oi_change_pct" in df.columns:
        fr = df["fr_funding_rate"].fillna(0)
        oi = df["oi_change_pct"].fillna(0)
        df["funding_oi_combo"] = fr * oi

    if "volatility_1d" in df.columns and "volatility_28d" in df.columns:
        vol_norm = df["volatility_28d"].replace(0, np.nan)
        df["vol_z_score"] = (
            (df["volatility_1d"] - df["volatility_28d"]) / vol_norm
        )

    if "price_to_sma_7d" in df.columns and "volatility_7d" in df.columns:
        vol7 = df["volatility_7d"].replace(0, np.nan)
        df["trend_strength"] = (df["price_to_sma_7d"] - 1).abs() / vol7

    if "fg_fear_greed_index" in df.columns:
        df["fg_reversal_signal"] = df["fg_fear_greed_index"].diff(24)

    if "volume_ratio" in df.columns and "ret_lag_1" in df.columns:
        df["vol_price_confirm"] = (
            df["volume_ratio"] * df["ret_lag_1"].abs()
        )

    if "ls_buy_ratio" in df.columns:
        df["ls_acceleration"] = df["ls_buy_ratio"].diff(6)

    if "ret_lag_1" in df.columns:
        log_ret = df["ret_lag_1"]
        ret_std = log_ret.rolling(672).std()
        big_move = (log_ret.abs() > 2 * ret_std).astype(int)
        counter = []
        c = 0
        for v in big_move:
            if v == 1:
                c = 0
            else:
                c += 1
            counter.append(c)
        df["bars_since_big_move"] = counter
        df["bars_since_big_move"] = df["bars_since_big_move"].clip(upper=672)

    logger.info("Производные моментум-признаки добавлены")
    return df


def _add_targets(df, threshold_1h=0.008, threshold_6h=0.02):
    """
    target_1h : движение за 1 бар (1H).  Порог ±1%.
    target_6h : движение за 6 баров (6H). Порог ±2%.

    Классы: -1 (падение), 0 (боковик), 1 (рост).
    """
    close = df["c_close"]

    future_ret_1h = close.shift(-1) / close - 1
    future_ret_6h = close.shift(-6) / close - 1

    def classify(ret, threshold):
        labels = pd.Series(0, index=ret.index, dtype=int)
        labels[ret >  threshold] =  1
        labels[ret < -threshold] = -1
        return labels

    df["target_1h"] = classify(future_ret_1h, threshold_1h)
    df["target_6h"] = classify(future_ret_6h, threshold_6h)
    df = df.dropna(subset=["target_1h", "target_6h"])

    logger.info(
        f"Таргеты 1H (±{threshold_1h * 100:.1f}%): "
        f"{df['target_1h'].value_counts().sort_index().to_dict()}"
    )
    logger.info(
        f"Таргеты 6H (±{threshold_6h * 100:.1f}%): "
        f"{df['target_6h'].value_counts().sort_index().to_dict()}"
    )
    return df


def _drop_leakage(df):
    """
    absolute_price_cols  : прямые цены и сырые объёмы — утечка в любой модели.
    confirmed_duplicates : корреляция r > 0.95 с другим признаком.
    legacy_cols          : устаревшие колонки из старых версий.
    zero_signal_cols     : нулевой или незначимый сигнал по анализу 4H данных.

    weak_signal_cols     : ОСТАВЛЕН ПУСТЫМ — требует переоценки на 1H данных.
                           Запустите корреляционный анализ на features_dataset.csv
                           и заполните этот список по результатам.
    """
    absolute_price_cols = [
        # Свечи
        "c_open", "c_high", "c_low", "c_close",
        "c_volume", "c_turnover",
        # Сырые taker-объёмы в USDT (scale-dependent, заменены z-score)
        "c_taker_buy_vol", "c_taker_sell_vol",
        # Макро абсолютные цены
        "m_btc_close", "m_sp500_close",
        "m_dxy_close", "m_gold_close", "m_tlt_close",
        "m_ief_close", "m_oil_close",
        "m_btc_sma_7d", "m_btc_sma_30d",
        "m_btc_sma_90d", "m_btc_sma_200d",
        "m_btc_spy_ratio",
        # Дневные свечи
        "d_open", "d_high", "d_low", "d_close",
        "d_volume",
        # Фьючерсы — сырые уровни
        "oi_open_interest",
        "ls_sell_ratio", "ls_ls_ratio",
        # On-chain — сырые абсолютные уровни (заменены z-score)
        "oc_tx_count", "oc_mempool_count", "oc_mempool_bytes",
        "oc_hash_rate", "oc_fees_usd", "oc_tx_volume_usd",
    ]

    confirmed_duplicates = [
        "m_btc_log_return",   # r=1.000 с m_btc_pct_change
        "macd_signal",        # r=0.955 с macd
    ]

    legacy_cols = [
        "move_z_score",       # дублируется в m_move_z_score
        "move_vix_spread",    # дублируется в m_move_vix_spread
        "ls_short_dominant",  # обратный к ls_long_dominant
        "candle_strength",    # устаревшее имя body_ratio
        "vwap_dist_tmp",      # мёртвый код из старой версии
    ]

    zero_signal_cols = [
        "m_btc_spy_corr_30d", "m_btc_spy_corr_90d",
        "m_btc_dxy_corr_30d", "m_btc_dxy_corr_90d",
        "m_btc_volatility_30d",
        "d_turnover",
        "vix_medium",
        "oc_mempool_pct",
    ]

    # ── Требует переоценки на 1H данных ──────────────────────
    # После генерации features_dataset.csv выполните:
    #   corr = df[feature_cols].corrwith(df["target_1h"]).abs().sort_values()
    #   weak = corr[corr < 0.01].index.tolist()
    # И добавьте результат сюда.
    weak_signal_cols: list = []

    all_to_drop = (
        absolute_price_cols
        + confirmed_duplicates
        + legacy_cols
        + zero_signal_cols
        + weak_signal_cols
    )

    to_drop = [c for c in all_to_drop if c in df.columns]
    df = df.drop(columns=to_drop)

    n_price = len([c for c in absolute_price_cols if c in to_drop])
    n_dupes = len(
        [c for c in confirmed_duplicates + legacy_cols if c in to_drop]
    )
    n_zero = len([c for c in zero_signal_cols if c in to_drop])
    logger.info(
        f"Удалено {len(to_drop)} колонок: "
        f"{n_price} абсолютных цен, "
        f"{n_dupes} дубликатов/устаревших, "
        f"{n_zero} нулевого сигнала"
    )
    return df



def load_from_csv(csv_dir=".", prefix="btc"):
    """
    Загружает ранее сохранённые CSV из data_loader.save_all() и восстанавливает
    dict, совместимый с build_features(), без повторного обращения к API.
    """
    source_keys = [
        "candles_1h", "bybit_daily", "macro", "fear_greed",
        "open_interest", "funding_rate", "long_short", "onchain",
    ]
    result = {}

    for key in source_keys:
        pattern = os.path.join(csv_dir, f"{prefix}_{key}_*.csv")
        matches = sorted(glob.glob(pattern))

        if not matches:
            logger.warning(
                f"load_from_csv: файл для '{key}' не найден ({pattern})"
            )
            result[key] = pd.DataFrame()
            continue

        path = matches[-1]
        df = pd.read_csv(path, index_col=0, parse_dates=True)

        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df.index.name = "datetime"

        result[key] = df
        logger.info(
            f"  {key:<20} {len(df):>6} строк | "
            f"{df.index[0].date()} - {df.index[-1].date()} "
            f"<- {os.path.basename(path)}"
        )

    if result.get("candles_1h", pd.DataFrame()).empty:
        raise FileNotFoundError(
            f"Файл candles_1h не найден в '{csv_dir}'. "
            "Сначала запустите data_loader.py и сохраните данные "
            "через save_all()."
        )

    logger.info("load_from_csv: все источники загружены")
    return result




def build_features(data, threshold_1h=0.008, threshold_6h=0.02):
    """
    Строит полный датасет признаков из выхода data_loader.load_all().

    Аргументы:
        data          : dict из load_all() с ключом "candles_1h" как мастером.
        threshold_1h  : порог для 1H таргета (по умолчанию 0.8%).
        threshold_6h  : порог для 6H таргета (по умолчанию 2%).

    Возвращает:
        df           : DataFrame с признаками и таргетами.
        feature_cols : список колонок-признаков (без таргетов).

    Пайплайн:
        1.  merge_sources         — объединение источников (мастер: candles_1h)
        2.  handle_missing        — заполнение пропусков
        3.  add_technical         — RSI(14/24/48), MACD, BB(24), ATR(14),
                                    SMA(24/96/168/672), волатильность(24/168/672)
        4.  add_volume_features   — volume_ratio(168), spike, VWAP(24),
                                    taker z-scores, taker_vol_imbalance
        5.  add_candle_features   — body, wicks, doji, hl_pct
        6.  add_macro_features    — макро производные, shift(24) для pct-колонок
        7.  add_futures_features  — OI + Funding + L/S Ratio
        8.  add_onchain_features  — tx_count, hash_rate, mempool z-scores,
                                    fees_tx_ratio, network_stress
        9.  add_lags              — ret_lag(1/2/3/6/12/24), rsi_lag, funding_lag
        10. add_time_features     — hour, dayofweek, session (Asia/EU/NY/overlap)
        11. add_derived_momentum  — RSI velocity/accel, funding×OI, trend_strength
        12. add_targets           — target_1h (±1%), target_6h (±2%)
        13. drop_leakage          — удаление цен, дубликатов, устаревших

    Примечание по warmup:
        btc_to_sma200_1h требует 4800 баров (200 дней).
        Рекомендуемый start_date: минимум за 300 дней до начала анализируемого периода.
    """
    logger.info("Построение признаков (мастер-индекс: 1H)")

    df = _merge_sources(data)
    df = _handle_missing(df)
    df = _add_technical(df)
    df = _add_volume_features(df)
    df = _add_candle_features(df)
    df = _add_macro_features(df)
    df = _add_futures_features(df)
    df = _add_onchain_features(df)
    onchain_cols = [c for c in df.columns if c.startswith('oc_')]
    if onchain_cols:
        first_valid = df[onchain_cols].first_valid_index()
        if first_valid is not None:
            before = len(df)
            df = df.loc[first_valid:]
            logger.info(
                f"Обрезано по on-chain данным: удалено {before - len(df)} строк, "
                f"начало с {first_valid.date()}"
            )
        else:
            logger.warning("On-chain данные полностью отсутствуют (все NaN)")
    df = _add_lags(df)
    df = _add_time_features(df)
    df = _add_derived_momentum(df)
    df = _add_targets(df, threshold_1h, threshold_6h)
    df = _drop_leakage(df)

    target_cols  = ["target_1h", "target_6h"]
    feature_cols = [c for c in df.columns if c not in target_cols]

    nan_frac = df[feature_cols].isnull().mean()
    bad_cols = nan_frac[nan_frac > 0.50].index.tolist()
    if bad_cols:
        logger.warning(
            f"Удалены признаки с >50% NaN ({len(bad_cols)}): {bad_cols}"
        )
        df = df.drop(columns=bad_cols)
        feature_cols = [c for c in feature_cols if c not in bad_cols]

    # Диагностика NaN перед финальным dropna
    before = len(df)
    nan_per_col = df[feature_cols].isnull().sum()
    nan_per_col = nan_per_col[nan_per_col > 0].sort_values(ascending=False)
    if not nan_per_col.empty:
        logger.warning(
            f"Признаки с NaN перед dropna (топ-10): "
            f"{nan_per_col.head(10).to_dict()}"
        )
        rows_with_nan = df[feature_cols].isnull().any(axis=1).sum()
        logger.warning(
            f"Строк будет удалено dropna: {rows_with_nan} из {before}"
        )

    before = len(df)
    df = df.dropna(subset=feature_cols)
    logger.info(f"Удалено {before - len(df)} строк с NaN (rolling warmup)")

    if df.empty:
        logger.warning(
            "DataFrame пуст после dropna. Вероятная причина: недостаточно данных "
            "для rolling-окон (btc_to_sma200_1h требует >= 4800 1H-баров = ~200 дней)."
            " Минимальный рекомендуемый start_date: за 300+ дней до текущей даты."
        )
        return df, feature_cols

    logger.info(f"Итого: {len(df)} строк, {len(feature_cols)} признаков")
    logger.info(
        f"Период: {df.index[0].date()} - {df.index[-1].date()}"
    )
    logger.info(
        f"Баланс target_1h : "
        f"{df['target_1h'].value_counts().sort_index().to_dict()}"
    )
    logger.info(
        f"Баланс target_6h : "
        f"{df['target_6h'].value_counts().sort_index().to_dict()}"
    )

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

    logger.info(
        f"Train: {len(train)} строк | "
        f"{train.index[0].date()} - {train.index[-1].date()}"
    )
    logger.info(
        f"Test:  {len(test)} строк  | "
        f"{test.index[0].date()} - {test.index[-1].date()}"
    )

    return X_train, X_test, y_train, y_test


def walk_forward_splits(df, n_folds=5, test_months=3):
    """
    Walk-forward splits с расширяющимся окном трейна.
    Возвращает список кортежей (train_idx, test_idx).
    test_size рассчитывается в барах на основе фактического диапазона дат.
    """
    splits = []
    total  = len(df)
    test_size = int(
        total * test_months
        / ((df.index[-1] - df.index[0]).days / 30)
    )
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
    csv_files = sorted(glob.glob("btc_*.csv"))
    if csv_files:
        logger.info(f"Найдены CSV: {csv_files}")
        data = load_from_csv()
    else:
        logger.info("CSV не найдены, запускаем data_loader")
        from data_loader import load_all, START_DATE
        data = load_all(start_date=START_DATE)

    df, feature_cols = build_features(
        data, threshold_1h=0.008, threshold_6h=0.02
    )

    df.to_csv("features_dataset.csv")
    logger.info("Сохранено в features_dataset.csv")

    print(f"\nИтого признаков: {len(feature_cols)}")
    print("\nСписок признаков:")
    for i, f in enumerate(feature_cols, 1):
        print(f"  {i:3d}. {f}") 