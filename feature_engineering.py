"""
feature_engineering.py — Построение признаков для классификации скачков BTC.

Принимает на вход dict из data_loader.load_all().
Мастер-индекс: 1H свечи (candles_1h).
Возвращает готовый DataFrame с признаками и таргетами.

Таргеты строятся тройным барьером (Triple Barrier Method).
Параметры таргетов настраиваются независимо для каждого горизонта:
    - num_bars : вертикальный горизонт в барах
    - span     : период EWMA для волатильности
    - pt_sl    : (тейк-профит, стоп-лосс) в сигмах
    - min_target : минимальная прогнозная волатильность
"""

from __future__ import annotations

import glob
import logging
import os
import re
import warnings
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


WARMUP_DAYS = 210
RECOMMENDED_DAYS_1H = 90
RECOMMENDED_DAYS_6H = 365

FORCE_DROP_6H: List[str] = [
    "ret_lag_1",
    "ret_lag_2",
    "ret_lag_3",
    "rsi_lag_1",
    "rsi_lag_2",
    "rsi_lag_3",
    "funding_lag_8h",
    "funding_lag_16h",
    "session_asia",
    "session_europe",
    "session_ny",
    "session_overlap",
    "dayofweek",
]

_PRICE_RE = re.compile(
    r"close|open|high|low|sma|ratio|vix|yield|spread|gold|oil|tlt|ief"
    r"|sp500|nasdaq|dxy|btc_spy|open_interest|fear_greed|turnover|volume"
    r"|move_index"
)
_PCT_RE = re.compile(
    r"pct|log_return|volatility|funding_rate|risk_regime|ls_ratio"
    r"|ls_buy|ls_sell|move_pct|news"
)
_CORR_RE = re.compile(r"corr")

# Тройной барьер

def ewma_volatility(close: pd.Series, span: int = 72) -> pd.Series:
    """Экспоненциально взвешенная волатильность лог-доходностей."""
    log_ret = np.log(close).diff()
    return log_ret.ewm(span=span).std()


def barrier_target(
    close: pd.Series, num_bars: int, span: int = 72
) -> pd.Series:
    """Прогнозная волатильность на горизонт num_bars баров."""
    vol = ewma_volatility(close, span=span)
    return vol * np.sqrt(num_bars)


def get_events(
    close: pd.Series,
    events_index: pd.Index,
    num_bars: int,
    pt_sl: tuple[float, float],
    target: pd.Series,
    min_target: float = 0.0,
    side: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Генерирует события тройного барьера.

    Возвращает DataFrame с колонками:
        t1    : время выхода (индекс бара)
        trgt  : уровень барьера
        side  : направление ставки (по умолчанию 1)
        ret   : реализованная доходность
        touch : -1 (нижний), 0 (вертикальный), +1 (верхний)
    """
    bar_index = close.index
    close_values = close.to_numpy(dtype=float)
    target = target.reindex(events_index).dropna()
    target = target[target > min_target]
    idx = target.index
    start_pos = bar_index.get_indexer(idx)
    keep = start_pos >= 0
    idx = idx[keep]
    start_pos = start_pos[keep]
    target = target.loc[idx]

    if side is None:
        side_arr = np.ones(len(idx))
        pt_mult = sl_mult = pt_sl[0]
    else:
        side_arr = side.reindex(idx).to_numpy(dtype=float)
        pt_mult, sl_mult = pt_sl

    trgt_arr = target.to_numpy(dtype=float)
    n_bars = bar_index.shape[0]
    t1_pos = np.empty(len(idx), dtype=int)
    touch = np.zeros(len(idx), dtype=int)

    for i, sp in enumerate(start_pos):
        end = min(sp + num_bars, n_bars - 1)
        entry = close_values[sp]
        s = side_arr[i]
        pt = pt_mult * trgt_arr[i]
        sl = sl_mult * trgt_arr[i]
        hit = end
        kind = 0
        for j in range(sp + 1, end + 1):
            ret = (close_values[j] / entry - 1.0) * s
            if pt_mult > 0 and ret >= pt:
                hit, kind = j, 1
                break
            if sl_mult > 0 and ret <= -sl:
                hit, kind = j, -1
                break
        t1_pos[i] = hit
        touch[i] = kind

    events = pd.DataFrame(index=idx)
    events["t1"] = bar_index[t1_pos]
    events["trgt"] = trgt_arr
    events["side"] = side_arr
    events["ret"] = close_values[t1_pos] / close_values[start_pos] - 1.0
    events["touch"] = touch
    return events


def primary_labels(events: pd.DataFrame) -> pd.Series:
    """Знак доходности (только для ненулевых)."""
    labels = np.sign(events["ret"])
    labels = labels[labels != 0]
    return labels.astype(int)


def meta_labels(events: pd.DataFrame, side: pd.Series) -> pd.Series:
    """Прибыльность сделки с учётом стороны."""
    idx = side.dropna().index.intersection(events.index)
    profit = side.loc[idx] * events.loc[idx, "ret"] > 0
    return profit.astype(int)


def num_concurrent_events(
    bar_index: pd.Index, events: pd.DataFrame
) -> pd.Series:
    """Количество одновременных событий для каждого бара."""
    count = pd.Series(0, index=bar_index)
    start_pos = bar_index.get_indexer(events.index)
    end_pos = bar_index.get_indexer(pd.DatetimeIndex(events["t1"]))
    for sp, ep in zip(start_pos, end_pos):
        if sp < 0 or ep < 0:
            continue
        count.iloc[sp : ep + 1] += 1
    return count


def average_uniqueness(
    bar_index: pd.Index, events: pd.DataFrame
) -> pd.Series:
    """Средняя уникальность каждого события."""
    count = num_concurrent_events(bar_index, events).to_numpy(dtype=float)
    inv = np.divide(1.0, count, out=np.zeros_like(count), where=count > 0)
    start_pos = bar_index.get_indexer(events.index)
    end_pos = bar_index.get_indexer(pd.DatetimeIndex(events["t1"]))
    weight = np.ones(len(events))
    for i, (sp, ep) in enumerate(zip(start_pos, end_pos)):
        if sp < 0 or ep < 0 or ep < sp:
            continue
        segment = inv[sp : ep + 1]
        if segment.size:
            weight[i] = segment.mean()
    return pd.Series(weight, index=events.index)


# Технические индикаторы
def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index."""
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period, min_periods=1).mean()
    loss = (-delta.clip(upper=0)).rolling(period, min_periods=1).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential Moving Average."""
    return series.ewm(span=span, adjust=False).mean()


def _atr(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14
) -> pd.Series:
    """Average True Range."""
    prev_close = close.shift(1)
    tr = np.maximum(
        np.maximum(high - low, (high - prev_close).abs()),
        (low - prev_close).abs(),
    )
    return pd.Series(tr, index=close.index).rolling(
        period, min_periods=1
    ).mean()


def _zscore(
    series: pd.Series, window: int, min_periods: int = 1
) -> pd.Series:
    """Z-score нормализация с rolling-окном."""
    mean = series.rolling(window, min_periods=min_periods).mean()
    std = (
        series.rolling(window, min_periods=min_periods)
        .std()
        .replace(0, np.nan)
    )
    return ((series - mean) / std).fillna(0)


# Объединение источников

def _merge_sources(data: dict) -> pd.DataFrame:
    """Объединяет все источники на мастер-индексе 1H."""
    candles = data.get("candles_1h", pd.DataFrame())
    if candles.empty:
        raise ValueError("candles_1h пуст — нет мастер-индекса")

    df = candles.copy()
    df.columns = [f"c_{c.lower()}" for c in df.columns]

    sources = {
        "d": data.get("bybit_daily", pd.DataFrame()),
        "m": data.get("macro", pd.DataFrame()),
        "fg": data.get("fear_greed", pd.DataFrame()),
        "oi": data.get("open_interest", pd.DataFrame()),
        "fr": data.get("funding_rate", pd.DataFrame()),
        "ls": data.get("long_short", pd.DataFrame()),
    }

    for prefix, src in sources.items():
        if src.empty:
            logger.warning("Источник '%s' пуст, пропускаем", prefix)
            continue
        src = src.copy()
        src.columns = [f"{prefix}_{c}" for c in src.columns]
        df = df.join(src, how="left")

    sentiment = data.get("sentiment", pd.DataFrame())
    if not sentiment.empty:
        df = df.join(sentiment, how="left")
    else:
        logger.warning("Источник 'sentiment' пуст, пропускаем")

    logger.info(
        "Объединено: %d строк, %d исходных колонок",
        len(df),
        len(df.columns),
    )
    return df


def _handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Заполнение пропусков по типу колонки."""
    for col in df.columns:
        col_l = col.lower()
        if _CORR_RE.search(col_l):
            df[col] = df[col].ffill().fillna(0)
        elif _PCT_RE.search(col_l):
            df[col] = df[col].fillna(0)
        elif _PRICE_RE.search(col_l):
            df[col] = df[col].ffill()

    df = df.dropna(subset=["c_close"])

    remaining_nan_cols = [c for c in df.columns if df[c].isna().any()]
    if remaining_nan_cols:
        for col in remaining_nan_cols:
            df[col] = df[col].ffill().fillna(0)
        logger.info(
            "Финальный fillna: обработано %d колонок с NaN: %s",
            len(remaining_nan_cols),
            remaining_nan_cols,
        )

    missing_pct = df.isnull().mean() * 100
    high_missing = missing_pct[missing_pct > 5]
    if not high_missing.empty:
        logger.warning(
            "Колонки с >5%% пропусков после fillna: %s",
            high_missing.round(1).to_dict(),
        )
    else:
        logger.info("Пропуски обработаны, критических колонок нет")

    logger.info("После обработки пропусков: %d строк", len(df))
    return df


def _add_technical(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет технические индикаторы."""
    close = df["c_close"]
    high = df["c_high"]
    low = df["c_low"]

    for p in [14, 24, 48]:
        df[f"rsi_{p}"] = _rsi(close, p)

    macd_line = _ema(close, 12) - _ema(close, 26)
    macd_signal = _ema(macd_line, 9)
    df["macd"] = macd_line
    df["macd_hist"] = macd_line - macd_signal

    sma24 = close.rolling(24, min_periods=1).mean()
    std24 = close.rolling(24, min_periods=1).std()
    bb_upper = sma24 + 2 * std24
    bb_lower = sma24 - 2 * std24
    bb_width = (bb_upper - bb_lower) / sma24.replace(0, np.nan)
    df["bb_pos"] = (
        (close - bb_lower) / (bb_upper - bb_lower).replace(0, np.nan)
    ).fillna(0.5)
    df["bb_width"] = bb_width.fillna(0)
    df["bb_squeeze"] = (
        bb_width < bb_width.rolling(168, min_periods=1).mean()
    ).astype(int)

    atr14 = _atr(high, low, close, 14)
    df["atr_pct"] = atr14 / close
    df["atr_ratio"] = (
        atr14 / atr14.rolling(168, min_periods=1).mean().replace(0, np.nan)
    ).fillna(1.0)

    bars = {"1d": 24, "4d": 96, "7d": 168, "28d": 672}
    for label, w in bars.items():
        df[f"sma_{label}"] = close.rolling(w, min_periods=1).mean()

    df["price_to_sma_1d"] = close / df["sma_1d"]
    df["price_to_sma_4d"] = close / df["sma_4d"]
    df["price_to_sma_7d"] = close / df["sma_7d"]
    df["price_to_sma_28d"] = close / df["sma_28d"]
    df["sma_7d_to_28d"] = df["sma_7d"] / df["sma_28d"]

    rsi24 = df["rsi_24"]
    rsi_min = rsi24.rolling(24, min_periods=1).min()
    rsi_max = rsi24.rolling(24, min_periods=1).max()
    df["stoch_rsi"] = (
        (rsi24 - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan)
    ).fillna(0.5)

    log_ret = np.log(close / close.shift(1))
    df["volatility_1d"] = log_ret.rolling(24, min_periods=1).std()
    df["volatility_7d"] = log_ret.rolling(168, min_periods=1).std()
    df["volatility_28d"] = log_ret.rolling(672, min_periods=1).std()
    df["volatility_regime"] = (
        df["volatility_7d"] / df["volatility_28d"].replace(0, np.nan)
    ).fillna(1.0)

    df = df.drop(columns=["sma_1d", "sma_4d", "sma_7d", "sma_28d"])
    logger.info("Технические индикаторы добавлены")
    return df


def _add_volume_features(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет признаки объёма, VWAP и taker-volume."""
    vol = df["c_volume"]
    close = df["c_close"]

    vol_mean = vol.rolling(168, min_periods=1).mean()
    df["volume_ratio"] = (vol / vol_mean.replace(0, np.nan)).fillna(1.0)
    df["volume_spike"] = (df["volume_ratio"] > 2.0).astype(int)

    vwap = (
        (close * vol).rolling(24, min_periods=1).sum()
        / vol.rolling(24, min_periods=1).sum().replace(0, np.nan)
    )
    df["vwap_dist"] = ((close - vwap) / vwap.replace(0, np.nan)).fillna(0)

    price_dir = np.sign(close.pct_change())
    vol_dir = np.sign(vol.pct_change())
    df["vol_price_divergence"] = (price_dir != vol_dir).astype(int)

    if "d_turnover" in df.columns:
        d_turn_mean = df["d_turnover"].rolling(30, min_periods=1).mean()
        df["d_turnover_ratio"] = (
            df["d_turnover"] / d_turn_mean.replace(0, np.nan)
        ).fillna(1.0)
        df["d_turnover_ratio_lag1"] = df["d_turnover_ratio"].shift(1)
        df["d_turnover_ratio_ma3"] = (
            df["d_turnover_ratio"].rolling(3, min_periods=1).mean()
        )

    if "c_taker_buy_vol" in df.columns and "c_taker_sell_vol" in df.columns:
        buy = df["c_taker_buy_vol"]
        sell = df["c_taker_sell_vol"]
        df["taker_buy_zscore"] = _zscore(buy, 168)
        df["taker_sell_zscore"] = _zscore(sell, 168)
        total_taker = buy + sell
        df["taker_vol_imbalance"] = (
            (buy - sell) / total_taker.replace(0, np.nan)
        ).fillna(0)

    if "c_taker_vol_ratio" in df.columns:
        df["taker_vol_ratio_ma"] = (
            df["c_taker_vol_ratio"].rolling(24, min_periods=1).mean()
        )
        df["taker_vol_ratio_change"] = (
            df["c_taker_vol_ratio"].diff(6).fillna(0)
        )

    logger.info("Признаки объёма и taker volume добавлены")
    return df


def _add_candle_features(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет признаки свечных паттернов."""
    open_ = df["c_open"]
    high = df["c_high"]
    low = df["c_low"]
    close = df["c_close"]

    body = close - open_
    body_abs = body.abs()
    candle_range = (high - low).replace(0, np.nan)

    df["body_ratio"] = (body_abs / candle_range).fillna(0)
    df["body_dir"] = np.sign(body)

    oc_max = np.maximum(open_, close)
    oc_min = np.minimum(open_, close)
    df["upper_wick_ratio"] = ((high - oc_max) / candle_range).fillna(0)
    df["lower_wick_ratio"] = ((oc_min - low) / candle_range).fillna(0)

    df["doji"] = (df["body_ratio"] < 0.1).astype(int)
    df["hl_pct"] = ((high - low) / close).fillna(0)

    logger.info("Свечные паттерны добавлены")
    return df


def _add_macro_features(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет макро-признаки (лаг учтён в data_loader)."""
    close = df["c_close"]

    if "m_btc_sma_90d" in df.columns:
        df["btc_to_macro_sma90"] = (
            close / df["m_btc_sma_90d"].replace(0, np.nan)
        ).fillna(1.0)

    ema200_1h = close.ewm(span=4800, min_periods=1).mean()
    df["btc_to_ema200_1h"] = close / ema200_1h

    if "m_vix_close" in df.columns:
        df["vix_high"] = (df["m_vix_close"] > 30).astype(int)

    if "fg_fear_greed_index" in df.columns:
        fg = df["fg_fear_greed_index"]
        df["fg_extreme_fear"] = (fg < 25).astype(int)
        df["fg_fear"] = ((fg >= 25) & (fg < 45)).astype(int)
        df["fg_greed"] = ((fg >= 55) & (fg < 75)).astype(int)

    if "m_tlt_ief_spread" in df.columns:
        df["yield_spread_change"] = (
            df["m_tlt_ief_spread"].diff(24).fillna(0)
        )

    if "m_treasury_yield_10y" in df.columns:
        y = df["m_treasury_yield_10y"]
        df["yield_vs_mean"] = (
            y - y.rolling(1440, min_periods=24).mean()
        ).fillna(0)

    logger.info("Макро признаки добавлены (lag учтён в data_loader)")
    return df


def _add_futures_features(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет признаки Open Interest, Funding Rate, Long/Short."""
    if "oi_open_interest" in df.columns:
        oi = df["oi_open_interest"]
        df["oi_change_pct"] = oi.pct_change().fillna(0) * 100
        df["oi_change_6bar"] = oi.pct_change(6).fillna(0) * 100
        oi_mean = oi.rolling(168, min_periods=1).mean()
        df["oi_ratio"] = (oi / oi_mean.replace(0, np.nan)).fillna(1.0)

        price_dir = np.sign(df["c_close"].pct_change())
        oi_dir = np.sign(df["oi_change_pct"])
        df["oi_price_divergence"] = (price_dir != oi_dir).astype(int)

    if "fr_funding_rate" in df.columns:
        fr = df["fr_funding_rate"].fillna(0)
        df["funding_rate_abs"] = fr.abs()
        df["funding_change"] = fr.diff(8).fillna(0)

    if "ls_buy_ratio" in df.columns:
        br = df["ls_buy_ratio"].fillna(0.5)
        df["ls_buy_ratio"] = br
        df["ls_long_dominant"] = (br > 0.65).astype(int)
        if "ls_ls_ratio" in df.columns:
            ls = df["ls_ls_ratio"].fillna(1)
            df["ls_ratio_change"] = ls.diff().fillna(0)
            df["ls_ratio_vs_mean"] = (
                ls
                / ls.rolling(42, min_periods=1).mean().replace(0, np.nan)
            ).fillna(1.0)
        else:
            logger.warning(
                "ls_ls_ratio не найден, пропускаем ratio-признаки"
            )
    else:
        logger.warning(
            "Long/Short Ratio: данные отсутствуют (ls_buy_ratio не найден)"
        )

    logger.info("Фьючерсные признаки добавлены (OI + FR + L/S)")
    return df


def _add_lags(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет лаговые признаки."""
    log_ret = np.log(df["c_close"] / df["c_close"].shift(1)).fillna(0)

    for lag in [1, 2, 3, 6, 12, 24]:
        df[f"ret_lag_{lag}"] = log_ret.shift(lag).fillna(0)

    if "fr_funding_rate" in df.columns:
        for lag in [8, 16]:
            df[f"funding_lag_{lag}h"] = (
                df["fr_funding_rate"].fillna(0).shift(lag).fillna(0)
            )

    for lag in [1, 2, 3]:
        if "rsi_24" in df.columns:
            df[f"rsi_lag_{lag}"] = (
                df["rsi_24"].shift(lag).fillna(50.0)
            )

    df["cum_ret_1d"] = (
        log_ret.rolling(24, min_periods=1).sum().shift(1).fillna(0)
    )
    df["cum_ret_7d"] = (
        log_ret.rolling(168, min_periods=1).sum().shift(1).fillna(0)
    )

    logger.info("Лаговые признаки добавлены")
    return df


def _add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет временные признаки (час, день недели, сессии)."""
    idx = df.index

    df["hour"] = idx.hour
    df["dayofweek"] = idx.dayofweek
    df["is_weekend"] = (idx.dayofweek >= 5).astype(int)
    df["month"] = idx.month

    df["session_asia"] = ((idx.hour >= 0) & (idx.hour < 8)).astype(int)
    df["session_europe"] = ((idx.hour >= 7) & (idx.hour < 16)).astype(int)
    df["session_ny"] = ((idx.hour >= 13) & (idx.hour < 22)).astype(int)
    df["session_overlap"] = (
        (idx.hour >= 13) & (idx.hour < 16)
    ).astype(int)

    logger.info("Временные признаки добавлены")
    return df


def _add_derived_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет производные моментум-признаки."""
    if "rsi_24" in df.columns:
        df["rsi_velocity"] = df["rsi_24"].diff(6).fillna(0)
        df["rsi_acceleration"] = df["rsi_velocity"].diff(6).fillna(0)

    if "fr_funding_rate" in df.columns and "oi_change_pct" in df.columns:
        fr = df["fr_funding_rate"].fillna(0)
        oi = df["oi_change_pct"].fillna(0)
        df["funding_oi_combo"] = fr * oi

    if "volatility_1d" in df.columns and "c_close" in df.columns:
        log_ret = np.log(df["c_close"] / df["c_close"].shift(1))
        vol_28d_pure = (
            log_ret.shift(24).rolling(672, min_periods=48).std()
        )
        vol_norm = vol_28d_pure.replace(0, np.nan)
        df["vol_z_score"] = (
            (df["volatility_1d"] - vol_28d_pure) / vol_norm
        ).fillna(0)

    if "price_to_sma_7d" in df.columns and "volatility_7d" in df.columns:
        vol7 = df["volatility_7d"].replace(0, np.nan)
        df["trend_strength"] = (
            (df["price_to_sma_7d"] - 1).abs() / vol7
        ).fillna(0)

    if "fg_fear_greed_index" in df.columns:
        df["fg_reversal_signal"] = (
            df["fg_fear_greed_index"].diff(24).fillna(0)
        )

    if "volume_ratio" in df.columns and "ret_lag_1" in df.columns:
        df["vol_price_confirm"] = (
            df["volume_ratio"] * df["ret_lag_1"].abs()
        ).fillna(0)

    if "ls_buy_ratio" in df.columns:
        df["ls_acceleration"] = df["ls_buy_ratio"].diff(6).fillna(0)

    if "ret_lag_1" in df.columns:
        log_ret = df["ret_lag_1"]
        ret_std = (
            log_ret.shift(1).rolling(672, min_periods=24).std()
        )
        big_move = (
            (log_ret.abs() > 2 * ret_std).astype(int).fillna(0)
        )
        grp = big_move.groupby(big_move.cumsum())
        df["bars_since_big_move"] = (
            grp.cumcount().where(big_move == 0, 0).clip(upper=672)
        )

    logger.info("Производные моментум-признаки добавлены")
    return df


def _final_fillna(df: pd.DataFrame) -> pd.DataFrame:
    """Финальный проход заполнения NaN перед таргетами."""
    skip_cols = {
        "c_close",
        "target_1h",
        "target_6h",
        "fwd_ret_1h",
        "fwd_ret_6h",
    }
    nan_before = df.isnull().sum().sum()

    for col in df.columns:
        if col in skip_cols:
            continue
        if df[col].isna().any():
            df[col] = df[col].ffill().fillna(0)

    nan_after = df.isnull().sum().sum()
    if nan_before > 0:
        logger.info(
            "_final_fillna: устранено %d NaN (осталось %d)",
            nan_before - nan_after,
            nan_after,
        )
    return df


# Новые таргеты через тройной барьер (независимые параметры)

def _compute_labels_for_horizon(
    close: pd.Series,
    num_bars: int,
    span: int,
    pt_sl: Tuple[float, float],
    min_target: float,
) -> pd.DataFrame:
    """Рассчитывает метки и доходности для заданного горизонта."""
    target_vol = barrier_target(close, num_bars, span=span)
    n = len(close)
    idx_all = close.index
    touch = pd.Series(0, index=idx_all, dtype=int)
    ret = pd.Series(np.nan, index=idx_all, dtype=float)

    valid_mask = np.arange(n) + num_bars < n
    valid_idx = idx_all[valid_mask]
    ret.loc[valid_idx] = (
        close.shift(-num_bars).loc[valid_idx] / close.loc[valid_idx] - 1.0
    )

    event_candidates = valid_idx[target_vol.loc[valid_idx] > min_target]
    if len(event_candidates) > 0:
        events = get_events(
            close,
            event_candidates,
            num_bars,
            pt_sl,
            target_vol,
            min_target=0.0,
            side=None,
        )
        touch.loc[events.index] = events["touch"].astype(int)
        ret.loc[events.index] = events["ret"]

    return pd.DataFrame({"touch": touch, "ret": ret})


def _add_triple_barrier_targets(
    df: pd.DataFrame,
    num_bars_1h: int = 1,
    span_1h: int = 72,
    pt_sl_1h: Tuple[float, float] = (1.0, 1.0),
    min_target_1h: float = 0.0,
    num_bars_6h: int = 6,
    span_6h: int = 72,
    pt_sl_6h: Tuple[float, float] = (1.0, 1.0),
    min_target_6h: float = 0.0,
) -> pd.DataFrame:
    """Добавляет таргеты для 1H и 6H с независимыми параметрами."""
    close = df["c_close"]
    labels_1h = _compute_labels_for_horizon(
        close, num_bars_1h, span_1h, pt_sl_1h, min_target_1h
    )
    labels_6h = _compute_labels_for_horizon(
        close, num_bars_6h, span_6h, pt_sl_6h, min_target_6h
    )

    df["target_1h"] = labels_1h["touch"]
    df["fwd_ret_1h"] = labels_1h["ret"]
    df["target_6h"] = labels_6h["touch"]
    df["fwd_ret_6h"] = labels_6h["ret"]

    df = df.dropna(
        subset=["target_1h", "target_6h", "fwd_ret_1h", "fwd_ret_6h"]
    )

    logger.info(
        "Таргеты 1H (тройной барьер): %s",
        df["target_1h"].value_counts().sort_index().to_dict(),
    )
    logger.info(
        "Таргеты 6H (тройной барьер): %s",
        df["target_6h"].value_counts().sort_index().to_dict(),
    )
    return df


def _add_price_context(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет стационарные ценовые производные."""
    if "c_close" not in df.columns:
        logger.warning("_add_price_context: нет c_close — шаг пропущен")
        return df

    close = df["c_close"]
    win_90 = min(2160, max(60, len(df) // 8))
    win_200 = min(4800, max(120, len(df) // 4))

    df["price_log"] = np.log(close.clip(lower=1e-9))

    sma_90 = close.rolling(win_90, min_periods=win_90 // 4).mean()
    std_90 = close.rolling(win_90, min_periods=win_90 // 4).std()
    df["price_zscore_90d"] = (
        (close - sma_90) / std_90
    ).replace([np.inf, -np.inf], np.nan)

    df["price_rank_90d"] = (
        close.rolling(win_90, min_periods=win_90 // 4).rank(pct=True)
    )

    sma_200 = close.rolling(win_200, min_periods=win_200 // 4).mean()
    df["price_vs_sma_200d"] = (close / sma_200 - 1.0).replace(
        [np.inf, -np.inf], np.nan
    )

    n_added = sum(
        1
        for c in [
            "price_log",
            "price_zscore_90d",
            "price_rank_90d",
            "price_vs_sma_200d",
        ]
        if c in df.columns
    )
    logger.info(
        "_add_price_context: добавлено %d price_* колонок", n_added
    )
    return df


def _drop_leakage(df: pd.DataFrame) -> pd.DataFrame:
    """Удаляет абсолютные цены, дубликаты и нулевые-сигнальные колонки."""
    absolute_price_cols = [
        "c_open",
        "c_high",
        "c_low",
        "c_volume",
        "c_turnover",
        "c_taker_buy_vol",
        "c_taker_sell_vol",
        "m_btc_close",
        "m_sp500_close",
        "m_dxy_close",
        "m_gold_close",
        "m_tlt_close",
        "m_ief_close",
        "m_oil_close",
        "m_btc_sma_7d",
        "m_btc_sma_30d",
        "m_btc_sma_90d",
        "m_btc_sma_200d",
        "m_btc_spy_ratio",
        "d_open",
        "d_high",
        "d_low",
        "d_close",
        "d_volume",
        "oi_open_interest",
        "ls_sell_ratio",
        "ls_ls_ratio",
    ]
    confirmed_duplicates = [
        "m_btc_log_return",
        "macd_signal",
    ]
    legacy_cols = [
        "move_z_score",
        "move_vix_spread",
        "ls_short_dominant",
        "candle_strength",
        "vwap_dist_tmp",
    ]
    zero_signal_cols = [
        "m_btc_spy_corr_30d",
        "m_btc_spy_corr_90d",
        "m_btc_dxy_corr_30d",
        "m_btc_dxy_corr_90d",
        "m_btc_volatility_30d",
        "d_turnover",
        "vix_medium",
    ]

    all_to_drop = (
        absolute_price_cols
        + confirmed_duplicates
        + legacy_cols
        + zero_signal_cols
    )
    to_drop = [c for c in all_to_drop if c in df.columns]
    df = df.drop(columns=to_drop)

    n_price = len([c for c in absolute_price_cols if c in to_drop])
    n_dupes = len(
        [c for c in confirmed_duplicates + legacy_cols if c in to_drop]
    )
    n_zero = len([c for c in zero_signal_cols if c in to_drop])
    logger.info(
        "Удалено %d колонок: %d абсолютных цен, %d дубликатов, "
        "%d нулевого сигнала",
        len(to_drop),
        n_price,
        n_dupes,
        n_zero,
    )
    return df


def _trim_warmup(df: pd.DataFrame, warmup_bars: int) -> pd.DataFrame:
    """Отбрасывает первые warmup_bars (rolling-окна ещё накапливаются)."""
    if len(df) > warmup_bars + 100:
        df = df.iloc[warmup_bars:].copy()
        logger.info("Отброшен warmup: %d баров", warmup_bars)
    else:
        logger.warning(
            "Warmup cut пропущен: данных слишком мало (%d <= %d)",
            len(df),
            warmup_bars + 100,
        )
    return df


# Главная функция построения признаков

def build_features(
    data: dict,
    num_bars_1h: int = 1,
    span_1h: int = 72,
    pt_sl_1h: Tuple[float, float] = (1.0, 1.0),
    min_target_1h: float = 0.0,
    num_bars_6h: int = 6,
    span_6h: int = 72,
    pt_sl_6h: Tuple[float, float] = (1.0, 1.0),
    min_target_6h: float = 0.0,
) -> Tuple[pd.DataFrame, List[str]]:
    """Строит датасет признаков и таргетов из словаря источников."""
    df = _merge_sources(data)
    df = _handle_missing(df)
    df = _add_technical(df)
    df = _add_volume_features(df)
    df = _add_candle_features(df)
    df = _add_macro_features(df)
    df = _add_futures_features(df)
    df = _add_lags(df)
    df = _add_time_features(df)
    df = _add_derived_momentum(df)
    df = _final_fillna(df)
    df = _add_triple_barrier_targets(
        df,
        num_bars_1h=num_bars_1h,
        span_1h=span_1h,
        pt_sl_1h=pt_sl_1h,
        min_target_1h=min_target_1h,
        num_bars_6h=num_bars_6h,
        span_6h=span_6h,
        pt_sl_6h=pt_sl_6h,
        min_target_6h=min_target_6h,
    )
    df = _add_price_context(df)
    df = _drop_leakage(df)

    feature_cols = [
        c
        for c in df.columns
        if c
        not in ("target_1h", "target_6h", "fwd_ret_1h", "fwd_ret_6h")
    ]
    logger.info(
        "build_features: %d строк, %d признаков",
        len(df),
        len(feature_cols),
    )
    return df, feature_cols


# CSV-утилиты

def check_csv_files(csv_dir: str = ".", prefix: str = "btc") -> dict:
    """Ищет сохранённые CSV-файлы по ключам источников."""
    source_keys = [
        "candles_1h",
        "bybit_daily",
        "macro",
        "fear_greed",
        "open_interest",
        "funding_rate",
        "long_short",
        "sentiment",
    ]
    result = {}
    logger.info("Поиск CSV-файлов в '%s' (prefix='%s')", csv_dir, prefix)
    for key in source_keys:
        pattern = os.path.join(csv_dir, f"{prefix}_{key}_*.csv")
        matches = sorted(glob.glob(pattern))
        if matches:
            result[key] = matches[-1]
            logger.info(
                "  OK %-20s -> %s", key, os.path.basename(matches[-1])
            )
        else:
            result[key] = None
            logger.warning(
                "  MISS %-20s -> не найден (%s)", key, pattern
            )

    found = [k for k, v in result.items() if v is not None]
    missing = [k for k, v in result.items() if v is None]
    logger.info(
        "Итог: найдено %d/%d файлов%s",
        len(found),
        len(source_keys),
        (f", отсутствуют: {missing}" if missing else ""),
    )
    return result


def load_from_csv(csv_dir: str = ".", prefix: str = "btc") -> dict:
    """Загружает все источники из CSV-файлов."""
    csv_map = check_csv_files(csv_dir, prefix)
    data = {}
    for key, filepath in csv_map.items():
        if filepath is None:
            data[key] = pd.DataFrame()
            continue
        try:
            df = pd.read_csv(filepath, index_col=0, parse_dates=True)
            if hasattr(df.index, "tz") and df.index.tz is not None:
                df.index = df.index.tz_convert(None)
            data[key] = df
            logger.info("Загружен %-20s: %d строк", key, len(df))
        except Exception as exc:
            logger.error("Ошибка загрузки %s: %s", filepath, exc)
            data[key] = pd.DataFrame()
    return data


def _api_load(start_date: Optional[str] = None) -> dict:
    """Загружает данные через data_loader.load_all."""
    from data_loader import load_all  # noqa: PLC0415

    if start_date is None:
        start_date = (
            datetime.now()
            - timedelta(days=WARMUP_DAYS + RECOMMENDED_DAYS_6H)
        ).strftime("%Y-%m-%d")
    logger.info("API-загрузка данных с %s ...", start_date)
    return load_all(start_date=start_date)


def smart_load_data(
    csv_dir: str = ".",
    prefix: str = "btc",
    start_date: Optional[str] = None,
    force_api: bool = False,
) -> dict:
    """Умная загрузка: CSV если есть, иначе API."""
    if force_api:
        logger.info("smart_load_data: force_api=True -> API")
        return _api_load(start_date)

    csv_map = check_csv_files(csv_dir, prefix)
    candles_ok = csv_map.get("candles_1h") is not None
    missing = [k for k, v in csv_map.items() if v is None]

    if candles_ok:
        if missing:
            logger.warning(
                "smart_load_data: candles_1h найден, отсутствуют: %s. "
                "Загружаем из CSV.",
                missing,
            )
        else:
            logger.info(
                "smart_load_data: все CSV найдены -> load_from_csv()"
            )
        return load_from_csv(csv_dir, prefix)

    logger.warning(
        "smart_load_data: candles_1h не найден -> API-загрузка"
    )
    if start_date is None:
        start_date = (
            datetime.now()
            - timedelta(days=WARMUP_DAYS + RECOMMENDED_DAYS_6H)
        ).strftime("%Y-%m-%d")
        logger.info("start_date не задан, дефолт: %s", start_date)
    return _api_load(start_date)

# Pipeline для 1H

def build_features_1h(
    data: Optional[dict] = None,
    days: int = RECOMMENDED_DAYS_1H,
    num_bars: int = 1,
    span: int = 72,
    pt_sl: Tuple[float, float] = (1.0, 1.0),
    min_target: float = 0.0,
    csv_dir: str = ".",
    prefix: str = "btc",
    force_api: bool = False,
    warmup_days: int = 60,
) -> Tuple[pd.DataFrame, List[str]]:
    """Полный pipeline для 1H датасета."""
    if data is None:
        total_days = days + warmup_days
        start_date = (
            datetime.now() - timedelta(days=total_days)
        ).strftime("%Y-%m-%d")
        logger.info(
            "1H pipeline: данные с %s (%d + %d = %d дней)",
            start_date,
            days,
            warmup_days,
            total_days,
        )
        data = smart_load_data(
            csv_dir=csv_dir,
            prefix=prefix,
            start_date=start_date,
            force_api=force_api,
        )

    df, feature_cols = build_features(
        data,
        num_bars_1h=num_bars,
        span_1h=span,
        pt_sl_1h=pt_sl,
        min_target_1h=min_target,
    )
    if df.empty:
        logger.error("1H: build_features вернул пустой DataFrame")
        return df, []

    df = _trim_warmup(df, warmup_bars=warmup_days * 24)
    if df.empty:
        logger.error("1H: после warmup cut DataFrame пуст")
        return df, []

    actual_days = (df.index.max() - df.index.min()).days
    if actual_days < days * 0.9:
        logger.warning(
            "1H: запрошено %d дней, получено %d.", days, actual_days
        )

    target_and_ret = ["target_1h", "fwd_ret_1h"]
    keep = [
        c for c in feature_cols if c in df.columns
    ] + [c for c in target_and_ret if c in df.columns]
    df = df[keep].copy()
    df = df.rename(
        columns={"target_1h": "target", "fwd_ret_1h": "fwd_ret"}
    )
    feature_cols_out = [
        c for c in df.columns if c not in ("target", "fwd_ret")
    ]

    logger.info(
        "1H датасет: %d строк, %d признаков | %s - %s (%d дней)",
        len(df),
        len(feature_cols_out),
        df.index[0].date(),
        df.index[-1].date(),
        actual_days,
    )
    logger.info(
        "  target balance: %s",
        df["target"].value_counts().sort_index().to_dict(),
    )
    return df, feature_cols_out

# Pipeline для 6H

def build_features_6h(
    data: Optional[dict] = None,
    days: int = RECOMMENDED_DAYS_6H,
    num_bars: int = 6,
    span: int = 72,
    pt_sl: Tuple[float, float] = (1.0, 1.0),
    min_target: float = 0.0,
    csv_dir: str = ".",
    prefix: str = "btc",
    force_api: bool = False,
    warmup_days: int = WARMUP_DAYS,  # 210 дней, как и было
) -> Tuple[pd.DataFrame, List[str]]:
    """Полный pipeline для 6H датасета."""
    if data is None:
        total_days = days + warmup_days
        start_date = (
            datetime.now() - timedelta(days=total_days)
        ).strftime("%Y-%m-%d")
        logger.info(
            "6H pipeline: данные с %s (%d + %d = %d дней)",
            start_date,
            days,
            warmup_days,
            total_days,
        )
        data = smart_load_data(
            csv_dir=csv_dir,
            prefix=prefix,
            start_date=start_date,
            force_api=force_api,
        )

    df, feature_cols = build_features(
        data,
        num_bars_6h=num_bars,
        span_6h=span,
        pt_sl_6h=pt_sl,
        min_target_6h=min_target,
    )
    if df.empty:
        logger.error("6H: build_features вернул пустой DataFrame")
        return df, []

    df = _trim_warmup(df, warmup_bars=warmup_days * 24)
    if df.empty:
        logger.error("6H: после warmup cut DataFrame пуст")
        return df, []

    actual_days = (df.index.max() - df.index.min()).days
    if actual_days < days * 0.9:
        logger.warning(
            "6H: запрошено %d дней, получено %d.", days, actual_days
        )

    target_and_ret = ["target_6h", "fwd_ret_6h"]
    keep = [
        c for c in feature_cols if c in df.columns
    ] + [c for c in target_and_ret if c in df.columns]
    df_6h = df[keep].iloc[::6].copy()

    idx_6h = df_6h.index
    df_6h["hour"] = idx_6h.hour
    df_6h["is_weekend"] = (idx_6h.dayofweek >= 5).astype(int)
    df_6h["month"] = idx_6h.month

    df_6h = df_6h.rename(
        columns={"target_6h": "target", "fwd_ret_6h": "fwd_ret"}
    )
    df_6h = df_6h.dropna(subset=["target"])

    if "oi_change_6bar" in df_6h.columns:
        if "oi_change_pct" in df_6h.columns:
            df_6h = df_6h.drop(columns=["oi_change_pct"])
        df_6h = df_6h.rename(
            columns={"oi_change_6bar": "oi_change_pct"}
        )

    if "bars_since_big_move" in df_6h.columns:
        df_6h["bars_since_big_move"] = (
            (df_6h["bars_since_big_move"] / 6)
            .round()
            .astype(int)
            .clip(upper=112)
        )

    to_drop_6h = [c for c in FORCE_DROP_6H if c in df_6h.columns]
    if to_drop_6h:
        df_6h = df_6h.drop(columns=to_drop_6h)
        logger.info(
            "6H: удалены sub-hour/константные колонки: %s",
            to_drop_6h,
        )

    rename_map = {}
    if "ret_lag_6" in df_6h.columns:
        rename_map["ret_lag_6"] = "ret_lag_1_6h"
    if "ret_lag_12" in df_6h.columns:
        rename_map["ret_lag_12"] = "ret_lag_2_6h"
    if "ret_lag_24" in df_6h.columns:
        rename_map["ret_lag_24"] = "ret_lag_4_6h"
    if rename_map:
        df_6h = df_6h.rename(columns=rename_map)

    feature_cols_6h = [
        c for c in df_6h.columns if c not in ("target", "fwd_ret")
    ]
    logger.info(
        "6H датасет: %d строк, %d признаков | %s - %s (%d дней)",
        len(df_6h),
        len(feature_cols_6h),
        df_6h.index[0].date(),
        df_6h.index[-1].date(),
        actual_days,
    )
    logger.info(
        "  target balance: %s",
        df_6h["target"].value_counts().sort_index().to_dict(),
    )
    return df_6h, feature_cols_6h


# Точка входа

if __name__ == "__main__":
    import gc
    import time as _time
    from datetime import timezone

    DAYS_1H = 720
    DAYS_6H = 720
    CSV_DIR = "."
    PREFIX = "btc"
    FORCE_API = True

    _MSK = timezone(timedelta(hours=3))
    total_days = max(DAYS_1H, DAYS_6H) + WARMUP_DAYS
    start_date = (
        datetime.now(_MSK) - timedelta(days=total_days)
    ).strftime("%Y-%m-%d")
    logger.info(
        "Единая загрузка: %d дней (max(%d, %d) + %d warmup)",
        total_days,
        DAYS_1H,
        DAYS_6H,
        WARMUP_DAYS,
    )

    t0 = _time.time()
    data = load_from_csv(csv_dir=CSV_DIR, prefix=PREFIX)
    t_load = _time.time() - t0
    logger.info("Загрузка завершена за %.0f сек", t_load)

    # Настройка тройного баррьера 
    t0 = _time.time()
    df_1h, feat_1h = build_features_1h(
        data=data,
        days=DAYS_1H,
        num_bars=2,
        span=72,
        pt_sl=(2.0, 2.0),
        min_target=0.001,
    )
    t_1h = _time.time() - t0

    t0 = _time.time()
    df_6h, feat_6h = build_features_6h(
        data=data,
        days=DAYS_6H,
        num_bars=4,
        span=210,
        pt_sl=(2.5, 2.5),
        min_target=0.001,
    )
    t_6h = _time.time() - t0

    del data
    gc.collect()

    print("\n" + "=" * 62)
    print("СВОДКА")
    print("=" * 62)
    print(f"Загрузка: {t_load:.0f} сек")

    if not df_1h.empty:
        d1 = (df_1h.index[-1] - df_1h.index[0]).days
        fname_1h = "features_1h.csv"
        df_1h.to_csv(fname_1h)
        logger.info(
            "Сохранено %s | %s — %s",
            fname_1h,
            df_1h.index[0].date(),
            df_1h.index[-1].date(),
        )
        print(
            f"1H датасет:  {len(df_1h):>6} строк | "
            f"{len(feat_1h):>3} признаков | {d1} дней | {t_1h:.0f} сек"
        )
        print(
            f"  Период: {df_1h.index[0].date()} — "
            f"{df_1h.index[-1].date()}"
        )
        print(
            f"  Таргет: "
            f"{df_1h['target'].value_counts().sort_index().to_dict()}"
        )
        print(f"  Файл  : {fname_1h}")
    else:
        print("1H датасет:  ПУСТО")

    print()

    if not df_6h.empty:
        d6 = (df_6h.index[-1] - df_6h.index[0]).days
        fname_6h = "features_6h.csv"
        df_6h.to_csv(fname_6h)
        logger.info(
            "Сохранено %s | %s — %s",
            fname_6h,
            df_6h.index[0].date(),
            df_6h.index[-1].date(),
        )
        print(
            f"6H датасет:  {len(df_6h):>6} строк | "
            f"{len(feat_6h):>3} признаков | {d6} дней | {t_6h:.0f} сек"
        )
        print(
            f"  Период: {df_6h.index[0].date()} — "
            f"{df_6h.index[-1].date()}"
        )
        print(
            f"  Таргет: "
            f"{df_6h['target'].value_counts().sort_index().to_dict()}"
        )
        print(f"  Файл  : {fname_6h}")
    else:
        print("6H датасет:  ПУСТО")

    if not df_1h.empty and not df_6h.empty:
        print(
            f"\nСоотношение строк 1H/6H ~ "
            f"{len(df_1h) / len(df_6h):.1f}:1  (теория 6:1)"
        )

    del df_1h, df_6h, feat_1h, feat_6h
    gc.collect()