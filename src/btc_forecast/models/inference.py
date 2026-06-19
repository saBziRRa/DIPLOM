"""Cascade v9 inference: live_signal and horizon_table modes."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from btc_forecast.config import get_settings
from btc_forecast.models.bundle import ModelBundle, load_bundle


@dataclass
class BarSignal:
    signal: int
    p_move: float
    reg_pred: float
    confidence: float
    timestamp: datetime | None = None


@dataclass
class TimeframeForecast:
    tf: str
    live: BarSignal
    horizon: pd.DataFrame = field(default_factory=pd.DataFrame)
    last_price: float = 0.0
    data_updated_at: datetime | None = None


@dataclass
class ForecastResult:
    price: float
    updated_at: datetime | None
    forecasts: dict[str, TimeframeForecast] = field(default_factory=dict)
    bundle_meta: Any = None

    @property
    def agreement(self) -> str:
        sigs = {tf: f.live.signal for tf, f in self.forecasts.items()}
        active = {k: v for k, v in sigs.items() if v != 0}
        if len(active) == 0:
            return "нет активных сигналов"
        if len(active) == 1:
            only_tf = next(iter(active))
            return f"частичная ({only_tf.upper()} активен)"
        if len(set(active.values())) == 1:
            label = "UP" if active["1h"] == 1 else "DOWN"
            return f"согласованы ({label})"
        return "расхождение (1H и 6H в разные стороны)"


def _find_col(columns: list[str], patterns: list[str]) -> str | None:
    for pat in patterns:
        for col in columns:
            if re.search(pat, col, re.I):
                return col
    return None


def infer_bar_step(index: pd.DatetimeIndex) -> pd.Timedelta:
    deltas = pd.Series(index[-50:]).diff().dropna()
    if deltas.empty:
        raise ValueError("Not enough bars to infer step")
    return pd.Timedelta(deltas.median())


def build_future_frame(
    x_full: pd.DataFrame, n_bars: int, bar_step: pd.Timedelta
) -> pd.DataFrame:
    last_ts = x_full.index[-1]
    fut_idx = pd.date_range(last_ts + bar_step, periods=n_bars, freq=bar_step)
    fut = pd.concat([x_full.iloc[[-1]]] * n_bars, ignore_index=True)
    fut.index = fut_idx
    cols = list(fut.columns)

    fwd = _find_col(cols, [r"^fwd_ret"])
    if fwd:
        fut[fwd] = 0.0

    weekend = _find_col(cols, [r"weekend"])
    if weekend:
        fut[weekend] = (fut_idx.dayofweek >= 5).astype(int)

    sessions = {
        r"session_asia": (0, 8),
        r"session_europe": (7, 16),
        r"session_ny": (13, 22),
        r"session_overlap": (13, 16),
    }
    for pat, (lo, hi) in sessions.items():
        col = _find_col(cols, [pat])
        if col:
            h = fut_idx.hour
            fut[col] = ((h >= lo) & (h < hi)).astype(int)

    bs = _find_col(cols, [r"bars_since"])
    if bs:
        base = int(x_full[bs].iloc[-1])
        fut[bs] = np.arange(base + 1, base + 1 + n_bars)

    lags = [c for c in cols if re.search(r"ret_lag", c, re.I)]
    if lags:
        fut[lags] = 0.0

    return fut


def cascade_signal(
    gate_proba: float,
    reg_pred: float,
    tau: float,
    gate_threshold: float = 0.5,
) -> tuple[int, float]:
    if gate_proba < gate_threshold or abs(reg_pred) < tau:
        return 0, 0.0
    conf = gate_proba * min(abs(reg_pred) / max(tau, 1e-9), 1.0)
    return (1 if reg_pred > 0 else -1), conf


class ForecastEngine:
    def __init__(self, bundle: ModelBundle | None = None) -> None:
        self.bundle = bundle or load_bundle()
        self.settings = get_settings()
        self.gate_threshold = float(
            self.settings.get("inference", "gate_threshold", default=0.5)
        )
        self.eval_mode = self.settings.get("training", "eval_mode", default="static")

    def _tf_cfg(self, tf: str) -> dict[str, Any]:
        return self.settings.yaml_config.get("timeframes", {}).get(tf.lower(), {})

    @staticmethod
    def _extract_price(features: pd.DataFrame, tf_data: dict[str, Any]) -> float:
        if "c_close" in features.columns:
            value = float(features["c_close"].iloc[-1])
            if value > 0:
                return value
        if "price_log" in features.columns:
            return float(np.exp(features["price_log"].iloc[-1]))
        price_full = tf_data.get("price_full")
        if price_full is not None and len(price_full):
            return float(price_full.iloc[-1])
        return 0.0

    def _predict_rows(
        self, tf_data: dict[str, Any], x: pd.DataFrame
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        g = tf_data["g_deploy"]
        r = tf_data["r_deploy"]
        tau = tf_data["taus"][self.eval_mode]
        feat = tf_data["feat_cols"]
        feat_jump = tf_data["feat_cols_jump"]

        x_gate = x[feat]
        p_move = g.predict_proba(x_gate)[:, 1]
        reg = r.predict(x[feat_jump])
        signals = np.zeros(len(x), dtype=int)
        confs = np.zeros(len(x))
        for i in range(len(x)):
            signals[i], confs[i] = cascade_signal(
                float(p_move[i]), float(reg[i]), float(tau), self.gate_threshold
            )
        return p_move, reg, signals, confs

    def predict_live(self, tf: str, features: pd.DataFrame) -> BarSignal:
        tf_data = self.bundle.get_tf(tf)
        row = features.iloc[[-1]]
        p_move, reg, signals, confs = self._predict_rows(tf_data, row)
        ts = features.index[-1].to_pydatetime() if len(features) else None
        return BarSignal(
            signal=int(signals[0]),
            p_move=float(p_move[0]),
            reg_pred=float(reg[0]),
            confidence=float(confs[0]),
            timestamp=ts,
        )

    def predict_horizon(self, tf: str, n_bars: int | None = None) -> pd.DataFrame:
        tf_data = self.bundle.get_tf(tf)
        cfg = self._tf_cfg(tf)
        n_bars = n_bars or int(cfg.get("future_bars", 24))
        x_full = tf_data["X_full"]
        feat = tf_data["feat_cols"]
        bar_step = infer_bar_step(x_full.index)
        bar_hours = bar_step.total_seconds() / 3600.0
        unit_div = 24.0 if tf.lower() == "6h" else 1.0
        unit = "дн" if tf.lower() == "6h" else "ч"

        fut = build_future_frame(x_full[feat], n_bars, bar_step)
        p_move, reg, signals, confs = self._predict_rows(tf_data, fut)

        res = pd.DataFrame(index=fut.index)
        res["bar_num"] = np.arange(1, n_bars + 1)
        res["units_ahead"] = res["bar_num"] * bar_hours / unit_div
        res["p_move"] = p_move
        res["reg_pred"] = reg
        res["signal"] = signals
        res["confidence_pct"] = confs * 100.0
        res["signal_label"] = res["signal"].map({1: "UP", -1: "DOWN", 0: "FLAT"})
        res.attrs["unit"] = unit
        return res

    def run(
        self,
        features_1h: pd.DataFrame,
        features_6h: pd.DataFrame,
    ) -> ForecastResult:
        updated = features_1h.index[-1].to_pydatetime()
        forecasts: dict[str, TimeframeForecast] = {}
        prices: dict[str, float] = {}

        for tf, feats in [("1h", features_1h), ("6h", features_6h)]:
            tf_data = self.bundle.get_tf(tf)
            live = self.predict_live(tf, feats)
            horizon = self.predict_horizon(tf)
            tf_price = self._extract_price(feats, tf_data)
            prices[tf] = tf_price
            forecasts[tf] = TimeframeForecast(
                tf=tf,
                live=live,
                horizon=horizon,
                last_price=tf_price,
                data_updated_at=updated,
            )

        price = prices.get("1h") or prices.get("6h") or 0.0
        return ForecastResult(
            price=price,
            updated_at=updated,
            forecasts=forecasts,
            bundle_meta=self.bundle.meta,
        )
