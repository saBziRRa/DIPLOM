"""Benchmark current bundle and persist metrics history."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    matthews_corrcoef,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)

from btc_forecast.config import get_settings
from btc_forecast.features.targets import build_targets
from btc_forecast.models.bundle import load_bundle
from btc_forecast.models.inference import cascade_signal


def _safe_auc(y_true: pd.Series, y_proba: np.ndarray) -> float:
    if y_true.nunique() < 2:
        return float("nan")
    try:
        return float(roc_auc_score(y_true, y_proba))
    except ValueError:
        return float("nan")


def _safe_mcc(y_true: pd.Series, y_pred: np.ndarray) -> float:
    if y_true.nunique() < 2:
        return float("nan")
    return float(matthews_corrcoef(y_true, y_pred))


def _eval_cascade(
    y_true: pd.Series,
    gate_proba: np.ndarray,
    reg_pred: np.ndarray,
    tau: float,
    gate_threshold: float,
) -> dict[str, float]:
    out = np.zeros(len(y_true), dtype=int)
    conf = np.zeros(len(y_true), dtype=float)
    for i in range(len(y_true)):
        sig, c = cascade_signal(
            gate_proba=float(gate_proba[i]),
            reg_pred=float(reg_pred[i]),
            tau=float(tau),
            gate_threshold=gate_threshold,
        )
        out[i] = sig
        conf[i] = c
    return {
        "acc": float(accuracy_score(y_true, out)),
        "f1_macro": float(f1_score(y_true, out, labels=[-1, 0, 1], average="macro", zero_division=0)),
        "mcc": _safe_mcc(y_true, out),
        "active_rate": float(np.mean(out != 0)),
        "confidence_mean": float(np.mean(conf)),
    }


def _tf_threshold(settings, tf: str) -> float:
    default = 0.008 if tf == "1h" else 0.015
    return float(settings.get("timeframes", tf, "threshold_static", default=default))


def _benchmark_one_tf(bundle, tf: str, gate_threshold: float) -> dict[str, float | int | str]:
    settings = get_settings()
    tf_data = bundle.get_tf(tf)
    source_path = settings.features_dir / f"final_dataset_{tf}.csv"
    if not source_path.exists():
        raise FileNotFoundError(f"Missing dataset for benchmark: {source_path}")
    df = pd.read_csv(source_path, index_col=0, parse_dates=True)

    static_thresh = float(tf_data.get("static_thresh", _tf_threshold(settings, tf)))
    dynamic_k = float(tf_data.get("dynamic_k", 1.0))
    vol_indicator = str(
        tf_data.get("vol_indicator", "volatility_1d" if tf == "1h" else "volatility_7d")
    )
    full = build_targets(df, static_thresh=static_thresh, vol_indicator=vol_indicator, dynamic_k=dynamic_k)

    feat_cols = list(tf_data["feat_cols"])
    feat_cols_jump = list(tf_data["feat_cols_jump"])
    gate_input = full[feat_cols].copy()
    gate_mask = gate_input.notna().all(axis=1)
    gate_input = gate_input.loc[gate_mask]
    eval_df = full.loc[gate_input.index]
    y_gate = (eval_df["t_static"] != 0).astype(int)

    g_model = tf_data["g_deploy"]
    gate_proba = g_model.predict_proba(gate_input)[:, 1]
    gate_pred = (gate_proba >= gate_threshold).astype(int)

    jump = full[full["t_static"] != 0].copy()
    jump_input = jump[feat_cols_jump].copy()
    jump_mask = jump_input.notna().all(axis=1)
    jump_input = jump_input.loc[jump_mask]
    jump = jump.loc[jump_input.index]

    r_model = tf_data["r_deploy"]
    reg_sparse = r_model.predict(jump_input) if len(jump_input) else np.array([])
    reg_full = np.zeros(len(gate_input), dtype=float)
    if len(jump_input):
        common = gate_input.index.intersection(jump_input.index)
        if len(common):
            pos_gate = gate_input.index.get_indexer(common)
            pos_jump = jump_input.index.get_indexer(common)
            reg_full[pos_gate] = reg_sparse[pos_jump]

    y_jump = jump["fwd_ret"] if len(jump) else pd.Series(dtype=float)
    reg_mae = float(mean_absolute_error(y_jump, reg_sparse)) if len(jump) else float("nan")
    reg_rmse = (
        float(np.sqrt(mean_squared_error(y_jump, reg_sparse))) if len(jump) else float("nan")
    )
    reg_sign = (
        float(np.mean(np.sign(reg_sparse) == np.sign(y_jump.to_numpy())))
        if len(jump)
        else float("nan")
    )

    mode_static = _eval_cascade(
        y_true=eval_df["t_static"].astype(int),
        gate_proba=gate_proba,
        reg_pred=reg_full,
        tau=float(tf_data["taus"]["static"]),
        gate_threshold=gate_threshold,
    )
    mode_dynamic = _eval_cascade(
        y_true=eval_df["t_dynamic"].astype(int),
        gate_proba=gate_proba,
        reg_pred=reg_full,
        tau=float(tf_data["taus"]["dynamic"]),
        gate_threshold=gate_threshold,
    )

    return {
        "tf": tf,
        "n_total": int(len(eval_df)),
        "n_jump": int(len(jump)),
        "signal_rate_static": float((eval_df["t_static"] != 0).mean()),
        "signal_rate_dynamic": float((eval_df["t_dynamic"] != 0).mean()),
        "gate_auc": _safe_auc(y_gate, gate_proba),
        "gate_mcc": _safe_mcc(y_gate, gate_pred),
        "gate_f1_macro": float(f1_score(y_gate, gate_pred, average="macro", zero_division=0)),
        "reg_mae_jump": reg_mae,
        "reg_rmse_jump": reg_rmse,
        "reg_sign_acc_jump": reg_sign,
        "tau_static": float(tf_data["taus"]["static"]),
        "tau_dynamic": float(tf_data["taus"]["dynamic"]),
        "cascade_static_acc": mode_static["acc"],
        "cascade_static_f1_macro": mode_static["f1_macro"],
        "cascade_static_mcc": mode_static["mcc"],
        "cascade_static_active_rate": mode_static["active_rate"],
        "cascade_dynamic_acc": mode_dynamic["acc"],
        "cascade_dynamic_f1_macro": mode_dynamic["f1_macro"],
        "cascade_dynamic_mcc": mode_dynamic["mcc"],
        "cascade_dynamic_active_rate": mode_dynamic["active_rate"],
    }


def benchmark_csv_path() -> Path:
    settings = get_settings()
    return settings.artifacts_dir / "benchmark.csv"


def _next_run_id(path: Path) -> int:
    if not path.exists():
        return 1
    prev = pd.read_csv(path)
    if "run_id" not in prev.columns or prev.empty:
        return 1
    numeric = pd.to_numeric(prev["run_id"], errors="coerce").dropna()
    if numeric.empty:
        return 1
    return int(numeric.max()) + 1


def run_benchmark(
    name: str = "",
    description: str = "",
    bundle_path: Path | None = None,
) -> pd.DataFrame:
    settings = get_settings()
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)

    bundle = load_bundle(bundle_path)
    gate_threshold = float(settings.get("inference", "gate_threshold", default=0.5))
    run_ts = datetime.now(timezone.utc)
    path = benchmark_csv_path()
    run_id = _next_run_id(path)

    rows: list[dict[str, float | int | str]] = []
    for tf in ("1h", "6h"):
        tf_row = _benchmark_one_tf(bundle=bundle, tf=tf, gate_threshold=gate_threshold)
        tf_row.update(
            {
                "run_id": run_id,
                "run_name": name.strip(),
                "run_description": description.strip(),
                "created_at": run_ts.isoformat(),
                "bundle_version": bundle.meta.version,
                "bundle_hash": bundle.meta.feature_cols_hash,
                "bundle_trained_at": bundle.meta.trained_at,
            }
        )
        rows.append(tf_row)

    out = pd.DataFrame(rows)
    if path.exists():
        prev = pd.read_csv(path)
        out = pd.concat([prev, out], ignore_index=True)
    out.to_csv(path, index=False)
    return pd.DataFrame(rows)


def load_benchmark_history(limit: int = 300) -> list[dict[str, object]]:
    path = benchmark_csv_path()
    if not path.exists():
        return []
    df = pd.read_csv(path)
    if "created_at" in df.columns:
        df = df.sort_values("created_at", ascending=False)
    if limit > 0:
        df = df.head(limit)
    return df.to_dict(orient="records")
