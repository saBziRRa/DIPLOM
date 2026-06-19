"""Gate AUC drift detection on recent bars."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.metrics import roc_auc_score

from btc_forecast.config import get_settings
from btc_forecast.models.bundle import ModelBundle


@dataclass
class DriftReport:
    tf: str
    auc_recent: float
    auc_baseline: float
    window_bars: int
    drifted: bool


def check_drift(bundle: ModelBundle, tf: str) -> DriftReport | None:
    settings = get_settings()
    tf_data = bundle.get_tf(tf)
    path = settings.features_dir / f"final_dataset_{tf.lower()}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    if len(df) < 50:
        return None

    window = int(
        settings.drift_window_bars_1h
        if tf.lower() == "1h"
        else settings.drift_window_bars_6h
    )
    threshold = float(settings.drift_threshold)
    recent = df.iloc[-window:]
    if len(recent) < 30:
        return None

    static_thresh = float(tf_data.get("static_thresh", 0.008))
    feat_cols = tf_data["feat_cols"]
    g = tf_data["g_deploy"]

    if "t_static" in recent.columns:
        y = (recent["t_static"] != 0).astype(int)
    elif "fwd_ret" in recent.columns:
        y = (recent["fwd_ret"].abs() > static_thresh).astype(int)
    else:
        return None

    x = recent[feat_cols].dropna()
    y = y.loc[x.index]
    if y.nunique() < 2 or len(x) < 30:
        return None

    proba = g.predict_proba(x)[:, 1]
    auc_recent = float(roc_auc_score(y, proba))
    baseline = float(bundle.meta.metrics.get(tf, {}).get("gate_auc") or 0.5)
    drifted = auc_recent < baseline - threshold

    return DriftReport(
        tf=tf,
        auc_recent=auc_recent,
        auc_baseline=baseline,
        window_bars=len(x),
        drifted=drifted,
    )


def format_drift_alert(report: DriftReport) -> str:
    return (
        f"⚠ Drift: gate AUC {report.tf.upper()} упал до "
        f"{report.auc_recent:.2f} (было {report.auc_baseline:.2f}, "
        f"окно {report.window_bars} баров). Рекомендуется проверить данные."
    )
