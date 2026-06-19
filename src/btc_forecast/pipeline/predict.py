"""Prediction pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from btc_forecast.config import get_settings
from btc_forecast.features.engineering import build_features_1h, build_features_6h, load_from_csv
from btc_forecast.models.bundle import load_bundle
from btc_forecast.models.inference import ForecastEngine, ForecastResult

logger = logging.getLogger(__name__)


def _latest_feature_rows(
    bundle,
    df_1h: pd.DataFrame,
    df_6h: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Align engineering output to bundle feature columns."""
    out = {}
    for tf, df in [("1h", df_1h), ("6h", df_6h)]:
        tf_data = bundle.get_tf(tf)
        cols = list(dict.fromkeys(tf_data["feat_cols"] + ["c_close"]))
        missing = [c for c in cols if c not in df.columns]
        if missing:
            logger.warning("%s: missing columns %s — filling 0", tf, missing[:5])
        aligned = pd.DataFrame(index=df.index)
        for c in cols:
            aligned[c] = df[c] if c in df.columns else 0.0
        out[tf] = aligned
    return out["1h"], out["6h"]


def _load_final_if_fresh(features_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    p1 = features_dir / "final_dataset_1h.csv"
    p6 = features_dir / "final_dataset_6h.csv"
    if p1.exists() and p6.exists():
        return (
            pd.read_csv(p1, index_col=0, parse_dates=True),
            pd.read_csv(p6, index_col=0, parse_dates=True),
        )
    return None


def run_predict(bundle_path=None, use_cached_final: bool = True) -> ForecastResult:
    settings = get_settings()
    bundle = load_bundle(bundle_path) if bundle_path else load_bundle()

    if use_cached_final:
        cached = _load_final_if_fresh(settings.features_dir)
        if cached:
            df_1h, df_6h = cached
            engine = ForecastEngine(bundle)
            return engine.run(df_1h, df_6h)

    data = load_from_csv(csv_dir=str(settings.raw_dir), prefix="btc")
    if not data or not data.get("candles_1h", pd.DataFrame()).shape[0]:
        raise RuntimeError("No data — run btc-ingest")

    feat_cfg = settings.yaml_config.get("features", {})
    days = int(settings.get("download_days", default=720))

    df_1h, _ = build_features_1h(
        data=data,
        days=days,
        num_bars=int(feat_cfg.get("num_bars_1h", 2)),
        csv_dir=str(settings.raw_dir),
    )
    df_6h, _ = build_features_6h(
        data=data,
        days=days,
        num_bars=int(feat_cfg.get("num_bars_6h", 4)),
        csv_dir=str(settings.raw_dir),
    )
    df_1h, df_6h = _latest_feature_rows(bundle, df_1h, df_6h)

    engine = ForecastEngine(bundle)
    return engine.run(df_1h, df_6h)
