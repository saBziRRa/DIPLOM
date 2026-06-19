"""Feature building and selection pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

from btc_forecast.config import get_settings
from btc_forecast.features.engineering import load_from_csv
from btc_forecast.features.engineering import build_features_1h, build_features_6h
from btc_forecast.features.selection import process as run_selection
from btc_forecast.features.targets import prepare_jump_datasets

logger = logging.getLogger(__name__)


def run_features() -> dict[str, Path]:
    settings = get_settings()
    features_dir = settings.features_dir
    features_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = settings.raw_dir

    feat_cfg = settings.yaml_config.get("features", {})
    days_1h = int(settings.get("download_days", default=720))
    days_6h = days_1h

    data = load_from_csv(csv_dir=str(raw_dir), prefix="btc")
    if not data:
        raise RuntimeError("No cached data — run btc-ingest first")

    df_1h, _ = build_features_1h(
        data=data,
        days=days_1h,
        num_bars=int(feat_cfg.get("num_bars_1h", 2)),
        span=int(feat_cfg.get("span_1h", 72)),
        pt_sl=tuple(feat_cfg.get("pt_sl_1h", [2.0, 2.0])),
        min_target=float(feat_cfg.get("min_target", 0.001)),
    )
    df_6h, _ = build_features_6h(
        data=data,
        days=days_6h,
        num_bars=int(feat_cfg.get("num_bars_6h", 4)),
        span=int(feat_cfg.get("span_6h", 210)),
        pt_sl=tuple(feat_cfg.get("pt_sl_6h", [2.5, 2.5])),
        min_target=float(feat_cfg.get("min_target", 0.001)),
    )

    p1 = features_dir / "features_1h.csv"
    p6 = features_dir / "features_6h.csv"
    df_1h.to_csv(p1)
    df_6h.to_csv(p6)
    logger.info("Saved %s (%d rows), %s (%d rows)", p1, len(df_1h), p6, len(df_6h))

    out_1h = features_dir / "final_dataset_1h.csv"
    out_6h = features_dir / "final_dataset_6h.csv"
    run_selection("1H", str(p1), str(out_1h))
    run_selection("6H", str(p6), str(out_6h))
    paths = prepare_jump_datasets({"1h": out_1h, "6h": out_6h}, out_dir=features_dir)
    return paths
