"""Static/dynamic targets and jump datasets for cascade v9 training."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from btc_forecast.config import get_settings

FWD_COL = "fwd_ret"
LEAK_COLS = ("fwd_ret", "target", "t_static", "t_dynamic")


def add_static_target(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    out = df.dropna(subset=[FWD_COL]).copy()
    fwd = out[FWD_COL]
    out["t_static"] = np.where(
        fwd > threshold, 1, np.where(fwd < -threshold, -1, 0)
    )
    return out


def add_dynamic_target(
    df: pd.DataFrame,
    vol_col: str,
    k: float = 1.0,
    fallback: str = "atr_pct",
) -> pd.DataFrame:
    out = df.copy()
    vol = out[vol_col] if vol_col in out.columns else out[fallback]
    vol_shift = vol.shift(1)
    valid = vol_shift.notna() & (vol_shift > 0)
    out = out.loc[valid].copy()
    vs = vol_shift.loc[valid]
    fw = out[FWD_COL]
    out["t_dynamic"] = np.where(
        fw > k * vs, 1, np.where(fw < -k * vs, -1, 0)
    )
    return out


def build_targets(
    df: pd.DataFrame,
    static_thresh: float,
    vol_indicator: str,
    dynamic_k: float = 1.0,
) -> pd.DataFrame:
    out = add_static_target(df, static_thresh)
    return add_dynamic_target(out, vol_indicator, dynamic_k)


def build_jump_dataset(df: pd.DataFrame) -> pd.DataFrame:
    if "t_static" not in df.columns:
        raise KeyError("t_static missing — run build_targets first")
    return df[df["t_static"] != 0].copy()


def prepare_jump_datasets(
    final_paths: dict[str, Path],
    out_dir: Path | None = None,
) -> dict[str, Path]:
    """Add t_static/t_dynamic to selected final datasets and write jump CSVs."""
    settings = get_settings()
    out_dir = out_dir or settings.features_dir
    cfg = settings.yaml_config.get("timeframes", {})

    paths: dict[str, Path] = {}
    for tf, vol in [("1h", "volatility_1d"), ("6h", "volatility_7d")]:
        src = final_paths.get(tf) or out_dir / f"final_dataset_{tf}.csv"
        th = cfg.get(tf, {}).get("threshold_static", 0.008 if tf == "1h" else 0.015)
        df = pd.read_csv(src, index_col=0, parse_dates=True)
        full = build_targets(df, th, vol)
        jump = build_jump_dataset(full)
        p_all = out_dir / f"final_dataset_{tf}.csv"
        p_jump = out_dir / f"final_dataset_{tf}_jump.csv"
        full.to_csv(p_all)
        jump.to_csv(p_jump)
        paths[f"final_{tf}"] = p_all
        paths[f"jump_{tf}"] = p_jump
    return paths
