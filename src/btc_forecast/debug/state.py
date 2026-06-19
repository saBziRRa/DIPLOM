"""Collect system snapshot for admin debug UI."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_forecast.bot import formatters
from btc_forecast.bot.cache import cache_info, get_cached_predict
from btc_forecast.config import get_settings
from btc_forecast.debug.scheduler_state import scheduler_state
from btc_forecast.monitoring.drift import check_drift
from btc_forecast.pipeline.predict import run_predict


def _file_age_sec(path: Path) -> float | None:
    if not path.exists():
        return None
    return time.time() - path.stat().st_mtime


def _file_info(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "path": str(path)}
    st = path.stat()
    return {
        "exists": True,
        "path": str(path),
        "size_kb": round(st.st_size / 1024, 1),
        "mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
        "age_sec": round(time.time() - st.st_mtime, 1),
    }


def collect_status(
    include_forecast: bool = True,
    standalone_mode: bool = False,
) -> dict[str, Any]:
    settings = get_settings()
    models_dir = settings.models_dir
    features_dir = settings.features_dir
    raw_dir = settings.raw_dir
    bundle_path = models_dir / settings.get("bundle", "filename", default="cascade_bundle.pkl")

    sched = scheduler_state.to_dict()
    if not sched or standalone_mode:
        sched = {
            "retrain": {
                "job_id": "retrain",
                "interval_sec": settings.retrain_interval_sec,
                "next_run_in_sec": None,
                "running": False,
                "last_error": (
                    "standalone_mode: bot/scheduler not running"
                    if standalone_mode
                    else None
                ),
            },
            "drift": {
                "job_id": "drift",
                "interval_sec": settings.drift_check_interval_sec,
                "next_run_in_sec": None,
                "running": False,
                "last_error": (
                    "standalone_mode: bot/scheduler not running"
                    if standalone_mode
                    else None
                ),
            },
        }

    status: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cache": cache_info(),
        "scheduler": sched,
        "artifacts": {
            "bundle": _file_info(bundle_path),
            "final_1h": _file_info(features_dir / "final_dataset_1h.csv"),
            "final_6h": _file_info(features_dir / "final_dataset_6h.csv"),
            "benchmark": _file_info(settings.artifacts_dir / "benchmark.csv"),
            "raw_candles": _file_info(_latest_glob(raw_dir, "btc_candles_1h_*.csv")),
        },
        "config": {
            "retrain_interval_sec": settings.retrain_interval_sec,
            "drift_check_interval_sec": settings.drift_check_interval_sec,
            "inference_cache_ttl_sec": settings.inference_cache_ttl_sec,
        },
        "standalone_mode": standalone_mode,
        "bundle": None,
        "drift": {},
        "bot_preview": {},
        "errors": [],
    }

    try:
        from btc_forecast.models.bundle import load_bundle

        bundle = load_bundle(bundle_path)
        meta = bundle.meta
        status["bundle"] = {
            "version": meta.version,
            "feature_cols_hash": meta.feature_cols_hash,
            "trained_at": meta.trained_at,
            "metrics": meta.metrics,
        }
        for tf in ("1h", "6h"):
            report = check_drift(bundle, tf)
            if report:
                status["drift"][tf] = {
                    "auc_recent": report.auc_recent,
                    "auc_baseline": report.auc_baseline,
                    "window_bars": report.window_bars,
                    "drifted": report.drifted,
                }
    except Exception as exc:
        status["errors"].append(f"bundle: {exc}")

    if include_forecast:
        try:
            result = get_cached_predict(run_predict)
            status["bot_preview"] = {
                "start": formatters.format_start(),
                "summary": formatters.format_summary(result),
                "forecast": formatters.format_forecast(result),
                "agreement": result.agreement,
                "price": result.price,
                "updated_at": (
                    result.updated_at.isoformat() if result.updated_at else None
                ),
                "live_signals": {
                    tf: {
                        "signal": fc.live.signal,
                        "p_move": fc.live.p_move,
                        "reg_pred": fc.live.reg_pred,
                        "confidence": fc.live.confidence,
                    }
                    for tf, fc in result.forecasts.items()
                },
                "placeholder": False,
            }
        except Exception as exc:
            status["errors"].append(f"forecast: {exc}")
            if standalone_mode:
                status["bot_preview"] = {
                    "start": "standalone_mode: bot is not running",
                    "summary": "standalone_mode: no live bot output",
                    "forecast": "standalone_mode: no live bot output",
                    "agreement": "standalone_mode",
                    "price": None,
                    "updated_at": None,
                    "live_signals": {},
                    "placeholder": True,
                    "error": str(exc),
                }
            else:
                status["bot_preview"] = {"error": str(exc)}

    return status


def _latest_glob(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    return matches[-1] if matches else directory / pattern
