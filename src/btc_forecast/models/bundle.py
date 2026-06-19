"""Versioned model bundle save/load with schema validation."""

from __future__ import annotations

import hashlib
import json
import pickle
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_forecast.config import get_settings


@dataclass
class BundleMeta:
    version: str
    feature_cols_hash: str
    trained_at: str
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ModelBundle:
    meta: BundleMeta
    timeframes: dict[str, Any]

    def get_tf(self, tf: str) -> dict[str, Any]:
        key = tf.lower()
        if key not in self.timeframes:
            raise KeyError(f"Timeframe '{tf}' not in bundle")
        return self.timeframes[key]


def feature_cols_hash(cols: list[str]) -> str:
    payload = json.dumps(sorted(cols), ensure_ascii=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def build_meta(
    tf_results: dict[str, dict[str, Any]],
    version: str | None = None,
) -> BundleMeta:
    settings = get_settings()
    all_cols: list[str] = []
    metrics: dict[str, Any] = {}
    for tf, res in tf_results.items():
        all_cols.extend(res.get("feat_cols", []))
        casc = res.get("cascade", {}).get("static", {})
        metrics[tf] = {
            "gate_auc": res.get("m_gate", {}).get("auc"),
            "cascade_mcc": casc.get("mcc"),
            "gate_model": res.get("gate_model_name"),
            "reg_model": res.get("dir_model_name"),
            "trained_at": res.get("last_ts"),
        }
    return BundleMeta(
        version=version or settings.get("bundle", "version", default="1.0.0"),
        feature_cols_hash=feature_cols_hash(list(dict.fromkeys(all_cols))),
        trained_at=datetime.now(timezone.utc).isoformat(),
        metrics=metrics,
    )


def save_bundle(
    tf_results: dict[str, dict[str, Any]],
    path: Path | None = None,
) -> Path:
    settings = get_settings()
    path = path or (settings.models_dir / settings.get("bundle", "filename", default="cascade_bundle.pkl"))
    path.parent.mkdir(parents=True, exist_ok=True)

    meta = build_meta(tf_results)
    timeframes = {}
    for tf, res in tf_results.items():
        timeframes[tf] = {
            "g_deploy": res["g_deploy"],
            "r_deploy": res["r_deploy"],
            "taus": res["taus"],
            "gate_model_name": res["gate_model_name"],
            "dir_model_name": res["dir_model_name"],
            "feat_cols": res["feat_cols"],
            "feat_cols_jump": res["feat_cols_jump"],
            "X_full": res["X_full"],
            "price_full": res["price_full"],
            "vol_indicator": res.get("vol_indicator"),
            "vol_shift_full": res.get("vol_shift_full"),
            "dynamic_k": res.get("dynamic_k"),
            "static_thresh": res.get("static_thresh"),
            "m_gate": res.get("m_gate"),
            "m_reg": res.get("m_reg"),
            "cascade": res.get("cascade"),
        }

    payload = {"meta": meta, "timeframes": timeframes}
    with path.open("wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    return path


def load_bundle(path: Path | None = None, validate_hash: str | None = None) -> ModelBundle:
    settings = get_settings()
    path = path or (settings.models_dir / settings.get("bundle", "filename", default="cascade_bundle.pkl"))
    if not path.exists():
        raise FileNotFoundError(f"Bundle not found: {path}")

    with path.open("rb") as f:
        raw = pickle.load(f)

    if isinstance(raw, dict) and "meta" in raw:
        meta = raw["meta"]
        if isinstance(meta, dict):
            meta = BundleMeta(**meta)
        bundle = ModelBundle(meta=meta, timeframes=raw["timeframes"])
    else:
        meta = BundleMeta(
            version="0.9.0",
            feature_cols_hash="legacy",
            trained_at=datetime.now(timezone.utc).isoformat(),
            metrics={},
        )
        bundle = ModelBundle(meta=meta, timeframes=raw)

    if validate_hash and bundle.meta.feature_cols_hash != validate_hash:
        raise ValueError(
            f"Feature schema mismatch: bundle={bundle.meta.feature_cols_hash}, "
            f"current={validate_hash}. Run btc-train."
        )
    return bundle
