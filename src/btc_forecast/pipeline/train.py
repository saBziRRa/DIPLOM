"""Training pipeline orchestration."""

from __future__ import annotations

import logging
from pathlib import Path

from btc_forecast.config import get_settings
from btc_forecast.models.bundle import save_bundle
from btc_forecast.models.training import run_training

logger = logging.getLogger(__name__)


def run_train(tune: bool = True) -> Path:
    settings = get_settings()
    features_dir = settings.features_dir
    models_dir = settings.models_dir
    models_dir.mkdir(parents=True, exist_ok=True)

    if tune:
        from btc_forecast.models.tuning import run_tuning

        run_tuning(features_dir=features_dir, models_dir=models_dir)

    results = run_training(features_dir=features_dir, models_dir=models_dir)
    path = save_bundle(results, models_dir / settings.get("bundle", "filename", default="cascade_bundle.pkl"))
    logger.info("Bundle saved to %s", path)
    return path
