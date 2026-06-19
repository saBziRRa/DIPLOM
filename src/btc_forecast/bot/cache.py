"""Forecast result cache shared by bot handlers and debug UI."""

from __future__ import annotations

import time
from typing import Callable

from btc_forecast.config import get_settings
from btc_forecast.models.inference import ForecastResult

_cache: dict[str, tuple[float, ForecastResult]] = {}


def get_cached_predict(predict_fn: Callable[[], ForecastResult]) -> ForecastResult:
    settings = get_settings()
    ttl = settings.inference_cache_ttl_sec
    now = time.time()
    cached = _cache.get("forecast")
    if cached and now - cached[0] < ttl:
        return cached[1]
    result = predict_fn()
    _cache["forecast"] = (now, result)
    return result


def cache_info() -> dict:
    settings = get_settings()
    ttl = settings.inference_cache_ttl_sec
    now = time.time()
    cached = _cache.get("forecast")
    if not cached:
        return {
            "cached": False,
            "ttl_sec": ttl,
            "age_sec": None,
            "expires_in_sec": None,
        }
    ts, _ = cached
    age = now - ts
    return {
        "cached": True,
        "ttl_sec": ttl,
        "age_sec": round(age, 1),
        "expires_in_sec": round(max(0, ttl - age), 1),
        "cached_at": ts,
    }


def clear_cache() -> None:
    _cache.clear()
