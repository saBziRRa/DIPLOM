"""Data ingestion pipeline."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from btc_forecast.config import get_settings
from btc_forecast.data import loader as dl
from btc_forecast.features.engineering import load_from_csv

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))
_PREFIX = "btc"
_SOURCE_KEYS = [
    "candles_1h",
    "candles_6h",
    "bybit_daily",
    "macro",
    "fear_greed",
    "open_interest",
    "long_short",
    "funding_rate",
    "sentiment",
]


def _latest_source_file(raw_dir: Path, source: str, prefix: str = _PREFIX) -> Path | None:
    files = sorted(raw_dir.glob(f"{prefix}_{source}_*.csv"))
    return files[-1] if files else None


def _is_source_fresh(path: Path | None, max_age_hours: int) -> bool:
    if path is None or not path.exists():
        return False
    age = datetime.now(_MSK).timestamp() - path.stat().st_mtime
    return age <= max_age_hours * 3600


def _source_last_ts(df: pd.DataFrame | None) -> datetime | None:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    idx = pd.to_datetime(df.index)
    return idx.max().to_pydatetime()


def _source_start_date(
    cached_df: pd.DataFrame | None,
    fallback_start: str,
    overlap_days: int,
    force: bool,
) -> str:
    if force:
        return fallback_start
    last_ts = _source_last_ts(cached_df)
    if last_ts is None:
        return fallback_start
    return (last_ts - timedelta(days=overlap_days)).strftime("%Y-%m-%d")


def _merge_frames(old_df: pd.DataFrame | None, new_df: pd.DataFrame | None) -> pd.DataFrame:
    if old_df is None or old_df.empty:
        return new_df.sort_index() if isinstance(new_df, pd.DataFrame) else pd.DataFrame()
    if new_df is None or new_df.empty:
        return old_df.sort_index()
    combined = pd.concat([old_df, new_df], axis=0)
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined.sort_index()


def _fetch_source(
    source: str,
    start_date: str,
    end_date: str,
    master_index: pd.DatetimeIndex | None,
    cryptopanic_token: str | None,
) -> pd.DataFrame:
    if source == "candles_1h":
        return dl.load_candles_1h(start_date, end_date)
    if source == "candles_6h":
        return dl.load_candles_6h(start_date, end_date)

    if master_index is None:
        return pd.DataFrame()

    if source == "bybit_daily":
        return dl._align_daily(dl.load_bybit_daily(start_date, end_date), master_index)
    if source == "macro":
        return dl._align_daily(dl.load_macro(start_date, end_date), master_index)
    if source == "fear_greed":
        return dl._align_daily(dl.load_fear_greed(start_date), master_index)
    if source == "open_interest":
        return dl._align_intrabar(dl.load_open_interest(start_date, end_date), master_index)
    if source == "long_short":
        return dl._align_intrabar(
            dl.load_long_short_ratio(start_date, end_date), master_index
        )
    if source == "funding_rate":
        return dl._align_funding(dl.load_funding_rate(start_date, end_date), master_index)
    if source == "sentiment":
        return dl.load_sentiment(
            master_index,
            start_date,
            end_date,
            cryptopanic_token=cryptopanic_token,
        )
    return pd.DataFrame()


def run_ingest(force: bool = False) -> dict:
    settings = get_settings()
    raw_dir = settings.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    days = int(settings.get("download_days", default=720))
    warmup = int(settings.get("features", "warmup_days", default=210))
    total_days = days + warmup
    fallback_start = (
        datetime.now(_MSK) - timedelta(days=total_days)
    ).strftime("%Y-%m-%d")
    end_date = dl._end_date_default()
    freshness_hours = int(settings.get("ingest", "cache_freshness_hours", default=2))
    overlap_days = int(settings.get("ingest", "incremental_overlap_days", default=3))
    cryptopanic_token = settings.cryptopanic_token or None

    cached = load_from_csv(csv_dir=str(raw_dir), prefix=_PREFIX)
    refresh: dict[str, bool] = {}
    for source in _SOURCE_KEYS:
        src_file = _latest_source_file(raw_dir, source, prefix=_PREFIX)
        refresh[source] = force or (not _is_source_fresh(src_file, freshness_hours))

    if not force and not any(refresh.values()):
        logger.info("All source caches are fresh, using local data from %s", raw_dir)
        return cached

    logger.info(
        "Ingest start (force=%s): refreshing %s",
        force,
        [k for k, v in refresh.items() if v],
    )
    data: dict[str, pd.DataFrame] = {}

    # 1H master is mandatory for aligned sources.
    if refresh["candles_1h"]:
        start_1h = _source_start_date(
            cached.get("candles_1h"),
            fallback_start,
            overlap_days,
            force,
        )
        logger.info("Refresh candles_1h from %s to %s", start_1h, end_date)
        new_1h = _fetch_source(
            "candles_1h",
            start_1h,
            end_date,
            master_index=None,
            cryptopanic_token=cryptopanic_token,
        )
        data["candles_1h"] = _merge_frames(cached.get("candles_1h"), new_1h)
    else:
        data["candles_1h"] = cached.get("candles_1h", pd.DataFrame()).sort_index()

    if data["candles_1h"].empty:
        logger.warning("candles_1h cache missing/empty, doing full load_all fallback")
        full = dl.load_all(start_date=fallback_start)
        dl.save_all(full, directory=str(raw_dir), prefix=_PREFIX)
        return full

    master_idx = data["candles_1h"].index

    for source in [k for k in _SOURCE_KEYS if k != "candles_1h"]:
        if refresh[source]:
            src_start = _source_start_date(
                cached.get(source),
                fallback_start,
                overlap_days,
                force,
            )
            logger.info("Refresh %s from %s to %s", source, src_start, end_date)
            new_df = _fetch_source(
                source,
                src_start,
                end_date,
                master_index=master_idx,
                cryptopanic_token=cryptopanic_token,
            )
            data[source] = _merge_frames(cached.get(source), new_df)
        else:
            old = cached.get(source, pd.DataFrame()).sort_index()
            if source in {
                "bybit_daily",
                "macro",
                "fear_greed",
                "open_interest",
                "long_short",
                "funding_rate",
                "sentiment",
            } and not old.empty:
                old = old.reindex(master_idx)
            data[source] = old

    dl.save_all(data, directory=str(raw_dir), prefix=_PREFIX)
    return data
