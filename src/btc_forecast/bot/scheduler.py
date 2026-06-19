"""Background retrain and drift monitoring."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Awaitable

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from btc_forecast.bot import broadcast, userstate
from btc_forecast.bot.cache import get_cached_predict
from btc_forecast.config import get_settings
from btc_forecast.debug.scheduler_state import scheduler_state
from btc_forecast.models.bundle import load_bundle
from btc_forecast.monitoring.drift import check_drift, format_drift_alert
from btc_forecast.pipeline.features import run_features
from btc_forecast.pipeline.ingest import run_ingest
from btc_forecast.pipeline.predict import run_predict
from btc_forecast.pipeline.train import run_train

logger = logging.getLogger(__name__)


class BotScheduler:
    def __init__(
        self,
        on_bundle_reload: Callable[[], Awaitable[None]] | None = None,
        on_admin_message: Callable[[str], Awaitable[None]] | None = None,
        on_user_message: Callable[[int, str], Awaitable[None]] | None = None,
    ) -> None:
        self.settings = get_settings()
        self._scheduler = AsyncIOScheduler()
        self._on_bundle_reload = on_bundle_reload
        self._on_admin_message = on_admin_message
        self._on_user_message = on_user_message

    def _interval_for(self, job_id: str) -> int:
        return {
            "retrain": self.settings.retrain_interval_sec,
            "drift": self.settings.drift_check_interval_sec,
            "broadcast": self.settings.broadcast_interval_sec,
        }.get(job_id, 0)

    def _sync_job_times(self) -> None:
        for job in self._scheduler.get_jobs():
            interval = getattr(getattr(job, "trigger", None), "interval", None)
            interval_sec = int(interval.total_seconds()) if interval else 0
            st = scheduler_state.get_job(job.id, interval_sec=interval_sec)
            nrt = job.next_run_time
            st.next_run_at = nrt.timestamp() if nrt else None

    async def _run_tracked(self, job_id: str, coro_factory) -> None:
        st = scheduler_state.get_job(job_id, interval_sec=self._interval_for(job_id))
        st.running = True
        st.last_error = None
        t0 = time.time()
        try:
            await coro_factory()
        except Exception as exc:
            st.last_error = str(exc)
            raise
        finally:
            st.running = False
            st.last_run_at = time.time()
            st.last_duration_sec = round(time.time() - t0, 2)
            self._sync_job_times()

    async def _retrain(self) -> None:
        async def _work() -> None:
            try:
                logger.info("Scheduled retrain started")
                await asyncio.to_thread(run_ingest)
                await asyncio.to_thread(run_features)
                await asyncio.to_thread(run_train, True)
                if self._on_bundle_reload:
                    await self._on_bundle_reload()
                logger.info("Scheduled retrain completed")
            except Exception as exc:
                logger.exception("Scheduled retrain failed: %s", exc)
                if self._on_admin_message:
                    await self._on_admin_message(f"❌ Retrain failed: {exc}")
                raise

        await self._run_tracked("retrain", _work)

    async def _drift_check(self) -> None:
        async def _work() -> None:
            try:
                bundle = load_bundle()
                for tf in ("1h", "6h"):
                    report = check_drift(bundle, tf)
                    if report and report.drifted and self._on_admin_message:
                        await self._on_admin_message(format_drift_alert(report))
            except Exception as exc:
                logger.warning("Drift check failed: %s", exc)
                raise

        await self._run_tracked("drift", _work)

    async def _broadcast(self) -> None:
        async def _work() -> None:
            if not self._on_user_message:
                return
            subscribers = await asyncio.to_thread(userstate.list_subscribers)
            if not subscribers:
                return
            result = await asyncio.to_thread(get_cached_predict, run_predict)
            signal = broadcast.select_broadcast(result)
            if signal is None:
                return
            last_key = await asyncio.to_thread(userstate.get_last_broadcast)
            if last_key == signal.dedup_key:
                return
            recipients = [
                sub["user_id"]
                for sub in subscribers
                if signal.confidence >= sub["min_confidence"]
            ]
            if not recipients:
                return
            text = broadcast.format_broadcast(signal)
            for user_id in recipients:
                try:
                    await self._on_user_message(int(user_id), text)
                except Exception as exc:
                    logger.warning("Broadcast to %s failed: %s", user_id, exc)
            await asyncio.to_thread(userstate.set_last_broadcast, signal.dedup_key)

        await self._run_tracked("broadcast", _work)

    def start(self) -> None:
        interval = self.settings.retrain_interval_sec
        drift_interval = self.settings.drift_check_interval_sec
        broadcast_interval = self.settings.broadcast_interval_sec
        scheduler_state.get_job("retrain", interval_sec=interval)
        scheduler_state.get_job("drift", interval_sec=drift_interval)
        scheduler_state.get_job("broadcast", interval_sec=broadcast_interval)

        self._scheduler.add_job(
            self._retrain,
            "interval",
            seconds=interval,
            id="retrain",
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.add_job(
            self._drift_check,
            "interval",
            seconds=drift_interval,
            id="drift",
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.add_job(
            self._broadcast,
            "interval",
            seconds=broadcast_interval,
            id="broadcast",
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.start()
        self._sync_job_times()
        logger.info(
            "Scheduler started: retrain every %ds, drift every %ds, "
            "broadcast every %ds",
            interval,
            drift_interval,
            broadcast_interval,
        )

    def shutdown(self) -> None:
        self._scheduler.shutdown(wait=False)
