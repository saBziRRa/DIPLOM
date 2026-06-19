"""Mutable scheduler status for debug dashboard."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class JobStatus:
    job_id: str
    interval_sec: int
    next_run_at: float | None = None
    last_run_at: float | None = None
    last_duration_sec: float | None = None
    last_error: str | None = None
    running: bool = False

    def to_dict(self) -> dict[str, Any]:
        now = time.time()
        return {
            "job_id": self.job_id,
            "interval_sec": self.interval_sec,
            "next_run_at": self.next_run_at,
            "next_run_in_sec": (
                round(self.next_run_at - now, 1)
                if self.next_run_at is not None
                else None
            ),
            "last_run_at": self.last_run_at,
            "last_duration_sec": self.last_duration_sec,
            "last_error": self.last_error,
            "running": self.running,
        }


@dataclass
class SchedulerState:
    jobs: dict[str, JobStatus] = field(default_factory=dict)

    def get_job(self, job_id: str, interval_sec: int) -> JobStatus:
        if job_id not in self.jobs:
            self.jobs[job_id] = JobStatus(job_id=job_id, interval_sec=interval_sec)
        return self.jobs[job_id]

    def to_dict(self) -> dict[str, Any]:
        return {jid: job.to_dict() for jid, job in self.jobs.items()}


# Global instance updated by BotScheduler
scheduler_state = SchedulerState()
