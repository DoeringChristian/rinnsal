"""Resource-aware scheduler for the cluster coordinator.

Given a worker and a list of pending jobs, returns the oldest job whose
declared resource requirements fit in the worker's free capacity. In v1
this is FIFO-within-matching: pending jobs are scanned in submission
order, the first match wins.

Promoted from ``experimental/scheduler.py`` and adapted to the
event-driven coordinator's signature (it picks one assignment at a
time, not a batch).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ClusterScheduler(ABC):
    @abstractmethod
    def pick_job(
        self,
        worker_capabilities: dict[str, Any],
        worker_load: dict[str, float],
        pending_jobs: list[dict[str, Any]],
    ) -> int | None:
        """Return the index in ``pending_jobs`` to assign, or None."""


class FIFOClusterScheduler(ClusterScheduler):
    """Naive scheduler: always returns 0 if any pending job exists."""

    def pick_job(
        self,
        worker_capabilities: dict[str, Any],
        worker_load: dict[str, float],
        pending_jobs: list[dict[str, Any]],
    ) -> int | None:
        return 0 if pending_jobs else None


class ResourceMatchingClusterScheduler(ClusterScheduler):
    """FIFO within matching — first pending job whose resources fit wins.

    Resource requirements come from ``@task(resources=...)`` and are
    serialized to a flat dict (cpu, memory, gpu, gpu_memory, plus any
    ``extras`` keys flattened in).
    """

    def pick_job(
        self,
        worker_capabilities: dict[str, Any],
        worker_load: dict[str, float],
        pending_jobs: list[dict[str, Any]],
    ) -> int | None:
        free = self._free(worker_capabilities, worker_load)
        for idx, job in enumerate(pending_jobs):
            req = job.get("resources") or {}
            if self._fits(req, free):
                return idx
        return None

    @staticmethod
    def _free(
        capabilities: dict[str, Any], load: dict[str, float]
    ) -> dict[str, float]:
        """``capabilities - load`` per key (zero-valued capabilities = 0)."""
        out: dict[str, float] = {}
        for k, v in capabilities.items():
            try:
                out[k] = float(v) - float(load.get(k, 0))
            except (TypeError, ValueError):
                # Non-numeric capability: pass through as-is (e.g. labels).
                out[k] = v  # type: ignore[assignment]
        return out

    @staticmethod
    def _fits(
        req: dict[str, Any], free: dict[str, Any]
    ) -> bool:
        for k, v in req.items():
            if k not in free:
                return False
            free_v = free[k]
            try:
                if float(free_v) < float(v):
                    return False
            except (TypeError, ValueError):
                # Non-numeric requirement: must equal exactly.
                if free_v != v:
                    return False
        return True


_default_scheduler: ClusterScheduler = ResourceMatchingClusterScheduler()


def get_default_scheduler() -> ClusterScheduler:
    return _default_scheduler


def set_default_scheduler(s: ClusterScheduler) -> None:
    global _default_scheduler
    _default_scheduler = s
