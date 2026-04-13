"""Cluster mode: coordinator + worker registration and (later) job dispatch.

Phase 1 ships the registry only — workers register, send heartbeats, the
viewer can list them. Job dispatch lands in subsequent phases.
"""

from rinnsal.cluster.coordinator import CoordinatorState, router
from rinnsal.cluster.protocol import (
    Capabilities,
    HeartbeatResponse,
    HealthResponse,
    WorkerInfo,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
)
from rinnsal.cluster.worker import WorkerDaemon

__all__ = [
    "CoordinatorState",
    "router",
    "Capabilities",
    "WorkerInfo",
    "WorkerRegisterRequest",
    "WorkerRegisterResponse",
    "HeartbeatResponse",
    "HealthResponse",
    "WorkerDaemon",
]
