"""Shared request/response models for the cluster control plane.

One source of truth used by the coordinator (FastAPI route handlers),
the worker daemon (HTTP client), and the future ClusterExecutor
(also an HTTP client). Built on Pydantic v2 to leverage FastAPI's
automatic schema generation + validation.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Capabilities(BaseModel):
    """What a worker can run.

    All fields are optional and worker-declared. Resource matching
    (Phase 4) compares ``@task(resources=...)`` requirements against
    these. ``extras`` is for arbitrary key/value flags (e.g. labels
    like ``cuda_version=12.4``).
    """

    cpu: int = 0
    memory: int = 0          # MB
    gpu: int = 0
    gpu_memory: int = 0      # MB total across GPUs
    extras: dict[str, Any] = Field(default_factory=dict)


class WorkerRegisterRequest(BaseModel):
    name: str
    capabilities: Capabilities = Field(default_factory=Capabilities)


class WorkerRegisterResponse(BaseModel):
    worker_id: str
    heartbeat_interval: float = 10.0  # seconds


class HeartbeatResponse(BaseModel):
    ok: bool = True
    revoked_job_ids: list[str] = Field(default_factory=list)


class WorkerInfo(BaseModel):
    """View-side projection of a registered worker."""

    id: str
    name: str
    capabilities: Capabilities
    current_load: dict[str, float] = Field(default_factory=dict)
    assigned_jobs: list[str] = Field(default_factory=list)
    last_heartbeat: float = 0.0
    age_seconds: float = 0.0
    health: str = "green"   # green | amber | red


class HealthResponse(BaseModel):
    ok: bool = True
    uptime: float = 0.0
    workers: int = 0
    pending_jobs: int = 0
    running_jobs: int = 0


# ── Job dispatch (Phase 2+) ────────────────────────────────────────


class BlobUploadResponse(BaseModel):
    hash: str
    size: int


class JobSubmitRequest(BaseModel):
    flow_name: str = ""
    run_id: str = ""
    task_name: str = ""
    task_hash: str = ""
    func_blob_hash: str
    args_blob_hash: str
    kwargs_blob_hash: str
    project_hash: str = ""           # empty when no project archive yet
    capture: bool = True
    timeout: float | None = None
    resources: dict[str, Any] = Field(default_factory=dict)


class JobSubmitResponse(BaseModel):
    job_id: str


class JobAssignment(BaseModel):
    """Returned from GET /jobs/next when a job is available."""

    job_id: str
    flow_name: str
    run_id: str
    task_name: str
    task_hash: str
    func_blob_hash: str
    args_blob_hash: str
    kwargs_blob_hash: str
    project_hash: str = ""
    capture: bool = True
    timeout: float | None = None


class JobResultRequest(BaseModel):
    success: bool
    result_blob_hash: str = ""
    error_blob_hash: str = ""
    stdout: str = ""
    stderr: str = ""


class JobStatus(BaseModel):
    job_id: str
    status: str                      # pending | assigned | running | success | failed | revoked
    worker_id: str | None = None
    result_blob_hash: str = ""
    error_blob_hash: str = ""
    stdout: str = ""
    stderr: str = ""
    submitted_at: float = 0.0
    finished_at: float | None = None

