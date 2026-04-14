"""Cluster coordinator: in-process worker registry + job dispatch.

State lives in ``CoordinatorState`` and is mounted on ``app.state.cluster``.
Endpoints live under ``/api/cluster/`` and are mounted via the exported
``router``. A background GC task evicts stale workers whose heartbeats
expired and re-queues their assigned jobs.
"""

from __future__ import annotations

import asyncio
import collections
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
)

from rinnsal.cluster.protocol import (
    Capabilities,
    HealthResponse,
    HeartbeatResponse,
    JobAssignment,
    JobResultRequest,
    JobStatus,
    JobSubmitRequest,
    JobSubmitResponse,
    WorkerInfo,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
)


@dataclass
class WorkerState:
    """In-memory worker record."""

    id: str
    name: str
    capabilities: Capabilities
    current_load: dict[str, float] = field(default_factory=dict)
    assigned_jobs: set[str] = field(default_factory=set)
    last_heartbeat: float = 0.0
    revoked_jobs: list[str] = field(default_factory=list)


@dataclass
class JobRecord:
    """In-memory job record."""

    id: str
    request: JobSubmitRequest
    status: str = "pending"          # pending | assigned | running | success | failed | revoked
    worker_id: str | None = None
    result_blob_hash: str = ""
    error_blob_hash: str = ""
    stdout: str = ""
    stderr: str = ""
    submitted_at: float = 0.0
    finished_at: float | None = None
    # Set when the result lands so the submitter's GET /status long-poll
    # can wake up immediately.
    completion_event: threading.Event = field(default_factory=threading.Event)


class CoordinatorState:
    """In-process state for the cluster coordinator.

    Created once at server boot and stashed on ``app.state.cluster``.
    Workers are registered + heartbeated here; jobs are submitted by
    clients and assigned to workers via long-poll on /jobs/next.

    Heavy payloads (pickled func/args/kwargs, results) live as blobs
    on the configured blob store (when ``database`` is provided) or in
    an in-process dict when bare. The dict-only mode supports tests
    that don't need a real FileDatabase on disk.
    """

    HEARTBEAT_INTERVAL = 10.0
    DEAD_AFTER = 3 * HEARTBEAT_INTERVAL

    def __init__(self, database: Any = None, scheduler: Any = None) -> None:
        from rinnsal.cluster.scheduler import (
            ClusterScheduler,
            get_default_scheduler,
        )

        self.workers: dict[str, WorkerState] = {}
        self.jobs: dict[str, JobRecord] = {}
        self.pending: collections.deque[str] = collections.deque()
        # In-process lock — coordinator endpoints are async but state
        # mutations need atomicity across asyncio + the GC loop.
        self._lock = threading.Lock()
        self._started_at = time.time()
        self._gc_task: asyncio.Task | None = None
        self._shutting_down = False
        # Project archives keyed by hash → bytes (stored in blob store
        # when available, else in-memory).
        self._project_archives: dict[str, bytes] = {}
        # Optional content-addressed blob store (FileDatabase).
        self._database = database
        self._memory_blobs: dict[str, bytes] = {}
        # The aim tracking server URL the coordinator advertises via
        # GET /api/cluster/aim. Set by ``rinnsal cluster up --aim``;
        # ``None`` when not spawned so clients fall back to their local
        # aim repo.
        self.aim_repo_url: str | None = None
        # Asyncio Event that pulses whenever a job becomes assignable
        # (submission, worker registration, etc.) — wakes long-polls.
        self._job_available: asyncio.Event | None = None
        # Resource-aware scheduler. Phase 4 default is the resource
        # matcher; tests can inject FIFOClusterScheduler etc.
        self._scheduler: ClusterScheduler = (
            scheduler or get_default_scheduler()
        )

    # ── worker lifecycle ────────────────────────────────────────────

    def register_worker(self, req: WorkerRegisterRequest) -> WorkerState:
        worker_id = uuid.uuid4().hex
        ws = WorkerState(
            id=worker_id,
            name=req.name,
            capabilities=req.capabilities,
            last_heartbeat=time.time(),
        )
        with self._lock:
            self.workers[worker_id] = ws
        return ws

    def heartbeat(self, worker_id: str) -> list[str]:
        """Update last_heartbeat. Returns + clears any revoked job IDs."""
        with self._lock:
            ws = self.workers.get(worker_id)
            if ws is None:
                raise KeyError(worker_id)
            ws.last_heartbeat = time.time()
            revoked = ws.revoked_jobs[:]
            ws.revoked_jobs.clear()
            return revoked

    def list_workers(self) -> list[WorkerInfo]:
        now = time.time()
        with self._lock:
            return [
                self._project(ws, now) for ws in self.workers.values()
            ]

    def _project(self, ws: WorkerState, now: float) -> WorkerInfo:
        age = now - ws.last_heartbeat
        if age < self.HEARTBEAT_INTERVAL * 1.5:
            health = "green"
        elif age < self.DEAD_AFTER:
            health = "amber"
        else:
            health = "red"
        return WorkerInfo(
            id=ws.id,
            name=ws.name,
            capabilities=ws.capabilities,
            current_load=dict(ws.current_load),
            assigned_jobs=sorted(ws.assigned_jobs),
            last_heartbeat=ws.last_heartbeat,
            age_seconds=age,
            health=health,
        )

    # ── blob store ─────────────────────────────────────────────────

    def put_blob(self, data: bytes) -> str:
        """Store a blob; returns sha256 hex digest. Idempotent."""
        h = hashlib.sha256(data).hexdigest()
        if self._database is not None and hasattr(self._database, "put_blob"):
            try:
                self._database.put_blob(data)
                return h
            except Exception:
                pass
        with self._lock:
            self._memory_blobs[h] = data
        return h

    def get_blob(self, blob_hash: str) -> bytes | None:
        if self._database is not None and hasattr(self._database, "get_blob"):
            try:
                return self._database.get_blob(blob_hash)
            except (FileNotFoundError, OSError):
                pass
        with self._lock:
            return self._memory_blobs.get(blob_hash)

    # ── project archives ───────────────────────────────────────────

    def put_project_archive(self, data: bytes) -> str:
        """Store a project tarball; returns sha256 hex."""
        h = hashlib.sha256(data).hexdigest()
        with self._lock:
            self._project_archives[h] = data
        return h

    def get_project_archive(self, project_hash: str) -> bytes | None:
        with self._lock:
            return self._project_archives.get(project_hash)

    # ── jobs ───────────────────────────────────────────────────────

    def submit_job(self, req: JobSubmitRequest) -> JobRecord:
        job_id = uuid.uuid4().hex
        rec = JobRecord(
            id=job_id,
            request=req,
            submitted_at=time.time(),
        )
        with self._lock:
            self.jobs[job_id] = rec
            self.pending.append(job_id)
        self._notify_job_available()
        return rec

    def _notify_job_available(self) -> None:
        """Wake any long-polls waiting on /jobs/next."""
        ev = self._job_available
        if ev is None:
            return
        try:
            loop = ev._loop  # type: ignore[attr-defined]
            loop.call_soon_threadsafe(ev.set)
        except Exception:
            try:
                ev.set()
            except Exception:
                pass

    def assign_job(self, worker_id: str) -> JobRecord | None:
        """Pick the oldest pending job whose resources fit this worker.

        Uses the configured ``ClusterScheduler``. On assignment, the
        worker's ``current_load`` is incremented per resource so a
        worker can't over-commit. ``complete_job`` reverses this.
        """
        with self._lock:
            ws = self.workers.get(worker_id)
            if ws is None:
                return None
            if not self.pending:
                return None

            pending_views = []
            for jid in self.pending:
                rec = self.jobs.get(jid)
                if rec is None:
                    continue
                pending_views.append({
                    "job_id": jid,
                    "resources": dict(rec.request.resources or {}),
                })
            if not pending_views:
                return None

            idx = self._scheduler.pick_job(
                worker_capabilities=ws.capabilities.model_dump(),
                worker_load=dict(ws.current_load),
                pending_jobs=pending_views,
            )
            if idx is None:
                return None

            picked_id = pending_views[idx]["job_id"]
            # Remove it from the deque (preserve order of the rest).
            self.pending = collections.deque(
                jid for jid in self.pending if jid != picked_id
            )
            rec = self.jobs[picked_id]
            rec.status = "assigned"
            rec.worker_id = worker_id
            ws.assigned_jobs.add(picked_id)
            for k, v in (rec.request.resources or {}).items():
                try:
                    ws.current_load[k] = (
                        ws.current_load.get(k, 0.0) + float(v)
                    )
                except (TypeError, ValueError):
                    pass
            return rec

    def complete_job(
        self, job_id: str, body: JobResultRequest
    ) -> JobRecord | None:
        with self._lock:
            rec = self.jobs.get(job_id)
            if rec is None:
                return None
            rec.status = "success" if body.success else "failed"
            rec.result_blob_hash = body.result_blob_hash
            rec.error_blob_hash = body.error_blob_hash
            rec.stdout = body.stdout
            rec.stderr = body.stderr
            rec.finished_at = time.time()
            if rec.worker_id and rec.worker_id in self.workers:
                ws = self.workers[rec.worker_id]
                ws.assigned_jobs.discard(job_id)
                # Release the resources this job was holding.
                for k, v in (rec.request.resources or {}).items():
                    try:
                        new_val = ws.current_load.get(k, 0.0) - float(v)
                        if new_val <= 0:
                            ws.current_load.pop(k, None)
                        else:
                            ws.current_load[k] = new_val
                    except (TypeError, ValueError):
                        pass
        rec.completion_event.set()
        # A worker just freed up — pulse the long-poll signal.
        self._notify_job_available()
        return rec

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self.jobs.get(job_id)

    async def wait_for_job(
        self, worker_id: str, timeout: float = 30.0
    ) -> JobRecord | None:
        """Long-poll: return as soon as a job is assignable, or None on timeout."""
        ev = self._ensure_event()
        deadline = time.time() + timeout
        while True:
            assignment = self.assign_job(worker_id)
            if assignment is not None:
                return assignment
            remaining = deadline - time.time()
            if remaining <= 0:
                return None
            ev.clear()
            try:
                await asyncio.wait_for(ev.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return None

    async def wait_for_completion(
        self, job_id: str, timeout: float = 30.0
    ) -> JobRecord | None:
        """Submitter-side long-poll for terminal job state."""
        rec = self.get_job(job_id)
        if rec is None:
            return None
        if rec.status in {"success", "failed", "revoked"}:
            return rec
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, rec.completion_event.wait, timeout
            )
        except Exception:
            return self.get_job(job_id)
        return self.get_job(job_id)

    def _ensure_event(self) -> asyncio.Event:
        if self._job_available is None:
            self._job_available = asyncio.Event()
        return self._job_available

    # ── GC ─────────────────────────────────────────────────────────

    def gc_dead_workers(self, now: float | None = None) -> list[str]:
        """Evict workers whose heartbeat has timed out. Re-queues their jobs."""
        if now is None:
            now = time.time()
        cutoff = now - self.DEAD_AFTER
        dropped: list[str] = []
        with self._lock:
            dead = [w for w in self.workers.values() if w.last_heartbeat < cutoff]
            for ws in dead:
                # Re-queue any jobs the dead worker had taken.
                for job_id in list(ws.assigned_jobs):
                    rec = self.jobs.get(job_id)
                    if rec is not None and rec.status == "assigned":
                        rec.status = "pending"
                        rec.worker_id = None
                        self.pending.appendleft(job_id)
                self.workers.pop(ws.id, None)
                dropped.append(ws.id)
        if dropped:
            self._notify_job_available()
        return dropped

    async def _gc_loop(self) -> None:
        try:
            while not self._shutting_down:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                self.gc_dead_workers()
        except asyncio.CancelledError:
            pass

    def start_gc(self) -> None:
        """Start the background GC task. Idempotent."""
        if self._gc_task is not None and not self._gc_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # No event loop yet — startup hook will retry.
        self._gc_task = loop.create_task(self._gc_loop())

    async def shutdown(self) -> None:
        self._shutting_down = True
        if self._gc_task is not None:
            self._gc_task.cancel()
            try:
                await self._gc_task
            except asyncio.CancelledError:
                pass

    # ── health ─────────────────────────────────────────────────────

    def health(self) -> HealthResponse:
        with self._lock:
            return HealthResponse(
                ok=not self._shutting_down,
                uptime=time.time() - self._started_at,
                workers=len(self.workers),
                pending_jobs=len(self.pending),
                running_jobs=sum(
                    len(w.assigned_jobs) for w in self.workers.values()
                ),
            )


# ── FastAPI router ─────────────────────────────────────────────────

router = APIRouter()


def get_state(request: Request) -> CoordinatorState:
    """Dependency: pull CoordinatorState off ``app.state.cluster``."""
    state = getattr(request.app.state, "cluster", None)
    if state is None:
        raise HTTPException(
            status_code=503,
            detail="cluster mode not enabled on this server",
        )
    return state


@router.post("/workers/register", response_model=WorkerRegisterResponse)
def register_worker(
    req: WorkerRegisterRequest,
    state: CoordinatorState = Depends(get_state),
) -> WorkerRegisterResponse:
    state.start_gc()  # lazy-start the GC loop on first activity
    ws = state.register_worker(req)
    return WorkerRegisterResponse(
        worker_id=ws.id,
        heartbeat_interval=state.HEARTBEAT_INTERVAL,
    )


@router.post(
    "/workers/{worker_id}/heartbeat", response_model=HeartbeatResponse
)
def heartbeat(
    worker_id: str,
    state: CoordinatorState = Depends(get_state),
) -> HeartbeatResponse:
    try:
        revoked = state.heartbeat(worker_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"unknown worker {worker_id}",
        )
    return HeartbeatResponse(ok=True, revoked_job_ids=revoked)


@router.get("/workers", response_model=list[WorkerInfo])
def list_workers(
    state: CoordinatorState = Depends(get_state),
) -> list[WorkerInfo]:
    return state.list_workers()


@router.get("/health", response_model=HealthResponse)
def get_health(
    state: CoordinatorState = Depends(get_state),
) -> HealthResponse:
    return state.health()


@router.get("/aim")
def get_aim(
    state: CoordinatorState = Depends(get_state),
) -> dict[str, str | None]:
    """Advertise the aim tracking URL the coordinator spawned.

    ``rinnsal.aim.AimLogger`` queries this endpoint when it detects
    that the active executor is a :class:`ClusterExecutor` so remote
    tasks log to the shared tracking server rather than each worker's
    local filesystem. ``repo`` is ``null`` when the coordinator was
    started without ``--aim``.
    """
    return {"repo": state.aim_repo_url}


# ── Blob upload / download ────────────────────────────────────────


@router.put("/blobs/{blob_hash}")
async def upload_blob(
    blob_hash: str,
    request: Request,
    state: CoordinatorState = Depends(get_state),
) -> dict:
    body = await request.body()
    actual = hashlib.sha256(body).hexdigest()
    if actual != blob_hash:
        raise HTTPException(
            status_code=400,
            detail=f"hash mismatch: declared={blob_hash} actual={actual}",
        )
    state.put_blob(body)
    return {"hash": blob_hash, "size": len(body)}


@router.get("/blobs/{blob_hash}")
def download_blob(
    blob_hash: str,
    state: CoordinatorState = Depends(get_state),
) -> Response:
    data = state.get_blob(blob_hash)
    if data is None:
        raise HTTPException(status_code=404, detail="blob not found")
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "Cache-Control": "public, max-age=31536000, immutable",
            "ETag": f'"{blob_hash}"',
        },
    )


# ── Project archives ──────────────────────────────────────────────


@router.put("/projects/{project_hash}/archive")
async def upload_project_archive(
    project_hash: str,
    request: Request,
    state: CoordinatorState = Depends(get_state),
) -> dict:
    body = await request.body()
    actual = hashlib.sha256(body).hexdigest()
    if actual != project_hash:
        raise HTTPException(
            status_code=400,
            detail=f"hash mismatch: declared={project_hash} actual={actual}",
        )
    state.put_project_archive(body)
    return {"hash": project_hash, "size": len(body)}


@router.get("/projects/{project_hash}/archive")
def download_project_archive(
    project_hash: str,
    state: CoordinatorState = Depends(get_state),
) -> Response:
    data = state.get_project_archive(project_hash)
    if data is None:
        raise HTTPException(status_code=404, detail="archive not found")
    return Response(content=data, media_type="application/x-tar")


# ── Jobs ──────────────────────────────────────────────────────────


@router.post("/jobs/submit", response_model=JobSubmitResponse)
def submit_job(
    req: JobSubmitRequest,
    state: CoordinatorState = Depends(get_state),
) -> JobSubmitResponse:
    state.start_gc()
    rec = state.submit_job(req)
    return JobSubmitResponse(job_id=rec.id)


@router.get("/jobs/next")
async def get_next_job(
    worker_id: str = Query(...),
    timeout: float = Query(default=30.0, ge=1.0, le=120.0),
    state: CoordinatorState = Depends(get_state),
) -> Response:
    """Long-poll: return a job assigned to this worker, or 204 on timeout."""
    rec = await state.wait_for_job(worker_id, timeout=timeout)
    if rec is None:
        return Response(status_code=204)
    payload = JobAssignment(
        job_id=rec.id,
        flow_name=rec.request.flow_name,
        run_id=rec.request.run_id,
        task_name=rec.request.task_name,
        task_hash=rec.request.task_hash,
        func_blob_hash=rec.request.func_blob_hash,
        args_blob_hash=rec.request.args_blob_hash,
        kwargs_blob_hash=rec.request.kwargs_blob_hash,
        project_hash=rec.request.project_hash,
        capture=rec.request.capture,
        timeout=rec.request.timeout,
    )
    from fastapi.responses import JSONResponse

    return JSONResponse(content=payload.model_dump())


@router.post("/jobs/{job_id}/revoke")
def revoke_job(
    job_id: str,
    state: CoordinatorState = Depends(get_state),
) -> dict:
    """Mark a job revoked; the worker will see it on its next heartbeat."""
    rec = state.get_job(job_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="unknown job")
    with state._lock:
        rec.status = "revoked"
        if rec.worker_id and rec.worker_id in state.workers:
            state.workers[rec.worker_id].revoked_jobs.append(job_id)
            state.workers[rec.worker_id].assigned_jobs.discard(job_id)
        rec.finished_at = time.time()
    rec.completion_event.set()
    return {"ok": True}


@router.post("/jobs/{job_id}/result")
def post_job_result(
    job_id: str,
    body: JobResultRequest,
    state: CoordinatorState = Depends(get_state),
) -> dict:
    rec = state.complete_job(job_id, body)
    if rec is None:
        raise HTTPException(status_code=404, detail="unknown job")
    return {"ok": True}


@router.get("/jobs/{job_id}/status", response_model=JobStatus)
async def get_job_status(
    job_id: str,
    timeout: float = Query(default=30.0, ge=0.0, le=120.0),
    state: CoordinatorState = Depends(get_state),
) -> JobStatus:
    if timeout > 0:
        await state.wait_for_completion(job_id, timeout=timeout)
    rec = state.get_job(job_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="unknown job")
    return JobStatus(
        job_id=rec.id,
        status=rec.status,
        worker_id=rec.worker_id,
        result_blob_hash=rec.result_blob_hash,
        error_blob_hash=rec.error_blob_hash,
        stdout=rec.stdout,
        stderr=rec.stderr,
        submitted_at=rec.submitted_at,
        finished_at=rec.finished_at,
    )
