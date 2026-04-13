"""Cluster Phase 5: live event streaming, GC, and revocation."""

from __future__ import annotations

import threading
import time

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import cloudpickle  # noqa: E402

from rinnsal.cluster.coordinator import (
    CoordinatorState,
    router as cluster_router,
)
from rinnsal.cluster.protocol import Capabilities
from rinnsal.cluster.worker import WorkerDaemon
from rinnsal.compute.cluster import ClusterExecutor


@pytest.fixture
def app_and_state():
    app = FastAPI()
    state = CoordinatorState()
    app.state.cluster = state
    app.include_router(cluster_router, prefix="/api/cluster")
    return app, state


@pytest.fixture
def client(app_and_state):
    app, _ = app_and_state
    return TestClient(app)


class _TestClientTransport:
    def __init__(self, client: TestClient) -> None:
        self._client = client

    def __call__(
        self, method, path, *,
        json=None, _binary=False, _bytes=None,
    ):
        if _bytes is not None:
            r = self._client.request(method, path, content=_bytes)
        else:
            r = self._client.request(method, path, json=json)
        if r.status_code == 204:
            return None
        r.raise_for_status()
        if _binary:
            return r.content
        ct = r.headers.get("content-type", "")
        if ct.startswith("application/json"):
            return r.json()
        return r.text


# ── GC + re-queue ──────────────────────────────────────────────────


class TestGCRequeue:
    def test_dead_worker_jobs_requeued(self, app_and_state):
        _, state = app_and_state
        # Submit a job to a registered worker that will go stale.
        from rinnsal.cluster.protocol import (
            JobSubmitRequest,
            WorkerRegisterRequest,
        )

        ws = state.register_worker(WorkerRegisterRequest(name="dying"))
        rec = state.submit_job(
            JobSubmitRequest(
                func_blob_hash="f",
                args_blob_hash="a",
                kwargs_blob_hash="k",
            )
        )
        assigned = state.assign_job(ws.id)
        assert assigned is not None
        assert rec.id in state.workers[ws.id].assigned_jobs

        # Force the heartbeat into the past, then GC.
        state.workers[ws.id].last_heartbeat = time.time() - 1000
        dropped = state.gc_dead_workers()
        assert ws.id in dropped
        assert ws.id not in state.workers
        # Job is back in pending and reset to status="pending".
        assert rec.id in state.pending
        assert rec.status == "pending"


# ── revocation ─────────────────────────────────────────────────────


class TestRevocation:
    def test_revoke_sets_status_and_unblocks_status_long_poll(self, client):
        # Register worker so submit can be assigned.
        client.post(
            "/api/cluster/workers/register",
            json={"name": "w", "capabilities": {"cpu": 1}},
        )
        sub = client.post(
            "/api/cluster/jobs/submit",
            json={
                "func_blob_hash": "f",
                "args_blob_hash": "a",
                "kwargs_blob_hash": "k",
            },
        ).json()
        job_id = sub["job_id"]

        r = client.post(f"/api/cluster/jobs/{job_id}/revoke")
        assert r.status_code == 200

        status = client.get(
            f"/api/cluster/jobs/{job_id}/status?timeout=0"
        ).json()
        assert status["status"] == "revoked"

    def test_revoke_unknown_404(self, client):
        r = client.post(f"/api/cluster/jobs/{'0' * 32}/revoke")
        assert r.status_code == 404


# ── live event streaming (logger events round-trip) ────────────────


def _drive_worker_until(daemon: WorkerDaemon, predicate, timeout=5.0):
    t = threading.Thread(target=daemon._job_loop, daemon=True)
    t.start()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            break
        time.sleep(0.05)
    daemon._stop_event.set()
    t.join(timeout=2.0)


class _FakeTaskDef:
    resources = None


class _FakeTaskExpression:
    task_def = _FakeTaskDef()
    task_name = "logging_task"
    hash = "h"

    def __init__(self, func) -> None:
        self.func = func


class _CollectingEventWriter:
    """Captures Events written by replay_events."""

    def __init__(self) -> None:
        self.events: list = []

    def write(self, event):
        self.events.append(event)

    def flush(self) -> None:
        pass


class _CollectingLogger:
    """Stand-in logger that decodes events from its captured writer."""

    def __init__(self) -> None:
        self._event_writer = _CollectingEventWriter()

    @property
    def scalars(self) -> list[tuple[str, float, int]]:
        out = []
        for ev in self._event_writer.events:
            if ev.WhichOneof("data") == "scalar":
                out.append((ev.scalar.tag, ev.scalar.value, ev.iteration))
        return out

    @property
    def text(self) -> list[tuple[str, str, int]]:
        out = []
        for ev in self._event_writer.events:
            if ev.WhichOneof("data") == "text":
                out.append((ev.text.tag, ev.text.value, ev.iteration))
        return out


class TestLoggerEventStreaming:
    def test_worker_logger_events_replay_into_orchestrator_logger(
        self, client, app_and_state,
    ):
        _, state = app_and_state
        transport = _TestClientTransport(client)

        daemon = WorkerDaemon(
            "http://test",
            name="w-events",
            capabilities=Capabilities(cpu=1),
            transport=transport,
        )
        daemon.register()

        # ClusterExecutor pointed at the same coordinator. Inject a
        # fake logger so we can assert on what got replayed.
        executor = ClusterExecutor(
            "http://test",
            capture=False,
            transport=transport,
            poll_timeout=1.0,
            ship_project=False,
        )
        captured = _CollectingLogger()
        executor.set_logger(captured)

        def task_with_logging():
            from rinnsal.context import current

            current.logger.add_scalar("loss", 0.42, it=1)
            current.logger.add_text("status", "trained", it=1)
            return "done"

        future = executor.submit(
            _FakeTaskExpression(task_with_logging), (), {}
        )

        def _done() -> bool:
            return future.done() or any(
                j.status in {"success", "failed"}
                for j in state.jobs.values()
            )

        _drive_worker_until(daemon, _done, timeout=10.0)

        result = future.result(timeout=5.0)
        assert result.success is True
        assert result.value == "done"

        # Logger events should have been replayed into the captured logger.
        assert ("loss", 0.42, 1) in captured.scalars
        assert any(
            tag == "status" and val == "trained"
            for tag, val, _ in captured.text
        )
        executor.shutdown(wait=True)
