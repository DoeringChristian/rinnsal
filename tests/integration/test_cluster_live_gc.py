"""Cluster coordinator GC + revocation paths."""

from __future__ import annotations

import time

import pytest

fastapi = pytest.importorskip(
    "fastapi", reason="cluster extra not installed"
)
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from rinnsal.cluster.coordinator import (
    CoordinatorState,
    router as cluster_router,
)


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


# Prior builds had a LoggerEventStreaming test here. It was removed
# alongside the rinnsal Logger: runs now log directly to aim from the
# worker, so there's no orchestrator-side replay to assert.
