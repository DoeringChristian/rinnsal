"""Cluster Phase 2: blob upload, job submission, ClusterExecutor round-trip.

Single-process smoke: a CoordinatorState mounted on a FastAPI app, a
WorkerDaemon driven by the same TestClient, and a ClusterExecutor that
also speaks via TestClient. Verify a real ``@task`` round-trips end-to-end.
"""

from __future__ import annotations

import hashlib
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
from rinnsal.cluster.protocol import (
    Capabilities,
    JobSubmitRequest,
)
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


# Adapter: TestClient → WorkerDaemon/ClusterExecutor transport.
class _TestClientTransport:
    def __init__(self, client: TestClient) -> None:
        self._client = client

    def __call__(
        self,
        method: str,
        path: str,
        *,
        json=None,
        _binary: bool = False,
        _bytes: bytes | None = None,
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


# ── Blob endpoints ─────────────────────────────────────────────────


class TestBlobs:
    def test_round_trip(self, client):
        data = b"some content"
        h = hashlib.sha256(data).hexdigest()
        r = client.put(f"/api/cluster/blobs/{h}", content=data)
        assert r.status_code == 200
        assert r.json()["size"] == len(data)

        r2 = client.get(f"/api/cluster/blobs/{h}")
        assert r2.status_code == 200
        assert r2.content == data

    def test_hash_mismatch_400(self, client):
        r = client.put("/api/cluster/blobs/" + ("0" * 64), content=b"abc")
        assert r.status_code == 400

    def test_unknown_blob_404(self, client):
        r = client.get(f"/api/cluster/blobs/{'0' * 64}")
        assert r.status_code == 404


# ── Project archives ───────────────────────────────────────────────


class TestProjectArchives:
    def test_round_trip(self, client):
        data = b"<imagine a tarball here>"
        h = hashlib.sha256(data).hexdigest()
        r = client.put(
            f"/api/cluster/projects/{h}/archive", content=data
        )
        assert r.status_code == 200

        r2 = client.get(f"/api/cluster/projects/{h}/archive")
        assert r2.status_code == 200
        assert r2.content == data

    def test_missing_404(self, client):
        r = client.get(f"/api/cluster/projects/{'0' * 64}/archive")
        assert r.status_code == 404


# ── Job lifecycle (low-level) ──────────────────────────────────────


def _upload_pickle(client: TestClient, obj) -> str:
    data = cloudpickle.dumps(obj)
    h = hashlib.sha256(data).hexdigest()
    client.put(f"/api/cluster/blobs/{h}", content=data).raise_for_status()
    return h


class TestJobLifecycle:
    def test_submit_assign_complete(self, client, app_and_state):
        _, state = app_and_state

        # Register a worker first.
        r = client.post(
            "/api/cluster/workers/register",
            json={"name": "w", "capabilities": {"cpu": 1}},
        )
        worker_id = r.json()["worker_id"]

        # Upload pickled func/args/kwargs (computing an addition remotely).
        def add(a, b):
            return a + b

        func_hash = _upload_pickle(client, add)
        args_hash = _upload_pickle(client, (3, 4))
        kwargs_hash = _upload_pickle(client, {})

        # Submit the job.
        body = client.post(
            "/api/cluster/jobs/submit",
            json={
                "func_blob_hash": func_hash,
                "args_blob_hash": args_hash,
                "kwargs_blob_hash": kwargs_hash,
            },
        ).json()
        job_id = body["job_id"]
        assert job_id

        # Worker pulls the job (short timeout for the test).
        r = client.get(
            f"/api/cluster/jobs/next?worker_id={worker_id}&timeout=1",
        )
        assert r.status_code == 200
        job = r.json()
        assert job["job_id"] == job_id
        assert job["func_blob_hash"] == func_hash

        # Worker completes the job.
        result = 7
        result_hash = _upload_pickle(client, result)
        client.post(
            f"/api/cluster/jobs/{job_id}/result",
            json={"success": True, "result_blob_hash": result_hash},
        ).raise_for_status()

        # Submitter sees terminal status.
        status = client.get(
            f"/api/cluster/jobs/{job_id}/status?timeout=1"
        ).json()
        assert status["status"] == "success"
        assert status["result_blob_hash"] == result_hash

    def test_jobs_next_204_on_timeout(self, client):
        client.post(
            "/api/cluster/workers/register",
            json={"name": "w", "capabilities": {"cpu": 1}},
        )
        wid = client.get("/api/cluster/workers").json()[0]["id"]
        r = client.get(
            f"/api/cluster/jobs/next?worker_id={wid}&timeout=1"
        )
        assert r.status_code == 204


# ── End-to-end: ClusterExecutor + in-process worker ────────────────


class _FakeTaskDef:
    """Minimal task_def with a None resources field, matching @task."""
    resources = None


class _FakeTaskExpression:
    """Tiny stand-in for TaskExpression usable by ClusterExecutor.submit."""

    task_def = _FakeTaskDef()
    task_name = "remote_add"
    hash = "fakehash"

    def __init__(self, func) -> None:
        self.func = func


def _drive_worker_until(daemon: WorkerDaemon, predicate, timeout=5.0):
    """Run the daemon's job loop in a thread; stop once predicate()."""
    t = threading.Thread(target=daemon._job_loop, daemon=True)
    t.start()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            break
        time.sleep(0.05)
    daemon._stop_event.set()
    t.join(timeout=2.0)


class TestEndToEnd:
    def test_remote_task_round_trip(self, client, app_and_state):
        _, state = app_and_state
        transport = _TestClientTransport(client)

        # Worker side.
        daemon = WorkerDaemon(
            "http://test",
            name="w-e2e",
            capabilities=Capabilities(cpu=1),
            transport=transport,
        )
        daemon.register()

        # Submitter side: a ClusterExecutor that talks to the same coordinator.
        executor = ClusterExecutor(
            "http://test", capture=True, transport=transport,
            poll_timeout=1.0,
        )

        def add(a, b):
            print("hello from worker")
            return a + b

        expr = _FakeTaskExpression(add)
        future = executor.submit(expr, (10, 32), {})

        # Drive the worker until the job completes.
        def _done() -> bool:
            return future.done() or any(
                j.status in {"success", "failed"}
                for j in state.jobs.values()
            )

        _drive_worker_until(daemon, _done, timeout=10.0)

        result = future.result(timeout=5.0)
        assert result.success is True
        assert result.value == 42
        assert "hello from worker" in result.stdout
        executor.shutdown(wait=True)
