"""Cluster Phase 1: worker registration + heartbeat + GC."""

from __future__ import annotations

import time

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from rinnsal.cluster.coordinator import (
    CoordinatorState,
    router as cluster_router,
)
from rinnsal.cluster.protocol import (
    Capabilities,
    WorkerRegisterRequest,
)
from rinnsal.cluster.worker import WorkerDaemon


@pytest.fixture
def app_and_state():
    """Fresh app + state per test (no shared singleton)."""
    app = FastAPI()
    state = CoordinatorState()
    app.state.cluster = state
    app.include_router(cluster_router, prefix="/api/cluster")
    return app, state


@pytest.fixture
def client(app_and_state):
    app, _state = app_and_state
    return TestClient(app)


# ── unit-level state tests ──────────────────────────────────────────


class TestCoordinatorState:
    def test_register_assigns_unique_id(self):
        state = CoordinatorState()
        a = state.register_worker(WorkerRegisterRequest(name="a"))
        b = state.register_worker(WorkerRegisterRequest(name="b"))
        assert a.id != b.id
        assert {w.id for w in state.workers.values()} == {a.id, b.id}

    def test_heartbeat_updates_timestamp(self):
        state = CoordinatorState()
        ws = state.register_worker(WorkerRegisterRequest(name="a"))
        original = ws.last_heartbeat
        time.sleep(0.01)
        state.heartbeat(ws.id)
        assert state.workers[ws.id].last_heartbeat > original

    def test_heartbeat_unknown_raises(self):
        state = CoordinatorState()
        with pytest.raises(KeyError):
            state.heartbeat("nope")

    def test_gc_evicts_stale(self):
        state = CoordinatorState()
        ws = state.register_worker(WorkerRegisterRequest(name="dead"))
        # Force last_heartbeat far into the past.
        state.workers[ws.id].last_heartbeat = time.time() - 1000
        dropped = state.gc_dead_workers()
        assert ws.id in dropped
        assert ws.id not in state.workers

    def test_gc_keeps_fresh(self):
        state = CoordinatorState()
        ws = state.register_worker(WorkerRegisterRequest(name="alive"))
        dropped = state.gc_dead_workers()
        assert dropped == []
        assert ws.id in state.workers

    def test_health_counts(self):
        state = CoordinatorState()
        state.register_worker(WorkerRegisterRequest(name="a"))
        state.register_worker(WorkerRegisterRequest(name="b"))
        h = state.health()
        assert h.workers == 2
        assert h.pending_jobs == 0
        assert h.uptime >= 0


# ── HTTP endpoint tests ────────────────────────────────────────────


class TestRegisterEndpoint:
    def test_returns_worker_id(self, client):
        r = client.post(
            "/api/cluster/workers/register",
            json={"name": "alpha", "capabilities": {"cpu": 4, "gpu": 1}},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["worker_id"]
        assert body["heartbeat_interval"] > 0

    def test_register_then_list(self, client):
        client.post(
            "/api/cluster/workers/register",
            json={"name": "alpha", "capabilities": {"cpu": 4}},
        )
        client.post(
            "/api/cluster/workers/register",
            json={"name": "beta", "capabilities": {"cpu": 8}},
        )
        r = client.get("/api/cluster/workers")
        assert r.status_code == 200
        names = {w["name"] for w in r.json()}
        assert names == {"alpha", "beta"}


class TestHeartbeatEndpoint:
    def test_unknown_worker_404(self, client):
        r = client.post("/api/cluster/workers/nonexistent/heartbeat")
        assert r.status_code == 404

    def test_known_worker_ok(self, client):
        r = client.post(
            "/api/cluster/workers/register",
            json={"name": "x", "capabilities": {"cpu": 1}},
        )
        wid = r.json()["worker_id"]
        r2 = client.post(f"/api/cluster/workers/{wid}/heartbeat")
        assert r2.status_code == 200
        assert r2.json() == {"ok": True, "revoked_job_ids": []}


class TestHealthEndpoint:
    def test_empty_cluster(self, client):
        r = client.get("/api/cluster/health")
        body = r.json()
        assert body["ok"] is True
        assert body["workers"] == 0
        assert body["pending_jobs"] == 0

    def test_worker_count_increases(self, client):
        client.post(
            "/api/cluster/workers/register",
            json={"name": "a", "capabilities": {"cpu": 1}},
        )
        body = client.get("/api/cluster/health").json()
        assert body["workers"] == 1


# ── WorkerDaemon end-to-end via TestClient transport ───────────────


class _TestClientTransport:
    """Adapt FastAPI TestClient to the WorkerDaemon `transport` protocol."""

    def __init__(self, client: TestClient) -> None:
        self._client = client

    def __call__(self, method: str, path: str, *, json=None):
        r = self._client.request(method, path, json=json)
        r.raise_for_status()
        if r.headers.get("content-type", "").startswith("application/json"):
            return r.json()
        return r.text


class TestWorkerDaemon:
    def test_register_via_daemon(self, client):
        daemon = WorkerDaemon(
            "http://test",
            name="d1",
            capabilities=Capabilities(cpu=2),
            transport=_TestClientTransport(client),
        )
        wid = daemon.register()
        assert wid
        # Verify the row is in the coordinator.
        workers = client.get("/api/cluster/workers").json()
        assert any(w["id"] == wid for w in workers)

    def test_send_heartbeat(self, client):
        daemon = WorkerDaemon(
            "http://test",
            name="d2",
            capabilities=Capabilities(cpu=2),
            transport=_TestClientTransport(client),
        )
        daemon.register()
        revoked = daemon.send_heartbeat()
        assert revoked == []


class TestCapabilityDetection:
    def test_returns_sane_defaults(self):
        from rinnsal.cluster.worker import detect_capabilities

        c = detect_capabilities()
        assert c.cpu >= 1
        # memory may be 0 on non-Linux; just assert the field exists.
        assert hasattr(c, "memory")
        assert "hostname" in c.extras
