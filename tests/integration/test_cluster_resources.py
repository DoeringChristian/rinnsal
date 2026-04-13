"""Cluster Phase 4: resource-aware scheduling."""

from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from rinnsal.cluster.coordinator import (
    CoordinatorState,
    router as cluster_router,
)
from rinnsal.cluster.protocol import JobSubmitRequest
from rinnsal.cluster.scheduler import (
    FIFOClusterScheduler,
    ResourceMatchingClusterScheduler,
)


# ── unit-level scheduler tests ─────────────────────────────────────


class TestResourceMatcher:
    def setup_method(self):
        self.s = ResourceMatchingClusterScheduler()

    def test_no_pending_returns_none(self):
        assert self.s.pick_job({"cpu": 4}, {}, []) is None

    def test_first_match_wins(self):
        jobs = [
            {"job_id": "a", "resources": {"cpu": 8}},  # too big
            {"job_id": "b", "resources": {"cpu": 2}},  # fits
            {"job_id": "c", "resources": {"cpu": 1}},  # also fits
        ]
        assert self.s.pick_job({"cpu": 4}, {}, jobs) == 1

    def test_no_match_returns_none(self):
        jobs = [{"job_id": "a", "resources": {"gpu": 1}}]
        assert self.s.pick_job({"cpu": 4, "gpu": 0}, {}, jobs) is None

    def test_load_subtracts_from_capacity(self):
        # Worker has 8 cpu; 6 already in use; only 2 free → an 8-cpu
        # job no longer fits but a 2-cpu one does.
        jobs = [
            {"job_id": "a", "resources": {"cpu": 8}},
            {"job_id": "b", "resources": {"cpu": 2}},
        ]
        assert self.s.pick_job({"cpu": 8}, {"cpu": 6}, jobs) == 1

    def test_unknown_resource_key_blocks(self):
        # Worker has no `tpu` capability; job requires tpu → no match.
        jobs = [{"job_id": "a", "resources": {"tpu": 1}}]
        assert self.s.pick_job({"cpu": 8}, {}, jobs) is None

    def test_empty_requirements_always_fit(self):
        jobs = [{"job_id": "a", "resources": {}}]
        assert self.s.pick_job({"cpu": 1}, {}, jobs) == 0


# ── coordinator-level: resource-aware assignment ───────────────────


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


def _register_worker(client, name: str, **caps) -> str:
    r = client.post(
        "/api/cluster/workers/register",
        json={"name": name, "capabilities": caps},
    )
    return r.json()["worker_id"]


def _submit(client, **resources) -> str:
    r = client.post(
        "/api/cluster/jobs/submit",
        json={
            "func_blob_hash": "f",
            "args_blob_hash": "a",
            "kwargs_blob_hash": "k",
            "resources": resources,
        },
    )
    return r.json()["job_id"]


class TestCoordinatorResourceMatching:
    def test_gpu_job_lands_on_gpu_worker(self, client):
        gpu = _register_worker(client, "gpu-worker", cpu=4, gpu=1)
        cpu = _register_worker(client, "cpu-worker", cpu=4, gpu=0)
        job_id = _submit(client, gpu=1)

        # CPU-only worker should NOT get the job.
        r_cpu = client.get(
            f"/api/cluster/jobs/next?worker_id={cpu}&timeout=1"
        )
        assert r_cpu.status_code == 204

        # GPU worker should.
        r_gpu = client.get(
            f"/api/cluster/jobs/next?worker_id={gpu}&timeout=1"
        )
        assert r_gpu.status_code == 200
        assert r_gpu.json()["job_id"] == job_id

    def test_load_prevents_over_commit(self, client):
        wid = _register_worker(client, "w", cpu=4)
        # Two jobs each needing 3 cpus → only the first should fit;
        # the second waits until the first completes.
        j1 = _submit(client, cpu=3)
        j2 = _submit(client, cpu=3)

        r = client.get(
            f"/api/cluster/jobs/next?worker_id={wid}&timeout=1"
        )
        assert r.status_code == 200
        assert r.json()["job_id"] == j1

        # Worker pulls again — second job doesn't fit (load=3, free=1).
        r2 = client.get(
            f"/api/cluster/jobs/next?worker_id={wid}&timeout=1"
        )
        assert r2.status_code == 204

        # Complete j1 → worker frees up → j2 becomes assignable.
        client.post(
            f"/api/cluster/jobs/{j1}/result",
            json={"success": True, "result_blob_hash": "ok"},
        )
        r3 = client.get(
            f"/api/cluster/jobs/next?worker_id={wid}&timeout=1"
        )
        assert r3.status_code == 200
        assert r3.json()["job_id"] == j2


class TestSwappableScheduler:
    def test_fifo_scheduler_does_no_resource_matching(self, app_and_state):
        app, _ = app_and_state
        # Replace the scheduler in a fresh state.
        from rinnsal.cluster.coordinator import CoordinatorState

        state = CoordinatorState(scheduler=FIFOClusterScheduler())
        app.state.cluster = state
        client = TestClient(app)

        wid = _register_worker(client, "w", cpu=1)
        # Job wants 999 cpus — FIFO ignores resources and assigns anyway.
        job_id = _submit(client, cpu=999)
        r = client.get(
            f"/api/cluster/jobs/next?worker_id={wid}&timeout=1"
        )
        assert r.status_code == 200
        assert r.json()["job_id"] == job_id
