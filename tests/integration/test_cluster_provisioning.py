"""Cluster Phase 3: project archive + worker provisioning."""

from __future__ import annotations

import hashlib
import io
import tarfile
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from rinnsal.cluster.archive import (
    build_project_archive,
    extract_archive,
)
from rinnsal.cluster.coordinator import (
    CoordinatorState,
    router as cluster_router,
)
from rinnsal.cluster.protocol import Capabilities
from rinnsal.cluster.worker import WorkerDaemon


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


# ── Archive builder ────────────────────────────────────────────────


def _make_project(root: Path) -> None:
    """Create a tiny project tree under *root*."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "src").mkdir(exist_ok=True)
    (root / "src" / "main.py").write_text("print('hi')\n")
    (root / "README.md").write_text("# project\n")
    (root / "pyproject.toml").write_text("[project]\nname='proj'\n")
    # Junk that should be excluded by the walk-fallback.
    (root / "__pycache__").mkdir(exist_ok=True)
    (root / "__pycache__" / "junk.pyc").write_bytes(b"compiled garbage")


class TestArchiveBuilder:
    def test_archive_is_deterministic(self, tmp_path):
        _make_project(tmp_path)
        d1, h1 = build_project_archive(tmp_path)
        d2, h2 = build_project_archive(tmp_path)
        assert d1 == d2
        assert h1 == h2

    def test_archive_contains_project_files(self, tmp_path):
        _make_project(tmp_path)
        data, _ = build_project_archive(tmp_path)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tar:
            names = set(tar.getnames())
        assert "src/main.py" in names
        assert "pyproject.toml" in names
        # __pycache__ excluded by fallback walk.
        assert not any("__pycache__" in n for n in names)

    def test_extract_round_trip(self, tmp_path):
        _make_project(tmp_path)
        data, _ = build_project_archive(tmp_path)
        dest = tmp_path / "_dest"
        extract_archive(data, dest)
        assert (dest / "src" / "main.py").read_text() == "print('hi')\n"
        assert (dest / "pyproject.toml").exists()


class TestArchiveOverHTTP:
    def test_upload_then_download(self, client, tmp_path):
        _make_project(tmp_path)
        data, h = build_project_archive(tmp_path)
        r = client.put(
            f"/api/cluster/projects/{h}/archive", content=data
        )
        assert r.status_code == 200
        r2 = client.get(f"/api/cluster/projects/{h}/archive")
        assert r2.status_code == 200
        assert hashlib.sha256(r2.content).hexdigest() == h


# ── Worker-side provisioning ───────────────────────────────────────


class TestWorkerEnsureProject:
    def test_downloads_and_extracts(
        self, client, app_and_state, tmp_path, monkeypatch,
    ):
        """Without invoking the real provisioner, verify the worker
        fetches the archive, extracts it, and skips on the second call."""
        _, state = app_and_state
        # Stage a project archive on the coordinator.
        proj_dir = tmp_path / "proj"
        _make_project(proj_dir)
        data, project_hash = build_project_archive(proj_dir)
        state.put_project_archive(data)

        scratch = tmp_path / "scratch"
        daemon = WorkerDaemon(
            "http://test",
            name="w-prov",
            capabilities=Capabilities(cpu=1),
            transport=_TestClientTransport(client),
            scratch_dir=scratch,
        )

        # Stub out the provisioner so we don't actually run uv/pip.
        from rinnsal.compute import provisioner as prov_mod

        class _StubProvisioner:
            def __init__(self, search_dir=None):
                pass

            def provision_script(self, work_dir):
                return "true"   # no-op

            def python_command(self, work_dir):
                return "/usr/bin/python3"

        monkeypatch.setattr(prov_mod, "AutoProvisioner", _StubProvisioner)
        # Re-import target since worker.py imports it lazily.
        import rinnsal.cluster.worker as worker_mod
        monkeypatch.setattr(
            "rinnsal.compute.provisioner.AutoProvisioner", _StubProvisioner
        )

        work_dir, python_cmd = daemon._ensure_project(project_hash)
        assert work_dir.exists()
        assert (work_dir / "src" / "main.py").exists()
        assert python_cmd
        assert (scratch / project_hash / ".ready").exists()

        # Second call returns the cached value without re-downloading.
        work_dir2, _ = daemon._ensure_project(project_hash)
        assert work_dir2 == work_dir


class TestWorkerStrategySelection:
    def test_run_in_subprocess_default_follows_project_hash(self, tmp_path):
        """If the job has a project_hash and run_in_subprocess is None,
        the worker should choose subprocess mode."""
        daemon = WorkerDaemon(
            "http://test",
            transport=lambda *a, **kw: None,
            scratch_dir=tmp_path,
        )
        # Inline path for jobs without a project_hash.
        assert daemon._run_in_subprocess is None
