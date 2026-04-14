"""SDK snapshot workflow: fetch + extract + subprocess isolation.

These tests avoid exercising the real AutoProvisioner (which runs uv
and builds a venv) — provisioning is off by default so the tests stay
fast and hermetic. One test exercises ``run_in_snapshot_subprocess``
using the *system* python to confirm the subprocess + cwd plumbing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")

import httpx  # noqa: E402

from rinnsal.cluster.archive import build_project_archive  # noqa: E402
from rinnsal.sdk.snapshot import (  # noqa: E402
    run_in_snapshot_subprocess,
    with_snapshot,
)
from rinnsal.viewer._data import invalidate_caches  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_caches():
    invalidate_caches()
    yield
    invalidate_caches()


@pytest.fixture
def server_with_snapshot(tmp_path):
    """Stand up a server with a snapshot dir and return (client, hash)."""
    from fastapi.testclient import TestClient

    from rinnsal.viewer.backend.main import app

    # NOTE: log root cannot be named ".rinnsal" because the archive
    # walker excludes any path containing ".rinnsal" — which would make
    # the server produce an empty tarball for anything under it.
    root = tmp_path / "logroot"
    snap_src = tmp_path / "src"
    snap_src.mkdir()
    (snap_src / "hello.py").write_text(
        "import sys\nprint('from snapshot', sys.argv[1:])\n"
    )
    _data, snap_hash = build_project_archive(snap_src)
    # Place under <root>/snapshots/<hash>/ so the endpoint can find it.
    dest = root / "snapshots" / snap_hash
    dest.mkdir(parents=True)
    (dest / "hello.py").write_text((snap_src / "hello.py").read_text())

    tc = TestClient(app)

    def _handler(request: httpx.Request) -> httpx.Response:
        r = tc.request(
            request.method,
            str(request.url.raw_path, "ascii"),
            content=request.content,
            headers=dict(request.headers),
        )
        return httpx.Response(
            status_code=r.status_code,
            headers=dict(r.headers),
            content=r.content,
        )

    from rinnsal.sdk.client import Client
    c = Client("http://test", root=str(root))
    c._http.close()
    c._http = httpx.Client(
        transport=httpx.MockTransport(_handler),
        base_url="http://test",
    )
    return c, snap_hash, tmp_path


class TestWithSnapshot:
    def test_extract_only(self, server_with_snapshot, tmp_path):
        client, snap_hash, _ = server_with_snapshot
        cache_root = tmp_path / "cache"
        with with_snapshot(
            client, snap_hash,
            cache_root=cache_root, provision=False,
        ) as env:
            assert env.snapshot_hash == snap_hash
            assert env.src_dir == cache_root / snap_hash / "src"
            assert (env.src_dir / "hello.py").is_file()
            assert (cache_root / snap_hash / ".ready").exists()

    def test_reuses_cache(self, server_with_snapshot, tmp_path):
        client, snap_hash, _ = server_with_snapshot
        cache_root = tmp_path / "cache"
        with with_snapshot(
            client, snap_hash,
            cache_root=cache_root, provision=False,
        ) as env1:
            mtime1 = (env1.src_dir / "hello.py").stat().st_mtime
        # Second call must not re-download — we can detect via the
        # sentinel already being present.
        with with_snapshot(
            client, snap_hash,
            cache_root=cache_root, provision=False,
        ) as env2:
            assert env2.src_dir == env1.src_dir
            assert (env2.src_dir / "hello.py").stat().st_mtime == mtime1

    def test_run_subprocess(self, server_with_snapshot, tmp_path):
        client, snap_hash, _ = server_with_snapshot
        cache_root = tmp_path / "cache"
        # Materialize first (provision=False), then exec with the system
        # python since no venv was built.
        with with_snapshot(
            client, snap_hash,
            cache_root=cache_root, provision=False,
        ) as env:
            pass
        # Sneak the system interpreter in as the sentinel's python.
        (cache_root / snap_hash / ".venv" / "bin").mkdir(
            parents=True, exist_ok=True
        )
        (cache_root / snap_hash / ".venv" / "bin" / "python").symlink_to(
            sys.executable
        )

        result = run_in_snapshot_subprocess(
            client, snap_hash,
            ["hello.py", "world"],
            cache_root=cache_root,
            provision=False,
        )
        assert result.returncode == 0
        assert "from snapshot" in result.stdout
        assert "world" in result.stdout
