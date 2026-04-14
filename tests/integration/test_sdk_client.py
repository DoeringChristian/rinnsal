"""End-to-end SDK: Client + Run + Series against the real viewer app."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")

import httpx  # noqa: E402

from rinnsal.data.file_store import FileDatabase  # noqa: E402
from rinnsal.data.logger.logger import Logger  # noqa: E402
from rinnsal.data.metadata import (  # noqa: E402
    RunUpsert,
    SqliteMetadataStore,
)
from rinnsal.sdk import connect, parse_uri, resolve  # noqa: E402
from rinnsal.viewer._data import invalidate_caches  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_caches():
    invalidate_caches()
    yield
    invalidate_caches()


@pytest.fixture
def server_root(tmp_path):
    """Set up a server root with one run + metadata DB entry."""
    from rinnsal.viewer.backend.main import app

    root = tmp_path / ".rinnsal"
    rd = root / "flows/training/runs/r1"
    rd.mkdir(parents=True)

    db = FileDatabase(root=root)
    store = SqliteMetadataStore(root / "metadata.sqlite")
    store.upsert_flow("training")
    store.upsert_run(
        RunUpsert(
            run_id="r1",
            flow_name="training",
            run_dir=str(rd),
            status="success",
            started_at=time.time(),
            finished_at=time.time() + 1,
            snapshot_hash="deadbeef",
        )
    )

    lg = Logger(rd, database=db)
    lg.add_scalar("train/loss", 0.5, it=1)
    lg.add_scalar("train/loss", 0.3, it=2)
    lg.add_text("status", "trained", it=2)
    lg.close()

    # In-process transport: SDK Client talks to the viewer app via TestClient.
    from fastapi.testclient import TestClient

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

    return {
        "app": app,
        "root": str(root),
        "transport": httpx.MockTransport(_handler),
    }


def _make_client(server_root):
    from rinnsal.sdk.client import Client

    c = Client("http://test", root=server_root["root"])
    c._http.close()
    c._http = httpx.Client(
        transport=server_root["transport"],
        base_url="http://test",
    )
    return c


class TestClientBasic:
    def test_list_flows(self, server_root):
        with _make_client(server_root) as c:
            flows = c.list_flows()
            assert any(f["name"] == "training" for f in flows)

    def test_run_info(self, server_root):
        with _make_client(server_root) as c:
            info = c.run_info("r1")
            assert info["flow"] == "training"
            assert info["snapshot_hash"] == "deadbeef"

    def test_run_info_404(self, server_root):
        with _make_client(server_root) as c:
            with pytest.raises(httpx.HTTPStatusError):
                c.run_info("does-not-exist")


class TestRunHandles:
    def test_scalars_series(self, server_root):
        with _make_client(server_root) as c:
            run = c.run("r1")
            s = run.scalars("train/loss")
            assert s.iterations == [1, 2]
            assert s[1].value == pytest.approx(0.5)
            assert s.latest.it == 2
            assert s.latest.value == pytest.approx(0.3)
            assert len(list(s)) == 2

    def test_text_series(self, server_root):
        with _make_client(server_root) as c:
            run = c.run("r1")
            t = run.text("status")
            assert t.iterations == [2]
            assert t[2].value == "trained"

    def test_missing_iteration_raises(self, server_root):
        with _make_client(server_root) as c:
            run = c.run("r1")
            with pytest.raises(KeyError):
                _ = run.scalars("train/loss")[999]

    def test_run_snapshot_hash(self, server_root):
        with _make_client(server_root) as c:
            run = c.run("r1")
            assert run.snapshot_hash == "deadbeef"


class TestURI:
    def test_parse_host_only(self):
        ref = parse_uri("rinnsal://fermat:8800")
        assert ref.host_url == "http://fermat:8800"
        assert ref.flow is None
        assert ref.run is None

    def test_parse_full(self):
        ref = parse_uri("rinnsal://fermat:8800/training/r1/loss@10")
        assert ref.flow == "training"
        assert ref.run == "r1"
        assert ref.tag == "loss"
        assert ref.iteration == 10

    def test_parse_all_iterations(self):
        ref = parse_uri("rinnsal://fermat:8800/f/r/tag@*")
        assert ref.all_iterations is True
        assert ref.iteration is None

    def test_tls_scheme(self):
        ref = parse_uri("rinnsals://fermat:8800")
        assert ref.host_url == "https://fermat:8800"

    def test_bad_scheme(self):
        with pytest.raises(ValueError):
            parse_uri("ftp://x")


class TestResolve:
    def test_resolve_run(self, server_root, monkeypatch):
        # Monkeypatch connect() to use our in-process client.
        from rinnsal.sdk import client as client_mod

        def _fake_connect(host_url, *, root, timeout=30.0):
            return _make_client(server_root)

        monkeypatch.setattr(client_mod, "connect", _fake_connect)
        from rinnsal import sdk
        monkeypatch.setattr(sdk, "connect", _fake_connect)

        run = resolve(
            "rinnsal://test/training/r1", root=server_root["root"]
        )
        assert run.run_id == "r1"

    def test_resolve_series(self, server_root, monkeypatch):
        from rinnsal.sdk import client as client_mod

        def _fake_connect(host_url, *, root, timeout=30.0):
            return _make_client(server_root)

        monkeypatch.setattr(client_mod, "connect", _fake_connect)
        from rinnsal import sdk
        monkeypatch.setattr(sdk, "connect", _fake_connect)

        s = resolve(
            "rinnsal://test/training/r1/train/loss",
            root=server_root["root"],
        )
        assert s.kind == "scalar"
        assert s.iterations == [1, 2]
