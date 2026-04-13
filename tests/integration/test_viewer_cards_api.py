"""Integration tests for the viewer's unified Cards/Tags API."""

from __future__ import annotations

import json

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")
from fastapi.testclient import TestClient  # noqa: E402

from rinnsal.data.file_store import FileDatabase
from rinnsal.data.logger.components import Markdown, Scalar, Table
from rinnsal.data.logger.logger import Logger
from rinnsal.viewer._data import invalidate_caches


@pytest.fixture
def client():
    from rinnsal.viewer.backend.main import app

    return TestClient(app)


@pytest.fixture(autouse=True)
def _clean_cache():
    invalidate_caches()
    yield
    invalidate_caches()


def _make_run_with_card(tmp_path, *, card_components):
    db = FileDatabase(root=tmp_path / ".rinnsal")
    run_dir = tmp_path / ".rinnsal" / "flows" / "f" / "runs" / "r1"
    run_dir.mkdir(parents=True, exist_ok=True)
    logger = Logger(run_dir, database=db)
    with logger.card("report", task="analyze") as card:
        for c in card_components:
            card.append(c)
    logger.close()
    return run_dir


class TestCardsIndex:
    def test_lists_cards(self, client, tmp_path):
        run_dir = _make_run_with_card(
            tmp_path,
            card_components=[Markdown("# hi"), Scalar(0.42)],
        )
        r = client.get(f"/api/cards{run_dir}")
        assert r.status_code == 200
        body = r.json()
        assert len(body["cards"]) == 1
        card = body["cards"][0]
        assert card["name"] == "report"
        assert card["task"] == "analyze"
        assert card["component_kinds"] == ["markdown", "scalar"]

    def test_iterations_listed(self, client, tmp_path):
        db = FileDatabase(root=tmp_path / ".rinnsal")
        run_dir = tmp_path / ".rinnsal" / "flows" / "f" / "runs" / "r1"
        run_dir.mkdir(parents=True, exist_ok=True)
        logger = Logger(run_dir, database=db)
        for it in (1, 2, 3):
            logger.set_iteration(it)
            with logger.card("train", task="train_step") as c:
                c.append(Scalar(1.0 / it))
        logger.close()

        r = client.get(f"/api/cards{run_dir}")
        body = r.json()
        assert body["cards"][0]["iterations"] == [1, 2, 3]


class TestCardSnapshot:
    def test_latest_when_no_it(self, client, tmp_path):
        run_dir = _make_run_with_card(
            tmp_path,
            card_components=[Markdown("hello"), Table([[1, 2]], headers=["a", "b"])],
        )
        r = client.get(
            f"/api/card{run_dir}",
            params={"name": "report", "task": "analyze"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["name"] == "report"
        assert len(body["components"]) == 2
        assert body["components"][0]["kind"] == "markdown"
        assert body["components"][0]["content"] == "hello"
        assert body["components"][1]["kind"] == "table"
        assert json.loads(body["components"][1]["headers_json"]) == ["a", "b"]

    def test_specific_iteration(self, client, tmp_path):
        db = FileDatabase(root=tmp_path / ".rinnsal")
        run_dir = tmp_path / ".rinnsal" / "flows" / "f" / "runs" / "r1"
        run_dir.mkdir(parents=True, exist_ok=True)
        logger = Logger(run_dir, database=db)
        for it, val in enumerate([0.9, 0.5, 0.1], start=1):
            logger.set_iteration(it)
            with logger.card("loss", task="train") as c:
                c.append(Scalar(val))
        logger.close()

        r = client.get(
            f"/api/card{run_dir}",
            params={"name": "loss", "task": "train", "it": 2},
        )
        body = r.json()
        assert body["it"] == 2
        assert body["components"][0]["value"] == pytest.approx(0.5)


class TestTagsListing:
    def test_unified_tags(self, client, tmp_path):
        db = FileDatabase(root=tmp_path / ".rinnsal")
        run_dir = tmp_path / ".rinnsal" / "flows" / "f" / "runs" / "r1"
        run_dir.mkdir(parents=True, exist_ok=True)
        logger = Logger(run_dir, database=db)
        logger.add_scalar("train/loss", 0.5, it=1)
        logger.add_scalar("train/loss", 0.3, it=2)
        logger.add_markdown("notes", "hi")
        logger.add_table("metrics", [[1, 2]], headers=["a", "b"])
        logger.close()

        r = client.get(f"/api/tags{run_dir}")
        body = r.json()
        kinds = sorted({t["kind"] for t in body["tags"]})
        assert kinds == ["markdown", "scalar", "table"]
        loss = next(t for t in body["tags"] if t["tag"] == "train/loss")
        assert loss["iterations"] == [1, 2]
        assert loss["count"] == 2


class TestBlobEndpoint:
    def test_serves_blob_offloaded_by_logger(self, client, tmp_path):
        db = FileDatabase(root=tmp_path / ".rinnsal")
        run_dir = tmp_path / ".rinnsal" / "flows" / "f" / "runs" / "r1"
        run_dir.mkdir(parents=True, exist_ok=True)
        logger = Logger(run_dir, database=db)
        big = list(range(10_000))  # >16KB pickled → blob path
        logger.add_artifact("payload", big)
        logger.close()

        from rinnsal.data.logger.event_file import EventFileReader

        events = list(EventFileReader(run_dir / "events.pb").read_all())
        ckpt = events[0].checkpoint
        assert ckpt.blob_hash != ""

        r = client.get(f"/api/blob{run_dir}/{ckpt.blob_hash}")
        assert r.status_code == 200
        assert len(r.content) > 0

    def test_404_for_unknown_blob(self, client, tmp_path):
        run_dir = _make_run_with_card(
            tmp_path, card_components=[Markdown("x")]
        )
        r = client.get(f"/api/blob{run_dir}/{'0' * 64}")
        assert r.status_code == 404
