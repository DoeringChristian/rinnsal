"""Cache-header contracts for viewer endpoints.

Listing endpoints (scalars, text, figures, images, cards, tags) must
support If-Modified-Since → 304. Blob/image/figure endpoints must
carry immutable Cache-Control + ETag and honor If-None-Match → 304.
"""

from __future__ import annotations

from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="viewer extra not installed")
pytest.importorskip("httpx", reason="httpx required for fastapi.testclient")
from fastapi.testclient import TestClient  # noqa: E402

from rinnsal.data.file_store import FileDatabase
from rinnsal.data.logger.components import Markdown, Scalar
from rinnsal.data.logger.logger import Logger
from rinnsal.viewer._data import invalidate_caches


@pytest.fixture(autouse=True)
def _clean_cache():
    invalidate_caches()
    yield
    invalidate_caches()


@pytest.fixture
def client():
    from rinnsal.viewer.backend.main import app

    return TestClient(app)


def _make_run(tmp_path: Path) -> Path:
    db = FileDatabase(root=tmp_path / ".rinnsal")
    rd = tmp_path / ".rinnsal/flows/f/runs/r1"
    rd.mkdir(parents=True)
    lg = Logger(rd, database=db)
    lg.add_scalar("train/loss", 0.5, it=1)
    with lg.card("report", task="analyze") as c:
        c.append(Markdown("hi"))
        c.append(Scalar(0.42))
    lg.close()
    return rd


def _assert_listing_supports_revalidation(client, url: str) -> None:
    r = client.get(url)
    assert r.status_code == 200
    lm = r.headers.get("last-modified")
    cc = r.headers.get("cache-control")
    assert lm, f"{url} missing Last-Modified"
    assert cc and "must-revalidate" in cc, f"{url} missing Cache-Control"

    # Revalidation must use a timestamp STRICTLY newer than the file's
    # mtime: HTTP dates have 1s resolution, so equal timestamps could
    # mask sub-second writes. Give the client timestamp 2s of headroom.
    from email.utils import formatdate, parsedate_to_datetime

    ts = parsedate_to_datetime(lm).timestamp()
    newer = formatdate(ts + 2, usegmt=True)
    r2 = client.get(url, headers={"If-Modified-Since": newer})
    assert r2.status_code == 304, f"{url} did not return 304 on revalidation"
    assert r2.content == b""

    # And the equal-second case MUST return 200 (fresh) — that's the
    # whole point of strict inequality.
    r3 = client.get(url, headers={"If-Modified-Since": lm})
    assert r3.status_code == 200, (
        f"{url} returned 304 for equal-second If-Modified-Since — "
        f"this masks sub-second writes"
    )


class TestListingRevalidation:
    def test_scalars(self, client, tmp_path):
        rd = _make_run(tmp_path)
        _assert_listing_supports_revalidation(client, f"/api/scalars{rd}")

    def test_tags(self, client, tmp_path):
        rd = _make_run(tmp_path)
        _assert_listing_supports_revalidation(client, f"/api/tags{rd}")

    def test_cards_index(self, client, tmp_path):
        rd = _make_run(tmp_path)
        _assert_listing_supports_revalidation(client, f"/api/cards{rd}")


class TestBlobImmutable:
    def test_blob_etag_and_immutable(self, client, tmp_path):
        from rinnsal.data.logger.event_file import EventFileReader

        db = FileDatabase(root=tmp_path / ".rinnsal")
        rd = tmp_path / ".rinnsal/flows/f/runs/r1"
        rd.mkdir(parents=True)
        lg = Logger(rd, database=db)
        lg.add_artifact("big", list(range(10_000)))
        lg.close()

        ev = list(EventFileReader(rd / "events.pb").read_all())[0]
        h = ev.checkpoint.blob_hash
        assert h

        r = client.get(f"/api/blob{rd}/{h}")
        assert r.status_code == 200
        assert "immutable" in r.headers.get("cache-control", "")
        etag = r.headers.get("etag")
        assert etag == f'"{h}"'

        r2 = client.get(f"/api/blob{rd}/{h}", headers={"If-None-Match": etag})
        assert r2.status_code == 304
        assert r2.content == b""
