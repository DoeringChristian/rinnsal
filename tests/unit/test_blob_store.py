"""Tests for the BlobStore methods on FileDatabase and InMemoryDatabase."""

from __future__ import annotations

import hashlib
import threading

import pytest

from rinnsal.data.database import InMemoryDatabase
from rinnsal.data.file_store import FileDatabase


class TestInMemoryBlobStore:
    def test_put_and_get(self) -> None:
        db = InMemoryDatabase()
        h = db.put_blob(b"hello world")
        assert h == hashlib.sha256(b"hello world").hexdigest()
        assert db.get_blob(h) == b"hello world"
        assert db.blob_exists(h)

    def test_put_is_idempotent(self) -> None:
        db = InMemoryDatabase()
        h1 = db.put_blob(b"abc")
        h2 = db.put_blob(b"abc")
        assert h1 == h2

    def test_missing_blob_raises(self) -> None:
        db = InMemoryDatabase()
        with pytest.raises(FileNotFoundError):
            db.get_blob("deadbeef")
        assert db.blob_exists("deadbeef") is False


class TestFileBlobStore:
    def test_put_and_get(self, tmp_path) -> None:
        db = FileDatabase(root=tmp_path)
        data = b"some bytes \x00\x01\x02"
        h = db.put_blob(data)
        assert h == hashlib.sha256(data).hexdigest()
        assert db.blob_exists(h)
        assert db.get_blob(h) == data

    def test_put_is_idempotent(self, tmp_path) -> None:
        db = FileDatabase(root=tmp_path)
        h1 = db.put_blob(b"xyz")
        h2 = db.put_blob(b"xyz")
        assert h1 == h2

    def test_missing_blob_raises(self, tmp_path) -> None:
        db = FileDatabase(root=tmp_path)
        with pytest.raises(FileNotFoundError):
            db.get_blob("0" * 64)

    def test_sharded_layout(self, tmp_path) -> None:
        db = FileDatabase(root=tmp_path)
        h = db.put_blob(b"hello")
        path = tmp_path / "blobs" / h[:2] / h[2:4] / h[4:]
        assert path.exists()

    def test_concurrent_writers_same_data(self, tmp_path) -> None:
        db = FileDatabase(root=tmp_path)
        data = b"race" * 1024
        results: list[str] = []

        def worker():
            results.append(db.put_blob(data))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(set(results)) == 1
        assert db.get_blob(results[0]) == data
