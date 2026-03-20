"""Tests for FileDatabase."""

import pytest
import time
from datetime import datetime
from pathlib import Path

from rinnsal.core.types import Entry, Snapshot
from rinnsal.persistence.file_store import FileDatabase


@pytest.fixture
def file_db(tmp_path):
    """Create a FileDatabase with a temporary directory."""
    return FileDatabase(root=tmp_path / ".rinnsal")


class TestFileDatabase:
    """Tests for FileDatabase."""

    def test_store_and_fetch(self, file_db):
        entry = Entry(result=42, log="some output")

        file_db.store_task_result("hash1", entry, "mytask")
        fetched = file_db.fetch_task_result("hash1", "mytask")

        assert fetched is not None
        assert fetched.result == 42
        assert fetched.log == "some output"

    def test_fetch_nonexistent(self, file_db):
        assert file_db.fetch_task_result("nonexistent", "mytask") is None

    def test_task_exists(self, file_db):
        entry = Entry(result=42)

        assert not file_db.task_exists("hash1", "mytask")
        file_db.store_task_result("hash1", entry, "mytask")
        assert file_db.task_exists("hash1", "mytask")

    def test_fetch_history(self, file_db):
        # Store multiple results with small delay to ensure different timestamps
        for i in range(3):
            file_db.store_task_result("hash1", Entry(result=i + 1), "mytask")
            time.sleep(0.01)  # Small delay for different timestamps

        history = file_db.fetch_task_history("hash1", "mytask")
        assert len(history) == 3
        # Most recent first
        assert history[0].result == 3

    def test_store_with_metadata(self, file_db):
        entry = Entry(
            result=42,
            metadata={"key": "value", "number": 123},
        )

        file_db.store_task_result("hash1", entry, "mytask")
        fetched = file_db.fetch_task_result("hash1", "mytask")

        assert fetched.metadata == {"key": "value", "number": 123}

    def test_store_with_snapshot(self, file_db):
        entry = Entry(
            result=42,
            snapshot=Snapshot(hash="abc123", path=Path("/some/path")),
        )

        file_db.store_task_result("hash1", entry, "mytask")
        fetched = file_db.fetch_task_result("hash1", "mytask")

        assert fetched.snapshot is not None
        assert fetched.snapshot.hash == "abc123"
        assert fetched.snapshot.path == Path("/some/path")

    def test_store_complex_result(self, file_db):
        entry = Entry(
            result={"nested": {"data": [1, 2, 3]}, "flag": True},
        )

        file_db.store_task_result("hash1", entry, "mytask")
        fetched = file_db.fetch_task_result("hash1", "mytask")

        assert fetched.result == {"nested": {"data": [1, 2, 3]}, "flag": True}

    def test_store_flow_run(self, file_db):
        run_id = file_db.store_flow_run(
            "my_flow",
            task_hashes=["hash1", "hash2"],
            metadata={"key": "value"},
        )

        assert run_id is not None
        assert len(run_id) == 8  # UUID prefix

    def test_fetch_flow_runs(self, file_db):
        file_db.store_flow_run("my_flow", ["hash1"])
        time.sleep(0.1)  # Longer delay to ensure different timestamps
        file_db.store_flow_run("my_flow", ["hash2"])

        runs = file_db.fetch_flow_runs("my_flow")
        assert len(runs) == 2
        # Most recent first (runs are sorted by filename which includes timestamp)
        # Check that we have both runs
        task_hashes = [r["task_hashes"] for r in runs]
        assert ["hash1"] in task_hashes
        assert ["hash2"] in task_hashes

    def test_fetch_flow_runs_with_limit(self, file_db):
        for i in range(5):
            file_db.store_flow_run("my_flow", [f"hash{i}"])
            time.sleep(0.01)

        runs = file_db.fetch_flow_runs("my_flow", limit=2)
        assert len(runs) == 2

    def test_fetch_flow_runs_nonexistent(self, file_db):
        runs = file_db.fetch_flow_runs("nonexistent")
        assert runs == []

    def test_clear(self, file_db):
        file_db.store_task_result("hash1", Entry(result=42), "mytask")
        file_db.store_flow_run("my_flow", ["hash1"])

        file_db.clear()

        assert not file_db.task_exists("hash1", "mytask")
        assert file_db.fetch_flow_runs("my_flow") == []

    def test_directory_structure(self, file_db):
        file_db.store_task_result("hash1", Entry(result=42), "mytask")
        file_db.store_flow_run("my_flow", ["hash1"])

        assert (file_db.root / "tasks").exists()
        assert (file_db.root / "flows").exists()
        assert (file_db.root / "snapshots").exists()
        assert (file_db.root / "tasks" / "mytask-hash1").exists()
        assert (file_db.root / "flows" / "my_flow" / "runs").exists()

    def test_named_task_directory(self, file_db):
        """Task directory uses <name>-<hash> format."""
        file_db.store_task_result("abc123", Entry(result=99), "load_data")

        task_dir = file_db.root / "tasks" / "load_data-abc123"
        assert task_dir.exists()
        assert (task_dir / "results").exists()
        assert any((task_dir / "results").glob("*.dat"))
