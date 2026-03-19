"""Tests for database implementations."""

import pytest
from datetime import datetime

from rinnsal.core.types import Entry
from rinnsal.persistence.database import InMemoryDatabase


class TestInMemoryDatabase:
    """Tests for InMemoryDatabase."""

    def test_store_and_fetch(self):
        db = InMemoryDatabase()
        entry = Entry(result=42)

        db.store_task_result("hash1", entry, "mytask")
        fetched = db.fetch_task_result("hash1", "mytask")

        assert fetched is not None
        assert fetched.result == 42

    def test_fetch_nonexistent(self):
        db = InMemoryDatabase()
        assert db.fetch_task_result("nonexistent", "mytask") is None

    def test_task_exists(self):
        db = InMemoryDatabase()
        entry = Entry(result=42)

        assert not db.task_exists("hash1", "mytask")
        db.store_task_result("hash1", entry, "mytask")
        assert db.task_exists("hash1", "mytask")

    def test_fetch_history(self):
        db = InMemoryDatabase()

        db.store_task_result("hash1", Entry(result=1), "mytask")
        db.store_task_result("hash1", Entry(result=2), "mytask")
        db.store_task_result("hash1", Entry(result=3), "mytask")

        history = db.fetch_task_history("hash1", "mytask")
        assert len(history) == 3
        # Most recent first
        assert history[0].result == 3
        assert history[1].result == 2
        assert history[2].result == 1

    def test_task_name_in_key(self):
        """Different task_name values produce separate storage keys."""
        db = InMemoryDatabase()

        db.store_task_result("hash1", Entry(result=1), "task_a")
        db.store_task_result("hash1", Entry(result=2), "task_b")

        assert db.fetch_task_result("hash1", "task_a").result == 1
        assert db.fetch_task_result("hash1", "task_b").result == 2

    def test_store_flow_run(self):
        db = InMemoryDatabase()

        run_id = db.store_flow_run(
            "my_flow",
            task_hashes=["hash1", "hash2"],
            metadata={"key": "value"},
        )

        assert run_id.startswith("run_")

    def test_fetch_flow_runs(self):
        db = InMemoryDatabase()

        db.store_flow_run("my_flow", ["hash1"])
        db.store_flow_run("my_flow", ["hash2"])

        runs = db.fetch_flow_runs("my_flow")
        assert len(runs) == 2
        # Most recent first
        assert runs[0]["task_hashes"] == ["hash2"]
        assert runs[1]["task_hashes"] == ["hash1"]

    def test_fetch_flow_runs_with_limit(self):
        db = InMemoryDatabase()

        for i in range(5):
            db.store_flow_run("my_flow", [f"hash{i}"])

        runs = db.fetch_flow_runs("my_flow", limit=2)
        assert len(runs) == 2

    def test_fetch_flow_runs_nonexistent(self):
        db = InMemoryDatabase()
        runs = db.fetch_flow_runs("nonexistent")
        assert runs == []

    def test_clear(self):
        db = InMemoryDatabase()

        db.store_task_result("hash1", Entry(result=42), "mytask")
        db.store_flow_run("my_flow", ["hash1"])

        db.clear()

        assert not db.task_exists("hash1", "mytask")
        assert db.fetch_flow_runs("my_flow") == []
