"""Tests for TaskRuns and TaskExpression.runs()."""

import time

import pytest

from rinnsal.core.types import Entry, Runs, TaskRuns
from rinnsal.persistence.file_store import FileDatabase, set_database


@pytest.fixture
def file_db(tmp_path):
    """Create a FileDatabase and set it as default."""
    db = FileDatabase(root=tmp_path / ".rinnsal")
    set_database(db)
    return db


class TestTaskRuns:
    """Tests for TaskRuns (alias for Runs[Entry])."""

    def test_indexing(self):
        entries = [Entry(result=i) for i in range(5)]
        runs = Runs(entries)

        assert runs[0].result == 0
        assert runs[-1].result == 4
        assert runs[2].result == 2

    def test_slicing(self):
        entries = [Entry(result=i) for i in range(5)]
        runs = Runs(entries)

        sliced = runs[1:3]
        assert len(sliced) == 2
        assert sliced[0].result == 1
        assert sliced[1].result == 2

    def test_len(self):
        assert len(Runs([])) == 0
        assert len(Runs([Entry(result=1)])) == 1

    def test_bool(self):
        assert not Runs([])
        assert Runs([Entry(result=1)])

    def test_iter(self):
        entries = [Entry(result=i) for i in range(3)]
        runs = Runs(entries)
        assert [e.result for e in runs] == [0, 1, 2]

    def test_type_alias(self):
        assert TaskRuns is Runs[Entry]


class TestTaskExpressionRuns:
    """Tests for TaskExpression.runs() integration."""

    def test_runs_returns_chronological_order(self, file_db):
        from rinnsal import task

        @task
        def my_task(x):
            return x

        expr = my_task(42)

        # Store 3 entries with increasing timestamps
        for i in range(3):
            file_db.store_task_result(
                expr.hash, Entry(result=i), expr.task_name
            )
            time.sleep(0.01)

        runs = expr.runs

        assert len(runs) == 3
        # Chronological: oldest first, newest last
        assert runs[0].result == 0
        assert runs[-1].result == 2

    def test_runs_empty(self, file_db):
        from rinnsal import task

        @task
        def no_history(x):
            return x

        expr = no_history(99)
        runs = expr.runs

        assert len(runs) == 0
        assert not runs
