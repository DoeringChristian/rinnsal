#!/usr/bin/env python3
"""Caching with FileDatabase."""

import tempfile
from pathlib import Path

from rinnsal import task, flow
from rinnsal.persistence.file_store import FileDatabase
from rinnsal.runtime.engine import ExecutionEngine, set_engine
from rinnsal.core.registry import get_registry

run_count = 0


@task
def expensive(x):
    global run_count
    run_count += 1
    print(f"Computing x={x}...")
    return x ** 2


@flow
def pipeline(x=5):
    return expensive(x)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmpdir:
        db = FileDatabase(root=Path(tmpdir) / ".rinnsal")
        engine = ExecutionEngine(database=db, use_cache=True)
        set_engine(engine)

        # First run - cache miss
        run_count = 0
        get_registry().clear()
        r1 = pipeline(x=5)
        print(f"Run 1: {r1[-1].result}, executed {run_count} time(s)")

        # Second run - cache hit
        engine.clear_cache()
        get_registry().clear()
        run_count = 0
        r2 = pipeline(x=5)
        print(f"Run 2: {r2[-1].result}, executed {run_count} time(s)")
