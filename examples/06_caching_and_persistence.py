#!/usr/bin/env python3
"""Caching and persistence example.

This example demonstrates:
- Result caching and persistence
- Using FileDatabase
- Cache hits and misses
- Clearing cache
"""

import tempfile
import time
from pathlib import Path

from rinnsal import task, flow
from rinnsal.persistence.file_store import FileDatabase
from rinnsal.runtime.engine import ExecutionEngine, set_engine
from rinnsal.execution.inline import InlineExecutor
from rinnsal.core.registry import get_registry


# Track execution counts
execution_counts = {}


def reset_counts():
    global execution_counts
    execution_counts = {}


@task
def expensive_computation(x: int):
    """An expensive computation that we want to cache."""
    execution_counts["expensive"] = execution_counts.get("expensive", 0) + 1
    print(f"Running expensive computation for x={x}...")
    time.sleep(0.1)  # Simulate expensive work
    return x ** 2


@task
def process_result(result: int):
    """Process the result."""
    execution_counts["process"] = execution_counts.get("process", 0) + 1
    print(f"Processing result: {result}")
    return result * 2


@flow
def cached_pipeline(x: int = 5):
    """A pipeline that benefits from caching."""
    result = expensive_computation(x)
    process_result(result)


def demonstrate_caching():
    print("=== Caching with FileDatabase ===\n")

    # Create a temporary directory for the database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / ".rinnsal"

        # Create engine with caching
        db = FileDatabase(root=db_path)
        engine = ExecutionEngine(
            executor=InlineExecutor(),
            database=db,
            use_cache=True,
        )
        set_engine(engine)

        print("--- First Run (cache miss) ---")
        reset_counts()
        get_registry().clear()

        result1 = cached_pipeline(x=5)
        print(f"Result: {result1[-1].result}")
        print(f"Execution counts: {execution_counts}")
        print()

        # Clear engine's in-memory cache (but keep database)
        engine.clear_cache()
        get_registry().clear()

        print("--- Second Run (cache hit) ---")
        reset_counts()

        result2 = cached_pipeline(x=5)
        print(f"Result: {result2[-1].result}")
        print(f"Execution counts: {execution_counts}")
        print("(Tasks weren't executed - results loaded from cache!)")
        print()

        # Run with different argument (cache miss)
        engine.clear_cache()
        get_registry().clear()

        print("--- Third Run with different arg (cache miss) ---")
        reset_counts()

        result3 = cached_pipeline(x=10)
        print(f"Result: {result3[-1].result}")
        print(f"Execution counts: {execution_counts}")

        engine.shutdown()


def demonstrate_persistence():
    print("\n=== Persistence Across Sessions ===\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / ".rinnsal"

        # Session 1: Run and cache
        print("--- Session 1 ---")
        reset_counts()
        get_registry().clear()

        db1 = FileDatabase(root=db_path)
        engine1 = ExecutionEngine(
            executor=InlineExecutor(),
            database=db1,
            use_cache=True,
        )
        set_engine(engine1)

        result1 = cached_pipeline(x=7)
        print(f"Result: {result1[-1].result}")
        print(f"Execution counts: {execution_counts}")
        engine1.shutdown()
        print()

        # Session 2: New engine, same database path
        print("--- Session 2 (new process, same database) ---")
        reset_counts()
        get_registry().clear()

        db2 = FileDatabase(root=db_path)  # Same path!
        engine2 = ExecutionEngine(
            executor=InlineExecutor(),
            database=db2,
            use_cache=True,
        )
        set_engine(engine2)

        result2 = cached_pipeline(x=7)
        print(f"Result: {result2[-1].result}")
        print(f"Execution counts: {execution_counts}")
        print("(Results loaded from disk - persisted across sessions!)")
        engine2.shutdown()


def demonstrate_no_cache():
    print("\n=== Disabling Cache ===\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / ".rinnsal"

        # Create engine with caching disabled
        db = FileDatabase(root=db_path)
        engine = ExecutionEngine(
            executor=InlineExecutor(),
            database=db,
            use_cache=False,  # Disable caching
        )
        set_engine(engine)

        print("--- First Run ---")
        reset_counts()
        get_registry().clear()

        result1 = cached_pipeline(x=3)
        print(f"Execution counts: {execution_counts}")
        print()

        engine.clear_cache()
        get_registry().clear()

        print("--- Second Run (no cache, re-executes) ---")
        reset_counts()

        result2 = cached_pipeline(x=3)
        print(f"Execution counts: {execution_counts}")
        print("(Tasks executed again because caching is disabled)")

        engine.shutdown()


if __name__ == "__main__":
    demonstrate_caching()
    demonstrate_persistence()
    demonstrate_no_cache()
