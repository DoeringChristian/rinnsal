#!/usr/bin/env python3
"""Subprocess executor example.

This example demonstrates:
- Running tasks in separate processes
- Process isolation (crashes don't affect orchestrator)
- Parallel execution
"""

import os
import time

from rinnsal import task, flow
from rinnsal.execution.subprocess import SubprocessExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine
from rinnsal.core.registry import get_registry


@task
def get_process_info():
    """Get information about the current process."""
    return {
        "pid": os.getpid(),
        "ppid": os.getppid(),
    }


@task
def cpu_intensive(n: int):
    """A CPU-intensive task."""
    # Simulate CPU work
    total = 0
    for i in range(n * 100000):
        total += i ** 0.5
    return {
        "n": n,
        "result": total,
        "pid": os.getpid(),
    }


@task
def combine_results(results: list):
    """Combine results from multiple tasks."""
    return {
        "total": sum(r["result"] for r in results),
        "pids": [r["pid"] for r in results],
        "combiner_pid": os.getpid(),
    }


@flow
def parallel_flow():
    """A flow with tasks that can run in parallel."""
    # These tasks could run in parallel
    r1 = cpu_intensive(1).name("task_1")
    r2 = cpu_intensive(2).name("task_2")
    r3 = cpu_intensive(3).name("task_3")
    r4 = cpu_intensive(4).name("task_4")

    # This depends on all of them
    # Note: In current implementation, tasks run sequentially
    # but in separate processes


def demonstrate_subprocess_executor():
    print("=== Subprocess Executor ===\n")

    # Get main process info
    main_pid = os.getpid()
    print(f"Main process PID: {main_pid}\n")

    # Create subprocess executor
    executor = SubprocessExecutor(max_workers=4)
    engine = ExecutionEngine(executor=executor)
    set_engine(engine)

    get_registry().clear()

    print("Running tasks in subprocesses...")
    start = time.time()

    result = parallel_flow()

    elapsed = time.time() - start
    print(f"Completed in {elapsed:.2f}s\n")

    print("Results:")
    for task_expr in result:
        r = task_expr.result
        print(f"  {task_expr.task_name}: PID={r['pid']}, result={r['result']:.0f}")

    # Check that tasks ran in different processes
    pids = set(t.result["pid"] for t in result)
    print(f"\nUnique worker PIDs: {pids}")
    print(f"Main PID in workers: {main_pid in pids}")

    engine.shutdown()


@task
def crashing_task():
    """A task that crashes."""
    raise RuntimeError("This task crashed!")


@task
def safe_task():
    """A task that runs safely."""
    return "I'm safe!"


def demonstrate_isolation():
    print("\n=== Process Isolation ===\n")

    executor = SubprocessExecutor(max_workers=2)
    engine = ExecutionEngine(executor=executor)
    set_engine(engine)

    get_registry().clear()

    # Even if a task crashes, the main process continues
    print("Running a crashing task...")
    try:
        from rinnsal import eval
        eval(crashing_task())
    except RuntimeError as e:
        print(f"Task crashed with: {e}")
        print("But the main process is still running!")

    print("\nRunning a safe task after the crash...")
    get_registry().clear()

    from rinnsal import eval
    result = eval(safe_task())
    print(f"Safe task result: {result}")

    engine.shutdown()


@task
def capture_output():
    """A task that prints to stdout/stderr."""
    import sys
    print("This goes to stdout")
    print("This goes to stderr", file=sys.stderr)
    return "done"


def demonstrate_output_capture():
    print("\n=== Output Capture ===\n")

    # With capture enabled (default)
    executor = SubprocessExecutor(max_workers=1, capture=True)
    engine = ExecutionEngine(executor=executor)
    set_engine(engine)

    get_registry().clear()

    print("With capture=True:")
    from rinnsal import eval
    result = eval(capture_output())
    print(f"Result: {result}")
    print("(stdout/stderr captured, not shown)\n")

    engine.shutdown()

    # With capture disabled
    executor2 = SubprocessExecutor(max_workers=1, capture=False)
    engine2 = ExecutionEngine(executor=executor2)
    set_engine(engine2)

    get_registry().clear()

    print("With capture=False:")
    result2 = eval(capture_output())
    print(f"Result: {result2}")

    engine2.shutdown()


if __name__ == "__main__":
    demonstrate_subprocess_executor()
    demonstrate_isolation()
    demonstrate_output_capture()
