#!/usr/bin/env python3
"""Subprocess executor - tasks run in separate processes."""

import os
from rinnsal import task, flow
from rinnsal.execution.subprocess import SubprocessExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine


@task
def compute(n):
    total = sum(i ** 0.5 for i in range(n * 10000))
    return {"n": n, "result": total, "pid": os.getpid()}


@flow
def pipeline():
    compute(1).name("task_1")
    compute(2).name("task_2")
    compute(3).name("task_3")


if __name__ == "__main__":
    main_pid = os.getpid()
    print(f"Main PID: {main_pid}")

    executor = SubprocessExecutor(max_workers=4)
    set_engine(ExecutionEngine(executor=executor))

    result = pipeline()

    for t in result:
        print(f"{t.task_name}: pid={t.result['pid']}")

    worker_pids = {t.result["pid"] for t in result}
    print(f"Tasks ran in separate process: {main_pid not in worker_pids}")
