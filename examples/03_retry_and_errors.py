#!/usr/bin/env python3
"""Retry on failure example. Run with -s to see output."""

import sys
from rinnsal import task, flow
from rinnsal.runtime.engine import set_engine, ExecutionEngine
from rinnsal.execution.inline import InlineExecutor

attempt = 0


@task(retry=3)
def flaky_task():
    global attempt
    attempt += 1
    print(f"Attempt {attempt}...")
    if attempt < 3:
        raise RuntimeError("Not yet!")
    return "Success!"


@task
def process(data):
    print(f"Processing: {data}")
    return f"Processed: {data}"


@flow
def retry_flow():
    result = flaky_task()
    return process(result)


if __name__ == "__main__":
    if "-s" in sys.argv:
        set_engine(ExecutionEngine(executor=InlineExecutor(capture=False)))

    result = retry_flow().run()
    print(f"Result: {result.result}")
    print(f"Total attempts: {attempt}")
