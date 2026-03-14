#!/usr/bin/env python3
"""Basic task and flow example. Run with -s to see task output."""

import sys
from rinnsal import task, flow
from rinnsal.runtime.engine import set_engine, ExecutionEngine
from rinnsal.execution.inline import InlineExecutor


@task
def source():
    print("Running source...")
    return 10


@task
def double(x):
    print(f"Doubling {x}...")
    return x * 2


@task
def add(a, b):
    print(f"Adding {a} + {b}...")
    return a + b


@flow
def pipeline():
    data = source()
    doubled = double(data)
    return add(data, doubled)


if __name__ == "__main__":
    if "-s" in sys.argv:
        set_engine(ExecutionEngine(executor=InlineExecutor(capture=False)))

    result = pipeline()
    print(f"Result: {result[-1].result}")
