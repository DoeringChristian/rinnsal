#!/usr/bin/env python3
"""Diamond dependency - source runs once despite two branches. Run with -s to see output."""

import sys
from rinnsal import task, flow
from rinnsal.runtime.engine import set_engine, ExecutionEngine
from rinnsal.execution.inline import InlineExecutor

run_count = 0


@task
def source():
    global run_count
    run_count += 1
    print(f"source() called (count: {run_count})")
    return [1, 2, 3, 4, 5]


@task
def left(data):
    print(f"left: sum={sum(data)}")
    return sum(data)


@task
def right(data):
    print(f"right: len={len(data)}")
    return len(data)


@task
def merge(total, count):
    print(f"merge: {total}/{count}")
    return total / count


@flow
def diamond():
    """
         source
         /    \\
       left   right
         \\    /
          merge
    """
    data = source()
    return merge(left(data), right(data))


if __name__ == "__main__":
    if "-s" in sys.argv:
        set_engine(ExecutionEngine(executor=InlineExecutor(capture=False)))

    result = diamond()
    print(f"Result: {result[-1].result}")
    print(f"source() ran {run_count} time(s)")
