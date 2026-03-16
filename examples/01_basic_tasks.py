#!/usr/bin/env python3
"""Basic task and flow example. Run with -s to see task output."""

from rinnsal import task, flow


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
    result = pipeline()
    print(f"Result: {result[-1].result}")
