#!/usr/bin/env python3
"""Basic task and flow example. Run with -s to see task output."""

from rinnsal import task, flow


@task
def source(name=None):
    print("Running source...")
    return 10


@task
def double(x, name=None):
    print(f"Doubling {x}...")
    return x * 2


@task
def add(a, b, name=None):
    print(f"Adding {a} + {b}...")
    return a + b


@flow
def pipeline():
    data = source(name="source")
    doubled = double(data, name="double")
    return add(data, doubled, name="add")


if __name__ == "__main__":
    result = pipeline().run()
    print(f"Result: {result.result}")
