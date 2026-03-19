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


if __name__ == "__main__":
    # Tasks can also be run outside of a flow
    x_ = double(10)

    x = source()
    y = double(x)
    z = add(x, y)

    print(f"{z.runs[-1]=}")

    print(f"{z.is_evaluated=}")
    z.eval()
    print(f"{z.is_evaluated=}")
    print(f"{x_.is_evaluated=}")
