#!/usr/bin/env python3
"""Basic task and flow example.

This example demonstrates the fundamental concepts of rinnsal:
- Defining tasks with @task decorator
- Creating flows with @flow decorator
- Lazy evaluation and explicit eval()
- Task chaining
"""

from rinnsal import task, flow, eval
from rinnsal.core.registry import get_registry
from rinnsal.runtime.engine import get_engine


# Define simple tasks
@task
def source():
    """Return some initial data."""
    print("Running source task...")
    return 10


@task
def double(x):
    """Double a value."""
    print(f"Doubling {x}...")
    return x * 2


@task
def add(a, b):
    """Add two values."""
    print(f"Adding {a} + {b}...")
    return a + b


# Tasks are lazy - calling them returns a TaskExpression, not the result
def demonstrate_lazy_evaluation():
    print("=== Lazy Evaluation ===")

    # Clear registry and engine cache to start fresh
    get_registry().clear()
    get_engine().clear_cache()

    # This does NOT execute the task - it returns a TaskExpression
    expr = source()
    print(f"source() returned: {repr(expr)}")
    print(f"Type: {type(expr).__name__}")

    # To actually run it, use eval()
    result = eval(expr)
    print(f"eval(expr) returned: {result}")
    print()


# Tasks can be chained - passing one task's output to another
def demonstrate_chaining():
    print("=== Task Chaining ===")

    # Clear registry and engine cache to start fresh
    get_registry().clear()
    get_engine().clear_cache()

    # Build a chain: source() -> double() -> double()
    step1 = source()  # Will return 10
    step2 = double(step1)  # Will return 20
    step3 = double(step2)  # Will return 40

    # Evaluate the final task - this runs the whole chain
    result = eval(step3)
    print(f"Result: {result}")
    print()


# Multiple tasks can be evaluated together
def demonstrate_multiple_eval():
    print("=== Multiple Evaluation ===")

    # Clear registry and engine cache to start fresh
    get_registry().clear()
    get_engine().clear_cache()

    s = source()
    d = double(s)
    a = add(s, d)

    # Evaluate multiple tasks at once
    r1, r2, r3 = eval(s, d, a)
    print(f"source: {r1}, double: {r2}, add: {r3}")
    print()


# Flows organize tasks into a pipeline
@flow
def simple_pipeline():
    """A simple pipeline that doubles a value twice."""
    data = source()
    step1 = double(data)
    double(step1)


def demonstrate_flow():
    print("=== Flow Execution ===")

    # Clear registry and engine cache to start fresh
    get_registry().clear()
    get_engine().clear_cache()

    result = simple_pipeline()

    print(f"Flow completed with {len(result)} tasks")
    print(f"First task result: {result[0].result}")
    print(f"Last task result: {result[-1].result}")
    print()


if __name__ == "__main__":
    demonstrate_lazy_evaluation()
    demonstrate_chaining()
    demonstrate_multiple_eval()
    demonstrate_flow()
