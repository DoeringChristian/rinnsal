#!/usr/bin/env python3
"""Diamond dependency example.

This example demonstrates:
- Diamond-shaped DAG dependencies
- Automatic task deduplication
- Content-addressed hashing
"""

from rinnsal import task, flow, eval


# Track how many times each task runs
run_counts = {"source": 0, "left": 0, "right": 0, "merge": 0}


@task
def source():
    """The shared source task - should only run once."""
    run_counts["source"] += 1
    print(f"source() running (count: {run_counts['source']})")
    return {"data": [1, 2, 3, 4, 5]}


@task
def left_branch(data):
    """Process data through the left branch."""
    run_counts["left"] += 1
    print(f"left_branch() running (count: {run_counts['left']})")
    return sum(data["data"])


@task
def right_branch(data):
    """Process data through the right branch."""
    run_counts["right"] += 1
    print(f"right_branch() running (count: {run_counts['right']})")
    return len(data["data"])


@task
def merge(total, count):
    """Merge results from both branches."""
    run_counts["merge"] += 1
    print(f"merge() running (count: {run_counts['merge']})")
    return {"total": total, "count": count, "average": total / count}


@flow
def diamond_flow():
    """A diamond-shaped dependency graph.

           source
           /    \
         left   right
           \    /
           merge
    """
    # Both branches use the same source
    data = source()

    left_result = left_branch(data)
    right_result = right_branch(data)

    merge(left_result, right_result)


def demonstrate_deduplication():
    print("=== Diamond Dependency with Deduplication ===\n")

    # Reset counters
    for key in run_counts:
        run_counts[key] = 0

    result = diamond_flow()

    print(f"\n=== Results ===")
    print(f"Tasks executed: {len(result)}")

    # Show run counts
    print(f"\nRun counts:")
    for task_name, count in run_counts.items():
        print(f"  {task_name}: {count}")

    # Note: source only runs once despite being used by both branches!
    assert run_counts["source"] == 1, "Source should only run once!"

    # Get the final result
    final = result["merge"]
    print(f"\nFinal result: {final.result}")


def demonstrate_expression_equality():
    print("\n=== Expression Equality ===\n")

    # Same task with same arguments returns the same expression
    data1 = source()
    data2 = source()

    print(f"source() called twice:")
    print(f"  data1 hash: {data1.hash[:16]}...")
    print(f"  data2 hash: {data2.hash[:16]}...")
    print(f"  data1 is data2: {data1 is data2}")

    # Different arguments produce different expressions
    @task
    def process(x):
        return x * 2

    expr1 = process(10)
    expr2 = process(10)
    expr3 = process(20)

    print(f"\nprocess(10) called twice, process(20) once:")
    print(f"  expr1 is expr2: {expr1 is expr2}")  # True - same args
    print(f"  expr1 is expr3: {expr1 is expr3}")  # False - different args


if __name__ == "__main__":
    demonstrate_deduplication()
    demonstrate_expression_equality()
