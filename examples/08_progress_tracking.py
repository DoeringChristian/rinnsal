#!/usr/bin/env python3
"""Progress tracking example.

This example demonstrates:
- Progress bar during execution
- Progress event callbacks
- Custom progress handling
"""

import time
import sys

from rinnsal import task, flow
from rinnsal.progress.bar import ProgressBar, SilentProgress
from rinnsal.progress.reporter import (
    ProgressReporter,
    ProgressEvent,
    EventType,
    get_reporter,
    on_progress,
)


@task
def step_one():
    """First step."""
    time.sleep(0.2)
    return "step_one_done"


@task
def step_two(prev):
    """Second step."""
    time.sleep(0.3)
    return "step_two_done"


@task
def step_three(prev):
    """Third step."""
    time.sleep(0.2)
    return "step_three_done"


@task
def step_four(prev):
    """Fourth step."""
    time.sleep(0.1)
    return "step_four_done"


@flow
def multi_step_flow():
    """A flow with multiple steps."""
    s1 = step_one()
    s2 = step_two(s1)
    s3 = step_three(s2)
    step_four(s3)


def demonstrate_progress_bar():
    print("=== Progress Bar ===\n")

    # Create a progress bar
    total_tasks = 4
    bar = ProgressBar(total=total_tasks, width=30)

    # Simulate task execution
    tasks = ["step_one", "step_two", "step_three", "step_four"]

    for i, task_name in enumerate(tasks):
        bar.start(task_name)
        time.sleep(0.3)  # Simulate work
        bar.complete(task_name, cached=(i == 1))  # Mark one as cached

    bar.finish()
    print()

    # Show final state
    print(f"Final state:")
    print(f"  Total: {bar.state.total}")
    print(f"  Passed: {bar.state.passed}")
    print(f"  Cached: {bar.state.cached}")
    print(f"  Failed: {bar.state.failed}")


def demonstrate_progress_events():
    print("\n=== Progress Events ===\n")

    reporter = ProgressReporter()

    # Add a callback that logs events
    events_received = []

    def log_callback(event: ProgressEvent):
        events_received.append(event)
        if event.task_name:
            print(f"  Event: {event.event_type.name} - {event.task_name}")
        else:
            print(f"  Event: {event.event_type.name}")

    reporter.add_callback(log_callback)

    # Simulate a flow execution
    reporter.flow_started("my_flow")

    for task_name in ["load_data", "process", "save"]:
        reporter.task_started(task_name)
        time.sleep(0.1)
        reporter.task_completed(task_name)

    reporter.flow_completed("my_flow")

    print(f"\nTotal events: {len(events_received)}")


def demonstrate_custom_progress():
    print("\n=== Custom Progress Handler ===\n")

    class CustomProgressHandler:
        """A custom progress handler that tracks detailed statistics."""

        def __init__(self):
            self.tasks_started = []
            self.tasks_completed = []
            self.tasks_failed = []
            self.start_times = {}

        def __call__(self, event: ProgressEvent):
            if event.event_type == EventType.TASK_STARTED:
                self.tasks_started.append(event.task_name)
                self.start_times[event.task_name] = time.time()
                print(f"  ▶ Starting: {event.task_name}")

            elif event.event_type == EventType.TASK_COMPLETED:
                self.tasks_completed.append(event.task_name)
                elapsed = time.time() - self.start_times.get(event.task_name, 0)
                print(f"  ✓ Completed: {event.task_name} ({elapsed:.2f}s)")

            elif event.event_type == EventType.TASK_CACHED:
                self.tasks_completed.append(event.task_name)
                print(f"  ⚡ Cached: {event.task_name}")

            elif event.event_type == EventType.TASK_FAILED:
                self.tasks_failed.append(event.task_name)
                print(f"  ✗ Failed: {event.task_name}")

        def summary(self):
            print(f"\nSummary:")
            print(f"  Started: {len(self.tasks_started)}")
            print(f"  Completed: {len(self.tasks_completed)}")
            print(f"  Failed: {len(self.tasks_failed)}")

    # Use the custom handler
    handler = CustomProgressHandler()
    reporter = ProgressReporter()
    reporter.add_callback(handler)

    # Simulate execution
    tasks = ["load", "transform", "validate", "save"]

    for i, task_name in enumerate(tasks):
        reporter.task_started(task_name)
        time.sleep(0.15)

        if i == 2:  # Simulate a cached task
            reporter.task_cached(task_name)
        else:
            reporter.task_completed(task_name)

    handler.summary()


def demonstrate_silent_progress():
    print("\n=== Silent Progress ===\n")

    # SilentProgress tracks state without output
    progress = SilentProgress(total=5)

    # Simulate execution
    for i in range(5):
        progress.start(f"task_{i}")
        if i == 2:
            progress.complete(f"task_{i}", cached=True)
        elif i == 4:
            progress.fail(f"task_{i}")
        else:
            progress.complete(f"task_{i}")

    progress.finish()

    # State is still tracked
    print(f"Final state (no output during execution):")
    print(f"  Passed: {progress.state.passed}")
    print(f"  Cached: {progress.state.cached}")
    print(f"  Failed: {progress.state.failed}")


if __name__ == "__main__":
    demonstrate_progress_bar()
    demonstrate_progress_events()
    demonstrate_custom_progress()
    demonstrate_silent_progress()
