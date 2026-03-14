#!/usr/bin/env python3
"""Retry and error handling example.

This example demonstrates:
- Task retry on failure
- Configurable retry counts
- Error propagation
"""

from rinnsal import task, flow, eval


# Simulate a flaky external service
class FlakyService:
    def __init__(self, fail_count: int = 2):
        self.attempts = 0
        self.fail_count = fail_count

    def call(self):
        self.attempts += 1
        if self.attempts <= self.fail_count:
            raise ConnectionError(f"Service unavailable (attempt {self.attempts})")
        return f"Success on attempt {self.attempts}"


# Global service instance for demonstration
flaky_service = FlakyService(fail_count=2)


@task(retry=3)
def call_flaky_service():
    """Call a service that fails sometimes.

    With retry=3, the task will be attempted up to 3 times.
    """
    print(f"Attempting to call service (attempt {flaky_service.attempts + 1})...")
    result = flaky_service.call()
    print(f"Service call succeeded: {result}")
    return result


@task
def process_result(data):
    """Process the service result."""
    return f"Processed: {data}"


@flow
def retry_flow():
    """A flow that handles transient failures."""
    result = call_flaky_service()
    process_result(result)


def demonstrate_retry():
    print("=== Retry Example ===\n")

    # Reset service state
    global flaky_service
    flaky_service = FlakyService(fail_count=2)

    result = retry_flow()

    print(f"\nFlow completed successfully!")
    print(f"Final result: {result[-1].result}")
    print(f"Total attempts: {flaky_service.attempts}")


@task(retry=2)
def always_fails():
    """A task that always fails."""
    raise ValueError("This task always fails")


def demonstrate_retry_exhausted():
    print("\n=== Retry Exhausted Example ===\n")

    try:
        eval(always_fails())
    except ValueError as e:
        print(f"Task failed after all retries: {e}")


@task
def no_retry_task():
    """A task with no retry - fails immediately."""
    raise RuntimeError("Immediate failure")


def demonstrate_no_retry():
    print("\n=== No Retry Example ===\n")

    try:
        eval(no_retry_task())
    except RuntimeError as e:
        print(f"Task failed immediately: {e}")


if __name__ == "__main__":
    demonstrate_retry()
    demonstrate_retry_exhausted()
    demonstrate_no_retry()
