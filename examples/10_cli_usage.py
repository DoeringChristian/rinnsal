#!/usr/bin/env python3
"""CLI usage example.

This script can be run from the command line with auto-generated arguments.

Usage:
    python 10_cli_usage.py --help
    python 10_cli_usage.py --learning-rate 0.01 --epochs 100
    python 10_cli_usage.py --learning-rate 0.1 --epochs 50 --verbose
"""

from rinnsal import task, flow
from rinnsal.cli.runner import run_flow_from_cli


@task
def load_data(dataset: str = "default"):
    """Load a dataset."""
    print(f"Loading dataset: {dataset}")
    return {"dataset": dataset, "samples": 1000}


@task
def train(data, learning_rate: float, epochs: int, verbose: bool = False):
    """Train a model."""
    if verbose:
        print(f"Training with lr={learning_rate}, epochs={epochs}")
        print(f"Data has {data['samples']} samples")

    accuracy = min(0.99, 0.5 + learning_rate * epochs / 100)
    return {"accuracy": accuracy, "epochs": epochs}


@task
def evaluate(model):
    """Evaluate the model."""
    print(f"Final accuracy: {model['accuracy']:.3f}")
    return model


@flow
def training_flow(
    dataset: str = "mnist",
    learning_rate: float = 0.01,
    epochs: int = 10,
    verbose: bool = False,
):
    """Train and evaluate a model.

    This flow demonstrates CLI argument generation from the function signature.
    All parameters become command-line flags.
    """
    data = load_data(dataset)
    model = train(data, learning_rate, epochs, verbose)
    evaluate(model)


def main():
    """Main entry point for CLI usage."""
    import sys

    # Check if running with CLI arguments
    if len(sys.argv) > 1 and sys.argv[1] != "--demo":
        # Run with CLI
        result = run_flow_from_cli(training_flow)
        print(f"\nFlow completed with {len(result)} tasks")
    else:
        # Demo mode - show what CLI would look like
        print("=== CLI Demo Mode ===\n")
        print("This script can be run from the command line.\n")
        print("Available arguments (auto-generated from flow signature):")
        print("  --dataset DATASET       Dataset to use (default: mnist)")
        print("  --learning-rate FLOAT   Learning rate (default: 0.01)")
        print("  --epochs INT            Number of epochs (default: 10)")
        print("  --verbose               Enable verbose output")
        print()
        print("Built-in rinnsal arguments:")
        print("  --executor NAME         Executor type (inline, subprocess, etc.)")
        print("  --spin TASK_NAME        Re-run only one task")
        print("  -s, --no-capture        Disable output capture")
        print("  --no-cache              Disable result caching")
        print("  --db-path PATH          Database directory path")
        print()
        print("Example usage:")
        print("  python 10_cli_usage.py --learning-rate 0.1 --epochs 50")
        print("  python 10_cli_usage.py --dataset cifar --epochs 100 --verbose")
        print()

        # Run a demo
        print("--- Running Demo ---\n")
        result = training_flow(
            dataset="demo",
            learning_rate=0.05,
            epochs=20,
            verbose=True,
        )
        print(f"\nDemo completed with {len(result)} tasks")


if __name__ == "__main__":
    main()
