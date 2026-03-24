#!/usr/bin/env python3
"""Hyperparameter sweep on Slurm using .map() and resource declarations.

Submits multiple training jobs to Slurm, each with different
hyperparameters. Uses @task(resources=...) to request GPUs and
task.map() for fan-out.

Usage:

    python examples/14_slurm_sweep.py
    python examples/14_slurm_sweep.py --tag sweep-v1
    python examples/14_slurm_sweep.py --dry-run
"""

import platform

from rinnsal import task, flow, Resources
from rinnsal.execution.slurm import SlurmExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine
from rinnsal.persistence.file_store import FileDatabase


@task
def load_data():
    """Load training data (runs on any node)."""
    return {"samples": 50000, "features": 784}


@task(resources=Resources(gpu=1, gpu_memory=8000, memory=16000), timeout=3600)
def train_model(data, lr: float = 0.01, epochs: int = 10):
    """Train a model with given hyperparameters.

    Requests 1 GPU with 8GB VRAM and 16GB system memory.
    Times out after 1 hour.
    """
    # Simulate training
    loss = 1.0 / (lr * epochs + 1)
    return {
        "lr": lr,
        "epochs": epochs,
        "final_loss": loss,
        "host": platform.node(),
    }


@task(catch=True)
def evaluate_model(model):
    """Evaluate a trained model.

    Uses catch=True so the sweep continues even if one
    evaluation fails.
    """
    if model is None:
        return None
    score = 1.0 - model["final_loss"]
    return {"lr": model["lr"], "score": score}


@task
def select_best(results):
    """Pick the best model from all evaluated results."""
    valid = [r for r in results if r is not None]
    if not valid:
        return {"best": None}
    best = max(valid, key=lambda r: r["score"])
    return {"best_lr": best["lr"], "best_score": best["score"]}


@flow
def hyperparameter_sweep():
    data = load_data()

    # Fan-out: train with different learning rates
    learning_rates = [0.001, 0.005, 0.01, 0.05, 0.1]
    models = train_model.map(
        [data] * len(learning_rates),
        learning_rates,
    )

    # Evaluate each model (catch=True: continues on failure)
    scores = evaluate_model.map(models)

    # Select the best
    return select_best(scores)


if __name__ == "__main__":
    executor = SlurmExecutor(
        partition="gpu",
        time_min=60,
        cpus_per_task=4,
        setup=["module load cuda/12.1"],
    )
    db = FileDatabase(root=".rinnsal")
    set_engine(ExecutionEngine(executor=executor, database=db))

    result = hyperparameter_sweep()
    output = result.run()

    print(f"\nBest learning rate: {output.result['best_lr']}")
    print(f"Best score: {output.result['best_score']:.4f}")
