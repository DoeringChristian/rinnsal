#!/usr/bin/env python3
"""Checkpointing - resume long-running tasks from where they left off.

Tasks can save checkpoint state via ``current.checkpoint.save()`` and
restore it via ``current.checkpoint.load()`` on retry or resume. This
is essential for long GPU training runs that may be preempted or crash.

Usage:

    python examples/15_checkpointing.py
    python examples/15_checkpointing.py --resume   # continues from checkpoint
"""

from rinnsal import task, flow, current
from rinnsal.persistence.file_store import FileDatabase
from rinnsal.execution.inline import InlineExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine


@task(retry=2)
def train(epochs: int = 10, lr: float = 0.01):
    """Simulated training loop with checkpointing.

    On first run, processes all epochs and saves checkpoints.
    On retry (after a crash), resumes from the last checkpoint.
    """
    # Load checkpoint from a previous attempt (if any)
    state = current.checkpoint.load()

    if state is not None:
        start_epoch = state["epoch"]
        model_weights = state["weights"]
        print(f"Resuming from checkpoint at epoch {start_epoch}")
    else:
        start_epoch = 0
        model_weights = 0.0
        print("Starting fresh")

    for epoch in range(start_epoch, epochs):
        # Simulate one epoch of training
        model_weights += lr * (1.0 / (epoch + 1))
        loss = 1.0 / (epoch + 1)
        print(f"  Epoch {epoch + 1}/{epochs}: loss={loss:.4f}")

        # Save checkpoint after each epoch
        current.checkpoint.save({
            "epoch": epoch + 1,
            "weights": model_weights,
        })

    return {"weights": model_weights, "final_loss": loss}


@task
def evaluate(model):
    """Evaluate the trained model."""
    print(f"Model weights: {model['weights']:.4f}")
    print(f"Final loss: {model['final_loss']:.4f}")
    return {"score": 1.0 - model["final_loss"]}


@flow
def pipeline(epochs: int = 5, lr: float = 0.01):
    model = train(epochs=epochs, lr=lr)
    return evaluate(model)


if __name__ == "__main__":
    db = FileDatabase(root=".rinnsal")
    executor = InlineExecutor()
    set_engine(ExecutionEngine(executor=executor, database=db))

    result = pipeline()
    output = result.run()
    print(f"\nScore: {output.result['score']:.4f}")
