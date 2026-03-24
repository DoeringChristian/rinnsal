#!/usr/bin/env python3
"""Slurm executor - submit tasks to a Slurm cluster.

Each task is serialized, wrapped in a Python script, and submitted
via sbatch. Results are read back from pickle files once the job
completes.

Requirements:
    - Slurm (sbatch, sacct) available on the submission host
    - cloudpickle installed on compute nodes
    - rinnsal accessible from PYTHONPATH on compute nodes

Usage:

    python examples/13_slurm_executor.py
    python examples/13_slurm_executor.py -s              # show output
    python examples/13_slurm_executor.py --dry-run        # print DAG only
"""

import platform

from rinnsal import task, flow, Resources
from rinnsal.execution.slurm import SlurmExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine
from rinnsal.persistence.file_store import FileDatabase


@task
def preprocess(dataset: str):
    """CPU-only preprocessing step."""
    print(f"Preprocessing {dataset} on {platform.node()}")
    data = list(range(1000))
    return {"dataset": dataset, "size": len(data), "data": data}


@task(resources=Resources(gpu=1, memory=16000))
def train(data, lr: float = 0.01):
    """GPU training step — requests 1 GPU and 16GB memory."""
    print(f"Training on {platform.node()} with lr={lr}")
    result = sum(data["data"]) * lr
    return {"model": result, "lr": lr, "host": platform.node()}


@task
def evaluate(model):
    """Evaluate the trained model."""
    print(f"Evaluating on {platform.node()}")
    return {"score": model["model"] * 0.95, "host": platform.node()}


@flow
def training_pipeline(dataset: str = "cifar10", lr: float = 0.01):
    data = preprocess(dataset)
    model = train(data, lr=lr)
    score = evaluate(model)
    return {"data": data, "model": model, "score": score}


if __name__ == "__main__":
    # Configure Slurm executor with cluster defaults.
    # Per-task @task(resources=...) overrides these for specific tasks.
    executor = SlurmExecutor(
        partition="gpu",
        time_min=30,
        cpus_per_task=4,
        mem_gb=8,
        setup=[
            "module load cuda/12.1",
            "source ~/.bashrc",
        ],
    )
    db = FileDatabase(root=".rinnsal")
    set_engine(ExecutionEngine(executor=executor, database=db))

    result = training_pipeline()
    outputs = result.run()

    print(f"\nDataset: {outputs['data'].result['dataset']}")
    print(f"Model trained on: {outputs['model'].result['host']}")
    print(f"Score: {outputs['score'].result['score']:.4f}")
