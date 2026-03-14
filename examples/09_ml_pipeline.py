#!/usr/bin/env python3
"""Complete ML pipeline example.

This example demonstrates a realistic machine learning pipeline
using rinnsal, including:
- Data loading and preprocessing
- Model training with hyperparameters
- Evaluation and model selection
- Using Config for structured parameters
"""

import random

from rinnsal import task, flow, Config
from rinnsal.core.registry import get_registry
from rinnsal.runtime.engine import get_engine


# Simulated ML utilities
def simulate_data(n_samples: int, seed: int = 42) -> dict:
    """Generate simulated data."""
    random.seed(seed)
    return {
        "X": [[random.random() for _ in range(10)] for _ in range(n_samples)],
        "y": [random.randint(0, 1) for _ in range(n_samples)],
        "n_samples": n_samples,
        "n_features": 10,
    }


def simulate_training(data: dict, config: Config) -> dict:
    """Simulate model training."""
    random.seed(hash(str(config.to_dict())) % 2**32)

    # Simulate accuracy based on hyperparameters
    base_accuracy = 0.5
    lr_bonus = min(config.learning_rate * 10, 0.2)
    epoch_bonus = min(config.epochs / 100, 0.25)
    noise = random.uniform(-0.05, 0.05)

    accuracy = min(0.99, base_accuracy + lr_bonus + epoch_bonus + noise)

    return {
        "model_id": f"model_{random.randint(1000, 9999)}",
        "accuracy": accuracy,
        "config": config.to_dict(),
        "n_samples_trained": data["n_samples"],
    }


# ============= Tasks =============

@task
def load_data(dataset_name: str, n_samples: int = 1000):
    """Load raw data from a source."""
    print(f"Loading dataset '{dataset_name}' with {n_samples} samples...")
    return {
        "dataset": dataset_name,
        **simulate_data(n_samples),
    }


@task
def split_train(data: dict, train_ratio: float = 0.8):
    """Extract training split from data."""
    n = data["n_samples"]
    split_idx = int(n * train_ratio)
    print(f"Extracting training data: {split_idx} samples")
    return {
        "X": data["X"][:split_idx],
        "y": data["y"][:split_idx],
        "n_samples": split_idx,
        "n_features": data["n_features"],
    }


@task
def split_test(data: dict, train_ratio: float = 0.8):
    """Extract test split from data."""
    n = data["n_samples"]
    split_idx = int(n * train_ratio)
    print(f"Extracting test data: {n - split_idx} samples")
    return {
        "X": data["X"][split_idx:],
        "y": data["y"][split_idx:],
        "n_samples": n - split_idx,
        "n_features": data["n_features"],
    }


@task
def preprocess(data: dict, normalize: bool = True):
    """Preprocess the data."""
    print(f"Preprocessing {data['n_samples']} samples...")
    return {
        **data,
        "preprocessed": True,
        "normalized": normalize,
    }


@task
def train_model(train_data: dict, config: Config):
    """Train a model with the given configuration."""
    print(f"Training model with lr={config.learning_rate}, epochs={config.epochs}")
    return simulate_training(train_data, config)


@task
def evaluate_model(model: dict, test_data: dict):
    """Evaluate a model on test data."""
    random.seed(hash(model["model_id"]) % 2**32)
    test_accuracy = model["accuracy"] + random.uniform(-0.05, 0.02)
    test_accuracy = max(0.0, min(1.0, test_accuracy))

    print(f"Evaluating {model['model_id']}: test_accuracy={test_accuracy:.3f}")

    return {
        "model_id": model["model_id"],
        "train_accuracy": model["accuracy"],
        "test_accuracy": test_accuracy,
        "config": model["config"],
        "test_samples": test_data["n_samples"],
    }


# ============= Flows =============

@flow
def simple_training_pipeline(
    dataset: str = "mnist",
    n_samples: int = 1000,
    learning_rate: float = 0.01,
    epochs: int = 10,
):
    """Train and evaluate a model."""
    # Load data
    raw_data = load_data(dataset, n_samples)

    # Split into train/test (separate tasks, same source)
    train_split = split_train(raw_data)
    test_split = split_test(raw_data)

    # Preprocess each split
    train_data = preprocess(train_split)
    test_data = preprocess(test_split)

    # Train and evaluate
    config = Config(learning_rate=learning_rate, epochs=epochs)
    model = train_model(train_data, config)
    evaluate_model(model, test_data)


@flow
def hyperparameter_search_pipeline(
    dataset: str = "default",
    n_samples: int = 1000,
):
    """A pipeline that searches over hyperparameters."""
    # Load and split data
    raw_data = load_data(dataset, n_samples)
    train_split = split_train(raw_data)
    test_split = split_test(raw_data)

    # Preprocess
    train_data = preprocess(train_split).name("preprocess_train")
    test_data = preprocess(test_split).name("preprocess_test")

    # Define hyperparameter grid
    learning_rates = [0.001, 0.01, 0.1]
    epoch_counts = [10, 50, 100]

    # Train models with different configs
    for lr in learning_rates:
        for epochs in epoch_counts:
            config = Config(learning_rate=lr, epochs=epochs)
            model = train_model(train_data, config).name(f"train_lr{lr}_ep{epochs}")
            evaluate_model(model, test_data).name(f"eval_lr{lr}_ep{epochs}")


def run_simple_pipeline():
    print("=" * 60)
    print("Simple Training Pipeline")
    print("=" * 60 + "\n")

    get_registry().clear()
    get_engine().clear_cache()

    result = simple_training_pipeline(
        dataset="mnist",
        n_samples=500,
        learning_rate=0.01,
        epochs=50,
    )

    print("\n--- Results ---")
    eval_result = result["evaluate_model"]
    print(f"Model: {eval_result.result['model_id']}")
    print(f"Train Accuracy: {eval_result.result['train_accuracy']:.3f}")
    print(f"Test Accuracy: {eval_result.result['test_accuracy']:.3f}")


def run_hyperparameter_search():
    print("\n" + "=" * 60)
    print("Hyperparameter Search Pipeline")
    print("=" * 60 + "\n")

    get_registry().clear()
    get_engine().clear_cache()

    result = hyperparameter_search_pipeline(
        dataset="cifar",
        n_samples=500,
    )

    print("\n--- All Evaluations ---")
    eval_tasks = result["eval_.*"]

    results = []
    for task_expr in eval_tasks:
        r = task_expr.result
        results.append(r)
        print(f"{task_expr.task_name}: test_accuracy={r['test_accuracy']:.3f}")

    # Find best
    best = max(results, key=lambda r: r["test_accuracy"])
    print(f"\nBest Model: {best['model_id']}")
    print(f"Best Config: lr={best['config']['learning_rate']}, epochs={best['config']['epochs']}")
    print(f"Best Accuracy: {best['test_accuracy']:.3f}")


def demonstrate_flow_indexing():
    print("\n" + "=" * 60)
    print("Flow Result Indexing")
    print("=" * 60 + "\n")

    get_registry().clear()
    get_engine().clear_cache()

    result = hyperparameter_search_pipeline()

    # Get all training tasks
    train_tasks = result["train_.*"]
    print(f"Training tasks: {len(train_tasks)}")

    # Get tasks with specific learning rate using callable
    lr_01_tasks = result[lambda learning_rate: learning_rate == 0.01]
    print(f"Tasks with lr=0.01: {len(lr_01_tasks)}")

    # Access by position
    print(f"\nFirst task: {result[0].task_name}")
    print(f"Last task: {result[-1].task_name}")


if __name__ == "__main__":
    run_simple_pipeline()
    run_hyperparameter_search()
    demonstrate_flow_indexing()
