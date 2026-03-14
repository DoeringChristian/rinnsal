#!/usr/bin/env python3
"""Config and flow parameters example.

This example demonstrates:
- Using Config objects for structured configuration
- Flow parameters with defaults
- Parameter overrides
"""

from rinnsal import task, flow, Config


@task
def create_config(learning_rate: float, epochs: int, batch_size: int = 32):
    """Create a training configuration."""
    return Config(
        learning_rate=learning_rate,
        epochs=epochs,
        batch_size=batch_size,
    )


@task
def load_data(config: Config):
    """Load training data based on config."""
    print(f"Loading data with batch_size={config.batch_size}")
    return {
        "samples": 1000,
        "batch_size": config.batch_size,
        "batches": 1000 // config.batch_size,
    }


@task
def train(data, config: Config):
    """Train a model."""
    print(f"Training with lr={config.learning_rate}, epochs={config.epochs}")

    # Simulate training
    accuracy = min(0.99, 0.5 + config.learning_rate * config.epochs * 0.1)

    return {
        "accuracy": accuracy,
        "epochs_trained": config.epochs,
        "samples_seen": data["samples"] * config.epochs,
    }


@task
def evaluate(model_result, threshold: float = 0.8):
    """Evaluate if the model meets the threshold."""
    passed = model_result["accuracy"] >= threshold
    return {
        "accuracy": model_result["accuracy"],
        "threshold": threshold,
        "passed": passed,
    }


@flow
def training_pipeline(
    learning_rate: float = 0.01,
    epochs: int = 10,
    batch_size: int = 32,
    eval_threshold: float = 0.8,
):
    """A configurable training pipeline.

    Args:
        learning_rate: Learning rate for optimization
        epochs: Number of training epochs
        batch_size: Batch size for training
        eval_threshold: Minimum accuracy threshold
    """
    config = create_config(learning_rate, epochs, batch_size)
    data = load_data(config)
    model = train(data, config)
    evaluate(model, threshold=eval_threshold)


def demonstrate_defaults():
    print("=== Running with Defaults ===\n")

    result = training_pipeline()

    eval_result = result["evaluate"]
    print(f"Evaluation result: {eval_result.result}")
    print()


def demonstrate_overrides():
    print("=== Running with Overrides ===\n")

    # Override some parameters
    result = training_pipeline(
        learning_rate=0.1,
        epochs=20,
        eval_threshold=0.95,
    )

    eval_result = result["evaluate"]
    print(f"Evaluation result: {eval_result.result}")
    print()


def demonstrate_config_object():
    print("=== Using Config Object Directly ===\n")

    # Config provides attribute-style access
    config = Config(
        model="resnet50",
        optimizer="adam",
        learning_rate=0.001,
        epochs=100,
    )

    print(f"Config: {config}")
    print(f"  model: {config.model}")
    print(f"  optimizer: {config.optimizer}")
    print(f"  learning_rate: {config.learning_rate}")
    print()

    # Config is hashable (for use as task arguments)
    config2 = Config(
        model="resnet50",
        optimizer="adam",
        learning_rate=0.001,
        epochs=100,
    )

    print(f"Same config has same hash: {hash(config) == hash(config2)}")
    print()

    # Config can be modified
    config.epochs = 200
    print(f"After modification: epochs={config.epochs}")

    # Convert to dict
    print(f"As dict: {config.to_dict()}")


@flow
def hyperparameter_sweep():
    """Run multiple configurations in one flow."""
    results = []

    for lr in [0.001, 0.01, 0.1]:
        for epochs in [10, 50]:
            config = create_config(lr, epochs)
            data = load_data(config)
            model = train(data, config)
            evaluate(model).name(f"eval_lr{lr}_ep{epochs}")


def demonstrate_sweep():
    print("=== Hyperparameter Sweep ===\n")

    result = hyperparameter_sweep()

    # Get all evaluation results
    evals = result["eval_.*"]

    print("Results:")
    for e in evals:
        r = e.result
        print(f"  {e.task_name}: accuracy={r['accuracy']:.3f}, passed={r['passed']}")

    # Find best configuration
    best = max(evals, key=lambda e: e.result["accuracy"])
    print(f"\nBest: {best.task_name} with accuracy={best.result['accuracy']:.3f}")


if __name__ == "__main__":
    demonstrate_defaults()
    demonstrate_overrides()
    demonstrate_config_object()
    demonstrate_sweep()
