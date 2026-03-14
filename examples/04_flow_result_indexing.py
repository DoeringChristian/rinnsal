#!/usr/bin/env python3
"""FlowResult indexing example.

This example demonstrates the rich indexing capabilities of FlowResult:
- Integer indexing (positional)
- String indexing (regex match on name)
- Callable indexing (filter by arguments)
"""

from rinnsal import task, flow, Config


@task
def load_dataset(name):
    """Load a dataset by name."""
    return {"name": name, "size": len(name) * 100}


@task
def preprocess(data, normalize=True):
    """Preprocess the data."""
    return {
        **data,
        "preprocessed": True,
        "normalized": normalize,
    }


@task
def train_model(data, learning_rate=0.01, epochs=10):
    """Train a model on the data."""
    return {
        "model": f"model_{data['name']}",
        "lr": learning_rate,
        "epochs": epochs,
        "accuracy": 0.9 + learning_rate * epochs / 100,
    }


@task
def evaluate(model_result):
    """Evaluate a trained model."""
    return {
        "model": model_result["model"],
        "accuracy": model_result["accuracy"],
        "evaluated": True,
    }


@flow
def ml_pipeline():
    """A multi-branch ML pipeline."""
    # Load different datasets
    mnist = load_dataset("mnist").name("load_mnist")
    cifar = load_dataset("cifar").name("load_cifar")

    # Preprocess each
    mnist_prep = preprocess(mnist).name("prep_mnist")
    cifar_prep = preprocess(cifar).name("prep_cifar")

    # Train models with different hyperparameters
    model1 = train_model(mnist_prep, learning_rate=0.01).name("train_mnist_slow")
    model2 = train_model(mnist_prep, learning_rate=0.1).name("train_mnist_fast")
    model3 = train_model(cifar_prep, learning_rate=0.01).name("train_cifar")

    # Evaluate all models
    evaluate(model1).name("eval_mnist_slow")
    evaluate(model2).name("eval_mnist_fast")
    evaluate(model3).name("eval_cifar")


def demonstrate_indexing():
    print("=== FlowResult Indexing ===\n")

    result = ml_pipeline()

    print(f"Total tasks: {len(result)}")
    print()

    # Integer indexing
    print("--- Integer Indexing ---")
    print(f"result[0]: {result[0].task_name} = {result[0].result}")
    print(f"result[-1]: {result[-1].task_name} = {result[-1].result}")
    print()

    # String indexing - exact match
    print("--- String Indexing (exact) ---")
    mnist_task = result["load_mnist"]
    print(f'result["load_mnist"]: {mnist_task.result}')
    print()

    # String indexing - regex pattern
    print("--- String Indexing (regex) ---")
    train_tasks = result["train_.*"]
    print(f'result["train_.*"] matched {len(train_tasks)} tasks:')
    for t in train_tasks:
        print(f"  - {t.task_name}: {t.result}")
    print()

    eval_tasks = result["eval_.*"]
    print(f'result["eval_.*"] matched {len(eval_tasks)} tasks:')
    for t in eval_tasks:
        print(f"  - {t.task_name}: accuracy={t.result['accuracy']:.3f}")
    print()

    # Callable indexing - filter by arguments
    print("--- Callable Indexing ---")

    # Find tasks with learning_rate=0.01
    slow_models = result[lambda learning_rate: learning_rate == 0.01]
    print(f"Tasks with learning_rate=0.01: {len(slow_models)}")
    for t in slow_models:
        print(f"  - {t.task_name}")
    print()

    # Find tasks training on mnist
    mnist_tasks = result[lambda name: name == "mnist"]
    print(f'Tasks with name="mnist": {len(mnist_tasks)}')
    for t in mnist_tasks:
        print(f"  - {t.task_name}")


def demonstrate_chained_filtering():
    print("\n=== Chained Filtering ===\n")

    result = ml_pipeline()

    # Chain multiple filters
    # First get all training tasks, then filter by learning rate
    training = result["train_.*"]
    fast_training = training[lambda learning_rate: learning_rate == 0.1]

    print(f"All training tasks: {len(training)}")
    print(f"Fast training tasks (lr=0.1): {len(fast_training)}")

    if hasattr(fast_training, '__iter__'):
        for t in fast_training:
            print(f"  - {t.task_name}: {t.result}")
    else:
        print(f"  - {fast_training.task_name}: {fast_training.result}")


if __name__ == "__main__":
    demonstrate_indexing()
    demonstrate_chained_filtering()
