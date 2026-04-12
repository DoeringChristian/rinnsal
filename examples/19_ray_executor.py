#!/usr/bin/env python3
"""Running flows on a Ray cluster.

Connects to a Ray cluster and distributes tasks across workers.
Logger events are streamed back to the orchestrator in real-time
via a Ray actor, so you can watch progress in the viewer as it runs.

Setup:
    # On the head node:
    ray start --head --node-ip-address=<HEAD_IP> --port=6379 --dashboard-host=0.0.0.0

    # On each worker node:
    ray start --address=<HEAD_IP>:6379 --node-ip-address=<WORKER_IP>

Usage:
    # Connect to a remote Ray cluster:
    python examples/19_ray_executor.py --executor ray:<HEAD_IP>:6379

    # Or use a local Ray cluster:
    python examples/19_ray_executor.py --executor ray

Then view results:
    python -m rinnsal.viewer
"""

from rinnsal import task, flow, current


@task
def generate_data(n: int = 1000):
    """Generate synthetic training data."""
    import random

    current.logger.add_text("status", f"Generating {n} data points")

    data = [random.gauss(0, 1) for _ in range(n)]
    current.logger.add_scalar("data/size", n)
    current.logger.add_scalar("data/mean", sum(data) / len(data))

    current.card.text(f"Generated {n} data points")
    return data


@task
def train_model(data, epochs: int = 20, lr: float = 0.01):
    """Train a simple model, logging metrics each epoch."""
    import math

    logger = current.logger
    logger.add_text("config", f"epochs={epochs}, lr={lr}, data_size={len(data)}")

    variance = sum(x ** 2 for x in data) / len(data)
    weights = [0.0]

    for epoch in range(epochs):
        logger.set_iteration(epoch)

        # Simulate gradient descent on variance estimation
        pred = weights[0]
        loss = (pred - variance) ** 2
        grad = 2 * (pred - variance)
        weights[0] -= lr * grad

        logger.add_scalar("train/loss", loss)
        logger.add_scalar("train/prediction", weights[0])
        logger.add_scalar("train/lr", lr * math.exp(-epoch / epochs))

    logger.add_text("status", f"Training complete. Final loss: {loss:.6f}")
    current.card.text(
        f"Trained for {epochs} epochs\n"
        f"Final loss: {loss:.6f}\n"
        f"Prediction: {weights[0]:.4f} (true: {variance:.4f})"
    )

    return {"weights": weights, "final_loss": loss}


@task
def evaluate(model_result, data):
    """Evaluate the trained model."""
    logger = current.logger

    weights = model_result["weights"]
    true_var = sum(x ** 2 for x in data) / len(data)
    error = abs(weights[0] - true_var)

    logger.add_scalar("eval/error", error)
    logger.add_scalar("eval/true_variance", true_var)
    logger.add_scalar("eval/predicted", weights[0])

    current.card.text(
        f"Evaluation:\n"
        f"  True variance: {true_var:.4f}\n"
        f"  Predicted: {weights[0]:.4f}\n"
        f"  Error: {error:.6f}"
    )

    return {"error": error, "true": true_var, "predicted": weights[0]}


@flow
def ray_pipeline(n: int = 1000, epochs: int = 20, lr: float = 0.01):
    """Pipeline that runs across a Ray cluster."""
    data = generate_data(n=n)
    model = train_model(data, epochs=epochs, lr=lr)
    return evaluate(model, data)


if __name__ == "__main__":
    result = ray_pipeline().run()
    print(f"\nResult: {result.result}")
    print("\nView with: python -m rinnsal.viewer")
