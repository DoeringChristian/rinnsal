#!/usr/bin/env python3
"""Config objects and flow parameters."""

from rinnsal import task, flow, Config


@task
def train(config: Config):
    print(f"Training with lr={config.lr}, epochs={config.epochs}")
    return {"acc": 0.5 + config.lr * config.epochs * 0.01}


@task
def evaluate(result, threshold=0.8):
    return {"acc": result["acc"], "passed": result["acc"] >= threshold}


@flow
def pipeline(lr=0.01, epochs=10, threshold=0.8):
    config = Config(lr=lr, epochs=epochs)
    result = train(config)
    return evaluate(result, threshold=threshold)


if __name__ == "__main__":
    # Default parameters
    r1 = pipeline()
    print(f"Default: {r1[-1].result}")

    # Override parameters
    r2 = pipeline(lr=0.1, epochs=50, threshold=0.9)
    print(f"Override: {r2[-1].result}")
