#!/usr/bin/env python3
"""Config objects and flow parameters with card logging.

This example demonstrates using Config objects for structured parameters
and logging configuration and results to cards for visualization.
"""

from rinnsal import task, flow, Config, current


@task
def train(config: Config):
    print(f"Training with lr={config.lr}, epochs={config.epochs}")
    acc = 0.5 + config.lr * config.epochs * 0.01

    # Log config and result to card
    current.card.text(f"**Configuration:**\n- lr: {config.lr}\n- epochs: {config.epochs}")
    current.card.table(
        [[config.lr, config.epochs, f"{acc:.4f}"]],
        title="Training Result",
        headers=["Learning Rate", "Epochs", "Accuracy"],
    )

    return {"acc": acc}


@task
def evaluate(result, threshold=0.8):
    passed = result["acc"] >= threshold
    status = "PASSED" if passed else "FAILED"

    # Log evaluation to card
    current.card.html(
        f"<p>Accuracy: <b>{result['acc']:.4f}</b></p>"
        f"<p>Threshold: {threshold}</p>"
        f"<p>Status: <span style='color: {'green' if passed else 'red'}'>{status}</span></p>"
    )

    return {"acc": result["acc"], "passed": passed}


@flow
def pipeline(lr=0.01, epochs=10, threshold=0.8):
    config = Config(lr=lr, epochs=epochs)
    result = train(config)
    return evaluate(result, threshold=threshold)


if __name__ == "__main__":
    # Default parameters
    r1 = pipeline().run()
    print(f"Default: {r1.result}")

    # Override parameters
    r2 = pipeline(lr=0.1, epochs=50, threshold=0.9).run()
    print(f"Override: {r2.result}")

    # View cards in the web viewer
    print("\nView cards with: python -m rinnsal.viewer")
