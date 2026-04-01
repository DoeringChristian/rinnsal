#!/usr/bin/env python3
"""Using the Logger within flows and tasks.

When a flow runs, a logger is automatically created at:
    .rinnsal/flows/<flow_name>/runs/<run_id>/events.pb

Tasks can access this logger via `current.logger` to log:
- Scalars (metrics over iterations)
- Text (status messages, configs)
- Figures (matplotlib plots)
- Cards (rich content for the viewer)

IMPORTANT: `current.logger` is only available when using the inline executor.
With the default subprocess executor, use cards instead (which are automatically
captured and logged after task completion).

Usage:
    python examples/17_logger_in_tasks.py --executor inline

Then view the logs:
    python -m rinnsal.viewer
"""

import matplotlib.pyplot as plt

from rinnsal import task, flow, current


@task
def train(epochs: int = 10, lr: float = 0.01):
    """Training task that logs metrics to the flow's logger."""
    logger = current.logger

    if logger is None:
        print("Warning: No logger available (running outside a flow?)")
        return {"final_loss": 0.1}

    # Log configuration as text
    logger.add_text("config", f"epochs={epochs}, lr={lr}")

    losses = []
    for epoch in range(epochs):
        # Simulate training
        loss = 1.0 / (epoch + 1 + lr * 10)
        acc = 1.0 - loss
        losses.append(loss)

        # Log scalar metrics
        logger.set_iteration(epoch)
        logger.add_scalar("train/loss", loss)
        logger.add_scalar("train/accuracy", acc)

    # Log a figure showing training progress
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(range(epochs), losses, marker='o', label=f'lr={lr}')
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("Training Loss")
    ax.legend()
    ax.grid(True, alpha=0.3)
    logger.add_figure("train/loss_curve", fig)
    plt.close(fig)

    # Log final status
    logger.add_text("status", f"Training complete. Final loss: {losses[-1]:.4f}")

    # Also add to card for the Cards tab
    current.card.text(f"Trained for {epochs} epochs with lr={lr}")
    current.card.table(
        [[i+1, f"{losses[i]:.4f}"] for i in range(epochs)],
        title="Loss per Epoch",
        headers=["Epoch", "Loss"],
    )

    return {"final_loss": losses[-1]}


@task
def evaluate(train_result):
    """Evaluation task that logs results."""
    logger = current.logger

    # Simulate evaluation
    test_loss = train_result["final_loss"] * 1.1
    test_acc = 1.0 - test_loss

    if logger:
        logger.add_scalar("eval/loss", test_loss)
        logger.add_scalar("eval/accuracy", test_acc)
        logger.add_text("eval/result", f"Test accuracy: {test_acc:.4f}")

    current.card.html(
        f"<h3>Evaluation Results</h3>"
        f"<p>Test Loss: <b>{test_loss:.4f}</b></p>"
        f"<p>Test Accuracy: <b>{test_acc:.4f}</b></p>"
    )

    return {"test_loss": test_loss, "test_accuracy": test_acc}


@flow
def training_pipeline(epochs: int = 10, lr: float = 0.01):
    """A training pipeline that uses the auto-created logger."""
    result = train(epochs=epochs, lr=lr)
    return evaluate(result)


if __name__ == "__main__":
    # Run the pipeline - logger is automatically created
    flow_result = training_pipeline(epochs=10, lr=0.01)
    final = flow_result.run()

    print(f"\nFinal result: {final.result}")
    print("\nLogs saved to .rinnsal/flows/training_pipeline/runs/")
    print("View with: python -m rinnsal.viewer")
    print("\nIn the viewer you can see:")
    print("  - Scalars tab: train/loss, train/accuracy, eval/loss, eval/accuracy")
    print("  - Text tab: config, status, eval/result")
    print("  - Figures tab: train/loss_curve")
    print("  - Cards tab: training metrics table, evaluation results")
