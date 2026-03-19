"""Example: Using the Logger for experiment tracking.

The Logger provides TensorBoard-like experiment tracking with support for
scalars, text, figures, and checkpoints. Data is stored in protobuf format
by default for efficient storage and fast reads.
"""

import math
from pathlib import Path

import matplotlib.pyplot as plt

import rinnsal as rs


def train_loop(logger: rs.Logger, epochs: int = 10) -> dict:
    """Simulate a training loop with logging."""
    for epoch in range(epochs):
        # Set the current iteration for subsequent logs
        logger.set_iteration(epoch)

        # Log scalar metrics
        loss = 1.0 / (epoch + 1)
        accuracy = 1 - loss
        logger.add_scalar("loss", loss)
        logger.add_scalar("accuracy", accuracy)

        # Log text (e.g., for tracking hyperparameters or events)
        if epoch == 0:
            logger.add_text("config", "lr=0.001, batch_size=32")
        if epoch == epochs - 1:
            logger.add_text("status", "Training complete")

        # Log checkpoints (any picklable object)
        if epoch % 5 == 0:
            weights = {"layer1": [1, 2, 3], "layer2": [4, 5, 6]}
            logger.add_checkpoint("model", weights)

        # Log a non-interactive figure (viewer renders as static PNG)
        if epoch % 3 == 0:
            fig, ax = plt.subplots()
            ax.bar(["loss", "accuracy"], [loss, accuracy])
            ax.set_title(f"Metrics at epoch {epoch}")
            logger.add_figure("metrics_bar", fig, interactive=False)
            plt.close(fig)

        # Log an interactive 3D figure (viewer renders with ipympl)
        if epoch % 5 == 0:
            import numpy as np

            fig = plt.figure(figsize=(8, 6))
            ax = fig.add_subplot(111, projection="3d")

            x = np.linspace(-2, 2, 50)
            y = np.linspace(-2, 2, 50)
            X, Y = np.meshgrid(x, y)

            center_x = 1.5 * math.exp(-epoch / 8)
            center_y = 1.5 * math.exp(-epoch / 8)
            Z = (
                (X - center_x) ** 2
                + (Y - center_y) ** 2
                + 0.5 * np.sin(3 * X) * np.cos(3 * Y)
            )

            ax.plot_surface(X, Y, Z, cmap="viridis", alpha=0.8)
            ax.scatter([center_x], [center_y], [0], color="red", s=100)
            ax.set_xlabel("Weight 1")
            ax.set_ylabel("Weight 2")
            ax.set_zlabel("Loss")
            ax.set_title(f"Loss Landscape (epoch {epoch})")

            logger.add_figure("loss_landscape_3d", fig, interactive=True)
            plt.close(fig)

    return {"final_loss": loss, "final_accuracy": accuracy}


def read_logs(log_dir: Path) -> None:
    """Demonstrate reading logs with LogReader."""
    reader = rs.LogReader(log_dir)

    print(f"Log directory: {reader.path}")
    print(f"Is a run: {reader.is_run}")
    print(f"Iterations: {reader.iterations}")

    # Get available tags
    print(f"Scalar tags: {reader.scalar_tags}")
    print(f"Text tags: {reader.text_tags}")
    print(f"Checkpoint tags: {reader.checkpoint_tags}")
    print(f"Figure tags: {reader.figure_tags}")

    # Load scalar time series
    its, losses = reader.scalars("loss")
    print(f"\nLoss over time:")
    for it, loss in zip(its, losses):
        print(f"  Epoch {it}: {loss:.4f}")

    # Quick access to last value using [] syntax
    last_it, last_acc = reader["accuracy"]
    print(f"\nLast accuracy: {last_acc:.4f} (epoch {last_it})")

    # Load text entries
    text_data = reader.load_text("config")
    if text_data:
        print(f"\nConfig: {text_data[0][1]}")

    # Load checkpoint
    ckpt_iterations = reader.checkpoint_iterations("model")
    if ckpt_iterations:
        weights = reader.load_checkpoint("model", ckpt_iterations[-1])
        print(
            f"\nLoaded checkpoint from epoch {ckpt_iterations[-1]}: {weights}"
        )


if __name__ == "__main__":
    # Create logger and run training
    # Logger auto-creates a timestamped directory under runs/
    print("=== Training with Logger ===\n")
    with rs.Logger() as logger:
        log_dir = logger.log_dir
        print(f"Logging to: {log_dir}\n")
        result = train_loop(logger, epochs=10)
        print(f"Training result: {result}\n")

    # Read back the logs
    print("=== Reading Logs ===\n")
    read_logs(log_dir)
