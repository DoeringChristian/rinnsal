#!/usr/bin/env python3
"""Live monitoring: a task that runs forever, emitting metrics.

This simulates a long-running training loop that logs scalars and
figures continuously.  Open the viewer in another terminal to watch
metrics arrive in real-time:

    python -m rinnsal.viewer

Usage:
    python examples/21_live_monitoring.py
    python examples/21_live_monitoring.py --executor ssh:myhost
    python examples/21_live_monitoring.py --executor pssh:myhost

Press Ctrl+C to stop.
"""

import math
import time

import matplotlib.pyplot as plt

from rinnsal import task, flow, current


@task
def monitor():
    """Infinite training loop with live logging."""
    logger = current.logger

    logger.add_text("config", "lr=0.001, batch_size=64, model=resnet18")

    losses = []
    val_losses = []
    epoch = 0

    while True:
        logger.set_iteration(epoch)

        # Simulated metrics with noise
        import random

        loss = math.exp(-epoch / 50) + 0.1 * random.gauss(0, 1) ** 2
        val_loss = math.exp(-epoch / 50) * 1.1 + 0.15 * random.gauss(0, 1) ** 2
        accuracy = 1 - loss * 0.8 + random.gauss(0, 0.02)
        lr = 0.001 * math.exp(-epoch / 200)

        losses.append(loss)
        val_losses.append(val_loss)

        logger.add_scalar("train/loss", loss)
        logger.add_scalar("val/loss", val_loss)
        logger.add_scalar("train/accuracy", accuracy)
        logger.add_scalar("lr", lr)

        print(
            f"[epoch {epoch}] loss={loss:.4f} val_loss={val_loss:.4f} acc={accuracy:.4f}"
        )

        # Log a figure every 10 epochs
        if epoch % 10 == 0 and epoch > 0:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

            ax1.plot(losses, "b-", alpha=0.7, label="Train")
            ax1.plot(val_losses, "r-", alpha=0.7, label="Val")
            ax1.set_xlabel("Epoch")
            ax1.set_ylabel("Loss")
            ax1.set_title(f"Loss (epoch {epoch})")
            ax1.legend()
            ax1.grid(True, alpha=0.3)

            # Rolling average
            window = min(20, len(losses))
            if window > 1:
                roll = [
                    sum(losses[max(0, i - window) : i]) / min(i, window)
                    for i in range(1, len(losses) + 1)
                ]
                ax1.plot(roll, "b--", linewidth=2, label="Train (avg)")

            ax2.plot(
                [1 - l * 0.8 for l in losses], "g-", alpha=0.7, label="Accuracy"
            )
            ax2.set_xlabel("Epoch")
            ax2.set_ylabel("Accuracy")
            ax2.set_title(f"Accuracy (epoch {epoch})")
            ax2.legend()
            ax2.grid(True, alpha=0.3)

            plt.tight_layout()
            logger.add_figure("train/curves", fig)
            plt.close(fig)

            logger.add_text(
                "status",
                f"Epoch {epoch}: loss={loss:.4f}, val_loss={val_loss:.4f}, "
                f"acc={accuracy:.4f}",
            )

        # Compose a checkpoint card every 50 epochs.
        if epoch % 50 == 0 and epoch > 0:
            from rinnsal.data.logger.components import Markdown, Scalar

            with logger.card("checkpoint") as card:
                card.append(Markdown(f"## Checkpoint at epoch {epoch}"))
                card.append(Markdown(
                    f"- Train loss: **{loss:.4f}**\n"
                    f"- Val loss: **{val_loss:.4f}**\n"
                    f"- Accuracy: **{accuracy:.4f}**\n"
                    f"- LR: **{lr:.6f}**"
                ))
                card.append(Scalar(loss))

        epoch += 1
        time.sleep(1)  # 1 epoch per second


@flow
def live_training():
    """A flow that runs forever for live monitoring."""
    return monitor()


if __name__ == "__main__":
    try:
        live_training().run()
    except KeyboardInterrupt:
        print("\nStopped.")
