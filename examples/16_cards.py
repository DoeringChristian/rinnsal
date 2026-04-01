#!/usr/bin/env python3
"""Cards - attach rich content to task results.

Tasks can add text, HTML, images, and tables to a card via
``current.card``. When running flows, card events are automatically
logged to the run's events.pb file and are viewable in the web viewer.

Card types:
- ``current.card.text(content, title="")`` - Markdown/text content
- ``current.card.html(content, title="")`` - Raw HTML content
- ``current.card.table(data, title="", headers=None)`` - Tables from lists or DataFrames
- ``current.card.image(figure, title="")`` - Matplotlib figures

Usage:
    python examples/16_cards.py

Then view the cards in the web viewer:
    python -m rinnsal.viewer
"""

from rinnsal import task, flow, current

import matplotlib.pyplot as plt


@task
def train(lr: float = 0.01, epochs: int = 5):
    """Train a model and record metrics on its card."""
    metrics = []
    losses = []
    accs = []
    for epoch in range(epochs):
        loss = 1.0 / (epoch + 1 + lr * 10)
        acc = 1.0 - loss
        metrics.append([epoch + 1, f"{loss:.4f}", f"{acc:.4f}"])
        losses.append(loss)
        accs.append(acc)

    # Add text card
    current.card.text(f"Trained for {epochs} epochs with lr={lr}")

    # Add table card
    current.card.table(
        metrics,
        title="Training Metrics",
        headers=["Epoch", "Loss", "Accuracy"],
    )

    # Add image card (matplotlib figure)
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(range(1, epochs + 1), losses, label="Loss", marker="o")
    ax.plot(range(1, epochs + 1), accs, label="Accuracy", marker="s")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Value")
    ax.set_title(f"Training Progress (lr={lr})")
    ax.legend()
    ax.grid(True, alpha=0.3)
    current.card.image(fig, title="Training Curves")
    plt.close(fig)

    # Add HTML card
    current.card.html(f"<p>Final accuracy: <b>{metrics[-1][2]}</b></p>")

    return {"loss": float(metrics[-1][1]), "accuracy": float(metrics[-1][2])}


@task
def compare(results):
    """Compare multiple training runs."""
    rows = [
        [
            r["lr"],
            f"{r['result']['loss']:.4f}",
            f"{r['result']['accuracy']:.4f}",
        ]
        for r in results
    ]
    current.card.text("## Comparison of training runs")
    current.card.table(rows, headers=["LR", "Loss", "Accuracy"])

    best = max(results, key=lambda r: r["result"]["accuracy"])
    current.card.text(f"Best LR: **{best['lr']}**")
    return best


@flow
def experiment():
    learning_rates = [0.001, 0.01, 0.1]
    models = []
    for lr in learning_rates:
        m = train(lr=lr)
        models.append(m)

    # Wrap results with their LR for comparison
    return models


if __name__ == "__main__":
    result = experiment()
    result.run()

    for t in result:
        print(f"{t.task_name}: {t.result}")

    # Cards are automatically logged to events.pb during flow execution.
    # View them in the web viewer:
    print("\nCards logged to .rinnsal/flows/experiment/runs/")
    print("View with: python -m rinnsal.viewer")
