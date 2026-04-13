#!/usr/bin/env python3
"""Cards — user-composed reports built from typed components.

A card is a named grouping of components (Markdown, Table, Image, Plotly,
Scalar, ...) that the user explicitly assembles inside a task. Cards can
be re-emitted at multiple iterations; the viewer shows a slider.

Usage:
    python examples/16_cards.py
    python -m rinnsal.viewer   # view the composed cards
"""

from rinnsal import task, flow, current
from rinnsal.data.logger.components import (
    Artifact,
    Markdown,
    ProgressBar,
    PythonCode,
    Scalar,
    Table,
)

import matplotlib.pyplot as plt


@task
def train(lr: float = 0.01, epochs: int = 5):
    """Train a model and build a composed report card for this run."""
    losses, accs = [], []
    metrics_rows = []
    for epoch in range(epochs):
        loss = 1.0 / (epoch + 1 + lr * 10)
        acc = 1.0 - loss
        losses.append(loss)
        accs.append(acc)
        metrics_rows.append([epoch + 1, f"{loss:.4f}", f"{acc:.4f}"])

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(range(1, epochs + 1), losses, label="Loss", marker="o")
    ax.plot(range(1, epochs + 1), accs, label="Accuracy", marker="s")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Value")
    ax.set_title(f"Training Progress (lr={lr})")
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Compose a card for this task run.
    with current.logger.card(f"train_lr_{lr}") as card:
        card.append(Markdown(f"# Training run · lr={lr}"))
        card.append(Markdown(f"Trained for **{epochs}** epochs."))
        card.append(Table(metrics_rows, headers=["Epoch", "Loss", "Accuracy"]))
        card.append(fig)  # auto-detected as Figure (matplotlib)
        card.append(Markdown(f"Final accuracy: **{accs[-1]:.4f}**"))

    plt.close(fig)

    return {"lr": lr, "loss": losses[-1], "accuracy": accs[-1]}


@task
def compare(r0: dict, r1: dict, r2: dict):
    """Summarize and compare multiple training runs in a single card."""
    results = [r0, r1, r2]
    rows = [
        [r["lr"], f"{r['loss']:.4f}", f"{r['accuracy']:.4f}"]
        for r in results
    ]
    best = max(results, key=lambda r: r["accuracy"])

    with current.logger.card("comparison") as card:
        card.append(Markdown("## Comparison of training runs"))
        card.append(Table(rows, headers=["LR", "Loss", "Accuracy"]))
        card.append(Markdown(f"**Best LR:** `{best['lr']}`"))
        card.append(Scalar(best["accuracy"]))
        # Any Python value can be appended directly — autodetect routes
        # dicts/lists/numbers to the Artifact component (rendered + pickled).
        card.append({"loss": best["loss"], "accuracy": best["accuracy"]})
        card.append([r["accuracy"] for r in results])
        card.append(PythonCode(
            "best = max(results, key=lambda r: r['accuracy'])",
            language="python",
        ))
        card.append(ProgressBar(
            value=best["accuracy"], total=1.0, label="best accuracy"
        ))

    return best


@flow
def experiment():
    a = train(lr=0.001)
    b = train(lr=0.01)
    c = train(lr=0.1)
    return compare(a, b, c)


if __name__ == "__main__":
    result = experiment().run()
    print(f"\nResult: {result.result}")
    print("\nCards logged to .rinnsal/flows/experiment/runs/")
    print("View with: python -m rinnsal.viewer")
