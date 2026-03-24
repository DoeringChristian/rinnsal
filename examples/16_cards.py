#!/usr/bin/env python3
"""Cards - attach rich content to task results.

Tasks can add text, HTML, and tables to a card via
``current.card``. Card data is stored in the Entry metadata
and viewable in the web UI.

Usage:

    python examples/16_cards.py -s
"""

from rinnsal import task, flow, current
from rinnsal.persistence.file_store import FileDatabase
from rinnsal.execution.inline import InlineExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine


@task
def train(lr: float = 0.01, epochs: int = 5):
    """Train a model and record metrics on its card."""
    metrics = []
    for epoch in range(epochs):
        loss = 1.0 / (epoch + 1 + lr * 10)
        acc = 1.0 - loss
        metrics.append([epoch + 1, f"{loss:.4f}", f"{acc:.4f}"])

    # Add card content
    current.card.text(f"Trained for {epochs} epochs with lr={lr}")
    current.card.table(
        metrics,
        title="Training Metrics",
        headers=["Epoch", "Loss", "Accuracy"],
    )
    current.card.html(
        f"<p>Final accuracy: <b>{metrics[-1][2]}</b></p>"
    )

    return {"loss": float(metrics[-1][1]), "accuracy": float(metrics[-1][2])}


@task
def compare(results):
    """Compare multiple training runs."""
    rows = [
        [r["lr"], f"{r['result']['loss']:.4f}", f"{r['result']['accuracy']:.4f}"]
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
    db = FileDatabase(root=".rinnsal")
    executor = InlineExecutor()
    set_engine(ExecutionEngine(executor=executor, database=db))

    result = experiment()
    result.run()

    for t in result:
        print(f"{t.task_name}: {t.result}")

    # Card data is stored in .rinnsal and viewable with:
    #   python -m rinnsal.viewer .rinnsal
    print("\nCard data stored. View with: python -m rinnsal.viewer")
