#!/usr/bin/env python3
"""Cards with Plotly components, re-emitted across iterations.

Demonstrates the unified Logger+Cards API:
  * User composes a card from typed components.
  * The same card name is committed multiple times (once per epoch).
  * The viewer shows an iteration slider across the emissions.
  * Plotly figures render interactively in the viewer; a PNG fallback
    is generated server-side (via kaleido) if available.

Requires the plotly extra:
    pip install rinnsal[plotly]

Usage:
    python examples/22_plotly_cards.py
    python -m rinnsal.viewer
"""

from __future__ import annotations

import math

from rinnsal import task, flow, current
from rinnsal.data.logger.components import (
    Markdown,
    Plotly,
    ProgressBar,
    Scalar,
)


@task
def train(epochs: int = 10):
    """Simulate a training loop, emitting a composed card per epoch."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        print("Skipping: plotly not installed (pip install rinnsal[plotly])")
        return {"skipped": True}

    logger = current.logger
    losses: list[float] = []

    for epoch in range(1, epochs + 1):
        logger.set_iteration(epoch)
        loss = 1.0 / epoch + 0.05 * math.sin(epoch)
        losses.append(loss)

        # Interactive loss curve up to this epoch.
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=list(range(1, epoch + 1)),
                    y=losses,
                    mode="lines+markers",
                    name="loss",
                )
            ]
        )
        fig.update_layout(
            title=f"Training loss · epoch {epoch}",
            xaxis_title="Epoch",
            yaxis_title="Loss",
        )

        # Compose and commit the card at this iteration.
        with logger.card("training_progress") as card:
            card.append(Markdown(f"## Epoch {epoch}/{epochs}"))
            card.append(Markdown(f"Current loss: **{loss:.4f}**"))
            card.append(Plotly(fig))
            card.append(ProgressBar(epoch, total=epochs, label="training"))
            card.append(Scalar(loss))

    return {"final_loss": losses[-1], "epochs": epochs}


@flow
def training_flow():
    return train(epochs=10)


if __name__ == "__main__":
    result = training_flow().run()
    print(f"\nResult: {result.result}")
    print("\nView the card with: python -m rinnsal.viewer")
    print("The 'training_progress' card has 10 iterations — use the slider.")
