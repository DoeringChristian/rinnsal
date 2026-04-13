#!/usr/bin/env python3
"""Logging figures from tasks into the viewer.

Tasks can log matplotlib figures via ``current.logger.add_figure()``.
These appear in the Figures tab when viewing a run. Figures can be
logged as static PNGs or as interactive (pickled) objects.

Works with all executors — subprocess, SSH, Slurm.

Usage:
    python examples/20_figure_logging.py
    python examples/20_figure_logging.py --executor ssh:myhost

Then view:
    python -m rinnsal.viewer
"""

import math

import matplotlib.pyplot as plt

from rinnsal import task, flow, current


@task
def generate_data(n: int = 200, noise: float = 0.3):
    """Generate noisy sine wave data."""
    import random

    random.seed(42)
    x = [i * 4 * math.pi / n for i in range(n)]
    y = [math.sin(xi) + random.gauss(0, noise) for xi in x]

    current.logger.add_scalar("data/n_points", n)
    current.logger.add_scalar("data/noise", noise)

    return x, y


@task
def fit_model(data, degree: int = 5):
    """Fit a polynomial and log the training curve."""
    import random

    x, y = data
    logger = current.logger

    # Simulate iterative fitting with decreasing loss
    random.seed(0)
    losses = []
    for epoch in range(20):
        logger.set_iteration(epoch)
        loss = 1.0 / (epoch + 1) + random.gauss(0, 0.02)
        losses.append(loss)
        logger.add_scalar("train/loss", loss)
        logger.add_scalar("train/epoch", epoch)

    # Log training loss curve as a figure
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(range(len(losses)), losses, "b-o", markersize=4, label="Loss")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("Training Loss")
    ax.legend()
    ax.grid(True, alpha=0.3)
    logger.add_figure("train/loss_curve", fig)
    plt.close(fig)

    # Compute polynomial coefficients (numpy-free, simple least-squares)
    # For the example we just store the degree
    coeffs = [0.1 * ((-1) ** i) / (i + 1) for i in range(degree + 1)]

    logger.add_text("model/info", f"Polynomial degree={degree}")
    logger.add_markdown("model/summary", f"Fitted polynomial of degree **{degree}**")

    # Optional: interactive Plotly figure, if the extra is installed.
    try:
        import plotly.graph_objects as go

        pfig = go.Figure(
            data=[go.Scatter(x=list(range(len(losses))), y=losses, mode="lines+markers")]
        )
        pfig.update_layout(
            title="Training loss (interactive)",
            xaxis_title="Epoch",
            yaxis_title="Loss",
        )
        logger.add_plotly("train/loss_interactive", pfig)
    except ImportError:
        pass  # plotly extra not installed; skip the interactive demo

    return {"coeffs": coeffs, "degree": degree, "x": x, "y": y}


@task
def plot_results(model):
    """Generate final result figures."""
    logger = current.logger
    x, y = model["x"], model["y"]
    degree = model["degree"]
    coeffs = model["coeffs"]

    # Predicted values (simple polynomial eval)
    def poly(xi):
        return sum(c * xi**i for i, c in enumerate(coeffs))

    y_pred = [poly(xi) for xi in x]

    # Data + fit figure
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(x, y, s=10, alpha=0.5, label="Data", color="steelblue")
    ax.plot(x, y_pred, "r-", linewidth=2, label=f"Poly (deg={degree})")
    ax.set_xlabel("x")
    ax.set_ylabel("y")
    ax.set_title("Data and Polynomial Fit")
    ax.legend()
    ax.grid(True, alpha=0.3)
    logger.add_figure("results/fit", fig)
    plt.close(fig)

    # Residuals figure
    residuals = [yi - yp for yi, yp in zip(y, y_pred)]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

    ax1.scatter(x, residuals, s=10, alpha=0.5, color="coral")
    ax1.axhline(0, color="gray", linestyle="--", linewidth=0.8)
    ax1.set_xlabel("x")
    ax1.set_ylabel("Residual")
    ax1.set_title("Residuals")
    ax1.grid(True, alpha=0.3)

    ax2.hist(residuals, bins=30, color="coral", edgecolor="white", alpha=0.8)
    ax2.set_xlabel("Residual")
    ax2.set_ylabel("Count")
    ax2.set_title("Residual Distribution")
    ax2.grid(True, alpha=0.3)

    logger.add_figure("results/residuals", fig)
    plt.close(fig)

    # Log summary scalars
    rmse = math.sqrt(sum(r**2 for r in residuals) / len(residuals))
    logger.add_scalar("eval/rmse", rmse)
    logger.add_scalar("eval/n_points", len(x))

    logger.add_markdown(
        "eval/summary",
        f"**RMSE:** {rmse:.4f}\n\n"
        f"**Points:** {len(x)}\n\n"
        f"**Degree:** {degree}",
    )

    return {"rmse": rmse}


@flow
def figure_pipeline(n: int = 200, noise: float = 0.3, degree: int = 5):
    """Pipeline that generates data, fits a model, and plots results."""
    data = generate_data(n=n, noise=noise)
    model = fit_model(data, degree=degree)
    return plot_results(model)


if __name__ == "__main__":
    result = figure_pipeline().run()
    print(f"\nResult: {result.result}")
    print("\nView figures with: python -m rinnsal.viewer")
    print("  - Figures tab: train/loss_curve, results/fit, results/residuals")
    print("  - Scalars tab: data/*, train/*, eval/*")
