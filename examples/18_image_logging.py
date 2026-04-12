"""Example: Logging images to the viewer.

Demonstrates `logger.add_image()` which accepts numpy arrays,
torch tensors, PIL Images, or raw PNG bytes.

Run:
    python examples/18_image_logging.py

View:
    rinnsal-viewer
"""

import numpy as np
from rinnsal.logger import Logger


def make_gradient(it: int, size: int = 128) -> np.ndarray:
    """Create a gradient image that shifts with iteration."""
    x = np.linspace(0, 1, size)
    y = np.linspace(0, 1, size)
    xx, yy = np.meshgrid(x, y)
    # Shift hue with iteration
    r = np.sin(xx * 3 + it * 0.3) * 0.5 + 0.5
    g = np.sin(yy * 3 + it * 0.2) * 0.5 + 0.5
    b = np.sin((xx + yy) * 2 + it * 0.1) * 0.5 + 0.5
    img = np.stack([r, g, b], axis=-1)  # float [0, 1]
    return img


def make_noise(it: int, size: int = 64) -> np.ndarray:
    """Create a grayscale noise pattern."""
    rng = np.random.default_rng(seed=it)
    return rng.integers(0, 256, size=(size, size), dtype=np.uint8)


def main():
    with Logger() as logger:
        for i in range(20):
            logger.set_iteration(i)

            # Log a color gradient (float RGB)
            logger.add_image("gradient", make_gradient(i))

            # Log grayscale noise (uint8)
            logger.add_image("noise", make_noise(i))

            # Log a scalar alongside for context
            logger.add_scalar("step", float(i))

            print(f"Iteration {i}: logged gradient + noise")

    print("\nDone! View with: rinnsal-viewer")


if __name__ == "__main__":
    main()
