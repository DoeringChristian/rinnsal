"""Experiment logging utilities for rinnsal.

Provides TensorBoard-like logging for scalars, text, figures, and checkpoints.
"""

from rinnsal.data.logger.logger import Logger
from rinnsal.data.logger.reader import LazyFigure, LogReader

__all__ = ["Logger", "LogReader", "LazyFigure"]
