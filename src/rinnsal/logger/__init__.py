"""Experiment logging utilities for rinnsal.

Provides TensorBoard-like logging for scalars, text, figures, and checkpoints.
"""

from rinnsal.logger.logger import Logger
from rinnsal.logger.reader import LazyFigure, LogReader

__all__ = ["Logger", "LogReader", "LazyFigure"]
