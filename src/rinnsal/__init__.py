"""Rinnsal: A declarative DAG execution framework for Python."""

from rinnsal.core.task import task
from rinnsal.core.flow import flow
from rinnsal.core.types import Config
from rinnsal.runtime.engine import eval

__all__ = ["task", "flow", "eval", "Config"]
__version__ = "0.1.0"
