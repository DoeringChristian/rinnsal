"""Rinnsal: A declarative DAG execution framework for Python."""

from rinnsal.core.task import task
from rinnsal.core.flow import flow, set_progress
from rinnsal.core.types import Config
from rinnsal.core.registry_build import register, build
from rinnsal.runtime.engine import eval

__all__ = [
    "task",
    "flow",
    "eval",
    "Config",
    "register",
    "build",
    "set_progress",
]
__version__ = "0.1.0"
