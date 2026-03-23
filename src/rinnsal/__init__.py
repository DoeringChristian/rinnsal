"""Rinnsal: A declarative DAG execution framework for Python."""

from rinnsal.core.task import task
from rinnsal.core.flow import flow, FlowResult, set_progress
from rinnsal.core.types import Config, Entry, Resources, TaskRuns, to_dict
from rinnsal.core.registry_build import register, build
from rinnsal.core.snapshot import use_snapshot
from rinnsal.runtime.engine import eval
from rinnsal.logger import Logger, LogReader
from rinnsal.context import current

__all__ = [
    "task",
    "flow",
    "FlowResult",
    "eval",
    "Config",
    "Entry",
    "Resources",
    "TaskRuns",
    "to_dict",
    "register",
    "build",
    "set_progress",
    "Logger",
    "LogReader",
    "current",
    "use_snapshot",
]
__version__ = "0.1.0"
