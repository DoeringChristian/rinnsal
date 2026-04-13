"""Rinnsal: A declarative DAG execution framework for Python."""

from rinnsal.modeling.task import task
from rinnsal.modeling.flow import flow, FlowResult, set_progress
from rinnsal.modeling.types import Config, Entry, Resources, TaskRuns, to_dict
from rinnsal.modeling.registry_build import register, build
from rinnsal.versioning.snapshot import use_snapshot
from rinnsal.compute.engine import eval
from rinnsal.data.logger import Logger, LogReader
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
