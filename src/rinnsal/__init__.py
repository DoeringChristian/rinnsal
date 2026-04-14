"""Rinnsal: A declarative DAG execution framework for Python.

The logger/viewer surface was removed in favor of first-class aim
integration. See :mod:`rinnsal.aim` (``from rinnsal.aim import
AimLogger``) for the supported tracking API.
"""

from rinnsal.modeling.task import task
from rinnsal.modeling.flow import flow, FlowResult, set_progress
from rinnsal.modeling.types import Config, Entry, Resources, TaskRuns, to_dict
from rinnsal.modeling.registry_build import register, build
from rinnsal.versioning.snapshot import use_snapshot
from rinnsal.compute.engine import eval
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
    "current",
    "use_snapshot",
]
__version__ = "0.1.0"
