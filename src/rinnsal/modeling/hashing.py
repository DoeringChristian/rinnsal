"""Content-addressed hashing for tasks and expressions."""

from __future__ import annotations

import hashlib
import inspect
from typing import Any, Callable

import cloudpickle


def hash_function(func: Callable[..., Any]) -> str:
    """Compute a hash of a function's source code.

    This captures the function's bytecode and source to detect changes.
    For built-in functions or functions without source, falls back to
    using the function's qualified name.
    """
    hasher = hashlib.sha256()

    # Include the function's qualified name
    qual_name = getattr(func, "__qualname__", func.__name__)
    module = getattr(func, "__module__", "")
    hasher.update(f"{module}.{qual_name}".encode())

    # Try to include source code
    try:
        source = inspect.getsource(func)
        hasher.update(source.encode())
    except (OSError, TypeError):
        # No source available - use bytecode via cloudpickle
        try:
            hasher.update(cloudpickle.dumps(func.__code__))
        except (AttributeError, TypeError):
            # Built-in or C function - just use the name
            pass

    return hasher.hexdigest()


def hash_value(value: Any) -> str:
    """Compute a hash of an arbitrary Python value.

    Uses cloudpickle to serialize the value, then hashes the bytes.
    For hashable primitives, uses their built-in hash for speed.
    """
    hasher = hashlib.sha256()

    # Fast path for simple hashable types
    if isinstance(value, (type(None), bool, int, float, str, bytes)):
        hasher.update(f"{type(value).__name__}:{value!r}".encode())
        return hasher.hexdigest()

    # For tuples and frozensets of simple types
    if isinstance(value, (tuple, frozenset)):
        try:
            hasher.update(f"{type(value).__name__}:{value!r}".encode())
            return hasher.hexdigest()
        except (TypeError, RecursionError):
            pass

    # General case: use cloudpickle
    try:
        serialized = cloudpickle.dumps(value)
        hasher.update(serialized)
    except (
        (TypeError, pickle_error) if (pickle_error := Exception) else Exception
    ):
        # Last resort: use repr
        hasher.update(repr(value).encode())

    return hasher.hexdigest()


def hash_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """Compute a combined hash of positional and keyword arguments.

    Arguments that are TaskExpression objects use their expression hash
    instead of their value hash (hash chain approach).
    """
    hasher = hashlib.sha256()

    # Hash positional arguments
    for i, arg in enumerate(args):
        arg_hash = _get_content_hash(arg)
        hasher.update(f"arg{i}:{arg_hash}".encode())

    # Hash keyword arguments in sorted order for determinism
    for key in sorted(kwargs.keys()):
        value = kwargs[key]
        value_hash = _get_content_hash(value)
        hasher.update(f"kwarg:{key}:{value_hash}".encode())

    return hasher.hexdigest()


def _get_content_hash(value: Any) -> str:
    """Get the content hash of a value.

    For TaskExpression objects, returns their hash (hash chain).
    For other values, computes the value hash.
    """
    # Check if this is an expression with a hash attribute
    if (
        hasattr(value, "hash")
        and callable(getattr(value, "hash", None)) is False
    ):
        expr_hash = getattr(value, "hash")
        if isinstance(expr_hash, str):
            return expr_hash

    return hash_value(value)


def compute_task_hash(
    func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> str:
    """Compute the full content-addressed hash for a task invocation.

    Combines the function hash with the argument hash to produce a
    unique identifier for this specific task call.
    """
    hasher = hashlib.sha256()

    func_hash = hash_function(func)
    args_hash = hash_args(args, kwargs)

    hasher.update(f"task:{func_hash}:{args_hash}".encode())

    return hasher.hexdigest()


