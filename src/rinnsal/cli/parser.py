"""CLI argument parser generation from function signatures."""

from __future__ import annotations

import argparse
import inspect
from typing import Any, Callable, get_type_hints


def create_parser_from_signature(
    func: Callable[..., Any],
    description: str | None = None,
) -> argparse.ArgumentParser:
    """Create an argparse.ArgumentParser from a function signature.

    Generates CLI flags from function parameters with type coercion
    based on type annotations (int, float, bool, str).

    Args:
        func: The function to generate a parser for
        description: Optional description for the parser

    Returns:
        A configured ArgumentParser
    """
    parser = argparse.ArgumentParser(
        description=description or func.__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    sig = inspect.signature(func)

    # Try to get type hints
    try:
        hints = get_type_hints(func)
    except Exception:
        hints = {}

    for name, param in sig.parameters.items():
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        # Convert underscore to hyphen for CLI flags
        flag_name = f"--{name.replace('_', '-')}"

        # Determine type from annotation
        param_type = hints.get(name, str)
        if param_type is type(None) or param_type is None:
            param_type = str

        # Handle Optional types
        origin = getattr(param_type, "__origin__", None)
        if origin is not None:
            # For Union types like Optional[int], get the first non-None type
            args = getattr(param_type, "__args__", ())
            for arg in args:
                if arg is not type(None):
                    param_type = arg
                    break

        # Convert type for argparse
        arg_type = _get_argparse_type(param_type)

        # Build argument kwargs
        kwargs: dict[str, Any] = {}

        if param.default is inspect.Parameter.empty:
            # Required argument
            kwargs["required"] = True
        else:
            kwargs["default"] = param.default

        # Handle boolean type specially
        if param_type is bool:
            if param.default is True:
                # Use --no-flag to disable
                parser.add_argument(
                    f"--no-{name.replace('_', '-')}",
                    dest=name,
                    action="store_false",
                    default=True,
                    help=f"Disable {name}",
                )
            else:
                parser.add_argument(
                    flag_name,
                    action="store_true",
                    default=param.default if param.default is not inspect.Parameter.empty else False,
                    help=f"Enable {name}",
                )
        else:
            kwargs["type"] = arg_type
            parser.add_argument(flag_name, dest=name, **kwargs)

    return parser


def _get_argparse_type(python_type: type) -> type:
    """Convert a Python type to an argparse-compatible type."""
    type_map = {
        int: int,
        float: float,
        str: str,
        bool: bool,
    }
    return type_map.get(python_type, str)


def parse_args_for_function(
    func: Callable[..., Any],
    args: list[str] | None = None,
) -> dict[str, Any]:
    """Parse command-line arguments for a function.

    Args:
        func: The function to parse arguments for
        args: Command-line arguments (defaults to sys.argv[1:])

    Returns:
        Dictionary of argument names to values
    """
    parser = create_parser_from_signature(func)
    namespace = parser.parse_args(args)
    return vars(namespace)
