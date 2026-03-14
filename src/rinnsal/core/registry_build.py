"""Registry and build pattern for class instantiation from config."""

from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")

# Global registry for class instantiation from config
_registry: dict[str, type] = {}


def register(cls: type[T]) -> type[T]:
    """Decorator to register a class in the global registry.

    Allows classes to be instantiated by name from configuration.
    The class name becomes the key in the registry.

    Args:
        cls: Class to register

    Returns:
        The same class (allows use as a decorator)

    Raises:
        RuntimeError: If a class with the same name is already registered

    Example:
        @register
        class MyModel:
            def __init__(self, hidden_size: int = 256):
                self.hidden_size = hidden_size

        config = {"type": "MyModel", "hidden_size": 512}
        model = build(MyModel, config)
    """
    if cls.__name__ in _registry:
        raise RuntimeError(
            f"Class with name '{cls.__name__}' already exists in registry!"
        )
    _registry[cls.__name__] = cls
    return cls


def build(tp: type[T], cfg: dict | T | None, *args: object, **kwargs: object) -> T:
    """Instantiate an object from configuration using the registry.

    If cfg is a dict with a 'type' key, looks up the class in the registry
    and instantiates it with the remaining config parameters. Otherwise,
    assumes cfg is already an instance and returns it directly.

    Args:
        tp: Expected type of the returned object (for type checking)
        cfg: Configuration dict with 'type' key, or an existing instance
        *args: Additional positional arguments to pass to constructor
        **kwargs: Additional keyword arguments to pass to constructor

    Returns:
        Instance of the specified type

    Raises:
        TypeError: If the returned object is not of type tp
        ValueError: If cfg is None
        KeyError: If 'type' key missing or type not in registry

    Example:
        @register
        class Optimizer:
            def __init__(self, lr: float = 0.001):
                self.lr = lr

        # From dict config
        opt = build(Optimizer, {"type": "Optimizer", "lr": 0.01})

        # Pass existing instance (returned as-is)
        opt2 = build(Optimizer, opt)

        # With additional kwargs (override config)
        opt3 = build(Optimizer, {"type": "Optimizer"}, lr=0.1)
    """
    if cfg is None:
        raise ValueError("Cannot build from None config")

    if isinstance(cfg, dict):
        if "type" not in cfg:
            raise KeyError("Config dict must have a 'type' key to build from registry")
        type_name = cfg["type"]
        if type_name not in _registry:
            available = ", ".join(_registry.keys()) if _registry else "(none)"
            raise KeyError(
                f"Type '{type_name}' not in registry. Available: {available}"
            )
        cls = _registry[type_name]
        # Merge config values with kwargs (kwargs take precedence)
        for k, v in cfg.items():
            if k == "type":
                continue
            if k not in kwargs:
                kwargs[k] = v
        obj = cls(*args, **kwargs)
    else:
        obj = cfg

    if not isinstance(obj, tp):
        raise TypeError(f"Expected type {tp.__name__}, got {type(obj).__name__}")
    return obj


def get_registry() -> dict[str, type]:
    """Return a copy of the current registry."""
    return dict(_registry)


def clear_registry() -> None:
    """Clear all registered classes."""
    _registry.clear()
