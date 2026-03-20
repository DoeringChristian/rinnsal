"""Core type definitions for rinnsal."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Generic, Iterator, TypeVar, overload

import yaml


def to_dict(obj: Any) -> Any:
    """Recursively convert Config objects to dicts."""
    if isinstance(obj, Config):
        return {k: to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, dict):
        return {k: to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_dict(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(to_dict(v) for v in obj)
    return obj


def _wrap(value: Any) -> Any:
    """Recursively wrap dicts as Config objects."""
    if isinstance(value, dict) and not isinstance(value, Config):
        return Config(value)
    if isinstance(value, list):
        return [_wrap(v) for v in value]
    return value


class Config(dict):
    """A dict subclass with attribute-style access.

    Nested dicts are automatically wrapped as Config objects so that
    ``config.model.type`` works at any depth.  Because Config *is* a
    dict, it passes ``isinstance(cfg, dict)`` checks everywhere
    (e.g. ``build()``).
    """

    _RESERVED = frozenset(("save", "load", "get", "items", "keys", "values", "to_dict"))

    def __init__(
        self, _dict: dict[str, Any] | None = None, **kwargs: Any
    ) -> None:
        super().__init__()
        if _dict is not None:
            for k, v in _dict.items():
                self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"Config has no attribute '{name}'") from None

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        elif name in self._RESERVED:
            raise AttributeError(
                f"'{name}' is a reserved Config method and cannot be used as a key. "
                f"Use config['{name}'] instead."
            )
        else:
            self[name] = value

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key, _wrap(value))

    def __repr__(self) -> str:
        return yaml.dump(
            to_dict(self), default_flow_style=False, sort_keys=False
        ).rstrip("\n")

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Config):
            return dict.__eq__(self, other)
        return False

    __hash__ = None  # mutable dict — hashing is semantically wrong

    def update(self, _m=(), **kwargs):
        if hasattr(_m, 'items'):
            for k, v in _m.items():
                self[k] = v
        else:
            for k, v in _m:
                self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self[key]

    def __ior__(self, other):
        self.update(other)
        return self

    def copy(self):
        return Config(dict(self))

    def __copy__(self):
        return self.copy()

    def to_dict(self) -> dict[str, Any]:
        """Recursively convert to plain dicts."""
        return to_dict(self)

    def save(self, path: str | Path) -> None:
        """Save config to a YAML file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(to_dict(self), f, default_flow_style=False, sort_keys=False)

    @classmethod
    def load(cls, path: str | Path) -> Config:
        """Load config from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        if data is None:
            return cls()
        if not isinstance(data, dict):
            raise ValueError(f"Expected a YAML mapping, got {type(data).__name__}")
        return cls(data)


@dataclass
class Snapshot:
    """A code snapshot for reproducibility.

    Contains a hash of the source code at execution time and the path
    to the snapshot directory.
    """

    hash: str
    path: Path

    def __repr__(self) -> str:
        return f"Snapshot(hash={self.hash!r})"


@dataclass
class Entry:
    """A single execution result entry.

    Contains the result value, captured output, metadata, and timestamp.
    """

    result: Any
    log: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    snapshot: Snapshot | None = None

    def __repr__(self) -> str:
        return f"Entry(result={self.result!r}, timestamp={self.timestamp.isoformat()})"


T = TypeVar("T")


class Runs(Generic[T]):
    """A collection of execution entries with rich indexing.

    Supports integer indexing, regex string matching on metadata,
    and dict-based filtering.
    """

    def __init__(self, entries: list[T] | None = None) -> None:
        self._entries: list[T] = entries or []

    def __len__(self) -> int:
        return len(self._entries)

    def __bool__(self) -> bool:
        return bool(self._entries)

    def __iter__(self) -> Iterator[T]:
        return iter(self._entries)

    def __repr__(self) -> str:
        return f"Runs({self._entries!r})"

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> Runs[T]: ...

    @overload
    def __getitem__(self, pattern: str) -> Runs[T]: ...

    @overload
    def __getitem__(self, filter_fn: Callable[[T], bool]) -> Runs[T]: ...

    def __getitem__(
        self, key: int | slice | str | Callable[[T], bool]
    ) -> T | Runs[T]:
        if isinstance(key, int):
            return self._entries[key]
        elif isinstance(key, slice):
            return Runs(self._entries[key])
        elif isinstance(key, str):
            return self._filter_by_pattern(key)
        elif callable(key):
            return self._filter_by_callable(key)
        else:
            raise TypeError(f"Invalid key type: {type(key)}")

    def _filter_by_pattern(self, pattern: str) -> Runs[T]:
        """Filter entries by regex pattern matching on metadata."""
        regex = re.compile(pattern)
        matches: list[T] = []
        for entry in self._entries:
            if isinstance(entry, Entry):
                # Match against metadata values converted to strings
                for value in entry.metadata.values():
                    if regex.search(str(value)):
                        matches.append(entry)
                        break
        return Runs(matches)

    def _filter_by_callable(self, filter_fn: Callable[[T], bool]) -> Runs[T]:
        """Filter entries using a callable predicate."""
        return Runs([e for e in self._entries if filter_fn(e)])

    def append(self, entry: T) -> None:
        """Add a new entry to the collection."""
        self._entries.append(entry)

    def extend(self, entries: list[T]) -> None:
        """Add multiple entries to the collection."""
        self._entries.extend(entries)

    def clear(self) -> None:
        """Remove all entries."""
        self._entries.clear()

    @property
    def latest(self) -> T | None:
        """Return the most recent entry, or None if empty."""
        return self._entries[-1] if self._entries else None

    @property
    def first(self) -> T | None:
        """Return the first entry, or None if empty."""
        return self._entries[0] if self._entries else None

    def to_list(self) -> list[T]:
        """Return a copy of the entries as a list."""
        return list(self._entries)


# Convenience alias for task history lookups
TaskRuns = Runs[Entry]
