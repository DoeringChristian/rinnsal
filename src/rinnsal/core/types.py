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
        return {k: to_dict(v) for k, v in obj._data.items()}
    elif isinstance(obj, dict):
        return {k: to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_dict(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(to_dict(v) for v in obj)
    return obj


@dataclass
class Config:
    """A dictionary wrapper for task configuration.

    Provides attribute-style access to configuration values.
    """

    _data: dict[str, Any] = field(default_factory=dict)

    def __init__(
        self, _dict: dict[str, Any] | None = None, **kwargs: Any
    ) -> None:
        if _dict is not None:
            self._data = dict(_dict)
        else:
            self._data = {}
        self._data.update(kwargs)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"Config has no attribute '{name}'") from None

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            self._data[name] = value

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return yaml.dump(
            to_dict(self), default_flow_style=False, sort_keys=False
        ).rstrip("\n")

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Config):
            return self._data == other._data
        return False

    def __hash__(self) -> int:
        return hash(tuple(sorted(self._data.items())))

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def items(self) -> Any:
        return self._data.items()

    def keys(self) -> Any:
        return self._data.keys()

    def values(self) -> Any:
        return self._data.values()

    def to_dict(self) -> dict[str, Any]:
        return dict(self._data)


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
