"""Pluggable serialization for task results."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import cloudpickle


class Serializer(ABC):
    """Abstract base class for result serializers."""

    @property
    @abstractmethod
    def extension(self) -> str:
        """File extension for this serializer."""
        ...

    @abstractmethod
    def serialize(self, value: Any) -> bytes:
        """Serialize a value to bytes."""
        ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes back to a value."""
        ...

    def save(self, value: Any, path: Path) -> None:
        """Save a value to a file."""
        data = self.serialize(value)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def load(self, path: Path) -> Any:
        """Load a value from a file."""
        data = path.read_bytes()
        return self.deserialize(data)


class JSONSerializer(Serializer):
    """JSON serializer for simple types.

    Supports: None, bool, int, float, str, list, dict
    """

    @property
    def extension(self) -> str:
        return ".json"

    def serialize(self, value: Any) -> bytes:
        return json.dumps(value, indent=2, default=str).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))

    @staticmethod
    def can_serialize(value: Any) -> bool:
        """Check if a value can be serialized with JSON."""
        if value is None or isinstance(value, (bool, int, float, str)):
            return True
        if isinstance(value, (list, tuple)):
            return all(JSONSerializer.can_serialize(v) for v in value)
        if isinstance(value, dict):
            return all(
                isinstance(k, str) and JSONSerializer.can_serialize(v)
                for k, v in value.items()
            )
        return False


class PickleSerializer(Serializer):
    """Pickle serializer for arbitrary Python objects.

    Uses cloudpickle for better support of closures and lambdas.
    """

    @property
    def extension(self) -> str:
        return ".pkl"

    def serialize(self, value: Any) -> bytes:
        return cloudpickle.dumps(value)

    def deserialize(self, data: bytes) -> Any:
        return cloudpickle.loads(data)


class HybridSerializer(Serializer):
    """Hybrid serializer that prefers JSON but falls back to Pickle.

    Uses JSON for simple types (human-readable), Pickle for complex objects.
    """

    def __init__(self) -> None:
        self._json = JSONSerializer()
        self._pickle = PickleSerializer()

    @property
    def extension(self) -> str:
        return ".dat"

    def serialize(self, value: Any) -> bytes:
        if JSONSerializer.can_serialize(value):
            # Prefix with 'J' for JSON
            return b"J" + self._json.serialize(value)
        # Prefix with 'P' for Pickle
        return b"P" + self._pickle.serialize(value)

    def deserialize(self, data: bytes) -> Any:
        if not data:
            raise ValueError("Empty data")

        marker = data[0:1]
        payload = data[1:]

        if marker == b"J":
            return self._json.deserialize(payload)
        elif marker == b"P":
            return self._pickle.deserialize(payload)
        else:
            # Try to auto-detect (for backwards compatibility)
            try:
                return self._json.deserialize(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return self._pickle.deserialize(data)


# Default serializer instances
json_serializer = JSONSerializer()
pickle_serializer = PickleSerializer()
hybrid_serializer = HybridSerializer()


def get_serializer(name: str = "hybrid") -> Serializer:
    """Get a serializer by name."""
    serializers = {
        "json": json_serializer,
        "pickle": pickle_serializer,
        "hybrid": hybrid_serializer,
    }
    if name not in serializers:
        raise ValueError(f"Unknown serializer: {name}")
    return serializers[name]
