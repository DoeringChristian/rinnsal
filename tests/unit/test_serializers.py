"""Tests for serializers."""

import pytest
from pathlib import Path

from rinnsal.persistence.serializers import (
    JSONSerializer,
    PickleSerializer,
    HybridSerializer,
    get_serializer,
)


class TestJSONSerializer:
    """Tests for JSONSerializer."""

    def test_serialize_primitives(self):
        ser = JSONSerializer()

        assert ser.deserialize(ser.serialize(42)) == 42
        assert ser.deserialize(ser.serialize(3.14)) == 3.14
        assert ser.deserialize(ser.serialize("hello")) == "hello"
        assert ser.deserialize(ser.serialize(True)) is True
        assert ser.deserialize(ser.serialize(None)) is None

    def test_serialize_list(self):
        ser = JSONSerializer()
        data = [1, 2, 3, "four"]
        assert ser.deserialize(ser.serialize(data)) == data

    def test_serialize_dict(self):
        ser = JSONSerializer()
        data = {"a": 1, "b": [2, 3], "c": {"nested": True}}
        assert ser.deserialize(ser.serialize(data)) == data

    def test_can_serialize(self):
        assert JSONSerializer.can_serialize(42)
        assert JSONSerializer.can_serialize("hello")
        assert JSONSerializer.can_serialize([1, 2, 3])
        assert JSONSerializer.can_serialize({"a": 1})
        assert not JSONSerializer.can_serialize(object())
        assert not JSONSerializer.can_serialize({1: "non-string key"})

    def test_extension(self):
        assert JSONSerializer().extension == ".json"


class _PickleTestClass:
    """Helper class for pickle serialization tests."""
    def __init__(self, value):
        self.value = value


def _double(x):
    """Module-level function for pickle roundtrip tests."""
    return x * 2


class TestPickleSerializer:
    """Tests for PickleSerializer."""

    def test_serialize_primitives(self):
        ser = PickleSerializer()

        assert ser.deserialize(ser.serialize(42)) == 42
        assert ser.deserialize(ser.serialize("hello")) == "hello"

    def test_serialize_complex_objects(self):
        ser = PickleSerializer()

        obj = _PickleTestClass(42)
        restored = ser.deserialize(ser.serialize(obj))
        assert restored.value == 42

    def test_serialize_function(self):
        ser = PickleSerializer()

        restored = ser.deserialize(ser.serialize(_double))
        assert restored(10) == 20

    def test_extension(self):
        assert PickleSerializer().extension == ".pkl"


class TestHybridSerializer:
    """Tests for HybridSerializer."""

    def test_uses_json_for_primitives(self):
        ser = HybridSerializer()
        data = ser.serialize(42)
        assert data.startswith(b"J")

    def test_uses_pickle_for_complex(self):
        ser = HybridSerializer()

        data = ser.serialize(_PickleTestClass(1))
        assert data.startswith(b"P")

    def test_roundtrip_primitives(self):
        ser = HybridSerializer()

        assert ser.deserialize(ser.serialize(42)) == 42
        assert ser.deserialize(ser.serialize("hello")) == "hello"
        assert ser.deserialize(ser.serialize([1, 2, 3])) == [1, 2, 3]

    def test_roundtrip_complex(self):
        ser = HybridSerializer()

        restored = ser.deserialize(ser.serialize(_double))
        assert restored(10) == 20

    def test_extension(self):
        assert HybridSerializer().extension == ".dat"


class TestSerializerSaveLoad:
    """Tests for serializer file operations."""

    def test_save_load(self, tmp_path):
        ser = JSONSerializer()
        path = tmp_path / "test.json"

        data = {"key": "value"}
        ser.save(data, path)

        assert path.exists()
        assert ser.load(path) == data

    def test_save_creates_directories(self, tmp_path):
        ser = JSONSerializer()
        path = tmp_path / "nested" / "dir" / "test.json"

        ser.save(42, path)
        assert path.exists()


class TestGetSerializer:
    """Tests for get_serializer function."""

    def test_get_json(self):
        ser = get_serializer("json")
        assert isinstance(ser, JSONSerializer)

    def test_get_pickle(self):
        ser = get_serializer("pickle")
        assert isinstance(ser, PickleSerializer)

    def test_get_hybrid(self):
        ser = get_serializer("hybrid")
        assert isinstance(ser, HybridSerializer)

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown serializer"):
            get_serializer("unknown")
