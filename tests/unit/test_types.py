"""Tests for core types."""

import pytest
from datetime import datetime
from pathlib import Path

from rinnsal.core.types import Config, Entry, Runs, Snapshot


class TestConfig:
    """Tests for the Config class."""

    def test_init_with_dict(self):
        config = Config({"a": 1, "b": 2})
        assert config.a == 1
        assert config.b == 2

    def test_init_with_kwargs(self):
        config = Config(a=1, b=2)
        assert config.a == 1
        assert config.b == 2

    def test_init_with_both(self):
        config = Config({"a": 1}, b=2)
        assert config.a == 1
        assert config.b == 2

    def test_attribute_access(self):
        config = Config({"learning_rate": 0.01})
        assert config.learning_rate == 0.01

    def test_attribute_error(self):
        config = Config()
        with pytest.raises(AttributeError, match="no attribute"):
            _ = config.missing

    def test_item_access(self):
        config = Config({"a": 1})
        assert config["a"] == 1

    def test_item_set(self):
        config = Config()
        config["a"] = 1
        assert config.a == 1

    def test_contains(self):
        config = Config({"a": 1})
        assert "a" in config
        assert "b" not in config

    def test_len(self):
        config = Config({"a": 1, "b": 2})
        assert len(config) == 2

    def test_iter(self):
        config = Config({"a": 1, "b": 2})
        assert set(config) == {"a", "b"}

    def test_equality(self):
        config1 = Config({"a": 1})
        config2 = Config({"a": 1})
        config3 = Config({"a": 2})

        assert config1 == config2
        assert config1 != config3

    def test_hash(self):
        config1 = Config({"a": 1})
        config2 = Config({"a": 1})
        assert hash(config1) == hash(config2)

    def test_to_dict(self):
        config = Config({"a": 1, "b": 2})
        assert config.to_dict() == {"a": 1, "b": 2}

    def test_save_and_load_roundtrip(self, tmp_path):
        config = Config(lr=0.01, epochs=10, model="resnet")
        path = tmp_path / "config.yaml"
        config.save(path)

        loaded = Config.load(path)
        assert loaded == config

    def test_save_creates_parent_dirs(self, tmp_path):
        config = Config(x=1)
        path = tmp_path / "nested" / "dir" / "config.yaml"
        config.save(path)
        assert path.exists()

    def test_save_produces_valid_yaml(self, tmp_path):
        import yaml

        config = Config(a=1, b=[1, 2, 3], c="hello")
        path = tmp_path / "config.yaml"
        config.save(path)

        with open(path) as f:
            data = yaml.safe_load(f)
        assert data == {"a": 1, "b": [1, 2, 3], "c": "hello"}

    def test_load_nested_config(self, tmp_path):
        import yaml

        path = tmp_path / "config.yaml"
        with open(path, "w") as f:
            yaml.dump({"model": {"layers": 3, "hidden": 128}, "lr": 0.01}, f)

        config = Config.load(path)
        assert config.lr == 0.01
        assert config.model == {"layers": 3, "hidden": 128}

    def test_load_empty_file_returns_empty_config(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text("")
        config = Config.load(path)
        assert len(config) == 0

    def test_load_non_mapping_raises(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("- a\n- b\n")
        with pytest.raises(ValueError, match="YAML mapping"):
            Config.load(path)

    def test_load_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Config.load(tmp_path / "nope.yaml")

    def test_save_load_with_nested_config(self, tmp_path):
        inner = Config(layers=3, hidden=128)
        config = Config(model=inner, lr=0.01)
        path = tmp_path / "config.yaml"
        config.save(path)

        loaded = Config.load(path)
        assert loaded.lr == 0.01
        assert loaded.model == {"layers": 3, "hidden": 128}

    def test_reserved_name_attr_raises(self):
        config = Config()
        with pytest.raises(AttributeError, match="reserved"):
            config.save = "oops"

    def test_reserved_name_bracket_works(self):
        config = Config()
        config["save"] = "ok"
        assert config["save"] == "ok"


class TestEntry:
    """Tests for the Entry class."""

    def test_basic_entry(self):
        entry = Entry(result=42)
        assert entry.result == 42
        assert entry.log == ""
        assert entry.metadata == {}
        assert isinstance(entry.timestamp, datetime)

    def test_entry_with_log(self):
        entry = Entry(result=42, log="some output")
        assert entry.log == "some output"

    def test_entry_with_metadata(self):
        entry = Entry(result=42, metadata={"key": "value"})
        assert entry.metadata["key"] == "value"


class TestRuns:
    """Tests for the Runs collection."""

    def test_empty_runs(self):
        runs = Runs()
        assert len(runs) == 0
        assert not runs
        assert runs.latest is None
        assert runs.first is None

    def test_append(self):
        runs = Runs()
        entry = Entry(result=1)
        runs.append(entry)
        assert len(runs) == 1
        assert runs.latest == entry

    def test_integer_index(self):
        entries = [Entry(result=i) for i in range(3)]
        runs = Runs(entries)

        assert runs[0].result == 0
        assert runs[-1].result == 2

    def test_slice_index(self):
        entries = [Entry(result=i) for i in range(5)]
        runs = Runs(entries)

        sliced = runs[1:3]
        assert isinstance(sliced, Runs)
        assert len(sliced) == 2
        assert sliced[0].result == 1

    def test_callable_filter(self):
        entries = [Entry(result=i) for i in range(5)]
        runs = Runs(entries)

        filtered = runs[lambda e: e.result > 2]
        assert len(filtered) == 2
        assert all(e.result > 2 for e in filtered)

    def test_iteration(self):
        entries = [Entry(result=i) for i in range(3)]
        runs = Runs(entries)

        results = [e.result for e in runs]
        assert results == [0, 1, 2]

    def test_to_list(self):
        entries = [Entry(result=i) for i in range(3)]
        runs = Runs(entries)

        lst = runs.to_list()
        assert lst == entries
        assert lst is not runs._entries  # Should be a copy
