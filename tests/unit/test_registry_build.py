"""Tests for register/build pattern."""

import pytest

from rinnsal.core.registry_build import (
    register,
    build,
    get_registry,
    clear_registry,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear registry before and after each test."""
    clear_registry()
    yield
    clear_registry()


class TestRegister:
    def test_register_class(self):
        @register
        class MyClass:
            pass

        assert "MyClass" in get_registry()
        assert get_registry()["MyClass"] is MyClass

    def test_register_returns_class(self):
        @register
        class MyClass:
            pass

        assert isinstance(MyClass, type)
        assert MyClass.__name__ == "MyClass"

    def test_register_duplicate_raises(self):
        @register
        class DuplicateClass:
            pass

        with pytest.raises(RuntimeError, match="already exists"):

            @register
            class DuplicateClass:  # noqa: F811
                pass


class TestBuild:
    def test_build_from_dict(self):
        @register
        class Model:
            def __init__(self, size: int = 10):
                self.size = size

        obj = build(Model, {"type": "Model", "size": 20})
        assert isinstance(obj, Model)
        assert obj.size == 20

    def test_build_with_defaults(self):
        @register
        class Model:
            def __init__(self, size: int = 10):
                self.size = size

        obj = build(Model, {"type": "Model"})
        assert obj.size == 10

    def test_build_kwargs_override_config(self):
        @register
        class Model:
            def __init__(self, size: int = 10):
                self.size = size

        obj = build(Model, {"type": "Model", "size": 20}, size=30)
        assert obj.size == 30

    def test_build_passthrough_instance(self):
        @register
        class Model:
            def __init__(self, size: int = 10):
                self.size = size

        existing = Model(size=42)
        obj = build(Model, existing)
        assert obj is existing
        assert obj.size == 42

    def test_build_none_raises(self):
        @register
        class Model:
            pass

        with pytest.raises(
            AssertionError, match="did not match requested type"
        ):
            build(Model, None)

    def test_build_none_allowed_with_optional_type(self):
        @register
        class Model:
            pass

        # None is valid when type annotation allows it
        result = build(Model | None, None)
        assert result is None

    def test_build_missing_type_raises(self):
        @register
        class Model:
            pass

        with pytest.raises(KeyError, match="must have a 'type' key"):
            build(Model, {"size": 10})

    def test_build_unknown_type_raises(self):
        @register
        class Model:
            pass

        with pytest.raises(KeyError, match="not in registry"):
            build(Model, {"type": "UnknownClass"})

    def test_build_wrong_type_raises(self):
        @register
        class Model:
            pass

        @register
        class Other:
            pass

        with pytest.raises(
            AssertionError, match="did not match requested type"
        ):
            build(Model, {"type": "Other"})

    def test_build_with_args(self):
        @register
        class Model:
            def __init__(self, name: str, size: int = 10):
                self.name = name
                self.size = size

        obj = build(Model, {"type": "Model", "size": 20}, "my_model")
        assert obj.name == "my_model"
        assert obj.size == 20


class TestBuildWithConfig:
    def test_build_from_config_object(self):
        from rinnsal.core.types import Config

        @register
        class Model:
            def __init__(self, size: int = 10):
                self.size = size

        cfg = Config(type="Model", size=42)
        obj = build(Model, cfg)
        assert isinstance(obj, Model)
        assert obj.size == 42

    def test_build_from_nested_config(self):
        from rinnsal.core.types import Config

        @register
        class Model:
            def __init__(self, size: int = 10, name: str = "default"):
                self.size = size
                self.name = name

        cfg = Config.load.__func__  # just verify Config is a dict
        cfg = Config({"type": "Model", "size": 20, "name": "test"})
        obj = build(Model, cfg)
        assert obj.size == 20
        assert obj.name == "test"


class TestRegistryHelpers:
    def test_get_registry_returns_copy(self):
        @register
        class Model:
            pass

        reg = get_registry()
        reg["NewClass"] = object
        assert "NewClass" not in get_registry()

    def test_clear_registry(self):
        @register
        class Model:
            pass

        assert len(get_registry()) == 1
        clear_registry()
        assert len(get_registry()) == 0
