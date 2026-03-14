"""Pytest configuration and fixtures."""

import pytest

from rinnsal.core.registry import get_registry
from rinnsal.runtime.engine import ExecutionEngine, set_engine
from rinnsal.execution.inline import InlineExecutor


@pytest.fixture(autouse=True)
def clean_registry():
    """Clean the global registry before each test."""
    registry = get_registry()
    registry.clear()
    yield
    registry.clear()


@pytest.fixture
def inline_executor():
    """Create an inline executor for testing."""
    return InlineExecutor()


@pytest.fixture
def engine(inline_executor):
    """Create a fresh execution engine for testing."""
    engine = ExecutionEngine(executor=inline_executor)
    set_engine(engine)
    yield engine
    engine.shutdown()
