"""Tests for CLI argument parser generation."""

import pytest

from rinnsal.cli.parser import (
    create_parser_from_signature,
    parse_args_for_function,
)


class TestCreateParserFromSignature:
    """Tests for parser creation from function signatures."""

    def test_simple_function(self):
        def my_func(name: str, count: int = 10):
            pass

        parser = create_parser_from_signature(my_func)

        # Parse with all args
        args = parser.parse_args(["--name", "test", "--count", "5"])
        assert args.name == "test"
        assert args.count == 5

    def test_default_values(self):
        def my_func(value: int = 42):
            pass

        parser = create_parser_from_signature(my_func)
        args = parser.parse_args([])
        assert args.value == 42

    def test_required_args(self):
        def my_func(required: str):
            pass

        parser = create_parser_from_signature(my_func)

        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_underscore_to_hyphen(self):
        def my_func(learning_rate: float = 0.01):
            pass

        parser = create_parser_from_signature(my_func)
        args = parser.parse_args(["--learning-rate", "0.001"])
        assert args.learning_rate == 0.001

    def test_boolean_flag_enable(self):
        def my_func(verbose: bool = False):
            pass

        parser = create_parser_from_signature(my_func)

        # Default is False
        args = parser.parse_args([])
        assert args.verbose is False

        # --verbose enables it
        args = parser.parse_args(["--verbose"])
        assert args.verbose is True

    def test_boolean_flag_disable(self):
        def my_func(enabled: bool = True):
            pass

        parser = create_parser_from_signature(my_func)

        # Default is True
        args = parser.parse_args([])
        assert args.enabled is True

        # --no-enabled disables it
        args = parser.parse_args(["--no-enabled"])
        assert args.enabled is False

    def test_type_coercion(self):
        def my_func(i: int, f: float, s: str):
            pass

        parser = create_parser_from_signature(my_func)
        args = parser.parse_args(["--i", "42", "--f", "3.14", "--s", "hello"])

        assert args.i == 42
        assert isinstance(args.i, int)

        assert args.f == 3.14
        assert isinstance(args.f, float)

        assert args.s == "hello"
        assert isinstance(args.s, str)

    def test_no_type_annotation(self):
        def my_func(value="default"):
            pass

        parser = create_parser_from_signature(my_func)
        args = parser.parse_args(["--value", "test"])
        assert args.value == "test"


class TestParseArgsForFunction:
    """Tests for parse_args_for_function helper."""

    def test_returns_dict(self):
        def my_func(name: str, count: int = 10):
            pass

        result = parse_args_for_function(my_func, ["--name", "test"])

        assert isinstance(result, dict)
        assert result["name"] == "test"
        assert result["count"] == 10
