"""Tests for the Logger module."""

import struct
import zlib
import tempfile
from pathlib import Path

import pytest

from rinnsal.logger import LazyFigure, Logger, LogReader


class MockFigure:
    """Mock figure with savefig for testing (module-level to avoid
    cloudpickle recursion issues on Python 3.13+)."""

    def __init__(self, value: int):
        self.value = value

    def savefig(self, buf, **kwargs):
        """Write a minimal valid PNG to buf."""
        raw = b"\x00\xff\xff\xff"  # filter byte + RGB
        compressed = zlib.compress(raw)

        def _chunk(ctype, data):
            c = ctype + data
            return (
                struct.pack(">I", len(data))
                + c
                + struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)
            )

        png = b"\x89PNG\r\n\x1a\n"
        png += _chunk(
            b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)
        )
        png += _chunk(b"IDAT", compressed)
        png += _chunk(b"IEND", b"")
        buf.write(png)


class TestLoggerProtobuf:
    """Test Logger with protobuf format (default)."""

    def test_log_scalar(self, tmp_path: Path) -> None:
        """Test logging and reading scalars."""
        with Logger(tmp_path) as logger:
            logger.set_iteration(0)
            logger.add_scalar("loss", 0.5)
            logger.set_iteration(1)
            logger.add_scalar("loss", 0.4)
            logger.add_scalar("accuracy", 0.9)
            logger.flush()

        reader = LogReader(tmp_path)
        assert reader.is_run
        assert "loss" in reader.scalar_tags
        assert "accuracy" in reader.scalar_tags

        loss_data = reader.load_scalars("loss")
        assert len(loss_data) == 2
        assert loss_data[0] == (0, 0.5)
        assert loss_data[1] == (1, 0.4)

    def test_log_text(self, tmp_path: Path) -> None:
        """Test logging and reading text."""
        with Logger(tmp_path) as logger:
            logger.add_text("info", "Hello world", it=0)
            logger.add_text("info", "Training started", it=1)
            logger.flush()

        reader = LogReader(tmp_path)
        assert "info" in reader.text_tags

        text_data = reader.load_text("info")
        assert len(text_data) == 2
        assert text_data[0] == (0, "Hello world")
        assert text_data[1] == (1, "Training started")

    def test_log_figure(self, tmp_path: Path) -> None:
        """Test logging and reading figures."""
        with Logger(tmp_path) as logger:
            logger.add_figure("plot", MockFigure(42), it=0)
            logger.flush()

        reader = LogReader(tmp_path)
        assert "plot" in reader.figure_tags

        fig = reader.load_figure("plot", 0)
        assert fig.value == 42

    def test_log_checkpoint(self, tmp_path: Path) -> None:
        """Test logging and reading checkpoints."""
        with Logger(tmp_path) as logger:
            logger.add_checkpoint("model", {"weights": [1, 2, 3]}, it=0)
            logger.flush()

        reader = LogReader(tmp_path)
        assert "model" in reader.checkpoint_tags

        ckpt = reader.load_checkpoint("model", 0)
        assert ckpt == {"weights": [1, 2, 3]}

    def test_iterations(self, tmp_path: Path) -> None:
        """Test getting all iterations."""
        with Logger(tmp_path) as logger:
            for i in range(5):
                logger.add_scalar("loss", 0.5 - i * 0.1, it=i)
            logger.flush()

        reader = LogReader(tmp_path)
        assert reader.iterations == [0, 1, 2, 3, 4]

    def test_scalars_helper(self, tmp_path: Path) -> None:
        """Test the scalars() convenience method."""
        with Logger(tmp_path) as logger:
            for i in range(3):
                logger.add_scalar("loss", 1.0 - i * 0.1, it=i)
            logger.flush()

        reader = LogReader(tmp_path)
        its, vals = reader.scalars("loss")
        assert its == [0, 1, 2]
        assert vals == [1.0, 0.9, 0.8]

    def test_getitem(self, tmp_path: Path) -> None:
        """Test the [] access pattern."""
        with Logger(tmp_path) as logger:
            logger.add_scalar("loss", 0.5, it=0)
            logger.add_scalar("loss", 0.3, it=1)
            logger.flush()

        reader = LogReader(tmp_path)
        it, val = reader["loss"]
        assert it == 1
        assert val == 0.3

    def test_lazy_figure(self, tmp_path: Path) -> None:
        """Test lazy figure loading."""
        with Logger(tmp_path) as logger:
            for i in range(3):
                logger.add_figure("plot", MockFigure(i), it=i)
            logger.flush()

        reader = LogReader(tmp_path)
        its, figs = reader.figures("plot")
        assert its == [0, 1, 2]

        # Figures are lazy - accessing one should work
        assert figs[2].value == 2


class TestLogReaderDiscovery:
    """Test LogReader's run discovery features."""

    def test_discover_runs(self, tmp_path: Path) -> None:
        """Test discovering multiple runs."""
        # Create multiple runs
        for name in ["run1", "run2", "run3"]:
            run_dir = tmp_path / name
            with Logger(run_dir) as logger:
                logger.add_scalar("loss", 0.5, it=0)
                logger.flush()

        reader = LogReader(tmp_path)
        assert not reader.is_run
        assert sorted(reader.runs) == ["run1", "run2", "run3"]

    def test_get_run(self, tmp_path: Path) -> None:
        """Test getting a specific run."""
        # Create a run
        run_dir = tmp_path / "experiment1"
        with Logger(run_dir) as logger:
            logger.add_scalar("loss", 0.5, it=0)
            logger.flush()

        reader = LogReader(tmp_path)
        run = reader.get_run("experiment1")
        assert run.is_run
        assert "loss" in run.scalar_tags
