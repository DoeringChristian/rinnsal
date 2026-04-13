"""Tests for Component classes and their round-trip through the Logger."""

from __future__ import annotations

import json

import pytest

from rinnsal.data.file_store import FileDatabase
from rinnsal.data.logger.components import (
    Artifact,
    Component,
    Image,
    Markdown,
    Plotly,
    ProgressBar,
    PythonCode,
    Scalar,
    Table,
    Text,
    autodetect,
)
from rinnsal.data.logger.logger import Logger
from rinnsal.data.logger.reader import LogReader


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _read_events(log_dir) -> list:
    from rinnsal.data.logger.event_file import EventFileReader

    reader = EventFileReader(log_dir / "events.pb")
    return list(reader.read_all())


def _make_logger(tmp_path, with_db: bool = False) -> tuple[Logger, FileDatabase | None]:
    db = None
    if with_db:
        db = FileDatabase(root=tmp_path / ".rinnsal")
    logger = Logger(tmp_path / "logs", database=db)
    return logger, db


# --------------------------------------------------------------------------- #
# Primitive components
# --------------------------------------------------------------------------- #


class TestScalar:
    def test_roundtrip(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add(Scalar(0.42), tag="loss")
        logger.close()
        events = _read_events(logger.log_dir)
        assert len(events) == 1
        assert events[0].WhichOneof("data") == "scalar"
        assert events[0].scalar.tag == "loss"
        assert events[0].scalar.value == pytest.approx(0.42)

    def test_repr_html(self) -> None:
        assert "0.5" in Scalar(0.5)._repr_html_()


class TestText:
    def test_roundtrip(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add(Text("hello"), tag="note")
        logger.close()
        events = _read_events(logger.log_dir)
        assert events[0].text.tag == "note"
        assert events[0].text.value == "hello"


class TestMarkdown:
    def test_roundtrip(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add_markdown("report", "# Results\n\nGood.")
        logger.close()
        events = _read_events(logger.log_dir)
        assert events[0].WhichOneof("data") == "markdown"
        assert events[0].markdown.tag == "report"
        assert "# Results" in events[0].markdown.content

    def test_has_markdown_repr(self) -> None:
        m = Markdown("# h")
        assert m._repr_markdown_() == "# h"


class TestTable:
    def test_list_of_lists_roundtrip(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add_table("t", [[1, 2], [3, 4]], headers=["a", "b"])
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert ev.WhichOneof("data") == "table"
        assert json.loads(ev.table.headers_json) == ["a", "b"]
        assert json.loads(ev.table.rows_json) == [[1, 2], [3, 4]]

    def test_repr_html_has_table_tag(self) -> None:
        html = Table([[1, 2]], headers=["x", "y"])._repr_html_()
        assert "<table" in html and "<th>x</th>" in html


class TestCode:
    def test_roundtrip(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add_code("snippet", "print('hi')", language="python")
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert ev.code.source == "print('hi')"
        assert ev.code.language == "python"


class TestProgress:
    def test_roundtrip(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add_progress("train", 3, total=10, label="epochs")
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert ev.progress.value == pytest.approx(3.0)
        assert ev.progress.total == pytest.approx(10.0)
        assert ev.progress.label == "epochs"


# --------------------------------------------------------------------------- #
# Heavy components
# --------------------------------------------------------------------------- #


def _png_bytes(size: int = 4) -> bytes:
    import io

    from PIL import Image as PILImage

    img = PILImage.new("RGB", (size, size), color=(255, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class TestImage:
    def test_inline_when_no_db(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add(Image(_png_bytes()), tag="pic")
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert ev.image.tag == "pic"
        assert len(ev.image.data) > 0
        assert ev.image.blob_hash == ""

    def test_blob_when_heavy_and_db(self, tmp_path) -> None:
        logger, db = _make_logger(tmp_path, with_db=True)
        big = _png_bytes(128)  # > 16KB threshold? 128x128 RGB PNG ≈ a few KB, pad out:
        # Build a larger payload by composing many copies as raw bytes.
        big = big * (16 * 1024 // len(big) + 2)
        logger.add(Image(big), tag="big")
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        # Raw bytes input short-circuits the PNG conversion, so the stored
        # blob is the raw input we passed in.
        assert ev.image.blob_hash != ""
        assert ev.image.data == b""
        assert db.get_blob(ev.image.blob_hash) == big


class TestArtifact:
    def test_inline(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        obj = {"a": 1, "b": [1, 2, 3]}
        logger.add_artifact("obj", obj, description="the thing")
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        # Routes through Checkpoint proto with description + type_name set.
        assert ev.checkpoint.description == "the thing"
        assert ev.checkpoint.type_name == "dict"
        assert len(ev.checkpoint.data) > 0

    def test_description_falls_back_to_preview(self, tmp_path) -> None:
        """When no description is given, Artifact uses a JSON preview
        of the value so the viewer has something to show."""
        logger, _ = _make_logger(tmp_path)
        logger.add_artifact("metrics", {"loss": 0.3, "acc": 0.92})
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert "loss" in ev.checkpoint.description
        assert "0.3" in ev.checkpoint.description

    def test_renders_dict_html(self) -> None:
        from rinnsal.data.logger.components import Artifact

        html = Artifact({"loss": 0.3, "acc": 0.92})._repr_html_()
        assert "loss" in html and "0.3" in html
        assert "<table" in html  # nested dict rendered as a tree

    def test_renders_list_html(self) -> None:
        from rinnsal.data.logger.components import Artifact

        html = Artifact([1, 2, 3])._repr_html_()
        assert "<ol>" in html
        assert "1" in html and "3" in html

    def test_blob_when_heavy_and_db(self, tmp_path) -> None:
        logger, db = _make_logger(tmp_path, with_db=True)
        big = list(range(10_000))  # >16 KB pickled
        logger.add_artifact("obj", big, description="many ints")
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert ev.checkpoint.blob_hash != ""
        assert ev.checkpoint.data == b""
        assert db.blob_exists(ev.checkpoint.blob_hash)


# --------------------------------------------------------------------------- #
# Plotly (skipped if plotly not installed)
# --------------------------------------------------------------------------- #


class TestPlotly:
    def _make_fig(self):
        pytest.importorskip("plotly", reason="plotly extra not installed")
        import plotly.graph_objects as go

        return go.Figure(data=[go.Scatter(x=[1, 2, 3], y=[4, 1, 2])])

    def test_inline_when_no_db(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add_plotly("p", self._make_fig())
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert ev.WhichOneof("data") == "plotly"
        assert ev.plotly.inline_json
        assert ev.plotly.blob_hash == ""
        assert ev.plotly.n_traces == 1

    def test_blob_when_db(self, tmp_path) -> None:
        logger, db = _make_logger(tmp_path, with_db=True)
        logger.add_plotly("p", self._make_fig())
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert ev.plotly.blob_hash != ""
        assert ev.plotly.inline_json == ""
        json_bytes = db.get_blob(ev.plotly.blob_hash)
        assert b'"scatter"' in json_bytes or b'"Scatter"' in json_bytes.lower()

    def test_auto_detect_in_add_figure(self, tmp_path) -> None:
        logger, _ = _make_logger(tmp_path)
        logger.add_figure("p", self._make_fig())
        logger.close()
        ev = _read_events(logger.log_dir)[0]
        assert ev.WhichOneof("data") == "plotly"


# --------------------------------------------------------------------------- #
# Autodetect
# --------------------------------------------------------------------------- #


class TestAutodetect:
    def test_passthrough_component(self) -> None:
        m = Markdown("x")
        assert autodetect(m) is m

    def test_string_to_markdown(self) -> None:
        c = autodetect("hello")
        assert isinstance(c, Markdown)

    def test_bytes_to_image(self) -> None:
        c = autodetect(b"\x89PNG\r\n\x1a\n")
        assert isinstance(c, Image)

    def test_dict_to_artifact(self) -> None:
        from rinnsal.data.logger.components import Artifact

        c = autodetect({"loss": 0.3, "acc": 0.92})
        assert isinstance(c, Artifact)
        assert c.obj == {"loss": 0.3, "acc": 0.92}

    def test_list_to_artifact(self) -> None:
        from rinnsal.data.logger.components import Artifact

        c = autodetect([1, 2, 3])
        assert isinstance(c, Artifact)

    def test_number_to_artifact(self) -> None:
        from rinnsal.data.logger.components import Artifact

        c = autodetect(42)
        assert isinstance(c, Artifact)

    def test_arbitrary_object_to_artifact(self) -> None:
        from rinnsal.data.logger.components import Artifact

        c = autodetect(object())
        assert isinstance(c, Artifact)
