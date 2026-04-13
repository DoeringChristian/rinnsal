"""Tests for the Card builder and its integration with Logger."""

from __future__ import annotations

import json

import pytest

from rinnsal.data.file_store import FileDatabase
from rinnsal.data.logger.card import Card
from rinnsal.data.logger.components import (
    Artifact,
    Markdown,
    ProgressBar,
    PythonCode,
    Scalar,
    Table,
)
from rinnsal.data.logger.logger import Logger


def _read_events(log_dir) -> list:
    from rinnsal.data.logger.event_file import EventFileReader

    return list(EventFileReader(log_dir / "events.pb").read_all())


def _make_logger(tmp_path, with_db: bool = False) -> Logger:
    db = FileDatabase(root=tmp_path / ".rinnsal") if with_db else None
    return Logger(tmp_path / "logs", database=db)


class TestCardBasics:
    def test_append_and_commit(self, tmp_path) -> None:
        logger = _make_logger(tmp_path)
        card = logger.card("report")
        card.append(Markdown("# hello"))
        card.append(Scalar(0.1))
        card.commit()
        logger.close()

        events = _read_events(logger.log_dir)
        assert len(events) == 1
        ev = events[0]
        assert ev.WhichOneof("data") == "card_event"
        assert ev.card_event.name == "report"
        assert len(ev.card_event.components) == 2
        kinds = [c.WhichOneof("data") for c in ev.card_event.components]
        assert kinds == ["markdown", "scalar"]

    def test_empty_card_commit_is_noop(self, tmp_path) -> None:
        logger = _make_logger(tmp_path)
        logger.card("empty").commit()
        logger.close()
        events = _read_events(logger.log_dir)
        assert events == []

    def test_context_manager_auto_commits(self, tmp_path) -> None:
        logger = _make_logger(tmp_path)
        with logger.card("auto") as c:
            c.append(Markdown("text"))
            c.append(Scalar(1.0))
        logger.close()

        events = _read_events(logger.log_dir)
        assert len(events) == 1
        assert events[0].card_event.name == "auto"
        assert len(events[0].card_event.components) == 2

    def test_context_manager_does_not_commit_on_exception(self, tmp_path) -> None:
        logger = _make_logger(tmp_path)
        with pytest.raises(RuntimeError):
            with logger.card("oops") as c:
                c.append(Markdown("text"))
                raise RuntimeError("boom")
        logger.close()

        events = _read_events(logger.log_dir)
        assert events == []


class TestCardIteration:
    def test_re_emit_same_card_at_different_iterations(self, tmp_path) -> None:
        logger = _make_logger(tmp_path)
        logger.set_iteration(1)
        with logger.card("train") as c:
            c.append(Scalar(0.9))
        logger.set_iteration(2)
        with logger.card("train") as c:
            c.append(Scalar(0.5))
        logger.set_iteration(3)
        with logger.card("train") as c:
            c.append(Scalar(0.1))
        logger.close()

        events = _read_events(logger.log_dir)
        assert [e.iteration for e in events] == [1, 2, 3]
        assert all(e.card_event.name == "train" for e in events)

    def test_explicit_iteration_on_commit(self, tmp_path) -> None:
        logger = _make_logger(tmp_path)
        card = logger.card("x")
        card.append(Markdown("a"))
        card.commit(it=42)
        logger.close()

        events = _read_events(logger.log_dir)
        assert events[0].iteration == 42


class TestCardAutodetect:
    def test_string_autodetected_as_markdown(self, tmp_path) -> None:
        logger = _make_logger(tmp_path)
        with logger.card("c") as c:
            c.append("# hi")
        logger.close()

        events = _read_events(logger.log_dir)
        comp = events[0].card_event.components[0]
        assert comp.WhichOneof("data") == "markdown"
        assert comp.markdown.content == "# hi"


class TestCardTaskCapture:
    def test_task_from_context(self, tmp_path) -> None:
        from rinnsal.context import current

        logger = _make_logger(tmp_path)
        current._set_task_name("train_step")
        try:
            with logger.card("progress") as c:
                c.append(Scalar(0.5))
        finally:
            current._set_task_name("")
        logger.close()

        events = _read_events(logger.log_dir)
        assert events[0].card_event.task == "train_step"

    def test_explicit_task_override(self, tmp_path) -> None:
        logger = _make_logger(tmp_path)
        with logger.card("c", task="explicit") as c:
            c.append(Markdown("x"))
        logger.close()

        events = _read_events(logger.log_dir)
        assert events[0].card_event.task == "explicit"


class TestCardMixedComponents:
    def test_all_component_types(self, tmp_path) -> None:
        logger = _make_logger(tmp_path, with_db=True)
        with logger.card("full") as c:
            c.append(Markdown("# header"))
            c.append(Table([[1, 2], [3, 4]], headers=["a", "b"]))
            c.append(PythonCode("print('x')"))
            c.append(ProgressBar(3, 10, label="epochs"))
            c.append(Artifact({"k": 1}, description="state"))
            c.append(Scalar(0.7))
        logger.close()

        events = _read_events(logger.log_dir)
        assert len(events) == 1
        kinds = [c.WhichOneof("data") for c in events[0].card_event.components]
        assert kinds == [
            "markdown",
            "table",
            "code",
            "progress",
            "artifact",  # Artifact → CardComponent.artifact slot (not checkpoint)
            "scalar",
        ]
        # Artifact metadata round-trips on the card slot:
        artifact_cc = events[0].card_event.components[4]
        assert artifact_cc.artifact.description == "state"
        assert artifact_cc.artifact.type_name == "dict"

    def test_blob_offload_inside_card(self, tmp_path) -> None:
        logger = _make_logger(tmp_path, with_db=True)
        big = list(range(10_000))
        with logger.card("big") as c:
            c.append(Artifact(big))
        logger.close()

        events = _read_events(logger.log_dir)
        artifact_cc = events[0].card_event.components[0]
        assert artifact_cc.artifact.blob_hash != ""
        assert artifact_cc.artifact.data == b""
