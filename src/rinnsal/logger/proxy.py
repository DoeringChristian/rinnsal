"""Logger proxy for remote workers.

Provides a Logger-compatible API that serializes events to a byte
buffer (for batch relay) or to a stream (for real-time relay).
The orchestrator replays collected events into the real Logger via
``replay_events()``.

Wire format: same as ``events.pb`` — length-prefixed protobuf records:
    4 bytes little-endian uint32 (message length) + serialized Event.

For stream mode over SSH stderr, each record is additionally prefixed
with 5 magic bytes (``RNNSL``) so the orchestrator can distinguish
binary event data from plain-text stderr output.
"""

from __future__ import annotations

import io
import struct
from datetime import datetime
from typing import Any, BinaryIO

# Magic prefix for stream-mode records (SSH stderr multiplexing).
STREAM_MAGIC = b"RNNSL"


def render_figure_to_png(figure: Any) -> bytes:
    """Render a matplotlib figure to PNG bytes."""
    try:
        import matplotlib
        matplotlib.use("Agg")
    except (ImportError, AttributeError):
        pass

    buf = io.BytesIO()
    figure.savefig(buf, format="png", dpi=100, bbox_inches="tight")
    buf.seek(0)
    return buf.read()


def convert_image_to_png(image: Any) -> tuple[bytes, int, int]:
    """Convert image data to PNG bytes. Returns (png_bytes, width, height)."""
    if isinstance(image, bytes):
        return image, 0, 0

    try:
        from PIL import Image as PILImage
        if isinstance(image, PILImage.Image):
            buf = io.BytesIO()
            image.save(buf, format="PNG")
            return buf.getvalue(), image.width, image.height
    except ImportError:
        pass

    try:
        import torch
        if isinstance(image, torch.Tensor):
            image = image.detach().cpu().numpy()
    except ImportError:
        pass

    import numpy as np
    if isinstance(image, np.ndarray):
        from PIL import Image as PILImage

        arr = image
        if arr.dtype.kind == "f":
            arr = np.clip(arr, 0, 1)
            arr = (arr * 255).astype(np.uint8)

        if arr.ndim == 2:
            pil = PILImage.fromarray(arr, mode="L")
        elif arr.ndim == 3 and arr.shape[2] == 1:
            pil = PILImage.fromarray(arr[:, :, 0], mode="L")
        elif arr.ndim == 3 and arr.shape[2] == 3:
            pil = PILImage.fromarray(arr, mode="RGB")
        elif arr.ndim == 3 and arr.shape[2] == 4:
            pil = PILImage.fromarray(arr, mode="RGBA")
        else:
            raise ValueError(f"Unsupported image shape: {arr.shape}")

        buf = io.BytesIO()
        pil.save(buf, format="PNG")
        return buf.getvalue(), pil.width, pil.height

    raise TypeError(f"Unsupported image type: {type(image)}")


class LoggerProxy:
    """Logger-compatible proxy for remote workers.

    Implements the same public API as ``Logger`` but serializes each
    event as a length-prefixed protobuf record.

    Args:
        stream: If provided, events are written to this binary stream
            immediately with a ``RNNSL`` magic prefix (for real-time
            relay over SSH stderr).  If *None*, events are collected
            into an internal buffer retrievable via ``get_buffer()``.
        ray_actor_name: If provided, events are sent to a named Ray
            actor for real-time relay.  Falls back to buffer mode if
            the actor is unavailable.
        event_file: If provided, events are written as raw
            length-prefixed records (same format as ``events.pb``)
            to this file path, flushed after each write.  Used by
            the persistent SSH executor so the orchestrator can tail
            the file incrementally.
    """

    def __init__(
        self,
        stream: BinaryIO | None = None,
        ray_actor_name: str | None = None,
        event_file: str | None = None,
    ) -> None:
        self._stream = stream
        self._ray_actor_name = ray_actor_name
        self._ray_actor: Any = None  # lazily resolved
        self._event_file_handle: BinaryIO | None = None
        if event_file is not None:
            from pathlib import Path

            Path(event_file).parent.mkdir(parents=True, exist_ok=True)
            self._event_file_handle = open(event_file, "ab")
        self._buffer = (
            io.BytesIO()
            if stream is None
            and ray_actor_name is None
            and event_file is None
            else None
        )
        self._iteration = 0

    # ------------------------------------------------------------------
    # Public API — mirrors Logger
    # ------------------------------------------------------------------

    def set_iteration(self, it: int) -> None:
        self._iteration = it

    set_global_it = set_iteration

    @property
    def iteration(self) -> int:
        return self._iteration

    def add_scalar(
        self, tag: str, value: float, it: int | None = None
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, Scalar

        event = Event()
        event.timestamp = self._ts()
        event.iteration = self._it(it)
        event.scalar.CopyFrom(Scalar(tag=tag, value=value))
        self._emit(event)

    def add_text(
        self, tag: str, text: str, it: int | None = None
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, Text

        event = Event()
        event.timestamp = self._ts()
        event.iteration = self._it(it)
        event.text.CopyFrom(Text(tag=tag, value=text))
        self._emit(event)

    def add_figure(
        self,
        tag: str,
        figure: Any,
        it: int | None = None,
        interactive: bool = True,
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, Figure

        import cloudpickle

        image = render_figure_to_png(figure)
        event = Event()
        event.timestamp = self._ts()
        event.iteration = self._it(it)

        if interactive:
            data = cloudpickle.dumps(figure)
            event.figure.CopyFrom(
                Figure(tag=tag, interactive=True, image=image, data=data)
            )
        else:
            event.figure.CopyFrom(
                Figure(tag=tag, interactive=False, image=image)
            )
        self._emit(event)

    def add_image(
        self, tag: str, image: Any, it: int | None = None
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, Image

        png_data, width, height = convert_image_to_png(image)
        event = Event()
        event.timestamp = self._ts()
        event.iteration = self._it(it)
        event.image.CopyFrom(
            Image(
                tag=tag, data=png_data, width=width,
                height=height, format="png",
            )
        )
        self._emit(event)

    def add_card(
        self,
        task: str,
        kind: str,
        title: str = "",
        content: str = "",
        image: bytes = b"",
        it: int | None = None,
    ) -> None:
        from rinnsal.logger.events_pb2 import Card, Event

        event = Event()
        event.timestamp = self._ts()
        event.iteration = self._it(it)
        event.card.CopyFrom(
            Card(task=task, kind=kind, title=title,
                 content=content, image=image)
        )
        self._emit(event)

    def add_checkpoint(
        self, tag: str, obj: Any, it: int | None = None
    ) -> None:
        import cloudpickle
        from rinnsal.logger.events_pb2 import Checkpoint, Event

        data = cloudpickle.dumps(obj)
        event = Event()
        event.timestamp = self._ts()
        event.iteration = self._it(it)
        event.checkpoint.CopyFrom(Checkpoint(tag=tag, data=data))
        self._emit(event)

    def add_task_node(
        self,
        task_name: str,
        task_hash: str,
        status: str,
        duration: float = 0.0,
        error: str = "",
        it: int | None = None,
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, TaskNode

        event = Event()
        event.timestamp = self._ts()
        event.iteration = self._it(it)
        event.task_node.CopyFrom(
            TaskNode(
                task_name=task_name, task_hash=task_hash,
                status=status, duration=duration, error=error,
            )
        )
        self._emit(event)

    def add_task_edge(
        self,
        from_task: str,
        to_task: str,
        it: int | None = None,
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, TaskEdge

        event = Event()
        event.timestamp = self._ts()
        event.iteration = self._it(it)
        event.task_edge.CopyFrom(
            TaskEdge(from_task=from_task, to_task=to_task)
        )
        self._emit(event)

    def flush(self) -> None:
        if self._stream is not None:
            self._stream.flush()
        if self._event_file_handle is not None:
            self._event_file_handle.flush()

    def get_buffer(self) -> bytes:
        """Return collected event bytes.

        In buffer mode this is the primary output.  In ray-actor mode
        this contains only fallback events (when the actor was
        unreachable).
        """
        if self._buffer is None:
            return b""
        return self._buffer.getvalue()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ts(self) -> float:
        return datetime.now().timestamp()

    def _it(self, it: int | None) -> int:
        return it if it is not None else self._iteration

    def _emit(self, event: Any) -> None:
        """Serialize and write one Event record."""
        data = event.SerializeToString()

        # Ray actor mode — send event directly to the relay actor
        if self._ray_actor_name is not None:
            try:
                if self._ray_actor is None:
                    import ray

                    self._ray_actor = ray.get_actor(self._ray_actor_name)
                self._ray_actor.log_event.remote(data)
                return
            except Exception:
                # Actor unavailable — fall back to buffer
                if self._buffer is None:
                    self._buffer = io.BytesIO()

        record = struct.pack("<I", len(data)) + data

        if self._event_file_handle is not None:
            # File mode — raw length-prefixed records, flush immediately
            # so the orchestrator can tail incrementally.
            self._event_file_handle.write(record)
            self._event_file_handle.flush()
        elif self._stream is not None:
            # Stream mode — prefix with magic so the receiver can
            # distinguish event records from plain-text stderr.
            self._stream.write(STREAM_MAGIC + record)
            self._stream.flush()
        elif self._buffer is not None:
            self._buffer.write(record)


def replay_events(logger: Any, event_bytes: bytes) -> None:
    """Replay length-prefixed protobuf events into a Logger.

    Reads the buffer produced by ``LoggerProxy.get_buffer()`` and writes
    each event directly to the logger's event file writer, bypassing
    the async queue (events are already fully formed).
    """
    if not event_bytes:
        return

    from rinnsal.logger.events_pb2 import Event

    offset = 0
    while offset + 4 <= len(event_bytes):
        length = struct.unpack_from("<I", event_bytes, offset)[0]
        offset += 4
        if offset + length > len(event_bytes):
            break
        event = Event()
        event.ParseFromString(event_bytes[offset : offset + length])
        offset += length
        logger._event_writer.write(event)

    logger._event_writer.flush()
