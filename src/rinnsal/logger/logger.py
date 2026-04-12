"""Logger for tracking experiment metrics."""

from __future__ import annotations

import atexit
import queue
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import cloudpickle

# File names for storage
MARKER_FILE = ".rinnsal"  # Marker file identifying a rinnsal run directory
EVENTS_FILE = "events.pb"  # Protobuf event file


class Logger:
    """Logger for tracking scalars, text, figures, and checkpoints.

    Saving is performed asynchronously in a background thread to avoid
    blocking the main training loop. Use flush() to wait for pending writes.

    All events are stored in a single protobuf file (events.pb).

    Storage structure:
        log_dir/
        ├── .rinnsal            # Marker file identifying this as a rinnsal log
        └── events.pb           # All events in a single protobuf file

    Args:
        log_dir: Directory to store log files. If None, auto-generates
                 a timestamped directory under runs/.

    Example:
        logger = Logger("/path/to/logs")
        logger.set_iteration(100)
        logger.add_scalar("loss", 0.5)
        logger.add_text("info", "Training started")
        logger.add_figure("plot", fig, interactive=True)
        logger.flush()  # Wait for all writes to complete
    """

    def __init__(self, log_dir: str | Path | None = None):
        if log_dir is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_dir = Path("runs") / f"run_{timestamp}"
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._iteration = 0

        # Create marker file to identify this as a rinnsal run directory
        marker_path = self._log_dir / MARKER_FILE
        marker_path.touch(exist_ok=True)

        # Protobuf event file writer
        from rinnsal.logger.event_file import EventFileWriter

        self._event_writer = EventFileWriter(
            self._log_dir / EVENTS_FILE
        )

        # Async saving infrastructure
        self._queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        self._closed = False
        self._worker = threading.Thread(
            target=self._worker_loop, daemon=True
        )
        self._worker.start()

        # Register auto-flush on exit
        atexit.register(self._atexit_flush)

    @property
    def log_dir(self) -> Path:
        """Return the log directory path."""
        return self._log_dir

    @property
    def iteration(self) -> int:
        """Return the current iteration."""
        return self._iteration

    def _worker_loop(self) -> None:
        """Background worker that processes save operations."""
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                task = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue

            op, args = task
            try:
                if op == "scalar":
                    self._write_scalar(*args)
                elif op == "text":
                    self._write_text(*args)
                elif op == "figure":
                    self._write_figure(*args)
                elif op == "checkpoint":
                    self._write_checkpoint(*args)
                elif op == "card":
                    self._write_card(*args)
                elif op == "image":
                    self._write_image(*args)
                elif op == "task_node":
                    self._write_task_node(*args)
                elif op == "task_edge":
                    self._write_task_edge(*args)
            except Exception:
                import traceback
                import sys
                traceback.print_exc(file=sys.stderr)
            finally:
                self._queue.task_done()

    def flush(self) -> None:
        """Wait for all pending writes to complete."""
        self._queue.join()
        self._event_writer.flush()

    def _atexit_flush(self) -> None:
        """Flush on exit, but only if not already closed."""
        if not self._closed:
            self.close()

    def close(self) -> None:
        """Stop the background worker and flush pending writes."""
        if self._closed:
            return
        self._closed = True
        self._stop_event.set()
        self.flush()
        self._worker.join(timeout=5.0)
        self._event_writer.close()

    def set_iteration(self, it: int) -> None:
        """Set the current iteration counter."""
        self._iteration = it

    # Alias for compatibility
    set_global_it = set_iteration

    def _get_timestamp(self) -> float:
        """Get current timestamp in seconds."""
        return datetime.now().timestamp()

    def add_scalar(
        self, tag: str, value: float, it: int | None = None
    ) -> None:
        """Log a scalar value.

        Args:
            tag: Name/tag for the scalar (e.g., "loss", "accuracy").
            value: The scalar value.
            it: Iteration number. If None, uses current iteration.
        """
        if it is None:
            it = self._iteration
        ts = self._get_timestamp()
        self._queue.put(("scalar", (tag, value, it, ts)))

    def add_text(
        self, tag: str, text: str, it: int | None = None
    ) -> None:
        """Log a text string.

        Args:
            tag: Name/tag for the text.
            text: The text string.
            it: Iteration number. If None, uses current iteration.
        """
        if it is None:
            it = self._iteration
        ts = self._get_timestamp()
        self._queue.put(("text", (tag, text, it, ts)))

    def add_figure(
        self,
        tag: str,
        figure: Any,
        it: int | None = None,
        interactive: bool = True,
    ) -> None:
        """Log a figure object.

        Figures are saved using cloudpickle, preserving the full object
        for later loading and modification.

        Args:
            tag: Name/tag for the figure.
            figure: The figure object (e.g., matplotlib figure).
            it: Iteration number. If None, uses current iteration.
            interactive: If True, render as interactive widget in viewer.
        """
        if it is None:
            it = self._iteration
        ts = self._get_timestamp()
        self._queue.put(("figure", (tag, figure, it, interactive, ts)))

    def add_checkpoint(
        self, tag: str, obj: Any, it: int | None = None
    ) -> None:
        """Log an arbitrary object as a checkpoint.

        Checkpoints are saved using cloudpickle.
        Use this to save model weights, optimizer state, or any
        serializable object.

        Args:
            tag: Name/tag for the checkpoint.
            obj: The object to save (must be picklable).
            it: Iteration number. If None, uses current iteration.
        """
        if it is None:
            it = self._iteration
        ts = self._get_timestamp()
        self._queue.put(("checkpoint", (tag, obj, it, ts)))

    def add_card(
        self,
        task: str,
        kind: str,
        title: str = "",
        content: str = "",
        image: bytes = b"",
        it: int | None = None,
    ) -> None:
        """Log a card event (rich task output).

        Card events are used to display rich content from task execution
        in the viewer.

        Args:
            task: Name of the task that produced this card.
            kind: Type of card content (text, image, table, html).
            title: Optional title for the card item.
            content: Text/HTML/markdown content.
            image: PNG bytes for image cards.
            it: Iteration number. If None, uses current iteration.
        """
        if it is None:
            it = self._iteration
        ts = self._get_timestamp()
        self._queue.put(("card", (task, kind, title, content, image, it, ts)))

    def add_task_node(
        self,
        task_name: str,
        task_hash: str,
        status: str,
        duration: float = 0.0,
        error: str = "",
        params: str = "",
        it: int | None = None,
    ) -> None:
        """Log a task node event (DAG structure + last run info).

        Args:
            task_name: Stable task identity across runs.
            task_hash: Specific invocation hash.
            status: "success" | "failed" | "cached" | "running".
            duration: Elapsed seconds.
            error: Error message (when failed).
            it: Iteration number. If None, uses current iteration.
        """
        if it is None:
            it = self._iteration
        ts = self._get_timestamp()
        self._queue.put(
            ("task_node", (task_name, task_hash, status, duration, error, params, it, ts))
        )

    def add_task_edge(
        self,
        from_task: str,
        to_task: str,
        it: int | None = None,
    ) -> None:
        """Log a task dependency edge.

        Args:
            from_task: Task name of the dependency.
            to_task: Task name of the dependent.
            it: Iteration number. If None, uses current iteration.
        """
        if it is None:
            it = self._iteration
        ts = self._get_timestamp()
        self._queue.put(("task_edge", (from_task, to_task, it, ts)))

    def add_image(
        self, tag: str, image: Any, it: int | None = None
    ) -> None:
        """Log an image (numpy array, torch tensor, PIL Image, or raw bytes).

        Accepts:
            - numpy ndarray (H, W) or (H, W, C) with uint8 or float values
            - torch tensor (same shapes)
            - PIL Image
            - raw bytes (assumed PNG)

        Args:
            tag: Name/tag for the image.
            image: The image data.
            it: Iteration number. If None, uses current iteration.
        """
        if it is None:
            it = self._iteration
        ts = self._get_timestamp()
        self._queue.put(("image", (tag, image, it, ts)))

    def _write_scalar(
        self, tag: str, value: float, it: int, ts: float
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, Scalar

        event = Event()
        event.timestamp = ts
        event.iteration = it
        event.scalar.CopyFrom(Scalar(tag=tag, value=value))
        self._event_writer.write(event)

    def _write_text(
        self, tag: str, text: str, it: int, ts: float
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, Text

        event = Event()
        event.timestamp = ts
        event.iteration = it
        event.text.CopyFrom(Text(tag=tag, value=text))
        self._event_writer.write(event)

    def _render_to_png(self, figure: Any) -> bytes:
        """Render a matplotlib figure to PNG bytes."""
        import io

        # Use Agg backend in worker thread to avoid tkinter
        # "main thread is not in main loop" errors.
        try:
            import matplotlib
            matplotlib.use("Agg")
        except (ImportError, AttributeError):
            pass

        buf = io.BytesIO()
        figure.savefig(buf, format="png", dpi=100, bbox_inches="tight")
        buf.seek(0)
        return buf.read()

    def _write_figure(
        self,
        tag: str,
        figure: Any,
        it: int,
        interactive: bool,
        ts: float,
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, Figure

        event = Event()
        event.timestamp = ts
        event.iteration = it

        # Always render PNG for display
        image = self._render_to_png(figure)

        if interactive:
            # Also store pickled figure for potential future interactivity
            data = cloudpickle.dumps(figure)
            event.figure.CopyFrom(
                Figure(tag=tag, interactive=True, image=image, data=data)
            )
        else:
            event.figure.CopyFrom(
                Figure(tag=tag, interactive=False, image=image)
            )

        self._event_writer.write(event)

    def _write_checkpoint(
        self, tag: str, obj: Any, it: int, ts: float
    ) -> None:
        from rinnsal.logger.events_pb2 import Checkpoint, Event

        data = cloudpickle.dumps(obj)
        event = Event()
        event.timestamp = ts
        event.iteration = it
        event.checkpoint.CopyFrom(Checkpoint(tag=tag, data=data))
        self._event_writer.write(event)

    def _write_card(
        self,
        task: str,
        kind: str,
        title: str,
        content: str,
        image: bytes,
        it: int,
        ts: float,
    ) -> None:
        from rinnsal.logger.events_pb2 import Card, Event

        event = Event()
        event.timestamp = ts
        event.iteration = it
        event.card.CopyFrom(
            Card(task=task, kind=kind, title=title, content=content, image=image)
        )
        self._event_writer.write(event)

    def _write_task_node(
        self,
        task_name: str,
        task_hash: str,
        status: str,
        duration: float,
        error: str,
        params: str,
        it: int,
        ts: float,
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, TaskNode

        event = Event()
        event.timestamp = ts
        event.iteration = it
        event.task_node.CopyFrom(
            TaskNode(
                task_name=task_name,
                task_hash=task_hash,
                status=status,
                duration=duration,
                error=error,
                params=params,
            )
        )
        self._event_writer.write(event)

    def _write_task_edge(
        self, from_task: str, to_task: str, it: int, ts: float
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, TaskEdge

        event = Event()
        event.timestamp = ts
        event.iteration = it
        event.task_edge.CopyFrom(
            TaskEdge(from_task=from_task, to_task=to_task)
        )
        self._event_writer.write(event)

    def _convert_to_png(self, image: Any) -> tuple[bytes, int, int]:
        """Convert image data to PNG bytes. Returns (png_bytes, width, height)."""
        import io

        # Raw bytes — assume PNG
        if isinstance(image, bytes):
            return image, 0, 0

        # PIL Image
        try:
            from PIL import Image as PILImage
            if isinstance(image, PILImage.Image):
                buf = io.BytesIO()
                image.save(buf, format="PNG")
                return buf.getvalue(), image.width, image.height
        except ImportError:
            pass

        # Torch tensor → numpy
        try:
            import torch
            if isinstance(image, torch.Tensor):
                image = image.detach().cpu().numpy()
        except ImportError:
            pass

        # Numpy array → PIL → PNG
        import numpy as np
        if isinstance(image, np.ndarray):
            from PIL import Image as PILImage

            arr = image
            # Float images: clamp to [0, 1] and convert to uint8
            if arr.dtype.kind == "f":
                arr = np.clip(arr, 0, 1)
                arr = (arr * 255).astype(np.uint8)

            # (H, W) grayscale
            if arr.ndim == 2:
                pil = PILImage.fromarray(arr, mode="L")
            # (H, W, 1) grayscale
            elif arr.ndim == 3 and arr.shape[2] == 1:
                pil = PILImage.fromarray(arr[:, :, 0], mode="L")
            # (H, W, 3) RGB
            elif arr.ndim == 3 and arr.shape[2] == 3:
                pil = PILImage.fromarray(arr, mode="RGB")
            # (H, W, 4) RGBA
            elif arr.ndim == 3 and arr.shape[2] == 4:
                pil = PILImage.fromarray(arr, mode="RGBA")
            else:
                raise ValueError(f"Unsupported image shape: {arr.shape}")

            buf = io.BytesIO()
            pil.save(buf, format="PNG")
            return buf.getvalue(), pil.width, pil.height

        raise TypeError(f"Unsupported image type: {type(image)}")

    def _write_image(
        self, tag: str, image: Any, it: int, ts: float
    ) -> None:
        from rinnsal.logger.events_pb2 import Event, Image

        png_data, width, height = self._convert_to_png(image)

        event = Event()
        event.timestamp = ts
        event.iteration = it
        event.image.CopyFrom(
            Image(tag=tag, data=png_data, width=width, height=height, format="png")
        )
        self._event_writer.write(event)

    def __enter__(self) -> Logger:
        return self

    def __exit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Logger('{self._log_dir}')"
