"""Component classes for the unified Logger+Cards system.

Each component is a typed, renderable artifact that can be written through
the Logger (as a standalone tagged event) or bundled into a Card. Components
know how to serialize themselves to a protobuf payload and how to render
themselves inline in a notebook.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from rinnsal.data.logger.logger import Logger


HEAVY_BLOB_THRESHOLD = 16 * 1024  # bytes — below this, inline even if blob store exists


def _is_plotly_figure(obj: Any) -> bool:
    return type(obj).__module__.split(".")[0] == "plotly"


def _is_matplotlib_figure(obj: Any) -> bool:
    mod = type(obj).__module__
    return mod.startswith("matplotlib.")


# --------------------------------------------------------------------------- #
# Base class
# --------------------------------------------------------------------------- #


class Component(ABC):
    """Base class for all loggable components."""

    kind: ClassVar[str]

    @abstractmethod
    def to_payload(
        self, logger: "Logger | None" = None
    ) -> tuple[str, Any]:
        """Produce (oneof_field_name, protobuf_message).

        If *logger* has a blob store and the payload is heavy, the
        component may offload bytes via ``logger._put_blob(...)`` and
        populate hash fields instead of inline bytes.
        """

    def _repr_html_(self) -> str:
        return f"<code>&lt;{self.kind} component&gt;</code>"


# --------------------------------------------------------------------------- #
# Primitive components (wrap existing proto messages)
# --------------------------------------------------------------------------- #


class Scalar(Component):
    kind = "scalar"

    def __init__(self, value: float) -> None:
        self.value = float(value)

    def to_payload(self, logger=None):
        from rinnsal.data.logger.events_pb2 import Scalar as ScalarMsg

        return "scalar", ScalarMsg(tag="", value=self.value)

    def _repr_html_(self) -> str:
        return f"<code>{self.value}</code>"


class Text(Component):
    kind = "text"

    def __init__(self, content: str) -> None:
        self.content = content

    def to_payload(self, logger=None):
        from rinnsal.data.logger.events_pb2 import Text as TextMsg

        return "text", TextMsg(tag="", value=self.content)

    def _repr_html_(self) -> str:
        from html import escape

        return f"<pre>{escape(self.content)}</pre>"


class Markdown(Component):
    kind = "markdown"

    def __init__(self, content: str) -> None:
        self.content = content

    def to_payload(self, logger=None):
        from rinnsal.data.logger.events_pb2 import Markdown as MarkdownMsg

        return "markdown", MarkdownMsg(tag="", content=self.content)

    def _repr_html_(self) -> str:
        # Jupyter can render markdown natively via _repr_markdown_.
        from html import escape

        return f"<div class='rnnsl-md'><pre>{escape(self.content)}</pre></div>"

    def _repr_markdown_(self) -> str:
        return self.content


class Table(Component):
    kind = "table"

    def __init__(self, data: Any, headers: list[str] | None = None) -> None:
        self.headers, self.rows = self._normalize(data, headers)

    @staticmethod
    def _normalize(data: Any, headers: list[str] | None) -> tuple[list[str], list[list[Any]]]:
        try:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                hdrs = [str(c) for c in data.columns]
                rows = [[_json_safe(v) for v in row] for row in data.values.tolist()]
                return hdrs, rows
        except ImportError:
            pass
        if not data:
            return list(headers or []), []
        rows = [list(r) for r in data]
        hdrs = list(headers) if headers is not None else []
        return hdrs, [[_json_safe(v) for v in r] for r in rows]

    def to_payload(self, logger=None):
        from rinnsal.data.logger.events_pb2 import Table as TableMsg

        return "table", TableMsg(
            tag="",
            headers_json=json.dumps(self.headers),
            rows_json=json.dumps(self.rows, default=str),
        )

    def _repr_html_(self) -> str:
        from html import escape

        out = ["<table border='1' style='border-collapse:collapse'>"]
        if self.headers:
            out.append("<tr>" + "".join(f"<th>{escape(str(h))}</th>" for h in self.headers) + "</tr>")
        for row in self.rows:
            out.append("<tr>" + "".join(f"<td>{escape(str(v))}</td>" for v in row) + "</tr>")
        out.append("</table>")
        return "".join(out)


class PythonCode(Component):
    kind = "code"

    def __init__(self, source: str, language: str = "python") -> None:
        self.source = source
        self.language = language

    def to_payload(self, logger=None):
        from rinnsal.data.logger.events_pb2 import Code as CodeMsg

        return "code", CodeMsg(tag="", source=self.source, language=self.language)

    def _repr_html_(self) -> str:
        from html import escape

        return (
            f"<pre><code class='language-{self.language}'>"
            f"{escape(self.source)}</code></pre>"
        )


class ProgressBar(Component):
    kind = "progress"

    def __init__(self, value: float, total: float = 1.0, label: str = "") -> None:
        self.value = float(value)
        self.total = float(total)
        self.label = label

    def to_payload(self, logger=None):
        from rinnsal.data.logger.events_pb2 import Progress as ProgressMsg

        return "progress", ProgressMsg(
            tag="", value=self.value, total=self.total, label=self.label
        )

    def _repr_html_(self) -> str:
        from html import escape

        label = f" <span>{escape(self.label)}</span>" if self.label else ""
        return f"<progress value='{self.value}' max='{self.total}'></progress>{label}"


# --------------------------------------------------------------------------- #
# Heavy components
# --------------------------------------------------------------------------- #


class Image(Component):
    kind = "image"

    def __init__(self, image: Any) -> None:
        self._raw = image
        self._png: bytes | None = None
        self._width = 0
        self._height = 0

    def _ensure_png(self) -> None:
        if self._png is not None:
            return
        png, w, h = _convert_image_to_png(self._raw)
        self._png, self._width, self._height = png, w, h

    def to_payload(self, logger=None):
        from rinnsal.data.logger.events_pb2 import Image as ImageMsg

        self._ensure_png()
        msg = ImageMsg(tag="", width=self._width, height=self._height, format="png")
        if (
            logger is not None
            and logger._blob_store is not None
            and len(self._png) >= HEAVY_BLOB_THRESHOLD
        ):
            msg.blob_hash = logger._put_blob(self._png)
        else:
            msg.data = self._png
        return "image", msg

    def _repr_html_(self) -> str:
        import base64

        self._ensure_png()
        b64 = base64.b64encode(self._png).decode("ascii")
        return f"<img src='data:image/png;base64,{b64}'/>"


class Figure(Component):
    """Matplotlib figure component. Preserves today's figure behavior."""

    kind = "figure"

    def __init__(self, figure: Any, interactive: bool = True) -> None:
        self.figure = figure
        self.interactive = interactive

    def to_payload(self, logger=None):
        import cloudpickle

        from rinnsal.data.logger.events_pb2 import Figure as FigureMsg

        png = _render_matplotlib_to_png(self.figure)
        msg = FigureMsg(tag="", interactive=self.interactive, format="matplotlib")
        use_blobs = (
            logger is not None
            and logger._blob_store is not None
            and len(png) >= HEAVY_BLOB_THRESHOLD
        )
        if use_blobs:
            msg.image_blob_hash = logger._put_blob(png)
        else:
            msg.image = png
        if self.interactive:
            data = cloudpickle.dumps(self.figure)
            if use_blobs and len(data) >= HEAVY_BLOB_THRESHOLD:
                msg.data_blob_hash = logger._put_blob(data)
            else:
                msg.data = data
        return "figure", msg

    def _repr_html_(self) -> str:
        import base64

        png = _render_matplotlib_to_png(self.figure)
        b64 = base64.b64encode(png).decode("ascii")
        return f"<img src='data:image/png;base64,{b64}'/>"


class Plotly(Component):
    kind = "plotly"

    def __init__(self, fig: Any) -> None:
        if not _is_plotly_figure(fig):
            raise TypeError(
                f"Plotly component expects a plotly figure, got {type(fig).__name__}"
            )
        self.fig = fig

    def to_payload(self, logger=None):
        try:
            import plotly.io as pio
        except ImportError as e:
            raise ImportError(
                "Plotly support requires `pip install rinnsal[plotly]`"
            ) from e

        from rinnsal.data.logger.events_pb2 import Plotly as PlotlyMsg

        figure_json = pio.to_json(self.fig)

        n_traces = 0
        title = ""
        try:
            n_traces = len(self.fig.data)
        except Exception:
            pass
        try:
            title = self.fig.layout.title.text or ""
        except Exception:
            pass

        msg = PlotlyMsg(tag="", n_traces=n_traces, title=title)
        if logger is not None and logger._blob_store is not None:
            msg.blob_hash = logger._put_blob(figure_json.encode("utf-8"))
        else:
            msg.inline_json = figure_json

        # Optional static PNG fallback via kaleido. Best-effort; silent on failure.
        try:
            png = pio.to_image(self.fig, format="png")
            if logger is not None and logger._blob_store is not None:
                msg.png_blob_hash = logger._put_blob(png)
            # If no blob store, we leave PNG out entirely — the interactive JSON
            # is inline and the viewer will render via Plotly.js.
        except Exception:
            pass

        return "plotly", msg

    def _repr_mimebundle_(self, include=None, exclude=None):
        try:
            import plotly.io as pio
        except ImportError:
            return {"text/plain": f"<Plotly figure, plotly not installed>"}
        return {"application/vnd.plotly.v1+json": json.loads(pio.to_json(self.fig))}

    def _repr_html_(self) -> str:
        try:
            import plotly.io as pio

            return pio.to_html(self.fig, include_plotlyjs="cdn", full_html=False)
        except Exception:
            return "<code>&lt;Plotly figure&gt;</code>"


class Artifact(Component):
    """Any Python value — rendered in the viewer and pickled for rehydration.

    ``Artifact`` is the general-purpose 'show me this' component. It
    handles the full spectrum from ``{"loss": 0.3}`` to a trained model.
    Two representations are stored:

    * A JSON preview (when the value is JSON-safe or a shallow collection
      of JSON-safe values) — small, cheap, rendered inline in the viewer
      as a nested tree / list / scalar.
    * The cloudpickled bytes — stored inline for small values, offloaded
      to the blob store for heavy ones. Always present so the value can
      be re-materialized at read time.

    The existing ``Checkpoint`` proto is reused (one message, distinguished
    from bare checkpoints by the ``description`` / ``type_name`` fields).
    """

    kind = "artifact"

    def __init__(self, obj: Any, description: str = "") -> None:
        self.obj = obj
        self.description = description

    def to_payload(self, logger=None):
        import cloudpickle

        from rinnsal.data.logger.events_pb2 import Checkpoint

        data = cloudpickle.dumps(self.obj)
        preview = _json_preview(self.obj)
        msg = Checkpoint(
            tag="",
            description=self.description or preview,
            type_name=type(self.obj).__name__,
        )
        if (
            logger is not None
            and logger._blob_store is not None
            and len(data) >= HEAVY_BLOB_THRESHOLD
        ):
            msg.blob_hash = logger._put_blob(data)
        else:
            msg.data = data
        # Top-level Event uses the ``checkpoint`` oneof slot; the
        # ``CardComponent`` message uses an ``artifact`` slot (same proto).
        # The card writer (Phase 2) remaps this field name on emission.
        return "checkpoint", msg

    def _repr_html_(self) -> str:
        from html import escape

        type_name = type(self.obj).__name__
        desc = escape(self.description) if self.description else ""
        preview_html = _value_to_html(self.obj)
        header = (
            f"<div><b>{escape(type_name)}</b>"
            + (f" — {desc}" if desc else "")
            + "</div>"
        )
        return f"<div class='rnnsl-artifact'>{header}{preview_html}</div>"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _json_safe(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return str(v)


_PREVIEW_MAX_LEN = 200


def _json_preview(value: Any) -> str:
    """Short, readable summary of an arbitrary Python value."""
    try:
        s = json.dumps(value, default=str, ensure_ascii=False)
    except Exception:
        s = repr(value)
    if len(s) > _PREVIEW_MAX_LEN:
        s = s[:_PREVIEW_MAX_LEN] + "…"
    return s


def _value_to_html(value: Any, depth: int = 0) -> str:
    """Render a Python value as a small HTML tree (dict/list/scalar)."""
    from html import escape

    if value is None or isinstance(value, (bool, int, float)):
        return f"<code>{escape(str(value))}</code>"
    if isinstance(value, str):
        s = value if len(value) <= _PREVIEW_MAX_LEN else value[:_PREVIEW_MAX_LEN] + "…"
        return f"<code>{escape(repr(s))}</code>"
    if isinstance(value, dict):
        if depth > 2 or len(value) > 50:
            return f"<code>{escape(_json_preview(value))}</code>"
        rows = []
        for k, v in value.items():
            rows.append(
                f"<tr><th style='text-align:left;padding-right:8px'>"
                f"{escape(str(k))}</th><td>{_value_to_html(v, depth + 1)}</td></tr>"
            )
        return "<table style='border-collapse:collapse'>" + "".join(rows) + "</table>"
    if isinstance(value, (list, tuple)):
        if depth > 2 or len(value) > 50:
            return f"<code>{escape(_json_preview(value))}</code>"
        items = "".join(f"<li>{_value_to_html(v, depth + 1)}</li>" for v in value)
        return f"<ol>{items}</ol>"
    return f"<code>{escape(_json_preview(value))}</code>"


def _render_matplotlib_to_png(figure: Any) -> bytes:
    import io

    try:
        import matplotlib

        matplotlib.use("Agg")
    except (ImportError, AttributeError):
        pass

    buf = io.BytesIO()
    figure.savefig(buf, format="png", dpi=100, bbox_inches="tight")
    buf.seek(0)
    return buf.read()


def _convert_image_to_png(image: Any) -> tuple[bytes, int, int]:
    """Convert image-like input to (png_bytes, width, height)."""
    import io

    if isinstance(image, (bytes, bytearray)):
        return bytes(image), 0, 0

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


def autodetect(value: Any) -> Component:
    """Best-effort conversion of a raw value to a Component.

    Routing:
      * Component instance     -> passthrough
      * plotly figure          -> Plotly
      * matplotlib figure      -> Figure
      * str                    -> Markdown
      * bytes/bytearray        -> Image
      * numpy ndarray / PIL    -> Image
      * everything else (dict, list, number, dataclass, ...) -> Artifact
        (the renderable, picklable catch-all)
    """
    if isinstance(value, Component):
        return value
    if _is_plotly_figure(value):
        return Plotly(value)
    if _is_matplotlib_figure(value):
        return Figure(value)
    if isinstance(value, str):
        return Markdown(value)
    if isinstance(value, (bytes, bytearray)):
        return Image(value)
    mod = type(value).__module__.split(".")[0]
    if mod in {"numpy", "PIL"}:
        # Disambiguate: numpy scalars / 0-d arrays should not become images.
        try:
            import numpy as np

            if isinstance(value, np.ndarray) and value.ndim == 0:
                return Artifact(value)
            if isinstance(value, np.generic):
                return Artifact(value)
        except ImportError:
            pass
        return Image(value)
    return Artifact(value)
