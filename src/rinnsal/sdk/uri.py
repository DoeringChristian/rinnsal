"""Parse ``rinnsal://host/flow/run/tag[@iter]`` URIs.

Mirrors pathspec conventions from MLflow/Metaflow: a self-contained
string that identifies one piece of data on one server. ``rinnsal://``
maps to http://; ``rinnsals://`` maps to https:// for TLS.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class Reference:
    """Parsed rinnsal:// URI.

    Every field past ``host_url`` is optional: a URI of just
    ``rinnsal://host:8800`` has flow/run/tag/iteration all None.
    """

    host_url: str
    flow: str | None = None
    run: str | None = None
    tag: str | None = None
    iteration: int | None = None
    all_iterations: bool = False   # True when @* suffix used


def parse_uri(uri: str) -> Reference:
    """Parse a ``rinnsal://`` URI into a :class:`Reference`.

    Examples::

        rinnsal://fermat:8800
        rinnsal://fermat:8800/training
        rinnsal://fermat:8800/training/20260413_124534
        rinnsal://fermat:8800/training/20260413_124534/mesh
        rinnsal://fermat:8800/training/20260413_124534/mesh@10
        rinnsal://fermat:8800/training/20260413_124534/mesh@*
    """
    parsed = urlparse(uri)
    if parsed.scheme == "rinnsal":
        http_scheme = "http"
    elif parsed.scheme == "rinnsals":
        http_scheme = "https"
    elif parsed.scheme in ("http", "https"):
        # Already an HTTP-style URL; honor as-is.
        http_scheme = parsed.scheme
    else:
        raise ValueError(
            f"unsupported scheme {parsed.scheme!r} (expected "
            "rinnsal:// or rinnsals://)"
        )

    if not parsed.netloc:
        raise ValueError(f"missing host in URI {uri!r}")

    host_url = f"{http_scheme}://{parsed.netloc}"
    segments = [s for s in parsed.path.split("/") if s]

    flow = segments[0] if len(segments) >= 1 else None
    run = segments[1] if len(segments) >= 2 else None

    tag: str | None = None
    iteration: int | None = None
    all_iterations = False

    if len(segments) >= 3:
        # Tags commonly contain slashes ("train/loss"), so everything
        # after the run segment is the tag up to the optional @iter.
        tag_part = "/".join(segments[2:])
        if "@" in tag_part:
            tag, it_raw = tag_part.rsplit("@", 1)
            if it_raw == "*":
                all_iterations = True
            else:
                try:
                    iteration = int(it_raw)
                except ValueError as e:
                    raise ValueError(
                        f"invalid iteration in {uri!r}: {it_raw!r}"
                    ) from e
        else:
            tag = tag_part

    return Reference(
        host_url=host_url,
        flow=flow,
        run=run,
        tag=tag,
        iteration=iteration,
        all_iterations=all_iterations,
    )


def format_uri(ref: Reference) -> str:
    """Inverse of :func:`parse_uri`."""
    host = ref.host_url
    # Convert back to rinnsal:// form for stability.
    if host.startswith("http://"):
        uri = "rinnsal://" + host[len("http://"):]
    elif host.startswith("https://"):
        uri = "rinnsals://" + host[len("https://"):]
    else:
        uri = host

    if ref.flow:
        uri += f"/{ref.flow}"
        if ref.run:
            uri += f"/{ref.run}"
            if ref.tag:
                uri += f"/{ref.tag}"
                if ref.all_iterations:
                    uri += "@*"
                elif ref.iteration is not None:
                    uri += f"@{ref.iteration}"
    return uri
