"""HTTP client for talking to a remote rinnsal viewer server.

The :class:`Client` is a thin wrapper around the viewer's JSON API. It
takes the ``host_url`` (e.g. ``http://fermat:8800``) plus the server's
log ``root`` directory and exposes helpers to list flows/runs, fetch
metadata, and stream blobs. Higher-level handles (:class:`Run`,
:class:`Series`, :class:`Artifact`) live in ``references.py`` and
delegate to this client.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx


class Client:
    """Connection to one rinnsal server + log root.

    Parameters
    ----------
    host_url:
        Base URL of the viewer server (``http://host:port``).
    root:
        The server-side log directory that contains ``flows/``,
        ``snapshots/`` and ``metadata.sqlite``. This is the path the
        viewer was started with via ``--root``.
    timeout:
        Per-request HTTP timeout in seconds.
    """

    def __init__(
        self,
        host_url: str,
        *,
        root: str,
        timeout: float = 30.0,
    ) -> None:
        try:
            import httpx
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "rinnsal.sdk needs httpx. Install the cluster extra: "
                "`pip install 'rinnsal[cluster]'` or `uv add httpx`."
            ) from e

        self.host_url = host_url.rstrip("/")
        self.root = root
        self._http = httpx.Client(
            base_url=self.host_url, timeout=timeout
        )

    # ── lifecycle ──────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    # ── low-level plumbing ─────────────────────────────────────────

    def _get_json(self, path: str, **params: Any) -> Any:
        params = {"root": self.root, **params}
        r = self._http.get(path, params=params)
        r.raise_for_status()
        return r.json()

    def _get_bytes(self, path: str, **params: Any) -> bytes:
        params = {"root": self.root, **params}
        r = self._http.get(path, params=params)
        r.raise_for_status()
        return r.content

    # ── discovery ──────────────────────────────────────────────────

    def list_flows(self) -> list[dict]:
        """Return the list of flows known to the server."""
        return self._get_json("/api/flows")["flows"]

    def list_runs(self) -> list[dict]:
        """Return every run under the root as ``{path, name, flow}`` dicts."""
        return self._get_json("/api/runs")

    def run_info(self, run_id: str) -> dict:
        """Fetch a single run's metadata (incl. ``snapshot_hash``)."""
        return self._get_json(f"/api/run/{run_id}/info")

    def run(self, run_id: str):
        """Return a :class:`~rinnsal.sdk.references.Run` handle."""
        from rinnsal.sdk.references import Run

        info = self.run_info(run_id)
        return Run(
            self,
            run_id=run_id,
            run_path=info["run_dir"],
            flow=info.get("flow"),
            snapshot_hash=info.get("snapshot_hash"),
        )

    # ── per-run payloads ───────────────────────────────────────────

    def scalars(self, run_path: str) -> dict[str, list[dict]]:
        return self._get_json(f"/api/scalars{_abs(run_path)}")

    def text(self, run_path: str) -> dict[str, list[dict]]:
        return self._get_json(f"/api/text{_abs(run_path)}")

    def figures_meta(self, run_path: str) -> dict[str, list[dict]]:
        return self._get_json(f"/api/figures{_abs(run_path)}")

    def figure_png(self, run_path: str, tag: str, it: int) -> bytes:
        return self._get_bytes(
            f"/api/figure{_abs(run_path)}", tag=tag, it=it
        )

    def images_meta(self, run_path: str) -> dict[str, list[dict]]:
        return self._get_json(f"/api/images{_abs(run_path)}")

    def image_png(self, run_path: str, tag: str, it: int) -> bytes:
        return self._get_bytes(
            f"/api/image{_abs(run_path)}", tag=tag, it=it
        )

    def tags(self, run_path: str) -> list[dict]:
        return self._get_json(f"/api/tags{_abs(run_path)}")["tags"]

    def cards(self, run_path: str) -> list[dict]:
        return self._get_json(f"/api/cards{_abs(run_path)}")["cards"]

    def card(
        self,
        run_path: str,
        name: str,
        task: str = "",
        it: int | None = None,
    ) -> dict:
        params: dict[str, Any] = {"name": name, "task": task}
        if it is not None:
            params["it"] = it
        return self._get_json(f"/api/card{_abs(run_path)}", **params)

    def blob(self, run_path: str, blob_hash: str) -> bytes:
        """Fetch one content-addressed blob attached to a run."""
        return self._get_bytes(f"/api/blob{_abs(run_path)}/{blob_hash}")

    # ── snapshots ──────────────────────────────────────────────────

    def snapshot_archive(self, snapshot_hash: str) -> bytes:
        """Download the deterministic tarball for a code snapshot."""
        return self._get_bytes(
            f"/api/snapshots/{snapshot_hash}/archive"
        )


def _abs(run_path: str) -> str:
    """Ensure run_path is mounted with a leading slash for the path route."""
    if run_path.startswith("/"):
        return run_path
    return "/" + run_path


def connect(host_url: str, *, root: str, timeout: float = 30.0) -> Client:
    """Open a :class:`Client` against a remote rinnsal server."""
    return Client(host_url, root=root, timeout=timeout)
