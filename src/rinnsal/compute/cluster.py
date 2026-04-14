"""ClusterExecutor: ship tasks to a remote rinnsal coordinator.

Submission flow per task:

1. cloudpickle (func, args, kwargs); upload each blob via PUT
   ``/api/cluster/blobs/{sha256}`` (idempotent — server checks the hash).
2. POST ``/api/cluster/jobs/submit`` with the blob hashes. Coordinator
   returns a ``job_id``.
3. In a worker thread (kept off the orchestrator's main loop), long-poll
   ``GET /api/cluster/jobs/{id}/status`` until terminal.
4. Fetch the result blob; cloudpickle.loads it; build an
   :class:`ExecutionResult`.

Phase 2 ships the minimum viable path. Phase 3 will add project-archive
shipping; Phase 5 will add live event streaming back to the orchestrator's
real Logger.
"""

from __future__ import annotations

import hashlib
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import cloudpickle

from rinnsal.compute.executor import ExecutionResult, Executor

if TYPE_CHECKING:
    from rinnsal.modeling.expression import TaskExpression


log = logging.getLogger("rinnsal.cluster.executor")


def _import_httpx():
    try:
        import httpx
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "ClusterExecutor needs httpx. Install the cluster extra: "
            "`pip install 'rinnsal[cluster]'` or `uv add httpx`."
        ) from e
    return httpx


class ClusterExecutor(Executor):
    """Submit tasks to a remote coordinator via HTTP.

    A ``transport`` callable can be injected for testing (e.g. wrapping
    a FastAPI ``TestClient``); when not provided, the default httpx-
    backed transport is used.
    """

    def __init__(
        self,
        host_url: str,
        *,
        capture: bool = True,
        project_root: Path | None = None,
        max_concurrent: int = 32,
        transport: Any = None,
        poll_timeout: float = 30.0,
        ship_project: bool = True,
    ) -> None:
        super().__init__(capture=capture, snapshot=False)
        self._host_url = host_url.rstrip("/")
        self._project_root = project_root or Path.cwd()
        self._transport = transport
        self._poll_timeout = poll_timeout
        self._ship_project = ship_project
        self._pool = ThreadPoolExecutor(
            max_workers=max_concurrent,
            thread_name_prefix="rinnsal-cluster-submit",
        )
        # Cache: per-process, build the archive at most once.
        self._project_hash: str | None = None
        self._project_lock = __import__("threading").Lock()

    # ── HTTP plumbing ──────────────────────────────────────────────

    def _request(self, method: str, path: str, json: Any = None) -> Any:
        if self._transport is not None:
            return self._transport(method, path, json=json)
        httpx = _import_httpx()

        url = urljoin(self._host_url + "/", path.lstrip("/"))
        with httpx.Client(timeout=60.0) as client:
            r = client.request(method, url, json=json)
        r.raise_for_status()
        if r.headers.get("content-type", "").startswith("application/json"):
            return r.json()
        return r.text

    def _request_bytes(self, method: str, path: str) -> bytes:
        if self._transport is not None:
            return self._transport(method, path, _binary=True)
        httpx = _import_httpx()

        url = urljoin(self._host_url + "/", path.lstrip("/"))
        with httpx.Client(timeout=120.0) as client:
            r = client.request(method, url)
        r.raise_for_status()
        return r.content

    def _put_bytes(self, path: str, data: bytes) -> Any:
        if self._transport is not None:
            return self._transport("PUT", path, _bytes=data)
        httpx = _import_httpx()

        url = urljoin(self._host_url + "/", path.lstrip("/"))
        with httpx.Client(timeout=120.0) as client:
            r = client.request("PUT", url, content=data)
        r.raise_for_status()
        return r.json() if r.headers.get("content-type", "").startswith(
            "application/json"
        ) else r.text

    # ── Executor protocol ──────────────────────────────────────────

    def submit(
        self,
        expr: "TaskExpression",
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        return self._pool.submit(
            self._run_remote, expr, resolved_args, resolved_kwargs
        )

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    # ── core: one task round-trip ──────────────────────────────────

    def _upload_blob(self, payload: bytes) -> str:
        h = hashlib.sha256(payload).hexdigest()
        try:
            self._put_bytes(f"/api/cluster/blobs/{h}", payload)
        except Exception:
            log.exception("blob upload failed for %s", h)
            raise
        return h

    def _ensure_project_uploaded(self) -> str:
        """Build + upload the project archive once per process. Returns hash."""
        if not self._ship_project:
            return ""
        with self._project_lock:
            if self._project_hash is not None:
                return self._project_hash
            from rinnsal.cluster.archive import build_project_archive

            try:
                data, h = build_project_archive(self._project_root)
            except Exception as e:
                log.warning(
                    "could not build project archive (%s); workers must "
                    "have the project pre-installed", e,
                )
                self._project_hash = ""
                return ""
            try:
                self._put_bytes(
                    f"/api/cluster/projects/{h}/archive", data
                )
            except Exception:
                log.exception("project archive upload failed")
                self._project_hash = ""
                return ""
            self._project_hash = h
            return h

    def _run_remote(
        self,
        expr: "TaskExpression",
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> ExecutionResult:
        try:
            project_hash = self._ensure_project_uploaded()
            func_bytes = cloudpickle.dumps(expr.func)
            args_bytes = cloudpickle.dumps(resolved_args)
            kwargs_bytes = cloudpickle.dumps(resolved_kwargs)
            func_hash = self._upload_blob(func_bytes)
            args_hash = self._upload_blob(args_bytes)
            kwargs_hash = self._upload_blob(kwargs_bytes)

            submit_body = self._request(
                "POST",
                "/api/cluster/jobs/submit",
                json={
                    "task_name": expr.task_name or "",
                    "task_hash": expr.hash or "",
                    "func_blob_hash": func_hash,
                    "args_blob_hash": args_hash,
                    "kwargs_blob_hash": kwargs_hash,
                    "project_hash": project_hash,
                    "capture": self._capture,
                    "resources": (
                        expr.task_def.resources.as_dict()
                        if expr.task_def.resources is not None
                        else {}
                    ),
                },
            )
            job_id = submit_body["job_id"]
        except Exception as e:
            return ExecutionResult(
                value=None, success=False, error=e
            )

        # Long-poll for terminal state.
        while True:
            status = self._request(
                "GET",
                f"/api/cluster/jobs/{job_id}/status?timeout={self._poll_timeout}",
            )
            if status["status"] in {"success", "failed", "revoked"}:
                break

        if status["status"] == "success":
            try:
                result_bytes = self._request_bytes(
                    "GET", f"/api/cluster/blobs/{status['result_blob_hash']}"
                )
                value = cloudpickle.loads(result_bytes)
            except Exception as e:
                return ExecutionResult(
                    value=None, success=False, error=e,
                    stdout=status.get("stdout", ""),
                    stderr=status.get("stderr", ""),
                )
            return ExecutionResult(
                value=value,
                success=True,
                stdout=status.get("stdout", ""),
                stderr=status.get("stderr", ""),
            )

        # failed / revoked: try to materialize the error from the blob.
        err: Exception = RuntimeError(
            f"job {job_id} ended with status={status['status']}"
        )
        if status.get("error_blob_hash"):
            try:
                err_bytes = self._request_bytes(
                    "GET",
                    f"/api/cluster/blobs/{status['error_blob_hash']}",
                )
                err = cloudpickle.loads(err_bytes)
            except Exception:
                pass
        return ExecutionResult(
            value=None,
            success=False,
            error=err,
            stdout=status.get("stdout", ""),
            stderr=status.get("stderr", ""),
        )
