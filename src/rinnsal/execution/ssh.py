"""SSH executor for remote task execution."""

from __future__ import annotations

import base64
import tempfile
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any

import cloudpickle

from rinnsal.execution.executor import ExecutionResult, Executor

if TYPE_CHECKING:
    from rinnsal.core.expression import TaskExpression

try:
    import asyncssh
    HAS_ASYNCSSH = True
except ImportError:
    HAS_ASYNCSSH = False


class SSHHost:
    """Configuration for an SSH host."""

    def __init__(
        self,
        hostname: str,
        username: str | None = None,
        port: int = 22,
        key_path: Path | str | None = None,
        python_path: str = "python3",
    ) -> None:
        self.hostname = hostname
        self.username = username
        self.port = port
        self.key_path = Path(key_path) if key_path else None
        self.python_path = python_path

    def __repr__(self) -> str:
        user_str = f"{self.username}@" if self.username else ""
        return f"SSHHost({user_str}{self.hostname}:{self.port})"


class SSHExecutor(Executor):
    """Executor that runs tasks on remote machines over SSH.

    Requires asyncssh to be installed: pip install asyncssh

    The executor:
    1. Serializes the function and arguments
    2. Transfers them to the remote host
    3. Executes the function remotely
    4. Retrieves the result
    """

    def __init__(
        self,
        hosts: list[SSHHost],
        capture: bool = True,
        snapshot: bool = False,
        max_connections_per_host: int = 4,
    ) -> None:
        if not HAS_ASYNCSSH:
            raise ImportError(
                "asyncssh is required for SSHExecutor. "
                "Install with: pip install rinnsal[ssh]"
            )

        super().__init__(capture=capture, snapshot=snapshot)
        self._hosts = hosts
        self._max_connections = max_connections_per_host
        self._thread_pool = ThreadPoolExecutor(
            max_workers=len(hosts) * max_connections_per_host
        )
        self._host_index = 0

    @property
    def hosts(self) -> list[SSHHost]:
        return list(self._hosts)

    def _get_next_host(self) -> SSHHost:
        """Round-robin host selection."""
        host = self._hosts[self._host_index]
        self._host_index = (self._host_index + 1) % len(self._hosts)
        return host

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for remote execution."""
        host = self._get_next_host()

        # Serialize function and arguments
        payload = {
            "func": cloudpickle.dumps(expr.func),
            "args": cloudpickle.dumps(resolved_args),
            "kwargs": cloudpickle.dumps(resolved_kwargs),
            "capture": self._capture,
        }

        serialized_payload = cloudpickle.dumps(payload)
        encoded_payload = base64.b64encode(serialized_payload).decode("ascii")

        # Submit to thread pool for async execution
        return self._thread_pool.submit(
            self._execute_on_host, host, encoded_payload
        )

    def _execute_on_host(
        self, host: SSHHost, encoded_payload: str
    ) -> ExecutionResult:
        """Execute a task on a remote host (synchronous wrapper)."""
        import asyncio

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    self._async_execute(host, encoded_payload)
                )
            finally:
                loop.close()
        except Exception as e:
            return ExecutionResult(
                value=None,
                success=False,
                error=e,
            )

    async def _async_execute(
        self, host: SSHHost, encoded_payload: str
    ) -> ExecutionResult:
        """Execute a task on a remote host asynchronously."""
        # Build remote Python script
        remote_script = f'''
import base64
import sys
import io
from contextlib import redirect_stdout, redirect_stderr

try:
    import cloudpickle
except ImportError:
    print("ERROR: cloudpickle not installed on remote", file=sys.stderr)
    sys.exit(1)

encoded_payload = """{encoded_payload}"""
serialized_payload = base64.b64decode(encoded_payload)
payload = cloudpickle.loads(serialized_payload)

func = cloudpickle.loads(payload["func"])
args = cloudpickle.loads(payload["args"])
kwargs = cloudpickle.loads(payload["kwargs"])
capture = payload["capture"]

stdout_capture = io.StringIO()
stderr_capture = io.StringIO()

try:
    if capture:
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = func(*args, **kwargs)
    else:
        result = func(*args, **kwargs)

    output = {{
        "success": True,
        "result": cloudpickle.dumps(result),
        "stdout": stdout_capture.getvalue(),
        "stderr": stderr_capture.getvalue(),
        "error": None,
    }}
except Exception as e:
    output = {{
        "success": False,
        "result": None,
        "stdout": stdout_capture.getvalue(),
        "stderr": stderr_capture.getvalue(),
        "error": cloudpickle.dumps(e),
    }}

# Output as base64-encoded cloudpickle
import base64
print(base64.b64encode(cloudpickle.dumps(output)).decode("ascii"))
'''

        # Connect to host
        connect_kwargs: dict[str, Any] = {
            "host": host.hostname,
            "port": host.port,
        }
        if host.username:
            connect_kwargs["username"] = host.username
        if host.key_path:
            connect_kwargs["client_keys"] = [str(host.key_path)]

        async with asyncssh.connect(**connect_kwargs) as conn:
            result = await conn.run(
                f"{host.python_path} -c '{remote_script}'",
                check=False,
            )

            if result.exit_status != 0:
                return ExecutionResult(
                    value=None,
                    stdout=result.stdout or "",
                    stderr=result.stderr or "",
                    success=False,
                    error=RuntimeError(f"Remote execution failed: {result.stderr}"),
                )

            # Parse output
            try:
                output_bytes = base64.b64decode(result.stdout.strip())
                output = cloudpickle.loads(output_bytes)

                if output["success"]:
                    return ExecutionResult(
                        value=cloudpickle.loads(output["result"]),
                        stdout=output["stdout"],
                        stderr=output["stderr"],
                        success=True,
                    )
                else:
                    return ExecutionResult(
                        value=None,
                        stdout=output["stdout"],
                        stderr=output["stderr"],
                        success=False,
                        error=cloudpickle.loads(output["error"]) if output["error"] else None,
                    )
            except Exception as e:
                return ExecutionResult(
                    value=None,
                    stdout=result.stdout or "",
                    stderr=result.stderr or "",
                    success=False,
                    error=e,
                )

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the executor."""
        self._thread_pool.shutdown(wait=wait)

    def __repr__(self) -> str:
        hosts_str = ", ".join(str(h) for h in self._hosts)
        return f"SSHExecutor(hosts=[{hosts_str}])"
