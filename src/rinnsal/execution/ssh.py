"""SSH executor for remote task execution."""

from __future__ import annotations

import base64
import threading
import tempfile
from concurrent.futures import Future
from pathlib import Path
from typing import TYPE_CHECKING, Any

import cloudpickle

from rinnsal.execution.executor import ExecutionResult, Executor
from rinnsal.execution.provisioner import AutoProvisioner, Provisioner

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
        known_hosts: Any = None,
    ) -> None:
        self.hostname = hostname
        self.username = username
        self.port = port
        self.key_path = Path(key_path) if key_path else None
        self.python_path = python_path
        self.known_hosts = known_hosts

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
        provisioner: Provisioner | None = None,
        work_dir: str = "~/.rinnsal/worker",
    ) -> None:
        if not HAS_ASYNCSSH:
            raise ImportError(
                "asyncssh is required for SSHExecutor. "
                "Install with: pip install rinnsal[ssh]"
            )

        super().__init__(capture=capture, snapshot=snapshot)
        self._hosts = hosts
        self._max_connections = max_connections_per_host
        self._semaphore = threading.Semaphore(
            len(hosts) * max_connections_per_host
        )
        self._host_index = 0
        self._provisioner = provisioner if provisioner is not None else AutoProvisioner()
        self._work_dir = work_dir
        self._provision_lock = threading.Lock()
        self._provision_events: dict[str, threading.Event] = {}
        self._provision_errors: dict[str, Exception] = {}

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

        result_future: Future[ExecutionResult] = Future()

        def _run() -> None:
            self._semaphore.acquire()
            try:
                result = self._execute_on_host(host, encoded_payload)
                result_future.set_result(result)
            except Exception as e:
                result_future.set_result(
                    ExecutionResult(value=None, success=False, error=e)
                )
            finally:
                self._semaphore.release()

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        return result_future

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
        connect_kwargs["known_hosts"] = host.known_hosts

        async with asyncssh.connect(**connect_kwargs) as conn:
            # Provision the host if not already done; other threads wait
            needs_provision = False
            with self._provision_lock:
                if host.hostname not in self._provision_events:
                    self._provision_events[host.hostname] = threading.Event()
                    needs_provision = True
                event = self._provision_events[host.hostname]

            if needs_provision:
                try:
                    await self._provision_host(conn, host)
                except Exception as e:
                    self._provision_errors[host.hostname] = e
                    raise
                finally:
                    event.set()
            else:
                event.wait()

            if host.hostname in self._provision_errors:
                raise self._provision_errors[host.hostname]

            python_cmd = self._provisioner.python_command(self._work_dir)
            result = await conn.run(
                f"{python_cmd} -c '{remote_script}'",
                check=False,
            )

            if result.exit_status != 0:
                return ExecutionResult(
                    value=None,
                    stdout=result.stdout or "",
                    stderr=result.stderr or "",
                    success=False,
                    error=RuntimeError(
                        f"Remote execution failed: {result.stderr}"
                    ),
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
                        error=(
                            cloudpickle.loads(output["error"])
                            if output["error"]
                            else None
                        ),
                    )
            except Exception as e:
                return ExecutionResult(
                    value=None,
                    stdout=result.stdout or "",
                    stderr=result.stderr or "",
                    success=False,
                    error=e,
                )

    async def _provision_host(self, conn: Any, host: SSHHost) -> None:
        """Run provisioning script on a remote host."""
        import asyncssh

        # Ensure work dir exists
        await conn.run(f"mkdir -p {self._work_dir}", check=True)

        # Sync project source files to remote (git-tracked files only)
        project_dir = getattr(self._provisioner, "project_dir", None)
        if project_dir and Path(project_dir).is_dir():
            import asyncio
            import subprocess as _sp
            port = host.port or 22
            user_host = f"{host.username}@{host.hostname}" if host.username else host.hostname

            ssh_cmd = f"ssh -p {port} -o StrictHostKeyChecking=no"

            # Package tracked files (like Metaflow's code packaging):
            # git ls-files --recurse-submodules → tar → ssh → extract
            import sys
            print(f"[rinnsal] Syncing code to {host.hostname}...", file=sys.stderr, flush=True)
            try:
                result = _sp.run(
                    ["git", "ls-files", "--recurse-submodules", "-z"],
                    capture_output=True, cwd=str(project_dir),
                )
                if result.returncode != 0 or not result.stdout:
                    raise RuntimeError("git ls-files failed")

                # Create tar from the file list and pipe it to the remote
                files = result.stdout.replace(b"\0", b"\n").strip()
                proc = await asyncio.create_subprocess_shell(
                    f"tar -cf - -C {project_dir} --files-from=- | "
                    f"{ssh_cmd} {user_host} 'mkdir -p {self._work_dir} && tar -xf - -C {self._work_dir}'",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await proc.communicate(input=files)
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"Code transfer to {host.hostname} failed: {stderr.decode()}"
                    )
            except (FileNotFoundError, RuntimeError):
                # Fallback for non-git projects: tar with excludes
                proc = await asyncio.create_subprocess_shell(
                    f"tar -cf - -C {project_dir} "
                    f"--exclude='.pixi' --exclude='.git' --exclude='__pycache__' "
                    f"--exclude='.rinnsal' --exclude='runs' --exclude='.venv' "
                    f"--exclude='node_modules' --exclude='.cache' --exclude='out' "
                    f"--exclude='data' --exclude='*.pyc' --exclude='*.so' "
                    f". | {ssh_cmd} {user_host} 'mkdir -p {self._work_dir} && tar -xf - -C {self._work_dir}'",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await proc.communicate()
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"Code transfer to {host.hostname} failed: {stderr.decode()}"
                    )

        script = self._provisioner.provision_script(self._work_dir)
        import sys
        print(f"[rinnsal] Provisioning {host.hostname}...", file=sys.stderr, flush=True)
        result = await conn.run(
            f"bash <<'__RINNSAL_PROVISION__'\n{script}\n__RINNSAL_PROVISION__",
            check=False,
        )
        # Always print provision output so users can see build progress/errors
        if result.stdout and result.stdout.strip():
            print(f"[rinnsal] {host.hostname} provision stdout:\n{result.stdout}", file=sys.stderr, flush=True)
        if result.stderr and result.stderr.strip():
            print(f"[rinnsal] {host.hostname} provision stderr:\n{result.stderr}", file=sys.stderr, flush=True)
        if result.exit_status != 0:
            raise RuntimeError(
                f"Provisioning failed on {host.hostname} (exit {result.exit_status}):\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )
        print(f"[rinnsal] {host.hostname} provisioned successfully.", file=sys.stderr, flush=True)

    def shutdown(self, wait: bool = True) -> None:
        """No persistent pool to shut down."""

    def __repr__(self) -> str:
        hosts_str = ", ".join(str(h) for h in self._hosts)
        return f"SSHExecutor(hosts=[{hosts_str}])"
