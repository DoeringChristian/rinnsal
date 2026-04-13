"""SSH executor for remote task execution."""

from __future__ import annotations

import base64
import threading
import tempfile
from concurrent.futures import Future
from pathlib import Path
from typing import TYPE_CHECKING, Any

import cloudpickle

from rinnsal.compute.executor import ExecutionResult, Executor
from rinnsal.compute.provisioner import AutoProvisioner, Provisioner

if TYPE_CHECKING:
    from rinnsal.modeling.expression import TaskExpression

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
            "task_name": expr.task_name or "",
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
        """Execute a task on a remote host asynchronously.

        Logger events are streamed back in real-time over stderr using a
        magic-prefixed binary protocol (see ``rinnsal.data.logger.proxy``).
        The task result is returned as base64-encoded cloudpickle on stdout.
        """
        import asyncio
        import struct

        from rinnsal.data.logger.proxy import STREAM_MAGIC

        # Build remote Python script.
        # The script sets up a LoggerProxy that streams events to
        # sys.stderr.buffer with the RNNSL magic prefix.  User-visible
        # stderr is captured via redirect_stderr into a StringIO and
        # included in the result dict (same as before).
        remote_script = f'''
import base64, sys, io, struct
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
task_name = payload.get("task_name", "")

# Set up logger proxy — streams events to real stderr in real-time
from rinnsal.data.logger.proxy import LoggerProxy
from rinnsal.context import current

proxy = LoggerProxy(stream=sys.stderr.buffer)
current._set_logger(proxy)
if task_name:
    current._set_task_name(task_name)

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
finally:
    current._reset_logger()

# Flush proxy, then print result to stdout
proxy.flush()
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

            # Use create_process for streaming stderr access
            async with conn.create_process(
                f"{python_cmd} -c '{remote_script}'",
            ) as proc:
                # Read stderr in a background task, extracting event
                # records (magic-prefixed) and relaying to local logger.
                plain_stderr_parts: list[str] = []

                async def _read_stderr() -> None:
                    magic_len = len(STREAM_MAGIC)
                    buf = b""
                    while True:
                        chunk = await proc.stderr.read(4096)
                        if not chunk:
                            break
                        if isinstance(chunk, str):
                            chunk = chunk.encode("latin-1")
                        buf += chunk
                        # Process complete records from buf
                        while True:
                            idx = buf.find(STREAM_MAGIC)
                            if idx < 0:
                                # No magic found — everything up to
                                # the last (magic_len - 1) bytes is
                                # plain text (keep the tail as potential
                                # partial magic).
                                safe = len(buf) - (magic_len - 1)
                                if safe > 0:
                                    plain_stderr_parts.append(
                                        buf[:safe].decode("utf-8", errors="replace")
                                    )
                                    buf = buf[safe:]
                                break
                            # Flush any plain text before the magic
                            if idx > 0:
                                plain_stderr_parts.append(
                                    buf[:idx].decode("utf-8", errors="replace")
                                )
                            buf = buf[idx + magic_len:]
                            # Need 4 bytes for length
                            if len(buf) < 4:
                                # Wait for more data
                                more = await proc.stderr.read(4 - len(buf))
                                if not more:
                                    break
                                if isinstance(more, str):
                                    more = more.encode("latin-1")
                                buf += more
                            if len(buf) < 4:
                                break
                            length = struct.unpack("<I", buf[:4])[0]
                            buf = buf[4:]
                            # Read full event data
                            while len(buf) < length:
                                more = await proc.stderr.read(length - len(buf))
                                if not more:
                                    break
                                if isinstance(more, str):
                                    more = more.encode("latin-1")
                                buf += more
                            if len(buf) < length:
                                break
                            # Parse and relay the event
                            if self._logger is not None:
                                from rinnsal.data.logger.events_pb2 import Event
                                ev = Event()
                                ev.ParseFromString(buf[:length])
                                self._logger._event_writer.write(ev)
                                self._logger._event_writer.flush()
                            buf = buf[length:]
                    # Remaining buf is plain text
                    if buf:
                        plain_stderr_parts.append(
                            buf.decode("utf-8", errors="replace")
                        )

                stderr_task = asyncio.ensure_future(_read_stderr())

                # Read stdout (result)
                stdout_data = await proc.stdout.read()
                if isinstance(stdout_data, bytes):
                    stdout_data = stdout_data.decode("utf-8", errors="replace")

                await stderr_task
                await proc.wait()

            exit_status = proc.exit_status
            plain_stderr = "".join(plain_stderr_parts)

            if exit_status != 0:
                return ExecutionResult(
                    value=None,
                    stdout=stdout_data or "",
                    stderr=plain_stderr,
                    success=False,
                    error=RuntimeError(
                        f"Remote execution failed (exit {exit_status}): "
                        f"{plain_stderr}"
                    ),
                )

            # Parse result from stdout
            try:
                output_bytes = base64.b64decode(stdout_data.strip())
                output = cloudpickle.loads(output_bytes)

                if output["success"]:
                    return ExecutionResult(
                        value=cloudpickle.loads(output["result"]),
                        stdout=output["stdout"],
                        stderr=output["stderr"] + plain_stderr,
                        success=True,
                    )
                else:
                    return ExecutionResult(
                        value=None,
                        stdout=output["stdout"],
                        stderr=output["stderr"] + plain_stderr,
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
                    stdout=stdout_data or "",
                    stderr=plain_stderr,
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


# ─── Persistent SSH Executor ─────────────────────────────────────


def _make_persistent_worker_script(
    job_dir: str,
    checkpoint_path: str | None = None,
    task_name: str = "",
) -> str:
    """Generate a self-contained worker script for persistent execution."""
    checkpoint_block = ""
    if checkpoint_path:
        checkpoint_block = f"""
import os
from pathlib import Path as _Path
from rinnsal.context import Checkpoint
current._set_checkpoint(Checkpoint(path=_Path("{checkpoint_path}")))
"""

    task_name_block = ""
    if task_name:
        escaped = task_name.replace('"', '\\"')
        task_name_block = f'current._set_task_name("{escaped}")\n'

    return f'''#!/usr/bin/env python3
"""Auto-generated rinnsal persistent SSH worker."""
import os, sys, traceback
from pathlib import Path

JOB_DIR = Path("{job_dir}")
SUBMISSION = JOB_DIR / "submission.pkl"
RESULT = JOB_DIR / "result.pkl"
EVENTS = JOB_DIR / "events.pb"
DONE = JOB_DIR / ".done"

# Write PID
(JOB_DIR / "pid").write_text(str(os.getpid()))

import cloudpickle
from rinnsal.data.logger.proxy import LoggerProxy
from rinnsal.context import current

# Logger writes directly to events.pb (flushed per event)
proxy = LoggerProxy(event_file=str(EVENTS))
current._set_logger(proxy)
{task_name_block}
{checkpoint_block}

# Load submission
with open(SUBMISSION, "rb") as _f:
    func, args, kwargs = cloudpickle.load(_f)

# Redirect stdout/stderr to log files
_stdout_log = open(JOB_DIR / "stdout.log", "w")
_stderr_log = open(JOB_DIR / "stderr.log", "w")

try:
    from contextlib import redirect_stdout, redirect_stderr
    with redirect_stdout(_stdout_log), redirect_stderr(_stderr_log):
        result = func(*args, **kwargs)
    with open(RESULT, "wb") as _f:
        cloudpickle.dump(("success", result, None), _f)
except Exception as e:
    tb = traceback.format_exc()
    with open(RESULT, "wb") as _f:
        cloudpickle.dump(("error", e, tb), _f)
finally:
    current._reset_logger()
    proxy.flush()
    _stdout_log.close()
    _stderr_log.close()

# Signal completion
DONE.touch()
'''


class PersistentSSHExecutor(SSHExecutor):
    """SSH executor that detaches tasks so they survive disconnects.

    Tasks are submitted via SSH, then run as background processes on
    the remote machine.  The orchestrator polls for completion and
    syncs logger events incrementally.  If the SSH connection drops
    or the local machine restarts, remote tasks keep running and
    results can be collected on reconnect.

    Usage::

        --executor pssh:user@host1,host2
    """

    def __init__(
        self,
        hosts: list[SSHHost],
        capture: bool = True,
        snapshot: bool = False,
        max_connections_per_host: int = 4,
        provisioner: Provisioner | None = None,
        work_dir: str = "~/.rinnsal/worker",
        poll_interval: float = 5.0,
        max_poll_interval: float = 30.0,
    ) -> None:
        super().__init__(
            hosts=hosts,
            capture=capture,
            snapshot=snapshot,
            max_connections_per_host=max_connections_per_host,
            provisioner=provisioner,
            work_dir=work_dir,
        )
        self._poll_interval = poll_interval
        self._max_poll_interval = max_poll_interval
        self._active_jobs: list[tuple[SSHHost, str]] = []

    def submit(
        self,
        expr: TaskExpression,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
    ) -> Future[ExecutionResult]:
        """Submit a task for persistent remote execution."""
        host = self._get_next_host()

        # Serialize function and arguments
        payload = cloudpickle.dumps(
            (expr.func, resolved_args, resolved_kwargs)
        )

        result_future: Future[ExecutionResult] = Future()

        def _run() -> None:
            self._semaphore.acquire()
            try:
                import asyncio

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    job_id = loop.run_until_complete(
                        self._submit_persistent(host, payload)
                    )
                finally:
                    loop.close()

                self._active_jobs.append((host, job_id))

                # Start polling in this thread
                self._poll_job(
                    result_future, host, job_id,
                )
            except Exception as e:
                result_future.set_result(
                    ExecutionResult(value=None, success=False, error=e)
                )
            finally:
                self._semaphore.release()

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        return result_future

    def _connect_kwargs(self, host: SSHHost) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "host": host.hostname,
            "port": host.port,
        }
        if host.username:
            kwargs["username"] = host.username
        if host.key_path:
            kwargs["client_keys"] = [str(host.key_path)]
        kwargs["known_hosts"] = host.known_hosts
        return kwargs

    async def _submit_persistent(
        self, host: SSHHost, payload: bytes,
    ) -> str:
        """SSH to host, write files, launch detached worker. Returns job_id."""
        import uuid

        job_id = uuid.uuid4().hex[:12]
        job_dir = f"{self._work_dir}/jobs/{job_id}"

        connect_kwargs = self._connect_kwargs(host)

        async with asyncssh.connect(**connect_kwargs) as conn:
            # Provision if needed (reuses parent's caching logic)
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

            # Create job directory and write files via SFTP
            async with conn.start_sftp_client() as sftp:
                await sftp.makedirs(job_dir)

                async with sftp.open(
                    f"{job_dir}/submission.pkl", "wb"
                ) as f:
                    await f.write(payload)

                worker_script = _make_persistent_worker_script(
                    job_dir=job_dir,
                    checkpoint_path=self._checkpoint_path,
                    task_name=expr.task_name or "",
                )
                async with sftp.open(f"{job_dir}/worker.py", "w") as f:
                    await f.write(worker_script)

            # Launch detached
            python_cmd = self._provisioner.python_command(self._work_dir)
            launch_cmd = (
                f"setsid nohup {python_cmd} {job_dir}/worker.py "
                f"</dev/null >{job_dir}/launch.log 2>&1 & echo $!"
            )
            result = await conn.run(launch_cmd, check=False)
            if result.exit_status != 0:
                raise RuntimeError(
                    f"Failed to launch worker on {host.hostname}: "
                    f"{result.stderr}"
                )

            import sys

            pid = result.stdout.strip()
            print(
                f"[rinnsal] Launched job {job_id} on "
                f"{host.hostname} (PID {pid})",
                file=sys.stderr,
                flush=True,
            )

        return job_id

    def _poll_job(
        self,
        future: Future[ExecutionResult],
        host: SSHHost,
        job_id: str,
    ) -> None:
        """Poll for job completion, syncing logger events incrementally."""
        import asyncio
        import sys
        import time

        from rinnsal.data.logger.proxy import replay_events

        job_dir = f"{self._work_dir}/jobs/{job_id}"
        connect_kwargs = self._connect_kwargs(host)
        delay = self._poll_interval
        events_offset = 0

        while True:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    done, events_offset = loop.run_until_complete(
                        self._poll_once(
                            connect_kwargs, job_dir, events_offset,
                        )
                    )
                finally:
                    loop.close()

                if done:
                    break
            except Exception as e:
                # Connection failed — retry
                print(
                    f"[rinnsal] Poll failed for {job_id} on "
                    f"{host.hostname}: {e}",
                    file=sys.stderr,
                    flush=True,
                )

            time.sleep(delay)
            delay = min(delay * 1.5, self._max_poll_interval)

        # Fetch result
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    self._fetch_result(connect_kwargs, job_dir)
                )
            finally:
                loop.close()
            future.set_result(result)
        except Exception as e:
            future.set_result(
                ExecutionResult(value=None, success=False, error=e)
            )

    async def _poll_once(
        self,
        connect_kwargs: dict[str, Any],
        job_dir: str,
        events_offset: int,
    ) -> tuple[bool, int]:
        """Single poll iteration. Returns (is_done, new_events_offset)."""
        from rinnsal.data.logger.proxy import replay_events

        async with asyncssh.connect(**connect_kwargs) as conn:
            # Sync new logger events incrementally
            if self._logger is not None and events_offset >= 0:
                try:
                    ev_result = await conn.run(
                        f"tail -c +{events_offset + 1} "
                        f"{job_dir}/events.pb 2>/dev/null || true",
                        check=False,
                        encoding=None,
                    )
                    raw = ev_result.stdout
                    if raw:
                        if isinstance(raw, str):
                            raw = raw.encode("latin-1")
                        if len(raw) > 0:
                            replay_events(self._logger, raw)
                            events_offset += len(raw)
                except Exception:
                    pass

            # Check completion
            result = await conn.run(
                f"test -f {job_dir}/.done && echo DONE || "
                f"(test -f {job_dir}/pid && "
                f"kill -0 $(cat {job_dir}/pid) 2>/dev/null && "
                f"echo RUNNING || echo DEAD)",
                check=False,
            )
            status = (result.stdout or "").strip()

            if status == "DONE":
                return True, events_offset
            if status == "DEAD":
                # Process died without writing .done
                return True, events_offset

            return False, events_offset

    async def _fetch_result(
        self,
        connect_kwargs: dict[str, Any],
        job_dir: str,
    ) -> ExecutionResult:
        """Fetch result.pkl, stdout.log, stderr.log from remote."""
        async with asyncssh.connect(**connect_kwargs) as conn:
            async with conn.start_sftp_client() as sftp:
                # Read stdout/stderr logs
                stdout = ""
                stderr = ""
                try:
                    async with sftp.open(
                        f"{job_dir}/stdout.log", "r"
                    ) as f:
                        stdout = await f.read()
                except Exception:
                    pass
                try:
                    async with sftp.open(
                        f"{job_dir}/stderr.log", "r"
                    ) as f:
                        stderr = await f.read()
                except Exception:
                    pass

                # Read result.pkl
                try:
                    async with sftp.open(
                        f"{job_dir}/result.pkl", "rb"
                    ) as f:
                        result_bytes = await f.read()
                except Exception as e:
                    return ExecutionResult(
                        value=None,
                        stdout=stdout,
                        stderr=stderr,
                        success=False,
                        error=RuntimeError(
                            f"Failed to read result.pkl: {e}"
                        ),
                    )

            outcome = cloudpickle.loads(result_bytes)

            # Tuple: ("success"|"error", result|error, None|tb)
            if outcome[0] == "success":
                return ExecutionResult(
                    value=outcome[1],
                    stdout=stdout,
                    stderr=stderr,
                    success=True,
                )
            else:
                return ExecutionResult(
                    value=None,
                    stdout=stdout,
                    stderr=stderr + (outcome[2] or ""),
                    success=False,
                    error=outcome[1],
                )

    def shutdown(self, wait: bool = True) -> None:
        """Cancel active remote jobs if not waiting."""
        if not wait:
            import asyncio

            for host, job_id in self._active_jobs:
                try:
                    job_dir = f"{self._work_dir}/jobs/{job_id}"
                    connect_kwargs = self._connect_kwargs(host)
                    loop = asyncio.new_event_loop()
                    try:
                        loop.run_until_complete(
                            self._kill_remote_job(connect_kwargs, job_dir)
                        )
                    finally:
                        loop.close()
                except Exception:
                    pass
        self._active_jobs.clear()

    @staticmethod
    async def _kill_remote_job(
        connect_kwargs: dict[str, Any], job_dir: str,
    ) -> None:
        async with asyncssh.connect(**connect_kwargs) as conn:
            await conn.run(
                f"test -f {job_dir}/pid && "
                f"kill $(cat {job_dir}/pid) 2>/dev/null; "
                f"touch {job_dir}/.done",
                check=False,
            )

    def __repr__(self) -> str:
        hosts_str = ", ".join(str(h) for h in self._hosts)
        return f"PersistentSSHExecutor(hosts=[{hosts_str}])"
