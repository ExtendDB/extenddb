# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Launcher for the ExtendDB dev server.

Spawns the ``extenddb`` dev binary (zero-config: in-memory, loopback, seeded
dev credential) and returns an endpoint plus credentials that any AWS SDK
accepts unmodified::

    import extenddb_launcher

    with extenddb_launcher.start() as eb:
        client = boto3.client(
            "dynamodb",
            endpoint_url=eb.endpoint,
            region_name=eb.region,
            aws_access_key_id=eb.access_key_id,
            aws_secret_access_key=eb.secret_access_key,
        )

Binary resolution order: ``binary=`` argument, ``EXTENDDB_BINARY`` environment
variable, a bundled platform binary (wired up by the release pipeline), then
``PATH``.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request
from dataclasses import dataclass, field

# AWS's documented example credential: the dev server seeds it as the
# well-known default signing credential (dev-mode builds only).
_DEFAULT_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
_DEFAULT_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"


def _resolve_binary(explicit: str | None) -> str:
    candidates = [explicit, os.environ.get("EXTENDDB_BINARY")]
    bundled = os.path.join(os.path.dirname(__file__), "bin", "extenddb")
    candidates.append(bundled)
    for c in candidates:
        if c and os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    found = shutil.which("extenddb")
    if found:
        return found
    raise FileNotFoundError(
        "extenddb dev binary not found; set EXTENDDB_BINARY or install a "
        "platform package"
    )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class ExtendDb:
    """A running ExtendDB dev server."""

    endpoint: str
    port: int
    region: str = "us-east-1"
    access_key_id: str = _DEFAULT_ACCESS_KEY_ID
    secret_access_key: str = _DEFAULT_SECRET
    _process: subprocess.Popen = field(default=None, repr=False)

    def stop(self, timeout: float = 5.0) -> None:
        """Terminate the server, escalating to SIGKILL after ``timeout``."""
        if self._process is None or self._process.poll() is not None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()

    def __enter__(self) -> "ExtendDb":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()


def start(
    db_path: str | None = None,
    port: int | None = None,
    binary: str | None = None,
    startup_timeout: float = 15.0,
) -> ExtendDb:
    """Start an ExtendDB dev server and wait until it is healthy.

    ``db_path`` persists to a SQLite file; the default is in-memory
    (everything vanishes on :meth:`ExtendDb.stop`).
    """
    resolved = _resolve_binary(binary)
    chosen_port = port or _free_port()
    env = dict(os.environ, EXTENDDB__SERVER__PORT=str(chosen_port))
    if db_path:
        env["EXTENDDB__STORAGE__SQLITE__PATH"] = os.path.abspath(db_path)

    # Zero-config: run from a scratch cwd so a stray extenddb.toml in the
    # project cannot leak in.
    cwd = tempfile.mkdtemp(prefix="extenddb-")
    process = subprocess.Popen(
        [resolved, "serve", "--foreground"],
        env=env,
        cwd=cwd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    endpoint = f"http://127.0.0.1:{chosen_port}"
    deadline = time.monotonic() + startup_timeout
    while True:
        if process.poll() is not None:
            stderr = (process.stderr.read() or b"").decode(errors="replace")[-4000:]
            raise RuntimeError(
                f"extenddb exited during startup (code {process.returncode}): {stderr}"
            )
        try:
            with urllib.request.urlopen(f"{endpoint}/health", timeout=1) as resp:
                if resp.status == 200:
                    break
        except OSError:
            pass
        if time.monotonic() > deadline:
            process.terminate()
            raise TimeoutError(
                f"extenddb did not become healthy within {startup_timeout}s"
            )
        time.sleep(0.1)

    return ExtendDb(endpoint=endpoint, port=chosen_port, _process=process)
