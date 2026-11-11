# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Access key import on a SQLite deployment with no ``allow_credential_import`` row.

``devtools/run-tests`` stores ``allow_credential_import = true`` on the shared
server before any suite starts, so no other integration test reaches the
import route on a catalog without the row. This file runs
``init --backend sqlite`` in a temporary directory, serves that deployment on a
free port on 127.0.0.1, and drives the route through the management API:

  * with no row, the import answers 403 and the key does not list;
  * after ``extenddb settings`` stores ``true`` while the server runs, the same
    import answers 201 and the key lists;
  * after the setting changes to ``false``, a second import answers 403 and the
    first key still lists.

The deployment has its own catalog, so the test does not change the shared
server's settings while other xdist workers import keys. The module skips
unless the ``extenddb`` binary is a SQLite build.
"""

from __future__ import annotations

import os
import re
import secrets
import shutil
import socket
import sqlite3
import ssl
import subprocess
import tempfile
import time
import urllib.request
import uuid

import pytest

from management_helpers import ManagementClient

EXTENDDB_BINARY = os.environ.get(
    "EXTENDDB_BINARY",
    os.path.join(os.path.dirname(__file__), "..", "target", "release", "extenddb"),
)

REFUSED = "Credential import is disabled (allow_credential_import = false)"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_health(port: int, timeout: float = 20.0) -> bool:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"https://127.0.0.1:{port}/health", context=ctx, timeout=2
            ) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def _patch_port(config_path: str, port: int) -> None:
    with open(config_path) as f:
        content = f.read()
    new, n = re.subn(
        r"(?m)^[ \t]*#?[ \t]*port[ \t]*=[ \t]*\d+.*$", f"port = {port}", content, count=1
    )
    if n == 0:
        new = content.replace(
            'bind_addr = "127.0.0.1"', f'bind_addr = "127.0.0.1"\nport = {port}', 1
        )
    with open(config_path, "w") as f:
        f.write(new)


class _Deployment:
    """A file-backed SQLite deployment in its own directory, served on 127.0.0.1."""

    def __init__(self, root: str) -> None:
        self.dir = root
        self.config = os.path.join(root, "extenddb.toml")
        self.log_path = os.path.join(root, "serve.log")
        self.port = _free_port()
        self.admin_password = secrets.token_urlsafe(24)
        # `init` writes the TLS certificate and key under ~/.extenddb. A home
        # directory inside the deployment keeps them out of the user's home.
        self.env = {**os.environ, "HOME": os.path.join(root, "home")}
        self.proc: subprocess.Popen | None = None

    def run(self, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
        # `--config` goes right after the subcommand: `settings` requires it
        # before its `set` action.
        return subprocess.run(
            [EXTENDDB_BINARY, args[0], "--config", self.config, *args[1:]],
            cwd=self.dir,
            env=env or self.env,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=60,
            check=False,
        )

    def init(self) -> None:
        # The admin password comes from the environment, so `init` does not
        # print it.
        res = self.run(
            "init",
            "--backend",
            "sqlite",
            "--bind-addr",
            "127.0.0.1",
            env={
                **self.env,
                "EXTENDDB_ADMIN_USER": "admin",
                "EXTENDDB_ADMIN_PASSWORD": self.admin_password,
            },
        )
        assert res.returncode == 0, f"init failed: {res.stderr}"
        _patch_port(self.config, self.port)

    def start(self) -> None:
        with open(self.log_path, "wb") as log:
            self.proc = subprocess.Popen(
                [EXTENDDB_BINARY, "serve", "--config", self.config, "--foreground"],
                cwd=self.dir,
                env=self.env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        if not _wait_for_health(self.port):
            with open(self.log_path, errors="replace") as f:
                tail = f.read()[-2000:]
            pytest.fail(f"server did not become healthy on 127.0.0.1:{self.port}:\n{tail}")

    def stop(self) -> None:
        if self.proc is None:
            return
        # `serve --foreground` shuts down gracefully on SIGTERM.
        self.proc.terminate()
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.proc = None

    def stored_setting(self, key: str) -> str | None:
        # `init --backend sqlite` creates the catalog in the working directory.
        conn = sqlite3.connect(os.path.join(self.dir, "extenddb.sqlite"), timeout=10)
        try:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        finally:
            conn.close()
        return None if row is None else row[0]

    def set_setting(self, key: str, value: str) -> None:
        res = self.run("settings", "set", key, value)
        assert res.returncode == 0, f"settings set {key} {value} failed: {res.stderr}"


@pytest.fixture()
def deployment(request) -> _Deployment:
    """A started deployment; stopped and deleted on teardown."""
    if not os.path.isfile(EXTENDDB_BINARY):
        pytest.skip(f"extenddb binary not found at {EXTENDDB_BINARY}")
    version = subprocess.run(
        [EXTENDDB_BINARY, "version"],
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=30,
        check=False,
    )
    # `version` prints `catalog <label> (<backend>)`.
    if "(sqlite)" not in version.stdout:
        pytest.skip("extenddb binary was not built with the SQLite backend")

    root = tempfile.mkdtemp(prefix="extenddb-import-")
    d = _Deployment(root)

    def teardown() -> None:
        try:
            d.stop()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    # Registered before `init`, so a failed init or start is cleaned up too.
    request.addfinalizer(teardown)
    d.init()
    d.start()
    return d


def _new_key() -> tuple[str, str]:
    return "AKIA" + uuid.uuid4().hex[:16].upper(), uuid.uuid4().hex + uuid.uuid4().hex[:8]


def _key_ids(mgmt: ManagementClient, account_id: str, user: str) -> list[str]:
    resp = mgmt.list_access_keys(account_id, user)
    assert resp.status_code == 200, resp.text
    return [k["access_key_id"] for k in resp.json()]


class TestAllowCredentialImport:
    """The import route with no row, then with `true`, then with `false`."""

    def test_import_follows_the_setting(self, deployment):
        assert deployment.stored_setting("allow_credential_import") is None

        mgmt = ManagementClient(
            f"https://127.0.0.1:{deployment.port}", "admin", deployment.admin_password
        )
        account_id = f"{uuid.uuid4().int % 10**12:012d}"
        resp = mgmt.create_account(account_id, f"import-{account_id}")
        assert resp.status_code == 201, resp.text
        user = f"importer-{uuid.uuid4().hex[:8]}"
        resp = mgmt.create_user(account_id, user)
        assert resp.status_code == 201, resp.text

        # No row: the import is refused before the key is stored.
        key_id, secret = _new_key()
        resp = mgmt.import_access_key(account_id, user, key_id, secret)
        assert (resp.status_code, resp.text) == (403, REFUSED)
        assert key_id not in _key_ids(mgmt, account_id, user)

        # `true`, stored while the server runs: the same import goes through.
        deployment.set_setting("allow_credential_import", "true")
        resp = mgmt.import_access_key(account_id, user, key_id, secret)
        assert resp.status_code == 201, resp.text
        assert key_id in _key_ids(mgmt, account_id, user)

        # `false`: a second import is refused, and the first key still lists.
        deployment.set_setting("allow_credential_import", "false")
        second_id, second_secret = _new_key()
        resp = mgmt.import_access_key(account_id, user, second_id, second_secret)
        assert (resp.status_code, resp.text) == (403, REFUSED)
        key_ids = _key_ids(mgmt, account_id, user)
        assert key_id in key_ids
        assert second_id not in key_ids
