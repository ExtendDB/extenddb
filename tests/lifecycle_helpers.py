# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""CLI lifecycle tests for extenddb.

These tests exercise the full extenddb CLI lifecycle:
  extenddb init → extenddb serve → extenddb status → extenddb stop → extenddb destroy

They require:
  - A built extenddb binary (cargo build)
  - A running PostgreSQL instance
  - EXTENDDB_TEST_PG_CONNECTION_STRING env var pointing to a PostgreSQL server
    (e.g., "postgresql://postgres:postgres@localhost:5432")

Each test creates an isolated catalog database, config, and TLS certs to
avoid interfering with other tests or a running extenddb instance.

These tests are NOT run via the standard pytest suite (which tests the
DynamoDB API). They are a separate test category for CLI behavior.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid

import pytest

# The extenddb binary path — built by cargo
EXTENDDB_BINARY = os.environ.get(
    "EXTENDDB_BINARY",
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "target",
        "release",
        "extenddb",
    ),
)

# PostgreSQL connection string for creating test databases.
PG_CONN = os.environ.get("EXTENDDB_TEST_PG_CONNECTION_STRING", "")

# Admin connection for privileged ops (CREATE/DROP DATABASE, CREATE ROLE).
# PG_CONN is the unprivileged app role; falls back to it for local superuser setups.
PG_ADMIN_CONN = os.environ.get("EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING", "") or PG_CONN


def _parse_pg_credentials(conn):
    """Extract user and password from a connection string (postgresql://user:pass@host:port)."""
    if not conn:
        return None, None
    from urllib.parse import urlparse
    parsed = urlparse(conn)
    return parsed.username, parsed.password


PG_USER, PG_PASS = _parse_pg_credentials(PG_CONN)
PG_ADMIN_USER, PG_ADMIN_PASS = _parse_pg_credentials(PG_ADMIN_CONN)


def _fail_if_no_pg():
    """Fail test if PostgreSQL is not available."""
    if not PG_CONN:
        pytest.fail(
            "MISCONFIGURED: EXTENDDB_TEST_PG_CONNECTION_STRING not set. "
            "CLI lifecycle tests require a PostgreSQL connection string."
        )


def _fail_if_no_binary():
    """Fail test if extenddb binary is not built."""
    if not os.path.isfile(EXTENDDB_BINARY):
        pytest.fail(
            f"MISCONFIGURED: extenddb binary not found at {EXTENDDB_BINARY}. "
            "Build with: cargo build --release"
        )


def _pg_args():
    """Return --pg-user and --pg-pass args for commands that connect as the
    PostgreSQL admin (catalog database and application-role creation)."""
    args = []
    if PG_ADMIN_USER:
        args.extend(["--pg-user", PG_ADMIN_USER])
    if PG_ADMIN_PASS:
        args.extend(["--pg-pass", PG_ADMIN_PASS])
    return args


def _patch_config_port(config_path, port):
    """Patch the generated config to use a specific port.

    Sets the HTTPS port whether the generated config ships it commented (the
    default) or active, and without assuming a particular default value (the
    default has changed across releases, e.g. 8000 -> 18443).
    """
    with open(config_path) as f:
        content = f.read()
    # Replace the first server `port = N` line (commented or not) with our port.
    new_content, n = re.subn(
        r"(?m)^[ \t]*#?[ \t]*port[ \t]*=[ \t]*\d+.*$",
        f"port = {port}",
        content,
        count=1,
    )
    if n == 0:
        # No port line at all — add one right after bind_addr.
        new_content = content.replace(
            'bind_addr = "127.0.0.1"',
            f'bind_addr = "127.0.0.1"\nport = {port}',
            1,
        )
    with open(config_path, "w") as f:
        f.write(new_content)


def _init_args(cli_env):
    """Return CLI args for extenddb init including all connection details."""
    args = list(_pg_args())
    args.extend(["--pg-host", cli_env["pg_host"]])
    args.extend(["--pg-port", cli_env["pg_port"]])
    args.extend(["--catalog-db", cli_env["db_name"]])
    if PG_USER:
        args.extend(["--extenddb-user", PG_USER])
    if PG_PASS:
        args.extend(["--extenddb-pass", PG_PASS])
    return args


def _run_extenddb(*args, config=None, timeout=30, check=True, env_override=None):
    """Run a extenddb CLI command and return the CompletedProcess.

    Never passes stdin — extenddb commands must be fully non-interactive.
    Use env_override to pass EXTENDDB_ADMIN_PASSWORD etc.
    """
    cmd = [EXTENDDB_BINARY]
    cmd.extend(args)
    if config:
        cmd.extend(["--config", config])
    env = None
    if env_override:
        env = os.environ.copy()
        env.update(env_override)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
        stdin=subprocess.DEVNULL,
        env=env,
    )


@pytest.fixture()
def cli_env(tmp_path):
    """Create an isolated environment for CLI lifecycle testing.

    Yields a dict with:
      - db_name: unique PostgreSQL database name (catalog)
      - config_path: path to the generated extenddb.toml
      - run_dir: directory for PID files
      - port: unique port for this test instance
      - binary: path to the extenddb binary
      - tls_dir: directory containing TLS certs (from ~/.extenddb/tls)
    """
    _fail_if_no_pg()
    _fail_if_no_binary()

    db_name = f"extenddb_test_{uuid.uuid4().hex[:8]}_catalog"
    port = _find_free_port()
    run_dir = str(tmp_path / "run")
    os.makedirs(run_dir, exist_ok=True)

    config_path = str(tmp_path / "extenddb.toml")

    # Parse PG connection details from PG_CONN
    from urllib.parse import urlparse
    parsed = urlparse(PG_CONN)
    pg_host = parsed.hostname or "localhost"
    pg_port = str(parsed.port or 5432)

    env = {
        "db_name": db_name,
        "config_path": config_path,
        "run_dir": run_dir,
        "tls_dir": os.path.expanduser("~/.extenddb/tls"),
        "port": port,
        "binary": EXTENDDB_BINARY,
        "conn_string": f"{PG_CONN}/{db_name}",
        "pg_host": pg_host,
        "pg_port": pg_port,
    }

    yield env

    # Cleanup: stop server if running, destroy, drop database
    try:
        _run_extenddb("stop", config=config_path, check=False, timeout=10)
    except Exception:
        pass
    try:
        _run_extenddb("destroy", "--yes", *_pg_args(), config=config_path, check=False, timeout=10)
    except Exception:
        pass
    _drop_database(db_name)
    # Also drop the data database (catalog name minus _catalog suffix)
    if db_name.endswith("_catalog"):
        _drop_database(db_name[:-len("_catalog")])


def _find_free_port():
    """Find a free TCP port."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _drop_database(db_name):
    """Drop a PostgreSQL test database."""
    import psycopg2

    try:
        conn = psycopg2.connect(PG_ADMIN_CONN + "/postgres")
        conn.autocommit = True
        with conn.cursor() as cur:
            # Terminate existing connections
            cur.execute(
                f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{db_name}' AND pid <> pg_backend_pid()
                """
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        conn.close()
    except Exception:
        pass  # Best-effort cleanup


def _wait_for_server(port, timeout=15):
    """Wait for the server to become healthy."""
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    import urllib.request
    import ssl

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(f"https://127.0.0.1:{port}/health")
            urllib.request.urlopen(req, context=ctx, timeout=2)
            return True
        except Exception:
            time.sleep(0.5)
    return False


