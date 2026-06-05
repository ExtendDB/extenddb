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
    """Patch the generated config to use a specific port."""
    with open(config_path) as f:
        content = f.read()
    # Replace commented port line or add port after bind_addr
    if "# port = 8000" in content:
        content = content.replace("# port = 8000", f"port = {port}")
    elif "port = " not in content:
        content = content.replace(
            'bind_addr = "127.0.0.1"',
            f'bind_addr = "127.0.0.1"\nport = {port}',
        )
    with open(config_path, "w") as f:
        f.write(content)


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


class TestCliLifecycle:
    """Test the full extenddb CLI lifecycle."""

    def test_version(self):
        """extenddb version prints version info."""
        _fail_if_no_binary()
        result = _run_extenddb("version")
        assert "extenddb" in result.stdout.lower()

    def test_init_creates_schema(self, cli_env):
        """extenddb init creates the catalog schema and TLS certs."""
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0

        # TLS certs should exist
        assert os.path.isfile(os.path.join(cli_env["tls_dir"], "cert.pem"))
        assert os.path.isfile(os.path.join(cli_env["tls_dir"], "key.pem"))

    def test_init_serve_status_stop(self, cli_env):
        """Full lifecycle: init → serve → status → stop."""
        # Init
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0
        _patch_config_port(cli_env["config_path"], cli_env["port"])

        # Serve (daemonizes)
        result = _run_extenddb("serve", config=cli_env["config_path"])
        assert result.returncode == 0

        # Wait for server to be healthy
        assert _wait_for_server(cli_env["port"]), "Server did not become healthy"

        # Status should report running
        result = _run_extenddb("status", config=cli_env["config_path"])
        assert result.returncode == 0
        assert "running" in result.stdout.lower() or "pid" in result.stdout.lower()

        # Stop
        result = _run_extenddb("stop", config=cli_env["config_path"])
        assert result.returncode == 0

        # Status should report not running
        time.sleep(1)
        result = _run_extenddb("status", config=cli_env["config_path"], check=False)
        # Status returns non-zero when not running
        assert result.returncode != 0 or "not running" in result.stdout.lower()

    def test_init_serve_stop_destroy(self, cli_env):
        """Full lifecycle including destroy."""
        # Init
        _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        _patch_config_port(cli_env["config_path"], cli_env["port"])

        # Serve
        _run_extenddb("serve", config=cli_env["config_path"])
        assert _wait_for_server(cli_env["port"])

        # Stop
        _run_extenddb("stop", config=cli_env["config_path"])
        time.sleep(1)

        # Destroy
        result = _run_extenddb("destroy", "--yes", *_pg_args(), config=cli_env["config_path"])
        assert result.returncode == 0

    def test_serve_without_init_fails(self, cli_env):
        """extenddb serve without init should fail."""
        result = _run_extenddb("serve", config=cli_env["config_path"], check=False)
        assert result.returncode != 0

    def test_destroy_without_yes_fails(self, cli_env):
        """extenddb destroy without --yes should fail."""
        _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        result = _run_extenddb("destroy", config=cli_env["config_path"], check=False)
        assert result.returncode != 0

    def test_double_init_fails(self, cli_env):
        """extenddb init on an already-initialized database should fail."""
        _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            check=False,
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        # Second init should fail (catalog already exists)
        assert result.returncode != 0

    def test_stop_when_not_running(self, cli_env):
        """extenddb stop when no server is running should handle gracefully."""
        _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        result = _run_extenddb("stop", config=cli_env["config_path"], check=False)
        # Should exit non-zero or report not running
        # (exact behavior depends on implementation)
        assert result.returncode != 0 or "not running" in result.stdout.lower()

    def test_init_with_unix_socket(self, cli_env):
        """extenddb init with Unix socket path generates valid connection string and daemon starts."""
        import platform
        import psycopg2

        # Only run on Linux or macOS
        if platform.system() not in ("Linux", "Darwin"):
            pytest.skip(f"Unix socket test only runs on Linux/macOS, not {platform.system()}")

        # Try to find PostgreSQL's Unix socket by connecting without host
        # (psycopg2 defaults to Unix socket on Linux/macOS)
        socket_path = None
        try:
            # Connect using Unix socket to discover the socket directory
            conn = psycopg2.connect(
                dbname="postgres",
                user=PG_ADMIN_USER,
                password=PG_ADMIN_PASS,
            )
            # Query the socket directory from PostgreSQL
            with conn.cursor() as cur:
                cur.execute("SHOW unix_socket_directories")
                socket_dirs = cur.fetchone()[0]
                # Take the first directory if multiple are listed
                socket_path = socket_dirs.split(",")[0].strip()
            conn.close()

            # Verify the socket directory exists and is accessible
            if not os.path.isdir(socket_path):
                socket_path = None
        except Exception as e:
            # If Unix socket connection fails, PostgreSQL might not be configured for it
            # This is expected and we skip the test rather than fail
            pytest.skip(f"PostgreSQL Unix socket not available: {e}")

        if not socket_path:
            pytest.skip("PostgreSQL Unix socket directory not found")

        # Override pg_host with Unix socket path
        cli_env["pg_host"] = socket_path

        # Init with Unix socket
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0, f"Init failed: {result.stderr}"

        # Verify config contains percent-encoded socket path
        with open(cli_env["config_path"]) as f:
            config_content = f.read()

        # Socket path should be percent-encoded in connection string
        encoded_path = socket_path.replace("/", "%2F")
        assert encoded_path in config_content, (
            f"Connection string should contain percent-encoded socket path {encoded_path}"
        )

        # Verify daemon can start with the generated config
        _patch_config_port(cli_env["config_path"], cli_env["port"])
        result = _run_extenddb("serve", config=cli_env["config_path"])
        assert result.returncode == 0, f"Serve failed: {result.stderr}"

        # Wait for server to be healthy
        assert _wait_for_server(cli_env["port"]), "Server did not become healthy with Unix socket connection"

        # Stop
        _run_extenddb("stop", config=cli_env["config_path"])


class TestCliMultiInstance:
    """Test multi-instance isolation — two extenddb instances on different ports/databases."""

    def test_two_instances_isolated(self, tmp_path):
        """Two extenddb instances with different configs don't interfere."""
        _fail_if_no_pg()
        _fail_if_no_binary()

        from urllib.parse import urlparse
        parsed = urlparse(PG_CONN)
        pg_host = parsed.hostname or "localhost"
        pg_port = str(parsed.port or 5432)

        instances = []
        for i in range(2):
            db_name = f"extenddb_multi_{uuid.uuid4().hex[:8]}_catalog"
            port = _find_free_port()
            inst_dir = tmp_path / f"inst{i}"
            os.makedirs(str(inst_dir), exist_ok=True)

            config_path = str(inst_dir / "extenddb.toml")

            instances.append(
                {
                    "db_name": db_name,
                    "config_path": config_path,
                    "port": port,
                    "pg_host": pg_host,
                    "pg_port": pg_port,
                }
            )

        try:
            # Init both
            for inst in instances:
                result = _run_extenddb(
                    "init", *_init_args(inst),
                    config=inst["config_path"],
                    env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
                )
                assert result.returncode == 0
                _patch_config_port(inst["config_path"], inst["port"])

            # Serve both
            for inst in instances:
                result = _run_extenddb("serve", config=inst["config_path"])
                assert result.returncode == 0

            # Both should be healthy
            for inst in instances:
                assert _wait_for_server(inst["port"]), (
                    f"Instance on port {inst['port']} did not become healthy"
                )

            # Both should report running
            for inst in instances:
                result = _run_extenddb("status", config=inst["config_path"])
                assert result.returncode == 0

        finally:
            # Cleanup: stop and destroy both
            for inst in instances:
                try:
                    _run_extenddb(
                        "stop", config=inst["config_path"], check=False, timeout=10
                    )
                except Exception:
                    pass
                try:
                    _run_extenddb(
                        "destroy",
                        "--yes",
                        *_pg_args(),
                        config=inst["config_path"],
                        check=False,
                        timeout=10,
                    )
                except Exception:
                    pass
                _drop_database(inst["db_name"])
                if inst["db_name"].endswith("_catalog"):
                    _drop_database(inst["db_name"][:-len("_catalog")])
