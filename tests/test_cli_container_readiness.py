# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Container-readiness CLI tests (Phase 1 / gaps G1, G3, G4).

Cover the container-oriented binary changes:

  G1  `extenddb init --tls-san <name>` adds extra Subject Alternative Names to
      the generated self-signed certificate, and refuses to drop them silently
      when a certificate already exists.
  G3  `extenddb healthcheck` exits 0 when the server is healthy and non-zero
      when it is not (used by the container HEALTHCHECK).
  G4  `extenddb serve --foreground` writes no PID file (so a read-only root
      filesystem needs no run directory), and still shuts down on SIGTERM.

Like the other CLI lifecycle tests these require a PostgreSQL instance
(EXTENDDB_TEST_PG_CONNECTION_STRING) and a built binary, and are excluded from
the backend-agnostic pytest suite.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time

import pytest

from lifecycle_helpers import (
    EXTENDDB_BINARY,
    _init_args,
    _patch_config_port,
    _run_extenddb,
    _wait_for_server,
)


def _cert_sans(cert_path: str) -> list[str]:
    """Return the Subject Alternative Names in a PEM certificate.

    Shells out to ``openssl`` rather than using a private CPython API
    (``ssl._ssl._test_decode_cert``) or adding a ``cryptography`` dependency.
    Uses ``-text`` rather than ``-ext subjectAltName``, which needs OpenSSL
    1.1.1 or later. The SAN values sit on the line after the extension header,
    formatted as ``DNS:localhost, IP Address:127.0.0.1``.
    """
    if shutil.which("openssl") is None:
        pytest.fail("MISCONFIGURED: openssl is required to decode certificate SANs")
    out = subprocess.run(
        ["openssl", "x509", "-in", cert_path, "-noout", "-text"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    lines = out.splitlines()
    for i, line in enumerate(lines):
        if "Subject Alternative Name" not in line or i + 1 >= len(lines):
            continue
        sans = []
        for entry in lines[i + 1].split(","):
            _typ, _, value = entry.strip().partition(":")
            if value:
                sans.append(value.strip())
        return sans
    return []


class _IsolatedHome:
    """A throwaway ``$HOME`` so tests never touch the real ~/.extenddb.

    ``init`` writes the TLS certificate, key, and run directory under ``$HOME``,
    since ``expand_tilde`` resolves ``~`` from the environment. Pointing HOME at
    a per-test directory stops the tests overwriting the developer's or CI
    user's real certificate, and makes them order-independent and safe to run in
    parallel.
    """

    def __init__(self, tmp_path):
        self.home = str(tmp_path / "home")
        os.makedirs(self.home, exist_ok=True)

    @property
    def env(self) -> dict[str, str]:
        return {"HOME": self.home}

    @property
    def cert(self) -> str:
        return os.path.join(self.home, ".extenddb", "tls", "cert.pem")

    @property
    def key(self) -> str:
        return os.path.join(self.home, ".extenddb", "tls", "key.pem")


def _init_with_home(cli_env, home: _IsolatedHome, *extra_args, check=True):
    """Run `extenddb init` against an isolated HOME."""
    env = {"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"}
    env.update(home.env)
    return _run_extenddb(
        "init", *_init_args(cli_env), *extra_args,
        config=cli_env["config_path"],
        env_override=env,
        check=check,
    )


class TestInitTlsSan:
    """G1: init --tls-san appends extra SANs to the self-signed cert."""

    def test_tls_san_added_to_cert(self, cli_env, tmp_path):
        home = _IsolatedHome(tmp_path)
        san = "extenddb.svc.cluster.local"
        result = _init_with_home(cli_env, home, "--tls-san", san)
        assert result.returncode == 0, result.stdout + result.stderr

        sans = _cert_sans(home.cert)
        # Defaults remain, plus the extra SAN we asked for.
        assert "localhost" in sans, sans
        assert "127.0.0.1" in sans, sans
        assert san in sans, sans

    def test_multiple_tls_sans(self, cli_env, tmp_path):
        home = _IsolatedHome(tmp_path)
        result = _init_with_home(
            cli_env, home, "--tls-san", "one.example.com", "--tls-san", "two.example.com"
        )
        assert result.returncode == 0, result.stdout + result.stderr

        sans = _cert_sans(home.cert)
        assert "one.example.com" in sans, sans
        assert "two.example.com" in sans, sans

    def test_blank_and_duplicate_sans_are_dropped(self, cli_env, tmp_path):
        """Blanks are skipped, and a duplicate in any case appears once."""
        home = _IsolatedHome(tmp_path)
        result = _init_with_home(
            cli_env, home,
            "--tls-san", "   ",
            "--tls-san", "dup.example.com",
            "--tls-san", "DUP.example.com",
            "--tls-san", "localhost",
        )
        assert result.returncode == 0, result.stdout + result.stderr

        sans = _cert_sans(home.cert)
        assert [s for s in sans if s.lower() == "dup.example.com"] == ["dup.example.com"], sans
        assert sans.count("localhost") == 1, sans
        assert "" not in sans, sans

    def test_tls_san_not_silently_dropped_when_cert_exists(self, cli_env, tmp_path):
        """init must not succeed having ignored --tls-san for an existing cert.

        init never regenerates an existing certificate, so a newly added
        --tls-san cannot take effect. It has to fail loudly rather than leave
        clients to discover the missing name as a TLS hostname-verification
        error.
        """
        home = _IsolatedHome(tmp_path)
        assert _init_with_home(cli_env, home).returncode == 0
        assert os.path.isfile(home.cert), "first init should have generated a cert"
        before = _cert_sans(home.cert)
        assert "late.example.com" not in before, before

        # Re-init with a SAN the existing certificate does not cover.
        result = _init_with_home(
            cli_env, home, "--overwrite", "--tls-san", "late.example.com", check=False
        )
        assert result.returncode != 0, (
            "init should fail rather than silently ignore --tls-san\n"
            + result.stdout + result.stderr
        )
        combined = result.stdout + result.stderr
        assert "late.example.com" in combined, combined
        # And it must not have quietly rotated the certificate either.
        assert _cert_sans(home.cert) == before

    def test_tls_san_already_covered_is_idempotent(self, cli_env, tmp_path):
        """Re-running init with an already-covered --tls-san must not fail on TLS.

        The idempotent container entrypoint re-runs init on every start, so a SAN
        the existing certificate already covers has to be accepted. The rest of
        that second init still stops at "database already exists", so this
        asserts only that the TLS step accepted the SAN and left the certificate
        alone.
        """
        home = _IsolatedHome(tmp_path)
        san = "extenddb.svc.cluster.local"
        assert _init_with_home(cli_env, home, "--tls-san", san).returncode == 0
        first = _cert_sans(home.cert)
        assert san in first, first

        again = _init_with_home(cli_env, home, "--overwrite", "--tls-san", san, check=False)
        combined = again.stdout + again.stderr
        assert "already covers" in combined, combined
        assert "not valid for the requested" not in combined, combined
        # Same certificate, not a rotated one.
        assert _cert_sans(home.cert) == first


class TestHealthcheck:
    """G3: `extenddb healthcheck` reflects server health via its exit code."""

    def test_healthcheck_up_and_down(self, cli_env):
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0
        _patch_config_port(cli_env["config_path"], cli_env["port"])

        # Server not started yet → healthcheck must fail.
        down = _run_extenddb("healthcheck", config=cli_env["config_path"], check=False)
        assert down.returncode != 0, "healthcheck should fail when server is down"

        # Start (daemon mode) and wait for health.
        assert _run_extenddb("serve", config=cli_env["config_path"]).returncode == 0
        assert _wait_for_server(cli_env["port"]), "server did not become healthy"

        # Server up → healthcheck must succeed.
        up = _run_extenddb("healthcheck", config=cli_env["config_path"], check=False)
        assert up.returncode == 0, up.stdout + up.stderr

        # Stop → healthcheck must fail again.
        _run_extenddb("stop", config=cli_env["config_path"])
        time.sleep(1)
        down2 = _run_extenddb("healthcheck", config=cli_env["config_path"], check=False)
        assert down2.returncode != 0, "healthcheck should fail after stop"

    def test_healthcheck_endpoint_override(self, cli_env):
        """--endpoint targets an explicit address regardless of config port."""
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0
        _patch_config_port(cli_env["config_path"], cli_env["port"])
        assert _run_extenddb("serve", config=cli_env["config_path"]).returncode == 0
        assert _wait_for_server(cli_env["port"])

        endpoint = f"https://127.0.0.1:{cli_env['port']}"
        up = _run_extenddb(
            "healthcheck", "--endpoint", endpoint,
            config=cli_env["config_path"], check=False,
        )
        assert up.returncode == 0, up.stdout + up.stderr

        # A trailing path is accepted and ignored.
        with_path = _run_extenddb(
            "healthcheck", "--endpoint", endpoint + "/health",
            config=cli_env["config_path"], check=False,
        )
        assert with_path.returncode == 0, with_path.stdout + with_path.stderr

        _run_extenddb("stop", config=cli_env["config_path"])

    def test_healthcheck_unreachable_endpoint_fails_promptly(self, cli_env):
        """An unreachable endpoint must fail fast, not hang past the probe interval.

        TcpStream::connect has no timeout of its own, so without an explicit
        connect timeout this blocks for the OS default of roughly two minutes.
        """
        started = time.monotonic()
        result = _run_extenddb(
            "healthcheck", "--endpoint", "https://192.0.2.1:18443",
            check=False, timeout=30,
        )
        elapsed = time.monotonic() - started
        assert result.returncode != 0
        assert elapsed < 15, f"healthcheck took {elapsed:.1f}s against an unreachable host"


class TestForegroundNoPidFile:
    """G4: `serve --foreground` writes no PID file and stops on SIGTERM."""

    def test_foreground_writes_no_pid_file(self, cli_env, tmp_path):
        home = _IsolatedHome(tmp_path)
        result = _init_with_home(cli_env, home)
        assert result.returncode == 0
        _patch_config_port(cli_env["config_path"], cli_env["port"])

        # The PID file would live at <run_dir>/extenddb-<port>.pid, and the
        # default run_dir is ~/.extenddb/run, inside our isolated HOME.
        pid_file = os.path.join(home.home, ".extenddb", "run", f"extenddb-{cli_env['port']}.pid")
        assert not os.path.exists(pid_file)

        env = os.environ.copy()
        env.update(home.env)
        # Logs go to stderr in foreground mode. Send them to a file rather than
        # a pipe nobody drains, which would block the server once the pipe
        # buffer filled up.
        log_path = tmp_path / "foreground.log"
        with open(log_path, "wb") as log:
            proc = subprocess.Popen(
                [EXTENDDB_BINARY, "serve", "--foreground", "--config", cli_env["config_path"]],
                stdout=log,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=env,
            )
            try:
                assert _wait_for_server(cli_env["port"]), (
                    "foreground server did not become healthy: "
                    + log_path.read_text(errors="replace")
                )
                # G4: no PID file, and no run directory at all.
                assert not os.path.exists(pid_file), f"unexpected PID file {pid_file}"
                assert not os.path.isdir(os.path.join(home.home, ".extenddb", "run")), (
                    "foreground mode should not create the run directory"
                )

                # healthcheck should also pass against the foreground server.
                hc = _run_extenddb(
                    "healthcheck", config=cli_env["config_path"], check=False,
                    env_override=home.env,
                )
                assert hc.returncode == 0, hc.stdout + hc.stderr

                # `stop` has no PID file to read, but it must not claim that
                # nothing is running while the port is live.
                stop = _run_extenddb(
                    "stop", config=cli_env["config_path"], check=False,
                    env_override=home.env,
                )
                assert stop.returncode != 0
                assert "listening on port" in (stop.stdout + stop.stderr), (
                    stop.stdout + stop.stderr
                )
                assert proc.poll() is None, "`stop` must not have killed the server"
            finally:
                # SIGTERM should shut it down gracefully (already-implemented).
                proc.terminate()
                try:
                    rc = proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    raise
            assert rc in (0, -signal.SIGTERM), (
                f"unexpected exit {rc}: " + log_path.read_text(errors="replace")
            )

