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
import socket
import ssl
import subprocess
import threading
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


def _start_tls_server(cert_path: str, key_path: str, handler):
    """Start a one-connection TLS server and return endpoint, cleanup, and state."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(0.2)
    port = listener.getsockname()[1]
    stop = threading.Event()
    request_received = threading.Event()
    errors = []

    def serve():
        try:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(cert_path, key_path)
            raw = None
            while not stop.is_set():
                try:
                    raw, _addr = listener.accept()
                    break
                except socket.timeout:
                    continue
            if raw is None:
                return
            with raw:
                # Bound a client that connects but never starts or completes TLS.
                raw.settimeout(2)
                with context.wrap_socket(raw, server_side=True) as tls:
                    request = tls.recv(4096)
                    if not request.startswith(b"GET /health "):
                        raise AssertionError(f"unexpected healthcheck request: {request!r}")
                    request_received.set()
                    handler(tls, stop)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLEOFError):
            # Expected when a deadline expires and the healthcheck disconnects.
            pass
        except Exception as exc:
            errors.append(exc)
        finally:
            listener.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    def cleanup():
        stop.set()
        thread.join(timeout=5)
        assert not thread.is_alive(), "TLS test server did not stop"
        if errors:
            raise AssertionError(f"TLS test server failed: {errors[0]}") from errors[0]

    return f"https://127.0.0.1:{port}", cleanup, request_received


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
        first, second = "one.example.com", "two.example.com"
        result = _init_with_home(
            cli_env, home, "--tls-san", first, "--tls-san", second
        )
        assert result.returncode == 0, result.stdout + result.stderr

        sans = _cert_sans(home.cert)
        assert sans.count(first) == 1, sans
        assert sans.count(second) == 1, sans

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

    def test_wildcard_tls_san_is_accepted_and_rechecked(self, cli_env, tmp_path):
        """A wildcard SAN survives a re-run of init.

        `*.svc.cluster.local` is a valid certificate entry but not a valid
        server name, so it cannot be verified directly. The coverage check
        substitutes a single label; without that, init succeeded on the first
        run and then failed on every later one, which is a crash loop for the
        idempotent container entrypoint.
        """
        home = _IsolatedHome(tmp_path)
        wildcard = "*.svc.cluster.local"
        assert _init_with_home(cli_env, home, "--tls-san", wildcard).returncode == 0
        assert wildcard in _cert_sans(home.cert), _cert_sans(home.cert)

        # Re-running with the same wildcard is accepted, not rejected.
        again = _init_with_home(
            cli_env, home, "--overwrite", "--tls-san", wildcard, check=False
        )
        combined = again.stdout + again.stderr
        assert "already covers" in combined, combined
        assert "not valid for the requested" not in combined, combined

        # A wildcard the certificate does not carry is still caught.
        other = _init_with_home(
            cli_env, home, "--overwrite", "--tls-san", "*.other.example.com", check=False
        )
        assert other.returncode != 0
        assert "*.other.example.com" in other.stdout + other.stderr

    def test_wildcard_must_be_leftmost_label(self, cli_env, tmp_path):
        """A misplaced wildcard fails on the first run, not a later one."""
        home = _IsolatedHome(tmp_path)
        result = _init_with_home(
            cli_env, home, "--tls-san", "foo.*.example.com", check=False
        )
        assert result.returncode != 0
        assert "leftmost label" in result.stdout + result.stderr
        assert not os.path.exists(home.cert), "no certificate should have been written"

    def test_tls_san_not_silently_dropped_when_cert_exists(self, cli_env, tmp_path):
        """init must not succeed having ignored --tls-san for an existing cert.

        init never regenerates an existing certificate, so a newly added
        --tls-san cannot take effect. It has to fail loudly rather than leave
        clients to discover the missing name as a TLS hostname-verification
        error.
        """
        home = _IsolatedHome(tmp_path)
        late = "late.example.com"
        assert _init_with_home(cli_env, home).returncode == 0
        assert os.path.isfile(home.cert), "first init should have generated a cert"
        before = _cert_sans(home.cert)
        assert before.count(late) == 0, before

        # Re-init with a SAN the existing certificate does not cover.
        result = _init_with_home(
            cli_env, home, "--overwrite", "--tls-san", late, check=False
        )
        assert result.returncode != 0, (
            "init should fail rather than silently ignore --tls-san\n"
            + result.stdout + result.stderr
        )
        combined = result.stdout + result.stderr
        assert late in combined, combined
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

        # Stop → healthcheck must fail again. Poll rather than sleeping a fixed
        # interval, so a slow drain cannot make this flaky.
        _run_extenddb("stop", config=cli_env["config_path"])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            down2 = _run_extenddb("healthcheck", config=cli_env["config_path"], check=False)
            if down2.returncode != 0:
                break
            time.sleep(0.25)
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

    def test_healthcheck_slow_drip_respects_one_deadline(self, cli_env, tmp_path):
        """Partial reads cannot reset the timeout and keep the probe alive."""
        home = _IsolatedHome(tmp_path)
        assert _init_with_home(cli_env, home).returncode == 0
        status_sent = threading.Event()
        drip_sent = threading.Event()

        def slow_drip(tls, stop):
            tls.sendall(b"HTTP/1.1 200 OK\r\n")
            status_sent.set()
            # Every byte arrives before the old 3s per-read timeout, so the old
            # read_to_string loop stayed alive forever despite being unhealthy.
            while not stop.wait(1):
                tls.sendall(b"x")
                drip_sent.set()

        endpoint, cleanup, request_received = _start_tls_server(
            home.cert, home.key, slow_drip
        )
        started = time.monotonic()
        try:
            result = _run_extenddb(
                "healthcheck", "--endpoint", endpoint,
                check=False, timeout=10, env_override=home.env,
            )
        finally:
            cleanup()
        elapsed = time.monotonic() - started
        combined = result.stdout + result.stderr

        assert request_received.is_set(), "TLS server never received GET /health"
        assert status_sent.is_set(), "TLS server never sent the HTTP status line"
        assert drip_sent.is_set(), "TLS server never made partial read progress"
        assert result.returncode != 0, combined
        assert "Read error:" in combined, combined
        assert elapsed < 6, f"slow-drip healthcheck exceeded its deadline: {elapsed:.1f}s"

    def test_healthcheck_caps_response_without_waiting_for_close(
        self, cli_env, tmp_path
    ):
        """Only a bounded response prefix is read before parsing the status."""
        home = _IsolatedHome(tmp_path)
        assert _init_with_home(cli_env, home).returncode == 0

        def oversized_response(tls, stop):
            tls.sendall(b"HTTP/1.1 200 OK\r\nX-Fill: " + b"x" * (16 * 1024))
            # Keep the connection open: a bounded reader returns after its cap,
            # while read_to_string waits for EOF and eventually times out.
            stop.wait(10)

        endpoint, cleanup, request_received = _start_tls_server(
            home.cert, home.key, oversized_response
        )
        started = time.monotonic()
        try:
            result = _run_extenddb(
                "healthcheck", "--endpoint", endpoint,
                check=False, timeout=10, env_override=home.env,
            )
        finally:
            cleanup()
        elapsed = time.monotonic() - started

        assert request_received.is_set(), "TLS server never received GET /health"
        assert result.returncode == 0, result.stdout + result.stderr
        assert elapsed < 5, f"bounded response read took {elapsed:.1f}s"


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

    def test_write_pid_file_opt_in(self, cli_env, tmp_path):
        """--write-pid-file restores the PID file, so `stop` works again.

        Foreground mode writes none by default. Tooling and shell users that
        want `extenddb stop` opt back in, and the file lands at the same path
        daemon mode uses, so `stop` needs no extra arguments.
        """
        home = _IsolatedHome(tmp_path)
        assert _init_with_home(cli_env, home).returncode == 0
        _patch_config_port(cli_env["config_path"], cli_env["port"])

        pid_file = os.path.join(
            home.home, ".extenddb", "run", f"extenddb-{cli_env['port']}.pid"
        )
        env = os.environ.copy()
        env.update(home.env)
        log_path = tmp_path / "foreground-pid.log"
        with open(log_path, "wb") as log:
            proc = subprocess.Popen(
                [
                    EXTENDDB_BINARY, "serve", "--foreground", "--write-pid-file",
                    "--config", cli_env["config_path"],
                ],
                stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env,
            )
            try:
                assert _wait_for_server(cli_env["port"]), (
                    "server did not become healthy: "
                    + log_path.read_text(errors="replace")
                )
                assert os.path.exists(pid_file), f"expected PID file {pid_file}"
                with open(pid_file) as f:
                    assert int(f.read().strip()) == proc.pid

                # `stop` reads the PID file and signals the server. Launch it
                # without blocking: until this test reaps the server process it
                # lingers as a zombie, and `stop`'s liveness check (kill(pid, 0))
                # cannot tell a zombie from a live process, so a blocking call
                # would sit through its full timeout.
                stop = subprocess.Popen(
                    [EXTENDDB_BINARY, "stop", "--config", cli_env["config_path"]],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL, text=True, env=env,
                )
                rc = proc.wait(timeout=15)
                out, err = stop.communicate(timeout=15)
                assert stop.returncode == 0, out + err
                assert rc in (0, -signal.SIGTERM), rc
                assert not os.path.exists(pid_file), "PID file should be cleaned up"
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)

