# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""What the catalog check scripts share: running a command with a time limit,
checking its exit code, reporting each check, and starting and stopping a
server, in the foreground or in daemon mode.

devtools/check-sqlite-catalog-lock, check-sqlite-older-binary,
check-postgres-catalog-lock, check-postgres-older-binary,
check-cassandra-catalog-lock, check-cassandra-older-binary,
check-cassandra-authorizer and the check-mongodb-* scripts import it; the
MongoDB ones start their replica sets with `start_mongo`. Each
check prints one line, `ok` or `FAIL` and what was checked, with the
command's output indented under a FAIL; `finish` prints the count of failed
checks or `all checks passed` and returns the script's exit code.
"""
import os
import signal
import subprocess
import time
import tomllib

failures = 0


def report(ok: bool, what: str, output: str = "") -> bool:
    """Print one check; a failed one prints `output` under it and is counted."""
    global failures
    print(f"{'ok  ' if ok else 'FAIL'} {what}", flush=True)
    if not ok:
        failures += 1
        for line in output.splitlines():
            print(f"     | {line}", flush=True)
    return ok


def _text(stream) -> str:
    if stream is None:
        return ""
    if isinstance(stream, bytes):
        return stream.decode("utf-8", "replace")
    return stream


def run(cmd: list[str], env: dict | None = None, timeout: float = 90, cwd: str | None = None) -> tuple[int, str]:
    """Run `cmd` and return its exit code and its output, stdout then stderr.
    A command still running after `timeout` seconds is killed, and its code
    is -1."""
    try:
        done = subprocess.run(
            cmd, env=env, cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired as e:
        return -1, f"still running after {timeout:g} s\n{_text(e.stdout)}{_text(e.stderr)}"
    return done.returncode, done.stdout + done.stderr


def run_split(cmd: list[str], env: dict | None, timeout: float = 120) -> tuple[int, str, str]:
    """Run `cmd` and return its exit code, stdout and stderr apart. A
    command still running after `timeout` seconds is killed, and its code
    is -1."""
    try:
        done = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as e:
        return -1, _text(e.stdout), f"still running after {timeout:g} s\n{_text(e.stderr)}"
    return done.returncode, done.stdout, done.stderr


def expect_exit(
    want: int,
    what: str,
    cmd: list[str],
    env: dict | None = None,
    contains: str = "",
    timeout: float = 90,
    cwd: str | None = None,
) -> str:
    """Run `cmd` and report whether it exited `want` and printed `contains`.
    Returns its output."""
    code, output = run(cmd, env, timeout, cwd)
    detail = f" (output names {contains!r})" if contains else ""
    if code == want and contains in output:
        report(True, f"exit {code}: {what}{detail}")
    else:
        report(False, f"exit {code}, expected {want}: {what}{detail}", output)
    return output


def healthy(url: str, proc: subprocess.Popen | None = None, seconds: float = 60) -> bool:
    """Whether `url` (a server's /health) answers `healthy` within `seconds`.
    Stops waiting when `proc`, the server's process, has exited."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            return False
        # A port that is bound but not accepting holds curl for its 300 s
        # connect timeout, so each probe has its own limit.
        probe = subprocess.run(
            ["curl", "-sk", "--max-time", "2", url], capture_output=True, text=True, check=False
        )
        if "healthy" in probe.stdout:
            return True
        time.sleep(0.5)
    return False


def watch_lost_registration(
    proc: subprocess.Popen, url: str, seconds: float
) -> tuple[float | None, int | None, float]:
    """Poll `url` (a server's /health) every 0.5 s, while the server's
    database does not answer, until `proc` exits or `seconds` pass. Returns
    the seconds to the first 503 (None without one), the exit code (None
    while the server runs) and the seconds waited."""
    started = time.monotonic()
    first_503 = None
    while time.monotonic() - started < seconds and proc.poll() is None:
        probe = subprocess.run(
            ["curl", "-sk", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "2", url],
            capture_output=True,
            text=True,
            check=False,
        )
        if first_503 is None and probe.stdout.strip() == "503":
            first_503 = time.monotonic() - started
        time.sleep(0.5)
    return first_503, proc.poll(), time.monotonic() - started


def start(cmd: list[str], env: dict | None, log: str, cwd: str | None = None) -> subprocess.Popen:
    """Start `cmd` in the background with its output in the file `log`."""
    with open(log, "w", encoding="utf-8") as out:
        return subprocess.Popen(cmd, env=env, cwd=cwd, stdout=out, stderr=subprocess.STDOUT)


def stop(proc: subprocess.Popen) -> None:
    """Stop a process `start` started: SIGTERM, then SIGKILL after 30 s."""
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def pid_file(config: str, env: dict, port: int) -> str:
    """The PID file of a daemon started with `config`, `env` and
    `--port port`: <run_dir>/extenddb-<port>.pid, where run_dir is
    EXTENDDB__SERVER__RUN_DIR, else `[server] run_dir` in `config`, else
    ~/.extenddb/run, and ~ is env's HOME."""
    run_dir = env.get("EXTENDDB__SERVER__RUN_DIR")
    if not run_dir:
        try:
            with open(config, "rb") as f:
                run_dir = tomllib.load(f).get("server", {}).get("run_dir")
        except (OSError, tomllib.TOMLDecodeError):
            run_dir = None
    run_dir = run_dir or "~/.extenddb/run"
    if run_dir == "~" or run_dir.startswith("~/"):
        run_dir = env.get("HOME", os.path.expanduser("~")) + run_dir[1:]
    return os.path.join(run_dir, f"extenddb-{port}.pid")


def _command_line(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().replace(b"\0", b" ").decode("utf-8", "replace")
    except OSError:
        done = subprocess.run(["ps", "-p", str(pid), "-o", "args="], capture_output=True, text=True, check=False)
        return done.stdout


def kill_daemon(path: str, binary: str) -> None:
    """Stop the daemon whose PID file is `path`, when one is running
    `binary`: SIGTERM, then SIGKILL after 30 s. A daemon is not one of the
    processes `start` started, and one left running would hold its port,
    and on SQLite the database's lock, after the script ends."""
    try:
        with open(path, encoding="utf-8") as f:
            pid = int(f.read().split()[0])
    except (OSError, ValueError, IndexError):
        return
    # A PID file left behind can name a process that reused the PID.
    command = _command_line(pid)
    if binary not in command or " serve" not in command:
        return
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(60):
            time.sleep(0.5)
            os.kill(pid, 0)
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def finish() -> int:
    """Print the outcome of every check reported, and return the exit code."""
    if failures:
        print(f"{failures} check(s) failed")
        return 1
    print("all checks passed")
    return 0


def docker(*args: str, timeout: float = 120) -> subprocess.CompletedProcess:
    """Run `docker` with `args`, capturing its output."""
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=False, timeout=timeout)


def mongosh(container: str, port: int, script: str, timeout: float = 60) -> tuple[int, str]:
    """Run `script` with mongosh in a container `start_mongo` started."""
    done = docker("exec", container, "mongosh", "--quiet", "--port", str(port), "--eval", script, timeout=timeout)
    return done.returncode, done.stdout + done.stderr


def start_mongo(container: str, port: int, mongod_args: tuple[str, ...] = (), mounts: tuple[str, ...] = ()) -> bool:
    """Start mongo:7 as a one-node replica set named rs0 in `container`,
    listening on `port` inside the container and published on 127.0.0.1
    only, so the same connection string works from the host and from a
    container that shares its network. Waits for the member to be primary.
    `mongod_args` are added to mongod's command line, `mounts` are `-v`
    arguments; a set started with `--keyFile` initiates through the
    localhost exception."""
    docker("rm", "-f", container)
    volumes = [arg for mount in mounts for arg in ("-v", mount)]
    started = docker(
        "run", "-d", "--name", container, "-p", f"127.0.0.1:{port}:{port}", *volumes,
        "mongo:7", "mongod", "--replSet", "rs0", "--bind_ip_all", "--port", str(port), *mongod_args,
    )
    if started.returncode != 0:
        return report(False, f"start {container}", started.stderr)
    for _ in range(60):
        if mongosh(container, port, "db.runCommand({ping: 1}).ok")[1].strip().endswith("1"):
            break
        time.sleep(1)
    mongosh(container, port, f"rs.initiate({{_id: 'rs0', members: [{{_id: 0, host: 'localhost:{port}'}}]}})")
    for _ in range(60):
        if mongosh(container, port, "db.hello().isWritablePrimary")[1].strip().endswith("true"):
            return True
        time.sleep(1)
    return report(False, f"{container} did not become primary", docker("logs", container).stdout[-2000:])
