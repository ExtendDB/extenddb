# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""What the catalog check scripts share: running a command with a time limit,
checking its exit code, reporting each check, and starting and stopping a
server.

devtools/check-sqlite-catalog-lock, check-sqlite-older-binary,
check-postgres-catalog-lock, check-postgres-older-binary,
check-cassandra-catalog-lock, check-cassandra-older-binary,
check-cassandra-authorizer and the check-mongodb-* scripts import it; the
MongoDB ones start their replica sets with `start_mongo`, except
check-mongodb-auth, which starts mongod with access control itself and
initiates the set with `initiate_replica_set`. Each
check prints one line, `ok` or `FAIL` and what was checked, with the
command's output indented under a FAIL; `finish` prints the count of failed
checks or `all checks passed` and returns the script's exit code.
"""
import os
import signal
import subprocess
import time

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
        probe = subprocess.run(["curl", "-sk", url], capture_output=True, text=True, check=False)
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


def with_filesystem_store(config: str, work: str) -> tuple[str, str]:
    """A copy of `config` with a filesystem backup store at `<work>/backups`
    (created `0700`), for the backup commands' store-configured runs; the
    copy's path and the root's canonical path, as the commands print it."""
    root = os.path.realpath(os.path.join(work, "backups"))
    os.mkdir(root, 0o700)
    store_config = os.path.join(work, "extenddb-store.toml")
    with open(config, encoding="utf-8") as f:
        base = f.read()
    with open(store_config, "w", encoding="utf-8") as f:
        f.write(f'{base}\n[backup.store]\nkind = "filesystem"\n\n[backup.store.filesystem]\npath = "{root}"\n')
    return store_config, root


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
    """Run `script` with mongosh, without credentials, in `container`, on the
    mongod listening on `port`."""
    done = docker("exec", container, "mongosh", "--quiet", "--port", str(port), "--eval", script, timeout=timeout)
    return done.returncode, done.stdout + done.stderr


def start_mongo(container: str, port: int, mongod_args: tuple[str, ...] = (), mounts: tuple[str, ...] = ()) -> bool:
    """Start mongo:7 as a one-node replica set named rs0 in `container`,
    listening on `port` inside the container and published on 127.0.0.1
    only, so the same connection string works from the host and from a
    container that shares its network. Initiates the set and waits for the
    member to be primary with `initiate_replica_set`.
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
    return initiate_replica_set(container, port)


def _last_line(output: str) -> str:
    lines = output.strip().splitlines()
    return lines[-1].strip() if lines else ""


# What `db.hello()` says about the member: `primary` once it is the writable
# primary, `initiated` while the set has a configuration but no primary yet,
# and `uninitiated` before any `rs.initiate` has taken effect (no `setName`).
_MEMBER_STATE = (
    "const h = db.hello(); print(h.isWritablePrimary ? 'primary' : h.setName ? 'initiated' : 'uninitiated')"
)


def initiate_replica_set(container: str, port: int) -> bool:
    """Initiate the one-node replica set rs0 on the mongod listening on
    `port` in `container`, and wait for its member to be primary.

    Each poll runs `_MEMBER_STATE` and reads only the last line of its
    output, which has to be one of the three states exactly. While mongod
    refuses connections the poll prints `MongoNetworkError: connect
    ECONNREFUSED 127.0.0.1:<port>`, and the wait goes on. `rs.initiate` is
    sent after each poll that prints `uninitiated`, so it goes only to a
    mongod that answered, and an initiate that returned an error is sent
    again. Every call runs without credentials: `hello` needs none, and a
    set started with `--keyFile` initiates through the localhost exception.
    A member that is not primary after 120 polls, with 1 s between them, is
    reported with the last poll's output, the last initiate's output and
    the end of the container's log."""
    initiate = f"rs.initiate({{_id: 'rs0', members: [{{_id: 0, host: 'localhost:{port}'}}]}})"
    last_poll = ""
    last_initiate = "(no rs.initiate sent: no poll printed `uninitiated`)"
    for _ in range(120):
        last_poll = mongosh(container, port, _MEMBER_STATE)[1]
        state = _last_line(last_poll)
        if state == "primary":
            return True
        if state == "uninitiated":
            last_initiate = mongosh(container, port, initiate)[1]
        time.sleep(1)
    return report(
        False,
        f"{container} did not become primary",
        f"last db.hello() poll output:\n{last_poll.strip()}\n"
        f"last rs.initiate output:\n{last_initiate.strip()}\n"
        "end of the container's log:\n" + docker("logs", container).stdout[-2000:],
    )
