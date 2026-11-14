# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""What the catalog check scripts share: running a command with a time limit,
checking its exit code, reporting each check, and starting and stopping a
server.

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
    return become_primary(container, port)


# Waits for mongod to answer, initiates the one-member set rs0 and waits for
# the member to be primary, all in one mongosh process against one deadline,
# so a slow start costs only time. Prints `primary`, or throws naming the
# step that ran out of time and what it last saw. The caller defines PORT and
# SECONDS. Every command here is allowed by the localhost exception.
_BECOME_PRIMARY_JS = """
const deadline = Date.now() + SECONDS * 1000;
const left = () => deadline - Date.now();
let last = "nothing tried";
let admin = null;
while (admin === null) {
  if (left() <= 0) throw new Error(`mongod on ${PORT} did not answer within ${SECONDS} s (last: ${last})`);
  try {
    const db = connect(`mongodb://localhost:${PORT}/admin?directConnection=true&serverSelectionTimeoutMS=1000`);
    const pong = db.runCommand({ping: 1});
    if (pong.ok === 1) {
      admin = db;
    } else {
      last = JSON.stringify(pong);
      db.getMongo().close();
      sleep(100);
    }
  } catch (e) {
    last = e.message;
    sleep(100);
  }
}
// An initiated member names its set in hello, which needs no login; once a
// user exists the localhost exception no longer allows replSetInitiate.
if (!admin.runCommand({hello: 1}).setName) {
  const initiated = admin.runCommand(
    {replSetInitiate: {_id: "rs0", members: [{_id: 0, host: `localhost:${PORT}`}]}});
  if (initiated.ok !== 1 && initiated.codeName !== "AlreadyInitialized") {
    throw new Error(`replSetInitiate failed: ${JSON.stringify(initiated)}`);
  }
}
// An awaitable hello answers when the topology changes, so this waits for
// the election instead of sleeping and asking again. An error reply has no
// topologyVersion to wait on, so that path pauses and asks plainly.
let hello = admin.runCommand({hello: 1});
while (!hello.isWritablePrimary) {
  if (left() <= 0) throw new Error(`not primary within ${SECONDS} s: ${JSON.stringify(hello)}`);
  if (hello.ok === 1 && hello.topologyVersion) {
    hello = admin.runCommand(
      {hello: 1, topologyVersion: hello.topologyVersion, maxAwaitTimeMS: Math.min(left(), 10000)});
  } else {
    sleep(100);
    hello = admin.runCommand({hello: 1});
  }
}
print("primary");
"""


def become_primary(container: str, port: int, seconds: int = 120) -> bool:
    """Make the mongod in `container` on `port` a one-member replica set rs0
    and wait for it to be primary. The check fails with what mongosh last saw
    and the end of the container's log, at the step that failed, instead of
    at whatever command runs next."""
    script = f"const PORT = {port}, SECONDS = {seconds};\n{_BECOME_PRIMARY_JS}"
    try:
        done = docker("exec", container, "mongosh", "--nodb", "--quiet", "--eval", script, timeout=seconds + 30)
        out = done.stdout + done.stderr
        # mongosh may add warnings on either stream; the script's own answer
        # is the last line it prints.
        lines = done.stdout.strip().splitlines()
        ok = done.returncode == 0 and bool(lines) and lines[-1].strip() == "primary"
    except subprocess.TimeoutExpired as e:
        ok, out = False, f"mongosh did not finish within {seconds + 30} s\n{_text(e.stdout)}{_text(e.stderr)}"
    if ok:
        return True
    logs = docker("logs", container)
    return report(False, f"{container} becomes primary", f"{out}\n--- docker logs ---\n"
                  f"{(logs.stdout + logs.stderr)[-2000:]}")
