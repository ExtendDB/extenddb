# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""What the catalog check scripts share: running a command with a time limit,
checking its exit code, reporting each check, and starting and stopping a
server.

devtools/check-sqlite-catalog-lock, check-sqlite-older-binary,
check-postgres-catalog-lock and check-postgres-older-binary import it. Each
check prints one line, `ok` or `FAIL` and what was checked, with the
command's output indented under a FAIL; `finish` prints the count of failed
checks or `all checks passed` and returns the script's exit code.
"""
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


def finish() -> int:
    """Print the outcome of every check reported, and return the exit code."""
    if failures:
        print(f"{failures} check(s) failed")
        return 1
    print("all checks passed")
    return 0
