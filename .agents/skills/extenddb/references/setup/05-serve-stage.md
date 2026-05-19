# Serve Stage

## Purpose

`extenddb serve` starts the server as a daemon. The process prints a startup banner, forks to background, and the foreground shell returns immediately. Use `extenddb status` to confirm the server is running before moving on.

## Start command

```bash
./target/release/extenddb serve --config extenddb.toml
```

Note: the command prints a startup banner, then forks. The foreground shell returns immediately, so a zero exit code from `extenddb serve` does not by itself mean the server is listening.

## Confirm the server is running

Wait roughly two seconds after starting, then run:

```bash
./target/release/extenddb status --config extenddb.toml
```

Expected output: listening host, port (default 8000), and PID. Exit code 0.

If the exit code is nonzero, the server did not start successfully. Hand off to ``references/troubleshooting/01-symptom-index.md``. Return here when the startup failure is resolved.

## Platform-specific log commands

### Linux (systemd journal)

```bash
# Follow the log
journalctl -t extenddb -f

# Last 50 lines
journalctl -t extenddb -n 50
```

### macOS (unified log)

```bash
# Follow the log
log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info

# Last 10 minutes
log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 10m
```

## Graceful shutdown

```bash
./target/release/extenddb stop --config extenddb.toml
```

Per Requirement 14, the skill does not run this command. The user invokes it when they want to stop the server.

## Emergency shutdown fallback

If `extenddb stop` fails, find the PID from `extenddb status` and send SIGTERM:

```bash
# Discover PID
./target/release/extenddb status --config extenddb.toml

# Stop (user invokes)
kill <PID>
```

The skill presents the command. Per Requirement 14, it does not run `kill`.

## Server not running after `extenddb serve`

Common causes, with pointers to the troubleshooting skill for detail:

- Port 8000 already in use. Consult ``references/troubleshooting/01-symptom-index.md``, symptom "Address already in use."
- TLS certificate missing or unreadable. Consult ``references/troubleshooting/01-symptom-index.md``, symptom "Failed to load TLS certificates."
- Config file permissions too open. Consult ``references/troubleshooting/01-symptom-index.md``, symptom "Config file permissions too open."
- Postgres unreachable. Consult ``references/postgres/01-readiness-checks.md``.
