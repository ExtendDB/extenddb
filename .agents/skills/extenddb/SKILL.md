---
name: extenddb
description: ExtendDB onboarding, configuration, sample walkthrough, and troubleshooting. Activates on any ExtendDB-related request including setup, build, init, serve, IAM, AWS CLI configuration, first CRUD, sample apps, streams, or any ExtendDB error message. Routes to the appropriate domain reference based on user intent.
last_synced_with_docs: "2026-05-18"
---

# ExtendDB

This skill covers the full ExtendDB lifecycle: from a cold clone to a running server with working CRUD, sample app walkthroughs, and troubleshooting. It dispatches to domain-specific reference files based on user intent.

## Routing

Determine what the user needs and load the appropriate reference domain:

| User intent | Domain | Entry point |
|---|---|---|
| Install, build, init, serve, first IAM user | **setup** | Start at state detection below |
| PostgreSQL not ready, pg_isready fails | **postgres** | `references/postgres/01-readiness-checks.md` |
| Configure AWS CLI or SDK, first CRUD round trip | **first-request** | `references/first-request/01-aws-cli-config.md` |
| Run sample_app.py or stream_consumer.py | **samples** | `references/samples/01-venv-setup.md` |
| Error message, stack trace, unexpected behavior | **troubleshooting** | `references/troubleshooting/01-symptom-index.md` |

When the user's intent spans multiple domains (e.g., "set up extenddb and run the samples"), work through them in the order listed above.

## Setup domain

### Platform detection

Run `uname -s` and branch on the output.

- `Linux`: use the Linux column of `references/setup/07-platform-commands.md`, the install script at `scripts/install-linux.sh`, and `journalctl -t extenddb -f` for log tailing.
- `Darwin`: use the macOS column of `references/setup/07-platform-commands.md`, the install script at `scripts/install-macos.sh`, and `log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info` for log tailing.
- Anything else: ExtendDB is not supported on native Windows. Use WSL2 (Ubuntu 22.04 or later), then restart this skill.

### Environment state detection

Run `bash scripts/detect-state.sh`. The script prints one of five resume-point words on stdout.

- `dependencies`: binary absent. Load `references/setup/02-dependency-checks.md` then `references/setup/03-build-stage.md`.
- `postgres`: binary built but `extenddb.toml` absent. Verify Postgres readiness (see postgres domain), then load `references/setup/04-init-stage.md`.
- `init`: `extenddb.toml` exists but TLS cert absent. Partial-init state. Load `references/setup/04-init-stage.md` with destroy-first warning.
- `iam`: server running. Load `references/setup/06-iam-first-user.md`.
- `running-server-stopped`: config and TLS present but server not running. Offer start (`references/setup/05-serve-stage.md`) or reinit (`references/setup/04-init-stage.md`).

### Linear walkthrough

1. Dependencies: `references/setup/02-dependency-checks.md`
2. Build: `references/setup/03-build-stage.md`
3. PostgreSQL readiness: route to postgres domain on failure
4. `extenddb init`: `references/setup/04-init-stage.md`
5. `extenddb serve`: `references/setup/05-serve-stage.md`
6. First IAM user: `references/setup/06-iam-first-user.md`
7. First CRUD: route to first-request domain

## Postgres domain

Load when `pg_isready -q` exits nonzero or the user reports a Postgres connectivity issue.

- `references/postgres/01-readiness-checks.md`: pg_isready, pg_ctl status, socket vs TCP checks
- `references/postgres/02-from-scratch.md`: full install from PGDG or Homebrew (only when no Postgres exists)
- `references/postgres/03-connection-strings.md`: connection string format, common variants

Return to the setup domain (init stage) when Postgres is confirmed ready.

## First-request domain

Load after the first IAM access key is created, or when the user asks about AWS CLI/SDK configuration.

- `references/first-request/01-aws-cli-config.md`: three configuration options (env vars, profile, per-command flags), SDK snippet
- `references/first-request/02-first-crud.md`: create-table, put-item, get-item round trip
- `references/first-request/03-next-steps.md`: sample apps, console, differences doc

## Samples domain

Load when the user wants to run `samples/sample_app.py` or `samples/stream_consumer.py`.

### Prerequisite check

Confirm the user has an access key and a configured endpoint. If missing, route to the first-request domain first.

### Python venv detection

Check both standard locations:
```bash
test -d .venv && echo "venv at .venv" || test -d ~/venvs/extenddb-venv && echo "venv at ~/venvs/extenddb-venv" || echo "no venv"
```

If no venv, load `references/samples/01-venv-setup.md`.

### Walkthroughs

- `references/samples/02-sample-app.md`: nine-stage lifecycle demo
- `references/samples/03-stream-consumer.md`: two-client pattern, streams demo

## Troubleshooting domain

Load when the user reports an error or unexpected behavior.

### Lookup procedure

1. Grep `references/troubleshooting/01-symptom-index.md` for the user's error text.
2. The index points to one of six category files (`02-postgres-symptoms.md` through `07-runtime-symptoms.md`).
3. Load the category file and present the verbatim Cause and Fix to the user.

### No speculation

Present only the Cause and Fix from the reference files. Do not add peer causes, alternative failure modes, secondary diagnostics, or invented commands. Each auth error (`InvalidSignatureException`, `UnrecognizedClientException`, `AccessDeniedException`) has a distinct Cause. Do not conflate them.

### Unknown-symptom fallback

If the symptom is not in the index, present the platform-specific log command:
- Linux: `journalctl -t extenddb -n 100`
- macOS: `log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 10m`

Ask the user to paste relevant lines back and retry the lookup.

## Non-destructive operation reminder

This skill presents commands but does not execute state-changing operations (`extenddb init`, `extenddb serve`, `extenddb destroy`, `cargo build`, `chmod`, `kill`). The user reviews each command before invoking it. Read-only checks (`pg_isready -q`, `extenddb status`, `which cargo`, `test -f`, `bash scripts/detect-state.sh`) are the only commands this skill runs directly.

## Reference file index

| Path | Contents |
|---|---|
| **Setup** | |
| `references/setup/01-environment-state.md` | Detection algorithm, state-to-stage mapping |
| `references/setup/02-dependency-checks.md` | Per-dependency checks, minimum versions, install hints |
| `references/setup/03-build-stage.md` | Binary presence check, `cargo build --release`, install scripts |
| `references/setup/04-init-stage.md` | `extenddb init`, six artifacts, re-init rules, `extenddb verify` |
| `references/setup/05-serve-stage.md` | `extenddb serve`, status confirmation, log commands |
| `references/setup/06-iam-first-user.md` | create-user, put-user-policy, create-access-key |
| `references/setup/07-platform-commands.md` | Linux and macOS command table |
| **Postgres** | |
| `references/postgres/01-readiness-checks.md` | pg_isready, pg_ctl status, socket/TCP |
| `references/postgres/02-from-scratch.md` | PGDG and Homebrew install paths |
| `references/postgres/03-connection-strings.md` | Connection string format |
| **First Request** | |
| `references/first-request/01-aws-cli-config.md` | Env vars, AWS profile, per-command flags |
| `references/first-request/02-first-crud.md` | create-table, put-item, get-item |
| `references/first-request/03-next-steps.md` | Recommended next steps |
| **Samples** | |
| `references/samples/01-venv-setup.md` | Venv detection, pip install |
| `references/samples/02-sample-app.md` | Nine-stage lifecycle walkthrough |
| `references/samples/03-stream-consumer.md` | Streams demo, two-client pattern |
| **Troubleshooting** | |
| `references/troubleshooting/01-symptom-index.md` | 16-symptom keyword-to-category lookup |
| `references/troubleshooting/02-postgres-symptoms.md` | Connection refused, password auth, migration |
| `references/troubleshooting/03-catalog-symptoms.md` | Version mismatch, not initialized, already exists |
| `references/troubleshooting/04-startup-symptoms.md` | Address in use, TLS, permissions, daemonize |
| `references/troubleshooting/05-feature-gate-symptoms.md` | Import/export disabled |
| `references/troubleshooting/06-auth-symptoms.md` | InvalidSignature, UnrecognizedClient, AccessDenied |
| `references/troubleshooting/07-runtime-symptoms.md` | Connection pool, streams capture, GSI propagation |
| **Scripts** | |
| `scripts/detect-state.sh` | Read-only environment-state detection |
