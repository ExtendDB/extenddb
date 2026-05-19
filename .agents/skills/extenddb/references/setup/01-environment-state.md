# Environment State Detection

## Purpose

This file is the reference for the environment-state detection step in `SKILL.md`. Detection is implemented as a read-only shell script, `scripts/detect-state.sh`, that emits one of five resume-point strings on stdout. The skill reads that string and branches the user directly to the correct stage instead of walking through every check in prose.

## Detection algorithm

The script runs four checks in order and falls through on the first one that fails. Every check is read-only.

```
1. test -x ${REPO_ROOT}/target/release/extenddb
   - fails    -> "dependencies"
   - succeeds -> continue

2. test -f ${REPO_ROOT}/extenddb.toml
   - fails    -> "postgres"
   - succeeds -> continue

3. test -f ${HOME}/.extenddb/tls/cert.pem
   - fails    -> "init" (partial init state; warn about extenddb destroy)
   - succeeds -> continue

4. ${REPO_ROOT}/target/release/extenddb status --config ${REPO_ROOT}/extenddb.toml
   - exit 0   -> "iam"
   - nonzero  -> "running-server-stopped"
```

## State-to-stage mapping

| Binary | `extenddb.toml` | Cert | `extenddb status` | Resume point | Requirement |
|---|---|---|---|---|---|
| absent | n/a | n/a | n/a | `dependencies` | 3 |
| present | absent | n/a | n/a | `postgres` | 5 |
| present | present | absent | n/a | `init` (warn about partial state) | 6 |
| present | present | present | running | `iam` | 8 |
| present | present | present | stopped | `running-server-stopped` (choice: start or reinit) | 7 or 6 |

## How to invoke the script

```bash
bash `scripts/detect-state.sh`
```

The script prints the resume point on stdout and a human-readable summary on stderr. Exit code is 0 on successful detection (any of the five states). Exit code is nonzero only if the script itself fails, for example, if it cannot resolve `REPO_ROOT`.

## Per-output interpretation

### `dependencies`

The extenddb binary is absent at `${REPO_ROOT}/target/release/extenddb`. The user has not completed the build stage, and upstream stages (dependency check, Postgres readiness, init, serve) have not started either. Load `references/setup/02-dependency-checks.md` next to walk the user through Rust, Postgres, and Python 3 verification, and load `references/setup/03-build-stage.md` after that for the `cargo build --release` command.

### `postgres`

The binary is present, but `${REPO_ROOT}/extenddb.toml` is absent. The user has built extenddb but has not yet run `extenddb init`. Load `references/setup/02-dependency-checks.md` to verify `pg_isready` reports ready, then hand off to `references/postgres/01-readiness-checks.md` if Postgres is not ready. Once Postgres is confirmed ready, load `references/setup/04-init-stage.md` for the init walkthrough.

### `init`

The binary and `extenddb.toml` are present, but `${HOME}/.extenddb/tls/cert.pem` is absent. This is a partial-init state: `extenddb init` completed far enough to create the `extenddb_catalog` and `extenddb` data databases in PostgreSQL (and almost certainly the admin user and encryption key) but did not emit the self-signed TLS cert. Warn the user that re-running `extenddb init` will abort with a "`Database '<name>' already exists`" error because the databases are still there. `extenddb destroy --config extenddb.toml --yes` drops both databases; run it first, then re-run `extenddb init`. Load `references/setup/04-init-stage.md` for the destroy-and-reinit sequence and the one-time credential capture warning.

Alternative path: if the user saved the admin credentials from the prior init run, they may be able to regenerate only the TLS cert pair with `openssl` (per `docs/getting-started.md`) and avoid destroying the databases. `references/setup/04-init-stage.md` covers both paths.

### `iam`

The binary, `extenddb.toml`, and TLS cert are all present, and `extenddb status` reports the server is running. The user has a fully initialized, running server and needs to create the first IAM user and access key. Load `references/setup/06-iam-first-user.md` next. No warnings apply at this stage.

### `running-server-stopped`

The binary, `extenddb.toml`, and TLS cert are all present, but `extenddb status` exits nonzero, indicating the server is not running. Offer the user a choice: start the server (load `references/setup/05-serve-stage.md` for the `extenddb serve --config extenddb.toml` command), or destroy and re-initialize (load `references/setup/04-init-stage.md` for the `extenddb destroy` then `extenddb init` sequence). The destroy path is only correct if the user explicitly wants a fresh deployment; the start path is correct in all other cases.

## Override by the user

The user can override the detection by naming a stage explicitly, for example, "skip detection, start at init." File presence is a weak signal, and a user may know their environment better than the detector. The valid stage names the user can name are `dependencies`, `build`, `postgres`, `init`, `serve`, `iam`, and `first-request`. When the user overrides, load the reference file for the named stage and skip the detection output.
