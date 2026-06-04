# Installing extenddb on macOS

> See [NOTICE](../NOTICE.md) for important disclaimers.

This guide is the macOS-specific path for the generic instructions in
`docs/getting-started.md`. See that doc for the full CLI surface and
feature walkthrough.

## Quick Install (recommended)

The installer script checks dependencies, builds extenddb, sets up a Python
venv for documentation, and builds PDF manuals:

```bash
scripts/install-macos.sh
```

The script does **not** install missing dependencies — it reports what's
missing so you can install them with Homebrew, then re-run.

After the script completes, skip to [Step 3: Initialize the deployment](#3-initialize-the-deployment).

## Manual Installation

If you prefer to run each step yourself, follow the sections below.

## Prerequisites

- Rust 1.88+ (`rustup update`)
- PostgreSQL 14+ via Homebrew (`brew install postgresql@17`)
- Python 3.10+ (for test suites)
- AWS CLI v2 (for testing)

## 1. Start PostgreSQL

Using `brew services` (recommended — survives reboots):

```bash
brew services start postgresql@17
```

Or manually:

```bash
pg_ctl -D /opt/homebrew/var/postgresql@17 \
       -l /opt/homebrew/var/postgresql@17/server.log start
```

Verify it's accepting connections:

```bash
pg_isready
# /tmp:5432 - accepting connections
```

On Homebrew macOS the superuser is your macOS username (`$(whoami)`), not
`postgres`, and uses trust auth over the local socket.

## 2. Build extenddb

```bash
cargo build --release
```

Binary lands at `target/release/extenddb`.

## 3. Initialize the deployment

`extenddb init` creates the PostgreSQL `extenddb` role, the catalog and data
databases, applies schema migrations, generates an encryption key,
creates a default account + admin user, and writes `extenddb.toml` for you.
Do **not** hand-write `extenddb.toml` before running `init`.

On macOS you must tell `init` which PostgreSQL user to connect as for
the `CREATE ROLE` / `CREATE DATABASE` steps — your macOS username:

```bash
./target/release/extenddb init --pg-user $(whoami)
```

This prints the admin credentials **once**. Save them — they cannot be
retrieved later.

`init` writes a `extenddb.toml` with `auth.provider = "builtin"` (the default).
All DynamoDB requests must be signed with valid access keys. Create an IAM
user and access key after starting the server (see step 7 below).

## 4. Verify

```bash
./target/release/extenddb verify --config extenddb.toml
```

Expected:

```
=== extenddb verify ===
...
  OK: Catalog version 0.0.2
...
=== HEALTHY: All checks passed ===
```

## 5. Start the server

```bash
./target/release/extenddb serve --config extenddb.toml
```

extenddb daemonizes automatically and logs to syslog.

Check status:

```bash
./target/release/extenddb status --config extenddb.toml
```

Read logs (macOS equivalent of `journalctl -t extenddb`):

```bash
log stream --predicate 'processImagePath ENDSWITH "extenddb"'        # follow live
log show   --predicate 'processImagePath ENDSWITH "extenddb"' --last 5m
```

Stop the server:

```bash
./target/release/extenddb stop --config extenddb.toml
```

## 6. Smoke test

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/health
# {"status":"healthy"}

export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
aws dynamodb list-tables \
    --endpoint-url https://127.0.0.1:8000 \
    --region us-east-1
# { "TableNames": [] }
```

## 7. Management console

Open `https://127.0.0.1:8000/console/` in a browser (accept the self-signed
certificate warning). Log in with the `admin` user and the password printed
during `init`.

## Upgrading after a `git pull`

If the binary's expected catalog version is ahead of the deployed
catalog, `extenddb serve` refuses to start and `extenddb verify` reports a
version mismatch. Apply migrations:

```bash
cargo build --release
./target/release/extenddb migrate --config extenddb.toml
```

No data is lost; only the catalog schema is updated.

## Tearing it all down

```bash
# Stop the server
./target/release/extenddb stop --config extenddb.toml

# Drop both databases and the extenddb role
./target/release/extenddb destroy --config extenddb.toml --yes
```

## Differences from Linux

| Item               | Linux                          | macOS (Homebrew)                                    |
|--------------------|--------------------------------|-----------------------------------------------------|
| PG admin user      | `postgres` (or custom)         | Your macOS username (`$(whoami)`), no password       |
| `extenddb init` flags  | defaults usually fine          | pass `--pg-user $(whoami)`                          |
| Service manager    | `systemctl` / `pg_ctl`         | `brew services` or `pg_ctl`                         |
| Syslog reader      | `journalctl -t extenddb`           | `log stream --predicate 'processImagePath ENDSWITH "extenddb"'` |

## Troubleshooting

| Symptom                                                | Fix                                                                 |
|--------------------------------------------------------|---------------------------------------------------------------------|
| `connection refused` on port 8000                      | Server not running. `./target/release/extenddb serve --config extenddb.toml`|
| `Catalog version X.Y.Z (binary expects A.B.C)`        | `./target/release/extenddb migrate --config extenddb.toml`                  |
| `role "extenddb" does not exist` during init               | Re-run with `--pg-user $(whoami)`                                   |
| DROP DATABASE hangs after hard kill                    | Check for lingering backends: `ps -eo pid,command \| grep postgres` |

See `docs/troubleshooting.md` for the full troubleshooting guide.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
