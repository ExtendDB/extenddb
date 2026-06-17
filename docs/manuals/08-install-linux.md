# Installing extenddb on Linux

> See [NOTICE](../NOTICE.md) for important disclaimers.

This guide is the Linux-specific path for the generic instructions in
`docs/getting-started.md`. See that doc for the full CLI surface and
feature walkthrough.

## Quick Install (recommended)

The installer script checks dependencies, builds extenddb, sets up a Python
venv for documentation, and builds PDF manuals:

```bash
scripts/install-linux.sh
```

The script does **not** install missing dependencies — it reports what's
missing so you can install them with your package manager, then re-run.

After the script completes, skip to [Step 3: Initialize the deployment](#3-initialize-the-deployment).

## Manual Installation

If you prefer to run each step yourself, follow the sections below.

## Prerequisites

- Rust 1.88+ (`rustup update`)
- PostgreSQL 14+
- Python 3.10+ (for test suites)
- AWS CLI v2 (for testing)

## 1. Install and start PostgreSQL

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install -y postgresql postgresql-client
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**Amazon Linux 2 (PGDG):**

> **Note:** These instructions target Amazon Linux 2 (EL-7 compatible). For Amazon Linux 2023, use `sudo dnf install -y postgresql15-server postgresql15` directly — no PGDG workaround needed.

```bash
sudo rpm -ivh --nodeps \
    https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
sudo yum install -y --releasever=7 \
    --disablerepo="*" --enablerepo="pgdg15" \
    postgresql15-server postgresql15
/usr/pgsql-15/bin/initdb -D ~/pgdata --auth=trust --no-locale --encoding=UTF8
/usr/pgsql-15/bin/pg_ctl -D ~/pgdata -l ~/pgdata/server.log start
```

**Fedora/RHEL 9+:**

```bash
sudo dnf install -y postgresql-server postgresql
sudo postgresql-setup --initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

Verify it's accepting connections:

```bash
pg_isready
# /var/run/postgresql:5432 - accepting connections
```

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

On most Linux systems the PostgreSQL admin user is `postgres`:

```bash
./target/release/extenddb init --pg-user postgres
```

If you run PostgreSQL as your own user (e.g., Amazon Linux 2 with a
user-owned data directory), omit `--pg-user` — it defaults to `$(whoami)`:

```bash
./target/release/extenddb init
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

Read logs:

```bash
journalctl -t extenddb -f          # follow live
journalctl -t extenddb --since "5 minutes ago"
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

## Troubleshooting

| Symptom                                                | Fix                                                                 |
|--------------------------------------------------------|---------------------------------------------------------------------|
| `connection refused` on port 8000                      | Server not running. `./target/release/extenddb serve --config extenddb.toml`|
| `Catalog version X.Y.Z (binary expects A.B.C)`        | `./target/release/extenddb migrate --config extenddb.toml`                  |
| `role "postgres" does not exist`                       | Use `--pg-user $(whoami)` if PG runs as your user                   |
| `FATAL: Peer authentication failed`                    | Edit `pg_hba.conf` to allow `trust` or `md5` for local connections  |
| DROP DATABASE hangs after hard kill                    | Check for lingering backends: `ps -eo pid,command \| grep postgres` |

See `docs/troubleshooting.md` for the full troubleshooting guide.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
