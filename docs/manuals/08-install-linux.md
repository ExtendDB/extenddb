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

- Rust 1.85+ (`rustup update`)
- TiDB 8.5.4+ and a MySQL-compatible client
- Python 3.10+ (for test suites)
- AWS CLI v2 (for testing)

## 1. Install a TiDB client and start TiDB

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install -y default-mysql-client
```

**Fedora/RHEL 9+ and Amazon Linux 2023:**

```bash
sudo dnf install -y mysql
```

Start a local TiDB playground in a separate terminal, or use an existing TiDB
cluster:

```bash
tiup playground v8.5.4 --db 1 --pd 1 --kv 3 --without-monitor
```

Verify it's accepting connections:

```bash
mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"
```

## 2. Build extenddb

```bash
cargo build -j12 --release
```

Binary lands at `target/release/extenddb`.

## 3. Initialize the deployment

`extenddb init` creates the TiDB `extenddb` SQL user, the catalog and data
databases, applies schema migrations, generates an encryption key,
creates a default account + admin user, and writes `extenddb.toml` for you.
Do **not** hand-write `extenddb.toml` before running `init`.

For local TiUP playground, the default TiDB admin user is `root` with no
password, so no storage flags are needed:

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
  OK: Catalog version 0.0.27
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
cargo build -j12 --release
./target/release/extenddb migrate --config extenddb.toml
```

No data is lost; only the catalog schema is updated.

## Tearing it all down

```bash
# Stop the server
./target/release/extenddb stop --config extenddb.toml

# Drop both databases and the extenddb SQL user
./target/release/extenddb destroy --config extenddb.toml --yes
```

## Troubleshooting

| Symptom                                                | Fix                                                                 |
|--------------------------------------------------------|---------------------------------------------------------------------|
| `connection refused` on port 8000                      | Server not running. `./target/release/extenddb serve --config extenddb.toml`|
| `Catalog version X.Y.Z (binary expects A.B.C)`        | `./target/release/extenddb migrate --config extenddb.toml`                  |
| `Cannot connect as admin` during init                  | Confirm TiDB is reachable: `mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"` |
| TiDB version is too old                                | Upgrade TiDB to 8.5.4+ so native non-unique `GLOBAL` indexes are available |
| DROP DATABASE hangs after hard kill                    | Check for lingering sessions with TiDB statement/processlist diagnostics |

See `docs/troubleshooting.md` for the full troubleshooting guide.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
