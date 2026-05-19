# Init Stage

## Purpose

`extenddb init` bootstraps a fresh deployment in a single command. It creates the Postgres user and two databases, generates the AES-256-GCM encryption key, provisions the admin user, emits a self-signed TLS certificate, and writes `extenddb.toml` to the repository root. The admin username, admin password, and account ID are printed to stdout exactly once. None of the three values can be retrieved later, so credential capture is part of this stage, not an afterthought.

## Prerequisite check

Before running `extenddb init`, confirm there is no existing deployment. Two checks matter, and they check different things.

First, check whether a config file is already present at the repo root.

```bash
test -f extenddb.toml && echo "extenddb.toml already exists" || echo "extenddb.toml absent"
```

`extenddb.toml` on its own does not block `extenddb init`. If `extenddb.toml` exists, `init` loads defaults from it and continues. The real blocker is whether the `extenddb_catalog` and `extenddb` data databases exist in PostgreSQL.

Second, check whether a prior deployment is present in PostgreSQL.

```bash
./target/release/extenddb status --config extenddb.toml
```

If `extenddb status` reports a deployment, `extenddb init` will abort with:

```
Database '<name>' already exists. Run 'extenddb destroy --config <config>' first, then re-run 'extenddb init'.
```

This abort is driven by the PostgreSQL databases, not by `extenddb.toml`. The docs are explicit: "`extenddb init` will abort if either the catalog or data database already exists" (`docs/getting-started.md`), and "`extenddb init` detected that the catalog or data database already exists in PostgreSQL" (`docs/troubleshooting.md`).

To clear the blocker, drop both databases:

```bash
./target/release/extenddb destroy --config extenddb.toml --yes
```

This command is destructive. It drops the `extenddb_catalog` and `extenddb` data databases and removes all table definitions, items, stream records, IAM users, and access keys. The user reviews and invokes it manually. This skill does not run `extenddb destroy` on the user's behalf.

## Partial-init recovery: the TLS-cert-only path

If `extenddb init` crashed partway through and the `~/.extenddb/tls/cert.pem` file is missing but the databases are intact, there are two recovery paths.

**Path A, cert-only regeneration.** Use this when the admin credentials from the prior init run were captured and saved. This avoids destroying the databases and keeps the existing admin user.

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout ~/.extenddb/tls/key.pem \
  -out ~/.extenddb/tls/cert.pem \
  -days 3650 \
  -subj "/CN=extenddb self-signed/O=extenddb" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

chmod 600 ~/.extenddb/tls/key.pem
chmod 644 ~/.extenddb/tls/cert.pem
```

Adjust the `-addext` SANs to match the addresses clients will use. Every address in `endpoint_url` must be a SAN or TLS verification fails. After regenerating, run `extenddb verify --config extenddb.toml` then `extenddb serve`.

**Path B, destroy and reinitialize.** Use this when the admin credentials are lost, when `extenddb verify` reports catalog problems, or when a clean slate is preferred. The admin password is bcrypt-hashed and cannot be retrieved; there is no documented admin password reset flow outside destroy-and-reinit.

```bash
./target/release/extenddb destroy --config extenddb.toml --yes
rm -f ~/.extenddb/tls/cert.pem ~/.extenddb/tls/key.pem
./target/release/extenddb init
```

## Standard init command

Once the prerequisite check confirms no prior deployment, present the standard command.

```bash
./target/release/extenddb init
```

The default flags work for a local PostgreSQL instance running on the same host with peer or ident authentication. Remote Postgres and Aurora variants are below.

## The six artifacts init creates

| Artifact | Where |
|---|---|
| `extenddb` PostgreSQL user | Postgres server |
| `extenddb_catalog` database | Postgres server |
| `extenddb` data database | Postgres server |
| AES-256-GCM encryption key | `~/.extenddb/keys/master.key` (confirm exact path from init output) |
| Admin user with one-time password | Printed to stdout once |
| Self-signed TLS certificate | `~/.extenddb/tls/cert.pem` and `~/.extenddb/tls/key.pem` |

Init also writes `extenddb.toml` at the repository root.

## Capture one-time credentials

> IMPORTANT. The admin username, admin password, and account ID are printed to stdout exactly once during `extenddb init`. The admin password cannot be retrieved later. Copy all three values to a password manager or secure note before continuing.

## Remote PostgreSQL or Aurora variant

If the user's Postgres is not local, supply connection flags explicitly.

```bash
./target/release/extenddb init \
    --pg-host <hostname> \
    --pg-port 5432 \
    --pg-user <postgres-superuser> \
    --pg-pass <password>
```

> The `--pg-pass` flag is visible in `ps` output on shared hosts. In shared environments, prefer the `PGPASSWORD` environment variable or a `.pgpass` file. See ``references/postgres/03-connection-strings.md`` for the full set of connection patterns.

## Custom catalog database name

```bash
./target/release/extenddb init --catalog-db my_catalog
```

The default `extenddb_catalog` works for most users. Override only when a naming convention or multi-tenant Postgres arrangement requires it.

## Post-init verification

```bash
./target/release/extenddb verify --config extenddb.toml
```

The verify command confirms schema version, catalog and data database connectivity, and TLS certificate validity. All checks must pass before proceeding to `extenddb serve`. If any check fails, do not advance; the failure indicates the init did not complete cleanly and the server will not start.

## Common init failures

For specific error messages during init, consult ``references/troubleshooting/01-symptom-index.md``. Return here when the error is resolved.
