# Connection Strings for `extenddb init`

## 1. Purpose

`extenddb init` connects to PostgreSQL with four flags: `--pg-host`, `--pg-port`, `--pg-user`, `--pg-pass`. This file lists the connection patterns for local Postgres, remote Postgres, and Aurora, plus the security considerations for passing credentials on the command line.

## 2. Local PostgreSQL (default)

```bash
./target/release/extenddb init
```

No flags needed when Postgres is on localhost, listening on port 5432, and the current user can authenticate via peer or ident. Works out of the box for:

- Linux system Postgres (superuser: `postgres`, requires `sudo -u postgres` or peer authentication on the socket)
- Homebrew Postgres on macOS (superuser: `$(whoami)`, peer auth on the socket)

If the default fails with "password authentication failed," supply the user explicitly:

```bash
./target/release/extenddb init --pg-user postgres --pg-pass <password>
```

## 3. Remote PostgreSQL

```bash
./target/release/extenddb init \
    --pg-host <hostname> \
    --pg-port 5432 \
    --pg-user <postgres-superuser> \
    --pg-pass <password>
```

The bootstrap user needs `CREATEROLE` and `CREATEDB` privileges. `postgres` has both by default. A custom admin user works if it has both privileges.

## 4. Aurora PostgreSQL

```bash
./target/release/extenddb init \
    --pg-host <cluster-endpoint>.region.rds.amazonaws.com \
    --pg-port 5432 \
    --pg-user <master-user> \
    --pg-pass <master-password>
```

Notes:

- Use the cluster endpoint (writer), not a read replica.
- The master user has the required privileges for `extenddb init`.
- SSL is typically required. If `extenddb init` fails with SSL errors, consult the Aurora documentation for `ssl_mode` settings. extenddb connects over SSL when the server requires it.

## 5. Password handling: avoid `--pg-pass` on shared hosts

> The `--pg-pass` flag is visible in `ps` output. Every user on the same host can read it. In shared environments, do not use this flag.

The three supported alternatives:

**Option 1: `PGPASSWORD` environment variable**

```bash
export PGPASSWORD='<password>'
./target/release/extenddb init --pg-host <hostname> --pg-user <user>
```

`extenddb init` reads `PGPASSWORD` from the environment when `--pg-pass` is not supplied. Unset `PGPASSWORD` after init completes.

**Option 2: `.pgpass` file**

Create `~/.pgpass` with 0600 permissions:

```
<hostname>:<port>:*:<user>:<password>
```

Then:

```bash
chmod 600 ~/.pgpass
./target/release/extenddb init --pg-host <hostname> --pg-user <user>
```

**Option 3: IAM authentication (Aurora only)**

For Aurora clusters with IAM authentication enabled:

```bash
export PGPASSWORD=$(aws rds generate-db-auth-token --hostname <cluster-endpoint> --port 5432 --username <user>)
./target/release/extenddb init --pg-host <hostname> --pg-user <user>
```

The IAM token is a 15-minute credential. `extenddb init` completes in seconds, so the window is sufficient.

## 6. Connection verification

Before running `extenddb init`, verify the connection works with the same credentials:

```bash
PGPASSWORD='<password>' psql -h <hostname> -U <user> -c "SELECT 1"
```

If this succeeds, `extenddb init` should also succeed. If this fails, debug the connection first; `extenddb init` will produce the same error with less clarity.
