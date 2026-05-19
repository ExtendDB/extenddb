# PostgreSQL Symptoms

This file holds verbatim Cause and Fix entries for PostgreSQL-related extenddb symptoms: server connectivity, authentication, and migration errors. These are the first symptoms a user typically hits during `extenddb init`. Content is copied from `docs/troubleshooting.md` and should not be paraphrased.

## Symptom 1: Connection refused

### `error connecting to server: Connection refused`

<a name="connection-refused"></a>

**Error text:**
```
error connecting to server: Connection refused
```

**Cause:** PostgreSQL is not running or not listening on the configured host/port.

**Fix:**
```bash
pg_ctl -D ~/pgdata status          # check if running
pg_ctl -D ~/pgdata -l ~/pgdata/server.log start  # start it
```

**Source:** `docs/troubleshooting.md`, section "`error connecting to server: Connection refused`", last synced 2026-05-12.

## Symptom 2: password authentication failed for user "extenddb"

### `password authentication failed for user "extenddb"`

<a name="password-authentication-failed"></a>

**Error text:**
```
password authentication failed for user "extenddb"
```

**Cause:** The PostgreSQL `extenddb` user doesn't exist or the password doesn't match.

**Fix:**
```bash
# Substitute "$(whoami)" with whichever PostgreSQL superuser owns the local cluster
# (commonly the same as the OS user on macOS Homebrew installs, or "postgres" on Linux).
psql -U "$(whoami)" -d postgres -c "CREATE USER extenddb WITH PASSWORD 'extenddb-local-dev';"
psql -U "$(whoami)" -d postgres -c "CREATE DATABASE extenddb OWNER extenddb;"
```

See `docs/local-postgres-setup.md` for full setup instructions.

**Source:** `docs/troubleshooting.md`, section "`password authentication failed for user "extenddb"`", last synced 2026-05-12.

## Symptom 3: migration failed

### `migration failed: ...`

<a name="migration-failed"></a>

**Error text:**
```
migration failed: ...
```

**Cause:** The PostgreSQL database exists but the migration SQL failed (permissions, schema conflicts, etc.).

**Fix:** Check the PostgreSQL logs (`~/pgdata/server.log`). Ensure the `extenddb` user has CREATE TABLE permissions on the `extenddb` database.

**Source:** `docs/troubleshooting.md`, section "`migration failed: ...`", last synced 2026-05-12.

## Non-destructive note

Remediation commands in the Fix sections (for example, `extenddb destroy --yes`) are destructive. The skill presents them but does not run them. The user invokes them.
