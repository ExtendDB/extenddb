# PostgreSQL From-Scratch Setup

## 1. Purpose

`docs/local-postgres-setup.md` is the authoritative from-scratch Postgres setup guide for extenddb. It was written for Amazon Linux 2 and uses the PGDG (PostgreSQL Global Development Group) repositories, with binaries at `/usr/pgsql-15/bin/` and a data directory at `$HOME/pgdata`. Users on other platforms will need to adapt it, and most users will not need it at all because Postgres is usually already installed via the system package manager or Homebrew and only needs to be started.

## 2. When to use this file

The skill points the user at `docs/local-postgres-setup.md` only when the user explicitly asks for help installing Postgres from scratch. For most users, Postgres is already installed and `references/01-readiness-checks.md` is the right starting point. Hand off to this file only after confirming the user has no Postgres install at all.

## 3. Adaptation: macOS with Homebrew

On macOS, the simpler path is Homebrew:

```bash
brew install postgresql@17
brew services start postgresql@17
```

The Homebrew Postgres installs the superuser as the current macOS user, not `postgres`. Use `$(whoami)` for the `--pg-user` flag in `extenddb init`. Skip the `docs/local-postgres-setup.md` steps.

## 4. Adaptation: Debian and Ubuntu

The system package installs, initializes, and starts Postgres in one command:

```bash
sudo apt install postgresql
```

Confirm with `pg_isready`. The superuser is `postgres`. Peer authentication is enabled for local connections, so `sudo -u postgres psql` works without a password. Skip the PGDG repo steps in `docs/local-postgres-setup.md`.

## 5. Adaptation: Fedora and RHEL

The system package installs but does not initialize or start:

```bash
sudo dnf install postgresql-server
sudo postgresql-setup --initdb
sudo systemctl enable --now postgresql
```

The PGDG steps in `docs/local-postgres-setup.md` are optional. The system package works for the onboarding walkthrough.

## 6. Adaptation: Amazon Linux 2

Follow `docs/local-postgres-setup.md` exactly. The doc was written for this platform.

## 7. What `docs/local-postgres-setup.md` covers

- PGDG repository setup
- PostgreSQL 14 or newer installation
- Initialization and service start
- `pg_hba.conf` adjustment for local connections
- Verification via `psql` and `pg_isready`

## 8. Do not follow the doc's user and database creation steps

If `docs/local-postgres-setup.md` or `docs/troubleshooting.md` contain instructions to create a `extenddb` user, a `extenddb_catalog` database, or the `extenddb` data database, skip those steps. `extenddb init` creates them. Creating them by hand can cause `extenddb init` to fail with `Database '<name>' already exists`, which requires `extenddb destroy --config extenddb.toml --yes` to recover.

The `docs/troubleshooting.md` snippet under `password authentication failed for user "extenddb"` is a post-init recovery step for when the server cannot authenticate to an existing `extenddb` role, not a pre-flight setup step. Do not run it before `extenddb init`. If a user has already run it, hand off to `extenddb-troubleshooting` for the destroy-then-init recovery sequence before attempting `extenddb init`.
