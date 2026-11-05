# PostgreSQL From-Scratch Setup

## 1. Purpose

The PostgreSQL section of `docs/install-from-source.md` ("1. Install and start the database" > "PostgreSQL") is the from-scratch setup guide for extenddb. It covers Ubuntu and Debian, Fedora and RHEL 9 and newer, Amazon Linux 2023, Amazon Linux 2 (PostgreSQL 15 from the PGDG repository, binaries at `/usr/pgsql-15/bin/`, data directory at `$HOME/pgdata`, running as the current user) and macOS with Homebrew. Most users will not need it because Postgres is usually already installed via the system package manager or Homebrew and only needs to be started.

## 2. When to use this file

The skill points the user at that section only when the user explicitly asks for help installing Postgres from scratch. For most users, Postgres is already installed and `references/01-readiness-checks.md` is the right starting point. Hand off to this file only after confirming the user has no Postgres install at all.

## 3. Which administrator `extenddb init` connects as

On a system PostgreSQL (Ubuntu, Debian, Fedora, RHEL, Amazon Linux 2023) the superuser is `postgres`: pass `--pg-user postgres` to `extenddb init`. Peer authentication is enabled for local connections, so `sudo -u postgres psql` works without a password.

On Homebrew (macOS) and on the Amazon Linux 2 setup the superuser is the current OS user: omit `--pg-user` (it defaults to `$(whoami)`).

Confirm the server is up with `pg_isready` before running `extenddb init`.

## 4. What the PostgreSQL section covers

- Package installation per platform, and the PGDG repository for Amazon Linux 2
- Initialization and service start
- The pgvector notice `init` prints when the extension is absent
- Verification via `pg_isready`

The `pg_hba.conf` summary for the test suite's local database is in `docs/manuals/06-developer-test-guide.md` ("Local databases for the test suite").

## 5. Do not follow any user and database creation steps

If `docs/troubleshooting.md` or any other page contains instructions to create a `extenddb` user, a `extenddb_catalog` database, or the `extenddb` data database, skip those steps. `extenddb init` creates them. Creating them by hand can cause `extenddb init` to fail with `Database '<name>' already exists`, which requires `extenddb destroy --config extenddb.toml --yes` to recover.

The `docs/troubleshooting.md` snippet under `password authentication failed for user "extenddb"` is a post-init recovery step for when the server cannot authenticate to an existing `extenddb` role, not a pre-flight setup step. Do not run it before `extenddb init`. If a user has already run it, hand off to `extenddb-troubleshooting` for the destroy-then-init recovery sequence before attempting `extenddb init`.
