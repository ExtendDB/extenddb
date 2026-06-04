# Dependency Checks

## Purpose

This file lists the dependency checks the `extenddb-setup` skill runs before proceeding past the dependencies stage. Rust, PostgreSQL (client and server), and Python 3 must all be present and at supported versions before the user runs `cargo build --release` or `extenddb init`. Missing dependencies should be discovered here, not partway through a failed build or a hanging `extenddb init`.

## Required dependencies

| Dependency | Check command | Minimum version | Rationale |
|---|---|---|---|
| Rust toolchain | `cargo --version` and `rustc --version` | 1.88 | extenddb is a Rust workspace; older toolchains fail `cargo build --release`. |
| PostgreSQL client | `psql --version` | 14 | extenddb speaks the Postgres protocol at init and runtime; older clients may lack required features. |
| PostgreSQL server readiness | `pg_isready` | n/a (server-side check) | Confirms the server is reachable before `extenddb init`. |
| Python 3 | `python3 --version` | 3.10 | Required by the sample apps and the docs build pipeline. |

## Per-dependency check logic

### cargo and rustc

Check:

```bash
which cargo && which rustc && cargo --version && rustc --version
```

If `which cargo` exits nonzero, Rust is not installed. Install:

- Linux (Ubuntu/Debian): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- Linux (Fedora/RHEL): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- macOS: `brew install rustup-init && rustup-init`

If Rust is installed but `rustc --version` reports older than 1.88:

```bash
rustup update
```

### psql

Check:

```bash
which psql && psql --version
```

If `which psql` exits nonzero, the PostgreSQL client is not installed. Install:

- Linux (Ubuntu/Debian): `sudo apt install postgresql`
- Linux (Fedora/RHEL): `sudo dnf install postgresql-server`
- macOS: `brew install postgresql@17`

If `psql --version` reports older than 14, see the Postgres version mismatch section below.

### pg_isready

Check:

```bash
which pg_isready && pg_isready
```

`pg_isready` ships with the PostgreSQL client, so `which pg_isready` normally succeeds whenever `psql` does. The meaningful check is the second invocation: `pg_isready` exits 0 when the server is accepting connections and nonzero otherwise. If the exit code is nonzero, the PostgreSQL server is not reachable at the default host and port. Start it:

- Linux (Ubuntu/Debian, Fedora/RHEL): `sudo systemctl start postgresql`
- macOS: `brew services start postgresql@17`

If the server still refuses connections after starting, hand off to ``references/postgres/01-readiness-checks.md``.

### python3

Check:

```bash
which python3 && python3 --version
```

If `which python3` exits nonzero, Python 3 is not installed. Install:

- Linux (Ubuntu/Debian): `sudo apt install python3 python3-venv`
- Linux (Fedora/RHEL): `sudo dnf install python3`
- macOS: `brew install python3`

If `python3 --version` reports older than 3.10, upgrade via the same package manager command.

## Version parsing

`rustc --version` prints `rustc 1.88.0 (abcdef0 2025-01-01)`. Extract the version field with `awk`:

```bash
rustc --version | awk '{print $2}'
```

`psql --version` prints `psql (PostgreSQL) 14.11`. Extract the version field with `awk`:

```bash
psql --version | awk '{print $3}'
```

Compare the extracted version against the minimum (1.88 for Rust, 14 for Postgres) by splitting on `.` and comparing numerically. For skill-level checks, a string prefix comparison (`[[ "$PG_VER" < "14" ]]`) is adequate because major versions are single or double digits.

## Rust version upgrade path

If Rust is installed via rustup and `rustc --version` reports older than 1.88, the fix is:

```bash
rustup update
```

If Rust was installed via `apt`, `dnf`, or another system package manager rather than rustup, `rustup update` will not work. Remove the system Rust first, then install via rustup:

- Linux (Ubuntu/Debian): `sudo apt remove rustc cargo && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- Linux (Fedora/RHEL): `sudo dnf remove rust cargo && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

System Rust packages lag behind the rustup channel by months or years. rustup is the supported path for extenddb development.

## Postgres version mismatch

If `psql --version` reports older than 14, do not proceed with `extenddb init`. Stop the setup flow and report the mismatch to the user. extenddb uses Postgres features that older servers do not support, and `extenddb init` will fail or produce an unusable catalog.

The fix is to upgrade Postgres to 14 or newer. The upgrade path is platform-specific and out of scope for this skill; point the user at ``references/postgres/01-readiness-checks.md`` for setup guidance and at their package manager documentation for the upgrade.

## Install script alternative

For users who prefer a one-command setup, `scripts/install-linux.sh` and `scripts/install-macos.sh` run all dependency checks automatically, report any missing pieces, and exit with a nonzero code so the user can install them. The install scripts do not install missing dependencies on the user's behalf, but they produce the same check output as the per-dependency commands above in a single pass, and they proceed to `cargo build --release` and the Python venv setup once dependencies are satisfied.

Invoke:

- Linux: `scripts/install-linux.sh`
- macOS: `scripts/install-macos.sh`

The skill never invokes the install script on the user's behalf. Present the command and let the user run it.
