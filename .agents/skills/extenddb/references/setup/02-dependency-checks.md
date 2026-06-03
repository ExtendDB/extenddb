# Dependency Checks

## Purpose

This file lists the dependency checks the `extenddb-setup` skill runs before proceeding past the dependencies stage. Rust, a TiDB/MySQL client, TiDB SQL endpoint readiness, and Python 3 must all be checked before the user runs `cargo build -j12 --release` or `extenddb init`. Missing dependencies should be discovered here, not partway through a failed build or a hanging `extenddb init`.

## Required dependencies

| Dependency | Check command | Minimum version | Rationale |
|---|---|---|---|
| Rust toolchain | `cargo --version` and `rustc --version` | 1.88 | extenddb is a Rust workspace; older toolchains fail `cargo build -j12 --release`. |
| TiDB/MySQL client | `mysql --version` | n/a | Confirms the operator can check the TiDB SQL endpoint before init. |
| TiDB SQL readiness | `mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"` | TiDB 8.5.4+ | Confirms the default backend is reachable before `extenddb init`. |
| PostgreSQL client | `psql --version` | 14 | Required only when explicitly building and selecting the PostgreSQL backend. |
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

### mysql

Check:

```bash
which mysql && mysql --version
```

If `which mysql` exits nonzero, the TiDB/MySQL client is not installed. Install:

- Linux (Ubuntu/Debian): `sudo apt install default-mysql-client`
- Linux (Fedora/RHEL): `sudo dnf install mysql`
- macOS: `brew install mysql-client`

### TiDB SQL endpoint

Check:

```bash
mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"
```

The command exits 0 when a local TiDB SQL endpoint is accepting connections.
If it cannot connect, start local TiDB or use remote TiDB init flags:

```bash
tiup playground v8.5.4 --db 1 --pd 1 --kv 3 --without-monitor
```

If the user will initialize against remote TiDB, hand off to
`references/tidb/01-readiness-checks.md` for the `--storage-host`,
`--storage-port`, and credential flags.

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

`mysql --version` prints a client-specific version string. Use the TiDB server
version from `SELECT VERSION()` as the authoritative backend version.

```bash
mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"
```

Compare Rust against 1.88, TiDB against 8.5.4+, and PostgreSQL against 14 only when the PostgreSQL backend is selected. Split dotted versions on `.` and compare numerically.

## Rust version upgrade path

If Rust is installed via rustup and `rustc --version` reports older than 1.88, the fix is:

```bash
rustup update
```

If Rust was installed via `apt`, `dnf`, or another system package manager rather than rustup, `rustup update` will not work. Remove the system Rust first, then install via rustup:

- Linux (Ubuntu/Debian): `sudo apt remove rustc cargo && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- Linux (Fedora/RHEL): `sudo dnf remove rust cargo && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

System Rust packages lag behind the rustup channel by months or years. rustup is the supported path for extenddb development.

## TiDB version mismatch

If `SELECT VERSION()` reports a TiDB version older than 8.5.4, do not proceed with `extenddb init`. The TiDB backend requires native non-unique `GLOBAL` indexes on partitioned tables; older TiDB versions cannot provide the physical layout ExtendDB uses for DynamoDB secondary-index reads.

The fix is to upgrade TiDB to 8.5.4 or newer.

## Install script alternative

For users who prefer a one-command setup, `scripts/install-linux.sh` and `scripts/install-macos.sh` run all dependency checks automatically, report any missing pieces, and exit with a nonzero code so the user can install them. The install scripts do not install missing dependencies on the user's behalf, but they produce the same check output as the per-dependency commands above in a single pass, and they proceed to `cargo build -j12 --release` and the Python venv setup once dependencies are satisfied.

Invoke:

- Linux: `scripts/install-linux.sh`
- macOS: `scripts/install-macos.sh`

The skill never invokes the install script on the user's behalf. Present the command and let the user run it.
