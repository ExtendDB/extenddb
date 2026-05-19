# Platform Commands

## Purpose

This file is the side-by-side Linux versus macOS command reference for each stage of the onboarding journey. The platform detection block in `SKILL.md` reads `uname -s` and routes the skill to the appropriate column below. This file also covers the Windows fallback (WSL2) and the extra initialization step required on Fedora and RHEL.

## Platform detection command

```bash
uname -s
```

- `Linux` routes to the Linux column below.
- `Darwin` routes to the macOS column below.
- Anything else (for example, `MINGW64_NT-*`, `CYGWIN_NT-*`, `MSYS_NT-*`) is Windows; see Section 5 for WSL2 guidance.

## Command table

| Stage | Linux | macOS |
|---|---|---|
| Install Postgres | `sudo apt install postgresql` (Debian/Ubuntu) or `sudo dnf install postgresql-server` (Fedora/RHEL) | `brew install postgresql@17` |
| Start Postgres | `sudo systemctl start postgresql` | `brew services start postgresql@17` |
| Verify Postgres running | `pg_isready -q && echo ready` | `pg_isready -q && echo ready` |
| `--pg-user` for `extenddb init` | `postgres` | `$(whoami)` |
| Tail extenddb logs | `journalctl -t extenddb -f` | `log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info` |
| Read last N log lines | `journalctl -t extenddb -n 50` | `log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 5m` |
| Install script | `scripts/install-linux.sh` | `scripts/install-macos.sh` |
| Install Rust toolchain | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` (or `brew install rustup-init && rustup-init`) |

## Why macOS uses `$(whoami)` for `--pg-user`

Homebrew PostgreSQL creates the superuser as the current macOS user, not `postgres`. The Linux system Postgres package follows convention and creates the `postgres` superuser. This difference matters only for `extenddb init`, where the flag names the bootstrap superuser used to create the `extenddb` role and the `extenddb_catalog` and `extenddb` databases. Every subsequent extenddb command uses the `extenddb` role that init creates, so the platform difference stops at this one flag.

## Windows: not a supported platform

extenddb does not support native Windows. The recommended path for Windows users is WSL2 (Windows Subsystem for Linux version 2) with Ubuntu 22.04 or later.

WSL2 setup steps:

1. Open PowerShell as Administrator and run `wsl --install -d Ubuntu-22.04`.
2. Reboot when prompted.
3. Launch Ubuntu from the Start menu and create a user when prompted.
4. Inside the Ubuntu shell, clone the repo and restart this skill from the top.

extenddb has not been tested on Windows native, WSL1, or WSL2 distributions older than Ubuntu 22.04. The Linux install script works inside WSL2 Ubuntu and is the expected path once the user is inside the WSL2 shell.

## Fedora and RHEL notes

On Fedora and RHEL, `sudo dnf install postgresql-server` installs the server package but does not initialize the data directory or start the service. Run the following two commands before `extenddb init`:

```bash
sudo postgresql-setup --initdb
sudo systemctl enable --now postgresql
```

The first command initializes the cluster at `/var/lib/pgsql/data`. The second enables the service for boot and starts it immediately. After these two commands, `pg_isready -q` should exit 0 and `extenddb init` can proceed. Debian and Ubuntu run both steps automatically as part of the `postgresql` package install, so the extra step is Fedora/RHEL only.
