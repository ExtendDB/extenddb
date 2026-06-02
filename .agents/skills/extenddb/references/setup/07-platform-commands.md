# Platform Commands

## Purpose

This file is the side-by-side Linux versus macOS command reference for each stage of the onboarding journey. The platform detection block in `SKILL.md` reads `uname -s` and routes the skill to the appropriate column below. This file also covers the Windows fallback (WSL2).

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
| Install MySQL client | `sudo apt install default-mysql-client` (Debian/Ubuntu) or `sudo dnf install mysql` (Fedora/RHEL) | `brew install mysql-client` |
| Start local TiDB | `tiup playground v8.5.4 --db 1 --pd 1 --kv 3 --without-monitor` | `tiup playground v8.5.4 --db 1 --pd 1 --kv 3 --without-monitor` |
| Verify TiDB running | `mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"` | `mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"` |
| `--storage-admin-user` for local TiDB playground | `root` | `root` |
| Tail extenddb logs | `journalctl -t extenddb -f` | `log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info` |
| Read last N log lines | `journalctl -t extenddb -n 50` | `log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 5m` |
| Install script | `scripts/install-linux.sh` | `scripts/install-macos.sh` |
| Install Rust toolchain | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` (or `brew install rustup-init && rustup-init`) |

## TiDB playground defaults

Local TiUP playground exposes TiDB SQL on `127.0.0.1:4000` and uses the `root`
SQL user without a password by default. That matches the standard `extenddb
init` defaults, so no storage flags are needed for local playground.

## Windows: not a supported platform

extenddb does not support native Windows. The recommended path for Windows users is WSL2 (Windows Subsystem for Linux version 2) with Ubuntu 22.04 or later.

WSL2 setup steps:

1. Open PowerShell as Administrator and run `wsl --install -d Ubuntu-22.04`.
2. Reboot when prompted.
3. Launch Ubuntu from the Start menu and create a user when prompted.
4. Inside the Ubuntu shell, clone the repo and restart this skill from the top.

extenddb has not been tested on Windows native, WSL1, or WSL2 distributions older than Ubuntu 22.04. The Linux install script works inside WSL2 Ubuntu and is the expected path once the user is inside the WSL2 shell.
