# Build Stage

## Purpose

This file is the reference for the build stage in `SKILL.md`. The build stage produces the `target/release/extenddb` binary from source. If the binary is already present at `${REPO_ROOT}/target/release/extenddb`, skip this stage and proceed to the Postgres readiness check. The skill presents build commands but does not execute them, per Requirement 14.

## Binary presence check

Run the following read-only check before presenting any build command:

```bash
test -x target/release/extenddb && echo "binary present" || echo "binary missing"
```

If the output is `binary present`, ask the user whether to skip the build or rebuild. The default is skip. A rebuild is only necessary when the user has pulled new commits, changed a `Cargo.toml` dependency, or has reason to believe the existing binary is stale.

If the output is `binary missing`, proceed to the build command section below.

## Build command

The direct build path uses Cargo:

```bash
cargo build --release
```

Notes on build time:

- First build takes several minutes on typical hardware. The cold Cargo cache, the transitive dependency graph, and release-mode optimization all contribute.
- Subsequent incremental builds take seconds. Cargo caches compiled dependencies in `target/` and only recompiles crates that changed.
- The binary appears at `target/release/extenddb`. Verify with `ls -lh target/release/extenddb` once the build completes.

## Platform install script alternative

Two scripts provide a one-command path that wraps the dependency check, the release build, and the Python venv setup.

- Linux: `scripts/install-linux.sh`
- macOS: `scripts/install-macos.sh`

Each script checks that `cargo`, `psql`, `pg_isready`, and `python3` are present, reports missing dependencies with platform-specific install hints, builds extenddb in release mode, creates `.venv` at the repository root, installs `requirements.txt`, and builds the PDF documentation. The scripts do not install missing dependencies. They report them and exit.

The user chooses between `cargo build --release` directly or the install script. The install script is the faster path for a first-time setup on a clean machine. The direct `cargo build --release` is the right path when the user already has a Python venv, does not need the PDFs, or wants to control each step.

## Do not execute

Per Requirement 14, the skill does not run `cargo build --release`, `scripts/install-linux.sh`, or `scripts/install-macos.sh` on the user's behalf. The skill presents the command and lets the user invoke it. Both the Cargo build and the install scripts have side effects (disk writes under `target/`, venv creation, PDF generation) that the user should consciously authorize.

## Verification after build

Once the user reports the build is complete, run:

```bash
./target/release/extenddb --version
```

This should print the extenddb version string. If it does not, the build failed silently. The next step is to re-run the build with the `--verbose` flag to surface the error:

```bash
cargo build --release --verbose
```

Common causes of silent build failure are a missing system library (for example, `libpq-dev` or `openssl-dev` on Linux), a Rust toolchain older than 1.85, or a disk full condition under `target/`. The verbose output names the failing crate and the missing dependency.
