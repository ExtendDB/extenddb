<!-- Copyright 2026 ExtendDB contributors -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# extenddb Tests

Dual-target test suite that runs against both real DynamoDB and extenddb.

## Running against extenddb

extenddb ships with TLS and builtin auth mandatory. All tests must run against
a server configured with TLS and valid credentials.

### Quick start

```bash
# 1. Build and initialize (first time only)
cargo build --release
./target/release/extenddb init --config extenddb.toml
# Save the admin password printed during init!

# 2. Start the server
./target/release/extenddb serve --config extenddb.toml

# 3. Provision test credentials and run tests
export EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:8000
export EXTENDDB_ADMIN_USER=admin
export EXTENDDB_ADMIN_PASSWORD=<password-from-init>
eval $(python3 devtools/provision-test-credentials)
pytest tests/ -v
```

### Using the test runner script (recommended)

```bash
export EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:8000
export EXTENDDB_ADMIN_USER=admin
export EXTENDDB_ADMIN_PASSWORD=<password-from-init>
devtools/run-tests --extenddb --all
```

The test runner script automatically provisions test credentials and configures the Java truststore for external tests.

### TiDB acceptance loop

For TiDB backend work, use the focused acceptance loop before falling back to
the full integration suite:

```bash
# Select checks from files changed from HEAD
devtools/tidb-acceptance --changed

# Select checks and start/stop a local TiUP playground when needed
devtools/tidb-acceptance --changed --with-playground

# Run the full TiDB backend developer gate
devtools/tidb-acceptance --full
```

`devtools/tidb-acceptance --changed` maps touched files to the relevant shell,
diff, live-smoke, Rust, and documentation checks. `--full` runs the complete
TiDB developer gate: shell checks, whitespace, the live native-read smoke,
`storage-tidb` tests and clippy, `extenddb --features tidb` tests and clippy,
and documentation build.

Pass `--with-playground` to let the acceptance loop start a local TiUP
playground only when `127.0.0.1:4000` is down, wait for TiDB readiness, run the
selected gate, and stop only the playground process it started. Use
`--playground-version` or `--playground-tag` to override the managed TiUP
defaults.

The live smoke uses `devtools/tidb-native-read-smoke`, which defaults to
`127.0.0.1:4000` as `root` with no password. It creates a throwaway database,
verifies session-level stale reads through both the base table and a forced TiDB
native secondary index, then drops the database on exit. If your mysql client
needs an explicit auth plugin directory, set `EXTENDDB_TIDB_MYSQL_PLUGIN_DIR` or
pass `--plugin-dir`.

## Running against real DynamoDB

```bash
# Ensure AWS credentials are configured
pytest tests/ -v
```

## Running auth integration tests

Auth tests require extenddb running with `auth.provider = "builtin"` (the default):

```bash
EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:8000 \
EXTENDDB_ADMIN_USER=admin \
EXTENDDB_ADMIN_PASSWORD=<admin-password-from-init> \
pytest tests/test_auth_integration.py tests/test_auth_error_fidelity.py -v
```

## Running auth error fidelity tests

Auth error fidelity tests validate that bad credentials produce the same errors
as real DynamoDB. They run automatically against real DynamoDB (no env vars needed).
Against extenddb, they require auth mode — set `EXTENDDB_ADMIN_USER` to signal this:

```bash
EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:8000 \
EXTENDDB_ADMIN_USER=admin \
pytest tests/test_auth_error_fidelity.py -v
```

These tests are skipped when `EXTENDDB_TEST_ENDPOINT` is set without `EXTENDDB_ADMIN_USER`
(i.e., extenddb running without auth credentials configured in the test environment).

## TLS and self-signed certificates

`extenddb init` generates a self-signed TLS certificate. The test infrastructure
handles this automatically:

- **Python tests:** `verify=False` is set on all boto3 and requests clients
  when the endpoint starts with `https://`.
- **Java tests:** The test runner script creates a temporary Java truststore
  from the self-signed cert and passes it via `JAVA_TOOL_OPTIONS`.
- **curl:** Health checks use `curl -sk` to accept self-signed certs.

## Design

- Tests use `conftest.py` fixtures for client creation and cleanup
- `EXTENDDB_TEST_ENDPOINT` controls the target: set for extenddb, unset for real DynamoDB
- All tests clean up created resources, even on failure (REQ-TEST-005)
- No target-specific branching — if behavior differs, it's a bug (REQ-TEST-004)
- Auth tests use `management_helpers.py` to provision identities via the management API
- Auth tests are skipped when env vars are not set (safe for CI without auth infra)

## External Java Test Suite

The external Java test suite lives in `tests/external/java/` and requires Java 17+ and Maven 3.6+.

### Installing Java and Maven

**Amazon Linux / RHEL / CentOS:**

```bash
# Java 17 (Amazon Corretto)
sudo yum install -y --disablerepo='pgdg*' java-17-amazon-corretto-devel

# Maven 3.9.6 (system Maven is often too old)
curl -sL https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz | sudo tar xz -C /opt
sudo ln -sf /opt/apache-maven-3.9.6/bin/mvn /usr/local/bin/mvn

# Verify
java -version   # should show 17.x
mvn --version   # should show 3.9.6
```

**Ubuntu / Debian:**

```bash
sudo apt-get install -y openjdk-17-jdk maven
```

**macOS (Homebrew):**

```bash
brew install openjdk@17 maven
```

### Running the external Java tests

```bash
# Start extenddb first, then:
devtools/run-external-tests --endpoint https://127.0.0.1:8000
```

Or manually:

```bash
cd tests/external/java
mvn test -Dextenddb.endpoint=https://localhost:8000 2>&1 | tail -20
```
