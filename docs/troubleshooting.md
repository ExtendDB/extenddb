# Troubleshooting

## Startup Errors

### `error: config file not found`

**Cause:** The `--config` path doesn't exist or isn't readable.

**Fix:** Ensure the config file exists. Copy from the sample if needed:
```bash
cp extenddb.sample.toml extenddb.toml
```

### `error: configuration property "X" not found`

**Cause:** Typo in `extenddb.toml` or an unrecognized config key.

**Fix:** Compare your config against `extenddb.sample.toml`. Config keys are case-sensitive.

### `error connecting to server: Connection refused`

**Cause:** PostgreSQL is not running or not listening on the configured host/port.

**Fix:**
```bash
pg_ctl -D ~/pgdata status          # check if running
pg_ctl -D ~/pgdata -l ~/pgdata/server.log start  # start it
```

### `Failed to connect to postgres: Connection error: error with configuration: empty host`

**Cause:** The connection string in `extenddb.toml` has an invalid format, typically from using a Unix socket path with `--pg-host` during `extenddb init` in version tagged 0.1.

**Fix:** Edit `extenddb.toml` and manually percent-encode the Unix socket path in the connection string:
```toml
# Before (invalid):
connection_string = "postgresql://extenddb:***@/var/run/postgresql:5432/extenddb_catalog"

# After (valid):
connection_string = "postgresql://extenddb:***@%2Fvar%2Frun%2Fpostgresql:5432/extenddb_catalog"
```

Also update the catalog database:
```bash
psql extenddb_catalog -c "UPDATE settings SET value='postgresql://extenddb:***@%2Fvar%2Frun%2Fpostgresql:5432/extenddb' WHERE key='data_database_connection_string';"
```

**Prevention:** This issue is fixed in versions after 0.1, which automatically encode Unix socket paths correctly.

### `password authentication failed for user "extenddb"`

**Cause:** The PostgreSQL `extenddb` user doesn't exist or the password doesn't match.

**Fix:**
```bash
psql -U $(id -nu) -d postgres -c "CREATE USER extenddb WITH PASSWORD 'extenddb-local-dev';"
psql -U $(id -nu) -d postgres -c "CREATE DATABASE extenddb OWNER extenddb;"
```

See `docs/local-postgres-setup.md` for full setup instructions.

### `migration failed: ...`

**Cause:** The PostgreSQL database exists but the migration SQL failed (permissions, schema conflicts, etc.).

**Fix:** Check the PostgreSQL logs (`~/pgdata/server.log`). Ensure the `extenddb` user has CREATE TABLE permissions on the `extenddb` database.

### `Catalog version mismatch: expected X, found Y. Run 'extenddb migrate' to update.`

**Cause:** The catalog database was initialized with a different version of extenddb. The binary expects catalog version X but the database has version Y.

**Fix:** Run `extenddb migrate` to apply schema migrations and update the catalog version. Back up the database first.

### `Catalog not initialized. Run 'extenddb init' to set up the catalog.`

**Cause:** The server connected to the database but the catalog tables don't exist. The database hasn't been initialized with `extenddb init`.

**Fix:** Run `extenddb init` to create the catalog schema and data database. See `docs/getting-started.md`.

### `Database '<name>' already exists. Run 'extenddb destroy --config <config>' first, then re-run 'extenddb init'.`

**Cause:** `extenddb init` detected that the catalog or data database already exists in PostgreSQL. To prevent accidental data loss, `extenddb init` refuses to proceed when either database is present.

**Fix:** If you want to re-initialize from scratch, run `extenddb destroy --config extenddb.toml` first to drop both databases, then run `extenddb init` again. If you want to keep the existing data and just apply migrations, use `extenddb migrate` instead.

### `Failed to bind <addr>: Address already in use`

**Cause:** Another process is already listening on the configured port (default 8000).

**Fix:** Check what's using the port and stop it, or use a different port:
```bash
ss -tlnp | grep :8000                    # find what's using the port
extenddb serve --port 8001 --config extenddb.toml  # use a different port
```

### `Failed to load TLS certificates: <error>`

**Cause:** TLS is enabled (the default) but the server could not load the certificate or private key files. Possible causes:
- The certificate or key file does not exist at the configured path
- The file exists but the extenddb process does not have read permission
- The file is not valid PEM format (e.g., DER-encoded, corrupted, or contains extra data)
- The path in `extenddb.toml` is wrong (note: `~` is expanded to `$HOME`)

The error intentionally does not name the specific file to avoid leaking filesystem path information to logs that may be aggregated.

**Fix:**
1. Verify the files exist:
   ```bash
   ls -la ~/.extenddb/tls/cert.pem ~/.extenddb/tls/key.pem
   ```
2. If missing, run `extenddb init` to generate a self-signed certificate, or provide your own CA-signed certificate.
3. Check permissions — the extenddb process must be able to read both files:
   ```bash
   chmod 600 ~/.extenddb/tls/key.pem
   chmod 644 ~/.extenddb/tls/cert.pem
   ```
4. Verify PEM format — the cert file should start with `-----BEGIN CERTIFICATE-----` and the key file with `-----BEGIN PRIVATE KEY-----` (or `-----BEGIN EC PRIVATE KEY-----`).

### `Config file <path> has permissions <mode>, which is too open.`

**Cause:** The config file has group or world-readable permissions. Since the config file may contain the encryption key for credential storage, it must be restricted to owner-only access.

**Fix:**
```bash
chmod 600 extenddb.toml
```

### `Import is disabled. Configure [import] paths in extenddb.toml to enable.`

**Cause:** An `ImportTable` request was made, but no `[import]` paths are configured. Import is disabled by default for security.

**Fix:** Add an `[import]` section to `extenddb.toml`:
```toml
[import]
paths = ["/path/to/imports"]
```

### `Export is disabled. Configure [export] paths in extenddb.toml to enable.`

**Cause:** An `ExportTableToPointInTime` request was made, but no `[export]` paths are configured. Export is disabled by default for security.

**Fix:** Add an `[export]` section to `extenddb.toml`:
```toml
[export]
paths = ["/path/to/exports"]
```

### `Path must resolve under one of the configured allowed paths`

**Cause:** An import or export file path resolves outside all configured allowed directories after canonicalization. This includes symlink escapes.

**Fix:** Ensure the file path is within one of the configured `[import]` or `[export]` paths. Do not use symlinks that point outside the allowed directories.

### `Failed to daemonize: <error>`

**Cause:** extenddb runs as a daemon by default. This error means the process could not fork.

**Fix:** Check that the process has permission to fork. If another extenddb instance is running, stop it first:
```bash
extenddb stop --config extenddb.toml     # preferred
extenddb status --config extenddb.toml   # shows PID (if stop is unavailable)
kill <pid>                        # manual fallback
```

To view logs in real time (useful for debugging):
```bash
journalctl -t extenddb -f
```

### `Failed to initialize syslog — another syslog logger may already be active`

**Cause:** extenddb logs to syslog. This error means another syslog connection is already active in the process.

**Fix:** Ensure no other library or init code opens syslog before extenddb starts.

### `Failed to connect for log-level polling: <error>`

**Cause:** The background task that polls the `log_level` setting from the database could not connect. The server continues to run with the initial log level from the config file.

**Fix:** Verify the `connection_string` in `extenddb.toml` is correct and PostgreSQL is reachable. The log level can still be set via the config file; runtime changes via `extenddb settings set log_level` will not take effect until the server is restarted.

### `Invalid log_level '<value>' in settings: <error>`

**Cause:** The `log_level` value in the settings table is not a valid tracing filter directive.

**Fix:** Run `extenddb settings set log_level <level>` with a valid level: `trace`, `debug`, `info`, `warn`, or `error`.

### `Log level changing to '<level>' (from settings table)`

**Cause:** The background log-level poller detected a change in the `log_level` setting and is applying it. This is informational, logged at `warn` level to ensure visibility even when switching to a restrictive level.

**Fix:** No action needed. This confirms the runtime log level change took effect.

## Stop Errors

### `No PID file found at ... Is extenddb running on port ...?`

**Cause:** `extenddb stop` could not find the PID file for the specified port. Either extenddb is not running, or it was started with a different config/port.

**Fix:** Check that extenddb is running with `extenddb status`. If using a non-default port or config, pass the same `--config` or `--port` to `extenddb stop`.

### `Failed to read PID file <path>: <error>`

**Cause:** The PID file exists but cannot be read (permission denied, I/O error).

**Fix:** Check file permissions on the PID file. The extenddb process must have read access.

### `Invalid PID in <path>: '<value>'`

**Cause:** The PID file contains a value that is not a valid integer. The file may be corrupted.

**Fix:** Delete the PID file manually and use `extenddb status` or `ps` to find the extenddb process. Stop it with `kill <pid>`.

### `Failed to send SIGTERM to pid <pid>: <error>`

**Cause:** `extenddb stop` found a valid PID but could not send SIGTERM. Common causes: permission denied (the extenddb process is owned by a different user) or the process exited between the liveness check and the signal send.

**Fix:** If permission denied, run `extenddb stop` as the same user that started extenddb. If the process already exited, the stale PID file will be cleaned up on the next `extenddb stop` invocation.

### `extenddb (pid <pid>) did not exit within 10s after SIGTERM`

**Cause:** The server received SIGTERM but did not shut down within the 10-second timeout. It may be stuck draining long-running requests or waiting on a database operation.

**Fix:** Send SIGKILL to force termination: `kill -9 <pid>`. Check PostgreSQL for long-running queries that may be blocking shutdown.

## Runtime Errors

### `ResourceNotFoundException: Requested resource not found: Table: <name> not found`

**Cause:** The table doesn't exist. It may have been deleted, or the table name is misspelled.

**Fix:** Run `ListTables` to see existing tables. Table names are case-sensitive.

### `ResourceInUseException: Table already exists: <name>`

**Cause:** A `CreateTable` call for a table that already exists.

**Fix:** Use `DescribeTable` to check if the table exists before creating.

### `ValidationException` on CreateTable

**Cause:** The request doesn't meet DynamoDB's validation rules. Common issues:
- Table name too short (< 3 chars) or too long (> 255 chars)
- Table name contains invalid characters (only `a-zA-Z0-9_.-` allowed)
- Key schema missing HASH key or has wrong ordering
- Attribute definitions don't cover all key attributes
- Unused attribute definitions (defined but not used in any key schema)

**Fix:** Check the error message — it describes the specific validation failure. Compare against the [DynamoDB CreateTable API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html).

### Empty response body or connection reset

**Cause:** Internal serialization failure. The server returns a 500 with `{"__type":"...#InternalServerError","message":"Internal error"}`.

**Fix:** Check the server logs for the underlying error. If reproducible, file a bug.

## Authentication Errors

These errors occur when `auth.provider = "builtin"` is enabled.

### `MissingAuthenticationToken: Request must contain a valid authentication token`

**Cause:** The request has no `Authorization` header. This happens when an SDK is configured without credentials, or when making raw HTTP requests without SigV4 signing.

**Fix:** Configure your SDK with valid access key credentials. If using the AWS CLI, set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables or configure a profile.

### `UnrecognizedClientException: The security token included in the request is invalid`

**Cause:** The access key ID in the request does not exist in extenddb's credential store. Either the key was never created, was deleted, or is misspelled.

**Fix:** Verify the access key ID with `extenddb manage list-access-keys`. Create a new access key if needed.

### `InvalidSignatureException: The request signature we calculated does not match the signature you provided`

**Cause:** The secret key used to sign the request does not match the secret key stored in extenddb. This can happen if the secret key was copied incorrectly (even a single character difference causes a completely different signature).

**Fix:** Verify you are using the exact secret key returned when the access key was created. Secret keys cannot be retrieved after creation — if lost, delete the access key and create a new one.

### `AccessDeniedException: User: <ARN> is not authorized to perform: <action>`

**Cause:** The authenticated user does not have an IAM policy granting the requested DynamoDB action. This can be an implicit deny (no matching Allow statement) or an explicit Deny.

**Fix:** Attach a policy granting the required action to the user, or to a group the user belongs to. Use `extenddb manage list-user-policies` to check current policies. Remember that explicit Deny always overrides Allow.

## Connection Issues

### AWS CLI returns `Could not connect to the endpoint URL`

**Cause:** extenddb is not running, or the endpoint URL is wrong.

**Fix:**
```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/health
# Should return: {"status":"healthy"}
```

If the health check fails, start extenddb. If it succeeds, check your `--endpoint-url` matches the server's bind address and port.

### SDK timeout errors

**Cause:** extenddb is running but slow to respond (e.g., PostgreSQL connection pool exhausted).

**Fix:** Check `extenddb.toml` `[storage.postgres] pool_size` — increase if under heavy concurrent load. Check PostgreSQL logs for slow queries.

### Table stuck in CREATING or DELETING state

**Cause:** The background transition poller processes status changes when notified by CreateTable/DeleteTable, or on a 60-second defensive sweep. If the server was stopped while a table was in a transitional state, the transition completes on the next server startup.

**Fix:** If a table appears stuck:
1. Check that extenddb is running (`extenddb status`).
2. Wait for the configured `control_plane_delay_seconds` (default: 5) — the poller is woken immediately when a table enters a transitional state.
3. If the server was restarted, transitions are recovered automatically at startup.

### CreateTable returns CREATING instead of ACTIVE

**Cause:** extenddb emulates real DynamoDB's async control plane behavior. Tables start in CREATING state and transition to ACTIVE after a configurable delay (default: 5 seconds).

**Fix:** This is expected behavior matching real DynamoDB. Poll with DescribeTable until `TableStatus` is `ACTIVE` before performing operations on the table. All test code should use a `wait_for_active()` helper after CreateTable.

### Failed to recover control plane transitions

**Cause:** At startup, extenddb attempts to complete any in-flight control plane transitions (CREATING→ACTIVE, DELETING→removed) left over from a previous server instance. This error means the recovery query failed, likely due to a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and that the catalog database is accessible. Tables may be stuck in CREATING or DELETING state until the issue is resolved. Once the database is reachable, restart extenddb to retry recovery.

### Control plane transition poll failed

**Cause:** The background poller that processes CREATING→ACTIVE and DELETING→removed transitions encountered a database error. Tables in transitional states will remain stuck until the poller succeeds.

**Fix:** Check PostgreSQL connectivity. The poller retries on the next wake (triggered by new CreateTable/DeleteTable requests or the 60-second defensive sweep). If the database is healthy and the error persists, check PostgreSQL logs for details.

## Management API

### `Management API: DB error during auth: <error>`

**Cause:** The management API could not query the `admin_users` table to verify credentials. The database may be unreachable or the catalog schema may be corrupted.

**Fix:** Check PostgreSQL connectivity and that the `admin_users` table exists in the catalog database. Run `extenddb verify --config extenddb.toml` to check catalog health.

### `Management API: bcrypt hash failed: <error>`

**Cause:** The bcrypt library failed to hash a password during admin creation or password change. This is extremely rare and usually indicates a system-level issue (e.g., out of memory).

**Fix:** Retry the operation. If the error persists, check system resources.

### `Management API: create admin failed: <error>`

**Cause:** The `INSERT INTO admin_users` query failed for a reason other than a unique constraint violation (which returns 409 Conflict). Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list admins failed: <error>`

**Cause:** The `SELECT FROM admin_users` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete admin failed: <error>`

**Cause:** The `DELETE FROM admin_users` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: change password failed: <error>`

**Cause:** The `UPDATE admin_users` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: create account failed: <error>`

**Cause:** The `INSERT INTO accounts` query failed for a reason other than a unique constraint violation (which returns 409 Conflict). Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list accounts failed: <error>`

**Cause:** The `SELECT FROM accounts` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: check tables failed: <error>`

**Cause:** During account deletion, the query to check whether the account owns tables failed. The delete was not attempted.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete account failed: <error>`

**Cause:** The `DELETE FROM accounts` query failed. Likely a database connectivity issue or an unexpected FK constraint violation.

**Fix:** Check PostgreSQL connectivity and logs. If the error mentions a foreign key violation, ensure all IAM entities for the account have been cleaned up (this should happen automatically via CASCADE).

### `Management API: begin transaction failed: <error>`

**Cause:** The management API could not start a database transaction. Likely a database connectivity or pool exhaustion issue.

**Fix:** Check PostgreSQL connectivity. If the management pool (2 connections) is exhausted, wait and retry.

### `Management API: commit delete account failed: <error>`

**Cause:** The account deletion succeeded but the transaction commit failed. The deletion was rolled back. Likely a database connectivity issue.

**Fix:** Retry the operation. Check PostgreSQL connectivity and logs.

### `Management API: DB error during IAM user auth: <error>`

**Cause:** The management API could not query the `iam_users` table to verify IAM user credentials. The database may be unreachable or the catalog schema may be corrupted.

**Fix:** Check PostgreSQL connectivity and that the `iam_users` table exists in the catalog database. Run `extenddb verify --config extenddb.toml` to check catalog health.

### `Management API: create IAM user failed: <error>`

**Cause:** The `INSERT INTO iam_users` query failed for a reason other than a unique constraint or FK violation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: seed self-service policy failed: <error>`

**Cause:** After creating an IAM user, the default self-service policy could not be inserted. The user was created successfully but may lack the default policy.

**Fix:** Manually attach a self-service policy using `extenddb manage put-user-policy`. Check PostgreSQL connectivity.

### `Management API: list IAM users failed: <error>`

**Cause:** The `SELECT FROM iam_users` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete IAM user failed: <error>`

**Cause:** The `DELETE FROM iam_users` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: tag IAM user failed: <error>`

**Cause:** The `INSERT INTO iam_user_tags` query failed for a reason other than a FK violation. Likely a database connectivity issue. The tag transaction is rolled back — no partial tags are applied.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: commit tag transaction failed: <error>`

**Cause:** All tag upserts succeeded but the transaction commit failed. Tags were rolled back. Likely a database connectivity issue.

**Fix:** Retry the operation. Check PostgreSQL connectivity and logs.

### `Management API: untag IAM user failed: <error>`

**Cause:** The `DELETE FROM iam_user_tags` query failed. The untag transaction is rolled back — no partial deletes are applied. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: commit untag transaction failed: <error>`

**Cause:** All tag deletions succeeded but the transaction commit failed. Deletions were rolled back. Likely a database connectivity issue.

**Fix:** Retry the operation. Check PostgreSQL connectivity and logs.

### `Management API: list IAM user tags failed: <error>`

**Cause:** The `SELECT FROM iam_user_tags` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: encryption key not found in settings`

**Cause:** The AES-256-GCM encryption key is missing from the `settings` table. This key is generated during `extenddb init` and is required for encrypting access key secrets.

**Fix:** Run `extenddb init` to regenerate the encryption key. If the settings table exists but the key is missing, the catalog may be corrupted.

### `Management API: fetch encryption key failed: <error>`

**Cause:** The query to retrieve the encryption key from the `settings` table failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: encrypt secret key failed: <error>`

**Cause:** AES-256-GCM encryption of the access key secret failed. This is extremely rare and usually indicates a corrupted encryption key or a system-level issue.

**Fix:** Verify the encryption key in the `settings` table is a valid base64-encoded 32-byte key. If corrupted, re-run `extenddb init` (this will generate a new key, invalidating existing access keys).

### `Management API: create access key failed: <error>`

**Cause:** The `INSERT INTO access_keys` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list access keys failed: <error>`

**Cause:** The `SELECT FROM access_keys` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete access key failed: <error>`

**Cause:** The `DELETE FROM access_keys` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: change IAM user password failed: <error>`

**Cause:** The `UPDATE iam_users` query to change the password failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: check allow_credential_import failed: <error>`

**Cause:** The query to check the `allow_credential_import` runtime setting failed during an access key import. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs. Retry the import operation.

### `Management API: encrypt imported secret failed: <error>`

**Cause:** AES-256-GCM encryption of the imported access key secret failed. This is extremely rare and usually indicates a corrupted encryption key or a system-level issue.

**Fix:** Verify the encryption key in the `settings` table is a valid base64-encoded 32-byte key. If corrupted, re-run `extenddb init` (this will generate a new key, invalidating existing access keys).

### `Management API: import access key failed: <error>`

**Cause:** The `INSERT INTO access_keys` query failed during an access key import for a reason other than a FK or unique constraint violation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: create IAM group failed: <error>`

**Cause:** The `INSERT INTO iam_groups` query failed for a reason other than a unique constraint or FK violation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list IAM groups failed: <error>`

**Cause:** The `SELECT FROM iam_groups` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete IAM group failed: <error>`

**Cause:** The `DELETE FROM iam_groups` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: add group member failed: <error>`

**Cause:** The `INSERT INTO iam_group_members` query failed for a reason other than a unique constraint or FK violation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: remove group member failed: <error>`

**Cause:** The `DELETE FROM iam_group_members` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: put user policy failed: <error>`

**Cause:** The `INSERT INTO iam_policies` query failed for a user policy for a reason other than a FK violation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: put group policy failed: <error>`

**Cause:** The `INSERT INTO iam_policies` query failed for a group policy for a reason other than a FK violation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list user policies failed: <error>`

**Cause:** The `SELECT FROM iam_policies` query failed for user policies. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list group policies failed: <error>`

**Cause:** The `SELECT FROM iam_policies` query failed for group policies. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete user policy failed: <error>`

**Cause:** The `DELETE FROM iam_policies` query failed for a user policy. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete group policy failed: <error>`

**Cause:** The `DELETE FROM iam_policies` query failed for a group policy. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

## IAM Role Management

### `Management API: create IAM role failed: <error>`

**Cause:** The `INSERT INTO iam_roles` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list IAM roles failed: <error>`

**Cause:** The `SELECT` query for listing IAM roles failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete IAM role failed: <error>`

**Cause:** The `DELETE FROM iam_roles` query failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: tag IAM role failed: <error>`

**Cause:** The `INSERT INTO iam_role_tags` query failed during a tag operation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: untag IAM role failed: <error>`

**Cause:** The `DELETE FROM iam_role_tags` query failed during an untag operation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list IAM role tags failed: <error>`

**Cause:** The `SELECT` query for listing IAM role tags failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: put role policy failed: <error>`

**Cause:** The `INSERT INTO iam_policies` query failed for a role policy. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: list role policies failed: <error>`

**Cause:** The `SELECT` query for listing role policies failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete role policy failed: <error>`

**Cause:** The `DELETE FROM iam_policies` query failed for a role policy. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

## AssumeRole

### `Management API: fetch role for assume-role failed: <error>`

**Cause:** The `SELECT` query to load the role and its trust policy failed during an AssumeRole operation. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: encrypt session secret failed: <error>`

**Cause:** AES-256-GCM encryption of the temporary session secret key failed. This could indicate a corrupted or invalid encryption key in the settings table.

**Fix:** Verify the `encryption_key` value in the `settings` table is a valid base64-encoded 32-byte key. If corrupted, re-run `extenddb init` to regenerate.

### `Management API: store assume-role session failed: <error>`

**Cause:** The `INSERT INTO iam_sessions` query failed when storing the temporary session. Likely a database connectivity issue or a unique constraint violation on the generated access key ID (extremely unlikely).

**Fix:** Check PostgreSQL connectivity and logs. Retry the assume-role operation.

## Permissions Boundaries

### `Management API: set user permissions boundary failed: <error>`

**Cause:** The `INSERT INTO iam_permissions_boundaries` query failed for a user boundary. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: get user permissions boundary failed: <error>`

**Cause:** The `SELECT` query for a user permissions boundary failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete user permissions boundary failed: <error>`

**Cause:** The `DELETE FROM iam_permissions_boundaries` query failed for a user boundary. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: set role permissions boundary failed: <error>`

**Cause:** The `INSERT INTO iam_permissions_boundaries` query failed for a role boundary. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: get role permissions boundary failed: <error>`

**Cause:** The `SELECT` query for a role permissions boundary failed. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

### `Management API: delete role permissions boundary failed: <error>`

**Cause:** The `DELETE FROM iam_permissions_boundaries` query failed for a role boundary. Likely a database connectivity issue.

**Fix:** Check PostgreSQL connectivity and logs.

## DynamoDB Streams

### `Stream capture: failed to assign shard for <table>: <error>`

**Cause:** After a successful write (PutItem, DeleteItem, UpdateItem), extenddb tried to capture a stream record but could not determine which shard to assign it to. The data write succeeded — only the stream record is missing.

**Fix:** Check PostgreSQL connectivity. Verify the table's stream shards exist in the `stream_shards` table. If the table was created before streams were enabled, the shards may not have been initialized.

### `Stream capture: failed to write record for <table>: <error>`

**Cause:** A stream record was constructed but could not be persisted to the `stream_records` table. The data write succeeded — only the stream record is missing.

**Fix:** Check PostgreSQL connectivity and disk space. If the error mentions a unique constraint violation, two writes to the same shard may have occurred in the same microsecond — retry the operation.

### `Stream capture: failed to get sequence number: <error>`

**Cause:** extenddb could not generate a sequence number for a stream record. The data write succeeded — only the stream record is missing.

**Fix:** Check PostgreSQL connectivity.

### `Stream cleanup worker: <error>`

**Cause:** The background worker that deletes stream records older than 24 hours encountered a database error. Expired records will accumulate until the worker succeeds.

**Fix:** Check PostgreSQL connectivity. The worker retries every hour automatically.

## Management Console Errors

### Console login page shows "Invalid credentials"

**Cause:** The username or password entered on the `/console/login` page does not match any admin user or IAM user in the catalog database.

**Fix:** For admin users, enter the admin username and password printed during `extenddb init`. For IAM users, enter `account_id/user_name` as the username and the console password set when the user was created. IAM users without a console password cannot log in to the web console.

### Console redirects to login page on every request

**Cause:** The session cookie has expired (sessions last 8 hours) or cookies are blocked by the browser.

**Fix:** Log in again. Ensure cookies are enabled for the extenddb server address. If using a reverse proxy, ensure it forwards the `Cookie` and `Set-Cookie` headers.

### Console shows "Encryption key not found" when creating access keys

**Cause:** The `encryption_key` setting is missing from the `settings` table. This happens if `extenddb init` was not run or the settings table was manually modified.

**Fix:** Run `extenddb init` to bootstrap the encryption key and admin user.

## Import/Export Errors

### `Source path does not exist: <path>`

**Cause:** The `FileSource.Path` in an `ImportTable` request points to a file or directory that does not exist on the server's filesystem.

**Fix:** Verify the path is correct and accessible to the extenddb process. Paths are relative to the server's working directory.

### `Cannot open source file: <error>`

**Cause:** The import source file exists but cannot be opened (permission denied, is a directory, etc.).

**Fix:** Check file permissions. The extenddb process must have read access to the file.

### `Invalid JSON at line N: <error>`

**Cause:** A line in the DYNAMODB_JSON import file is not valid JSON.

**Fix:** Each line must be a valid JSON object. The format is one item per line: `{"Item": {"pk": {"S": "val"}, ...}}` or bare `{"pk": {"S": "val"}, ...}`.

### `Cannot write export file: <error>`

**Cause:** The export destination path is not writable (permission denied, disk full, etc.).

**Fix:** Check that the extenddb process has write access to the destination directory. Parent directories are created automatically.

### `Invalid table ARN: <arn>`

**Cause:** The `TableArn` in an `ExportTableToPointInTime` request is not a valid DynamoDB table ARN.

**Fix:** Use the full ARN format: `arn:aws:dynamodb:<region>:<account>:table/<name>`. You can get the ARN from `DescribeTable`.

## GSI Async Update Behavior

### GSI query returns stale data after a write

**Cause:** GSI updates are applied asynchronously with a configurable propagation delay (default 10ms). This matches real DynamoDB's eventually consistent GSI behavior. Each GSI can have its own `propagation_delay_ms` setting; the system-wide default is controlled by the `gsi_propagation_delay_ms` runtime setting.

**Fix:** This is expected behavior. For tests that query GSIs after writes, poll/retry the GSI query until the expected data appears. To make all GSIs synchronous for testing, set `extenddb settings set gsi_propagation_delay_ms 0`. For production-like testing, keep the default async delay.

## Connection Pool Exhaustion

### HTTP 500 on all requests under heavy load

**Cause:** The PostgreSQL connection pool is exhausted. All connections are in use and new requests cannot acquire a connection within the timeout. extenddb currently returns HTTP 500 (Internal Server Error) instead of the more appropriate 503 (Service Unavailable).

**Fix:** Increase the pool size in `extenddb.toml`:
```toml
[storage.postgres]
pool_size = 50  # default is 20
```

If the problem persists, check for long-running queries or connection leaks with `SELECT * FROM pg_stat_activity WHERE datname = 'extenddb_data';`.

**Known limitation:** The HTTP status code should be 503 with a `Retry-After` header. This is tracked as technical debt.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
