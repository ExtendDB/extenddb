# Symptom Index

## 1. Purpose

This index maps each of the 16 known extenddb symptoms to the category file that holds the verbatim Cause and Fix from `docs/troubleshooting.md`. To use it, grep this file for the user's error text, follow the link to the category file, and present the entry to the user. The skill never executes a remediation command on the user's behalf. Requirement 14.4 applies to every entry.

## 2. Symptom table

| # | Symptom keyword / error text | Category file | Source section in `docs/troubleshooting.md` |
|---|---|---|---|
| 1 | Connection refused | `02-postgres-symptoms.md#connection-refused` | `error connecting to server: Connection refused` |
| 2 | password authentication failed for user "extenddb" | `02-postgres-symptoms.md#password-authentication-failed` | `password authentication failed for user "extenddb"` |
| 3 | migration failed | `02-postgres-symptoms.md#migration-failed` | `migration failed: ...` |
| 4 | Catalog version mismatch | `03-catalog-symptoms.md#catalog-version-mismatch` | `Catalog version mismatch: expected X, found Y. Run 'extenddb migrate' to update.` |
| 5 | Catalog not initialized | `03-catalog-symptoms.md#catalog-not-initialized` | `Catalog not initialized. Run 'extenddb init' to set up the catalog.` |
| 6 | Database already exists | `03-catalog-symptoms.md#database-already-exists` | `Database '<name>' already exists. Run 'extenddb destroy --config <config>' first, then re-run 'extenddb init'.` |
| 7 | Address already in use | `04-startup-symptoms.md#address-already-in-use` | `Failed to bind <addr>: Address already in use` |
| 8 | Failed to load TLS certificates | `04-startup-symptoms.md#failed-to-load-tls-certificates` | `Failed to load TLS certificates: <error>` |
| 9 | Config file permissions too open | `04-startup-symptoms.md#config-file-permissions` | `Config file <path> has permissions <mode>, which is too open.` |
| 10 | Import is disabled | `05-feature-gate-symptoms.md#import-disabled` | `Import is disabled. Configure [import] paths in extenddb.toml to enable.` |
| 11 | Export is disabled | `05-feature-gate-symptoms.md#export-disabled` | `Export is disabled. Configure [export] paths in extenddb.toml to enable.` |
| 12 | Failed to daemonize | `04-startup-symptoms.md#failed-to-daemonize` | `Failed to daemonize: <error>` |
| 13 | InvalidSignatureException | `06-auth-symptoms.md#invalidsignatureexception` | `InvalidSignatureException: The request signature we calculated does not match the signature you provided` |
| 14 | UnrecognizedClientException | `06-auth-symptoms.md#unrecognizedclientexception` | `UnrecognizedClientException: The security token included in the request is invalid` |
| 15 | AccessDeniedException | `06-auth-symptoms.md#accessdeniedexception` | `AccessDeniedException: User: <ARN> is not authorized to perform: <action>` |
| 16 | Connection pool exhausted / HTTP 500 under load | `07-runtime-symptoms.md#connection-pool-exhausted` | `HTTP 500 on all requests under heavy load` |


## 3. Per-entry summaries

The cause and fix summaries below are paraphrased for quick scanning. The category file holds the verbatim text from `docs/troubleshooting.md`.

### Connection refused

**Error text:** `error connecting to server: Connection refused`
**Cause summary:** PostgreSQL is not running or not listening on the configured host and port.
**Fix summary:** Check `pg_ctl status` and start Postgres if needed.
**Full entry:** `references/02-postgres-symptoms.md#connection-refused`

### password authentication failed for user "extenddb"

**Error text:** `password authentication failed for user "extenddb"`
**Cause summary:** The PostgreSQL `extenddb` user does not exist or the password does not match.
**Fix summary:** Create the `extenddb` user and database with the password extenddb expects, per `docs/local-postgres-setup.md`.
**Full entry:** `references/02-postgres-symptoms.md#password-authentication-failed`

### migration failed

**Error text:** `migration failed: ...`
**Cause summary:** The database exists but the migration SQL failed because of permissions or schema conflicts.
**Fix summary:** Check `~/pgdata/server.log` and confirm the `extenddb` user has CREATE TABLE on the `extenddb` database.
**Full entry:** `references/02-postgres-symptoms.md#migration-failed`

### Catalog version mismatch

**Error text:** `Catalog version mismatch: expected X, found Y. Run 'extenddb migrate' to update.`
**Cause summary:** The catalog database was initialized with a different version of extenddb than the running binary.
**Fix summary:** Back up the database, then run `extenddb migrate` to apply schema migrations.
**Full entry:** `references/03-catalog-symptoms.md#catalog-version-mismatch`

### Catalog not initialized

**Error text:** `Catalog not initialized. Run 'extenddb init' to set up the catalog.`
**Cause summary:** The server connected to Postgres but the catalog tables do not exist yet.
**Fix summary:** Run `extenddb init` to create the catalog schema and data database.
**Full entry:** `references/03-catalog-symptoms.md#catalog-not-initialized`

### Database already exists

**Error text:** `Database '<name>' already exists. Run 'extenddb destroy --config <config>' first, then re-run 'extenddb init'.`
**Cause summary:** `extenddb init` refuses to proceed when the catalog or data database is already present.
**Fix summary:** Run `extenddb destroy --config extenddb.toml` to drop both databases, or use `extenddb migrate` to keep the existing data.
**Full entry:** `references/03-catalog-symptoms.md#database-already-exists`

### Address already in use

**Error text:** `Failed to bind <addr>: Address already in use`
**Cause summary:** Another process is already listening on the configured port.
**Fix summary:** Identify the process with `ss -tlnp` and stop it, or start extenddb on a different port.
**Full entry:** `references/04-startup-symptoms.md#address-already-in-use`

### Failed to load TLS certificates

**Error text:** `Failed to load TLS certificates: <error>`
**Cause summary:** The TLS certificate or private key file is missing, unreadable, or not valid PEM.
**Fix summary:** Verify both files exist at the configured paths, are readable by the extenddb process, and start with the expected PEM header.
**Full entry:** `references/04-startup-symptoms.md#failed-to-load-tls-certificates`

### Config file permissions too open

**Error text:** `Config file <path> has permissions <mode>, which is too open.`
**Cause summary:** The config file has group or world read permissions, which is unsafe because it may contain the encryption key.
**Fix summary:** Run `chmod 600 extenddb.toml`.
**Full entry:** `references/04-startup-symptoms.md#config-file-permissions`

### Import is disabled

**Error text:** `Import is disabled. Configure [import] paths in extenddb.toml to enable.`
**Cause summary:** An `ImportTable` request was made but no `[import]` paths are configured.
**Fix summary:** Add an `[import]` section with explicit `paths` to `extenddb.toml`.
**Full entry:** `references/05-feature-gate-symptoms.md#import-disabled`

### Export is disabled

**Error text:** `Export is disabled. Configure [export] paths in extenddb.toml to enable.`
**Cause summary:** An `ExportTableToPointInTime` request was made but no `[export]` paths are configured.
**Fix summary:** Add an `[export]` section with explicit `paths` to `extenddb.toml`.
**Full entry:** `references/05-feature-gate-symptoms.md#export-disabled`

### Failed to daemonize

**Error text:** `Failed to daemonize: <error>`
**Cause summary:** extenddb could not fork into the background, often because another extenddb instance is already running.
**Fix summary:** Stop the existing instance with `extenddb stop --config extenddb.toml`, or check fork permissions.
**Full entry:** `references/04-startup-symptoms.md#failed-to-daemonize`

### InvalidSignatureException

**Error text:** `InvalidSignatureException: The request signature we calculated does not match the signature you provided`
**Cause summary:** The secret key used to sign the request does not match the secret key stored in extenddb.
**Fix summary:** Verify the exact secret key, then delete the access key and create a new one if lost.
**Full entry:** `references/06-auth-symptoms.md#invalidsignatureexception`

### UnrecognizedClientException

**Error text:** `UnrecognizedClientException: The security token included in the request is invalid`
**Cause summary:** The access key ID does not exist in extenddb's credential store.
**Fix summary:** List existing access keys with `extenddb manage list-access-keys` and create a new one if needed.
**Full entry:** `references/06-auth-symptoms.md#unrecognizedclientexception`

### AccessDeniedException

**Error text:** `AccessDeniedException: User: <ARN> is not authorized to perform: <action>`
**Cause summary:** The authenticated user has no IAM policy granting the requested DynamoDB action, or an explicit Deny applies.
**Fix summary:** Attach a policy with the required action to the user or a group the user belongs to.
**Full entry:** `references/06-auth-symptoms.md#accessdeniedexception`

### Connection pool exhausted

**Error text:** `HTTP 500 on all requests under heavy load`
**Cause summary:** The PostgreSQL connection pool is exhausted and new requests cannot acquire a connection within the timeout.
**Fix summary:** Raise `pool_size` under `[storage.postgres]` in `extenddb.toml` and investigate long-running queries in `pg_stat_activity`.
**Full entry:** `references/07-runtime-symptoms.md#connection-pool-exhausted`

## 4. Unknown-symptom fallback

If the user's error text does not match any entry above, ask the user to pull the last 100 lines of the extenddb log, then retry the lookup on the new text.

```bash
# Linux
journalctl -t extenddb -n 100

# macOS
log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 10m
```

Once the user pastes the last one or two relevant lines back, retry the index lookup on the new text. If the symptom still does not match, report that the symptom is not in the catalog and suggest the user check `docs/troubleshooting.md` directly.
