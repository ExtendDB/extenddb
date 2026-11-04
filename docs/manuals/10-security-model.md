# Security Model

> See [NOTICE](../NOTICE.md) for important disclaimers.

This document describes the security architecture of extenddb, including the threat model, authentication and authorization mechanisms, transport security, and operational security controls.

## Threat Model

### What extenddb Protects

- **Data confidentiality**: Items stored in DynamoDB tables are accessible only to authenticated and authorized principals.
- **Data integrity**: Write operations are atomic (including stream records, index updates, and side effects). Concurrent writes serialize on PostgreSQL row locks.
- **Access control**: IAM policies enforce least-privilege access. Explicit Deny always takes precedence.
- **Credential security**: Access key secrets are encrypted at rest (AES-256-GCM). Console passwords are hashed (bcrypt).
- **Transport security**: TLS encrypts data in transit between clients and extenddb. TLS is mandatory — the server refuses to start without it.

### Trust Boundaries

1. **Client ↔ extenddb**: Untrusted. All input is validated. SigV4 signatures are verified. IAM policies are evaluated.
2. **extenddb ↔ PostgreSQL**: Trusted network. The PostgreSQL connection string contains credentials. Use TLS for the PostgreSQL connection in production (`sslmode=require` in the connection string).
3. **Admin ↔ Management API/Console**: Authenticated via admin credentials or IAM user credentials. CSRF tokens protect the web console.

### Out of Scope

- **PostgreSQL security**: extenddb relies on PostgreSQL access controls and network security. Securing the PostgreSQL instance (firewall rules, TLS, authentication) is the operator's responsibility.
- **Operating system security**: File permissions on `extenddb.toml`, TLS keys, and the PID file are the operator's responsibility.
- **Key management**: Access key secrets are encrypted with a locally generated AES key stored in the catalog database. For HSM-grade key management, use a KMS-backed encryption layer at the PostgreSQL level.

## Authentication

### Builtin IAM (`auth.provider = "builtin"`)

extenddb uses SigV4 signature verification with a local credential store and IAM policy engine. This is the only supported authentication mode — the server refuses to start without it. The `auth.provider` setting defaults to `"builtin"`. Setting it to `"none"` causes a startup error.

#### SigV4 Verification

1. Extract the `Authorization` header (AWS4-HMAC-SHA256 scheme)
2. Parse credential scope, signed headers, and signature
3. Look up the access key in the credential store
4. Decrypt the secret key (AES-256-GCM)
5. Compute the signing key: HMAC-SHA256 chain over date, region, service, "aws4_request"
6. Compute the string-to-sign from the canonical request. The hashed-payload line is the SHA-256 of the body the server received; a client-supplied `x-amz-content-sha256` header is not used for it, so a request whose body was altered after signing fails at step 7
7. Compare computed signature against the provided signature (constant-time comparison)
8. On mismatch: return `InvalidSignatureException` (HTTP 400)
9. On unknown key: return `UnrecognizedClientException` (HTTP 400)

#### Credential Types

| Prefix | Type | Lifetime |
|--------|------|----------|
| `AKIAEXTENDDB` | Long-term access key | Until deleted |
| `ASIAEXTENDDB` | Temporary (AssumeRole) | Configurable, default 1 hour |

#### Credential Storage

- Secret keys encrypted with AES-256-GCM using a per-catalog encryption key
- Encryption key generated during `extenddb init` and stored in the catalog database
- Console passwords hashed with bcrypt (cost factor 12)
- Credentials and the inputs to authorization are cached in each server's memory; see Credential and Policy Caching below

#### Credential and Policy Caching

Each server keeps in-memory caches between the request path and the catalog database, so that a request does not read the catalog for every credential and policy lookup. The caches hold decrypted credentials (long-term access keys and AssumeRole session credentials), parsed user, group and role inline policies, permissions boundaries, principal tags, resource tags, role-session data, user and role creation times, and each table's key and index definitions. Authorization decisions themselves are not cached: policies are evaluated on every request. Item data is not cached. This section covers the credentials that sign API requests; web console logins are described under Web Console Security.

The caches are on by default and are configured under `[auth.cache]` in `extenddb.toml` (see the Admin Guide):

| Setting | Default | Effect |
|---------|---------|--------|
| `enabled` | `true` | `false` makes every lookup read the catalog |
| `ttl_seconds` | `60` | The oldest a cached entry can be when it is used. Must be greater than 0 |
| `soft_ttl_seconds` | `30` | After this age an entry is still used, and a background read replaces it. Must not exceed `ttl_seconds` |
| `negative_ttl_seconds` | `5` | How long an empty answer is cached: an unknown access key ID, a missing table, or a principal with no policies, boundary or tags. Must not exceed `ttl_seconds` |
| `max_entries` | `10000` | Entries per cache; beyond this, entries are evicted. Must be greater than 0 |

These settings, whether set in the file or through `EXTENDDB__AUTH__CACHE__*` environment variables, are read at startup; changing them takes a restart. A server whose settings break one of the limits above refuses to start.

**How a change reaches the cache.** A change made on a server through the management API, the web console, or one of the DynamoDB operations named below takes effect on that server immediately. Deleting an access key, user or role, changing an inline policy, permissions boundary, group membership or a user's or role's tags, and calling `TagResource` or `UntagResource` drop the cached entries they affect before the call returns. `CreateTable`, `UpdateTable`, `DeleteTable`, `RestoreTableFromBackup` and `ImportTable` drop the table's key and index definitions, and `CreateTable` and `DeleteTable` also drop its tags. There are two exceptions. If a group change cannot list the group's members, the server logs a warning and those members' entries wait for `ttl_seconds`. The console's Delete user and Delete role do not drop the principal's cached creation time, so a user or role created again with the same name within `ttl_seconds` resolves to the old `aws:userid`, and a condition on `aws:userid` matches the old id in that window (#787). Every other change is seen when the cached entry is next refreshed or expires, within `ttl_seconds`. That covers:

- a change made on one server, as seen by the other servers that serve the same catalog;
- a change written to the catalog database directly rather than through a server.

On those servers, a change to something that was cached as present (an access key, or a principal's existing policies, boundary or tags) takes up to `ttl_seconds` to arrive. A change to something that was cached as empty (a principal's first policy, boundary or tag) arrives within `negative_ttl_seconds`, because that is how long an empty answer is kept. Running more than one server on a catalog is not supported in this release (see the Deployment Guide); on SQLite, on Unix with the database on a local filesystem, a second server refuses to start. A request that has already been authorized when a credential is revoked is not stopped.

**Revoking access at once.** To make a revocation take effect immediately, for example during incident response, take these steps in order.

*Step 1: change the catalog.* Through the management API or console, delete the access key, the user, or the inline policy that grants the access, or remove the user from the group. The server that handles the call applies the change at once.

AssumeRole session credentials (`ASIAEXTENDDB...`) cannot be deleted one at a time, and a session can last up to 12 hours. A session's permissions come from its role's inline policies, so deleting the role's inline policies stops the role's sessions. This affects every session of that role.

*Step 2: flush the other servers.* On every other server that serves the same catalog, flush that server's cache. Run the command on that server's host with that server's config file, so that the CLI connects to that server and trusts its TLS certificate:

```bash
read -rs EXTENDDB_PASSWORD && export EXTENDDB_PASSWORD   # the admin password, not echoed
extenddb manage --config /path/to/extenddb.toml --user ADMIN_NAME cache invalidate SCOPE
unset EXTENDDB_PASSWORD
```

Replace `SCOPE` with the scope for what changed:

| Change | Scope |
|--------|-------|
| A deleted access key | `credential --access-key-id <id>` |
| A deleted account | `account --account-id <id>` |
| A user's keys, inline policies, boundary or tags, or a deleted user | `user --account-id <id> --user-name <name>` |
| A role's inline policies, boundary or tags, or a deleted role | `role --account-id <id> --role-name <name>` |
| A group's inline policy or membership | `group-members --account-id <id> --user-names <name>,<name>` (every member), or `all --yes` |
| A table's tags (`TagResource`, `UntagResource`) | `resource-tags --arn <table ARN>` |
| A deleted table, a table created again with the same name, or an updated table | `table-key-info --account-id <id> --table-name <name>`, and for a deleted or recreated table also `resource-tags --arn <table ARN>` |
| Unsure | `all --yes` |

Without `--endpoint`, the CLI connects to `server.bind_addr` and `server.port` from the config file (`127.0.0.1` when `bind_addr` is `0.0.0.0`). With `--endpoint https://<host>:<port>`, it connects to `<host>`. When TLS is on (`server.tls.enabled`, or an `https://` endpoint), it trusts only the certificate at `server.tls.cert_path` in the config file it reads, and that certificate must cover the address the CLI connects to. The certificate `extenddb init` creates covers `localhost`, `127.0.0.1`, `bind_addr` when it is a specific address, and each name given with `--tls-san`. A CA-signed certificate usually covers none of the first three, so with one use `--endpoint https://<name in the certificate>:<port>`, and make `cert_path` a PEM file that also holds the issuing CA's certificate. Do not use a load balancer's address: each call reaches one server behind it and reports success. The same flush is available to admins at `POST /management/cache/invalidate` and on the console's cache page, and each acts only on the server it reaches.

Instead of flushing, you can restart each server, which starts with empty caches. A server started with `auth.cache.enabled = false` (or `EXTENDDB__AUTH__CACHE__ENABLED=false` in its environment) reads the catalog on every request, at the cost of several catalog queries per request.

Flushing a cache does not revoke anything by itself: it only makes the server read the catalog again, so step 1 comes first. If step 2 is not done and the servers are not restarted, a change made in step 1 reaches the other servers within `ttl_seconds` (60 seconds by default). Lowering `ttl_seconds` shortens that window and adds catalog reads; lower `soft_ttl_seconds` and `negative_ttl_seconds` with it so that neither exceeds it.

## Authorization

### Policy Evaluation — 5-Phase IAM Algorithm

When `auth.provider = "builtin"`, every DynamoDB API request is authorized against IAM policies. The evaluation follows the same algorithm as real AWS IAM:

1. **Explicit Deny** — scan all policies (identity, permissions boundary, session). If any Deny statement matches the action, resource, and conditions → **DENY**.
2. **Permissions Boundary** — if a permissions boundary is set on the user or role, it must contain an Allow statement matching the action, resource, and conditions. If not → **DENY**.
3. **Session Policy** — if the request uses AssumeRole credentials with a session policy, the session policy must contain a matching Allow. If not → **DENY**.
4. **Identity Allow** — scan identity policies (user inline policies, group inline policies, role inline policies). If any Allow statement matches → **ALLOW**.
5. **Implicit Deny** — no matching Allow found → **DENY**.

Policy sources collected for evaluation:
- User inline policies
- Group inline policies (for all groups the user belongs to)
- Role inline policies (if using AssumeRole)
- Session policies (if using AssumeRole)
- Permissions boundary (if set on the user or role)

### Fail-Closed Design

- **Unparseable policies deny**: A stored policy that cannot be parsed results in access denied, not silent skip. A corrupted Deny policy still denies; a corrupted Allow policy is treated as absent.
- **Auth before JSON parse**: SigV4 signature verification runs before the request body is parsed. Invalid signatures are rejected with constant-time comparison before any business logic executes.
- **Concurrent policy fetching**: Identity policies, group policies, and permissions boundaries are fetched concurrently from the catalog when they are not in the server's cache (see Credential and Policy Caching). All must succeed for evaluation to proceed.
- **Constant-time rejection for inactive keys**: Inactive or expired access keys are rejected without timing differences that could reveal key existence.
- **Policy document validation on write**: Policy documents are validated for JSON structure and size-capped (6,144 bytes) when attached via the management API. Invalid documents are rejected before storage. A `Version` other than the string `2012-10-17` or `2008-10-17` is refused; only a `2012-10-17` document expands policy variables.
- **Expression depth and token limits**: Expression parsing enforces configurable depth (default 150) and token limits (default 4,096) to prevent resource exhaustion.

### Supported Condition Operators

The IAM policy engine supports the full set of condition operators relevant to DynamoDB access control:

| Category | Operators |
|----------|-----------|
| String | `StringEquals`, `StringNotEquals`, `StringEqualsIgnoreCase`, `StringLike`, `StringNotLike` |
| Numeric | `NumericEquals`, `NumericNotEquals`, `NumericLessThan`, `NumericLessThanEquals`, `NumericGreaterThan`, `NumericGreaterThanEquals` |
| Date | `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateLessThanEquals`, `DateGreaterThan`, `DateGreaterThanEquals` |
| Boolean | `Bool` |
| Null check | `Null` |
| ARN | `ArnEquals`, `ArnNotEquals`, `ArnLike`, `ArnNotLike` |
| Set operators | `ForAllValues:*`, `ForAnyValue:*` (prefix applied to any base operator) |
| Existence | `IfExists` suffix (condition passes if key is absent) |

Set operators and `IfExists` can be combined: `ForAllValues:StringEqualsIfExists`.

### Supported Condition Keys

| Key | Type | Description |
|-----|------|-------------|
| `aws:PrincipalTag/<key>` | String | Tag on the authenticated principal (user or role) |
| `aws:ResourceTag/<key>` | String | Tag on the target DynamoDB table |
| `dynamodb:LeadingKeys` | Multi-valued string | Partition key values being accessed |
| `dynamodb:Attributes` | Multi-valued string | Attribute names being read or written |
| `dynamodb:Select` | String | The `Select` parameter value |
| `dynamodb:ReturnValues` | String | The `ReturnValues` parameter value |
| `dynamodb:ReturnConsumedCapacity` | String | The `ReturnConsumedCapacity` parameter value |
| `dynamodb:FullTableScan` | Boolean | `true` for Scan operations |
| `dynamodb:EnclosingOperation` | String | The enclosing operation for batch/transact sub-operations |

Policy variables (e.g., `${aws:PrincipalTag/Team}`) are expanded in condition values. In a `2012-10-17` document, under a string or ARN operator (with or without `ForAnyValue:`, `ForAllValues:` or `IfExists`), the `${key, 'default'}` form expands to `default` when the request does not carry `key`, and code points up to U+0020 around the key name are ignored. Elsewhere the text between the braces is read as the key name as written, so `${key, 'default'}` is compared as written; IAM does not read the default form there either. A role trust policy follows its own `Version`. A multivalued key such as `dynamodb:LeadingKeys` is not expanded.

### Access Control Patterns

extenddb supports the same access control patterns as real AWS IAM:

**Identity-Based Access Control (IBAC)**: Attach policies directly to IAM users. Each user's policies define what they can do.

**Role-Based Access Control (RBAC)**: Create IAM groups with policies, add users to groups. Users inherit group policies. Create IAM roles for cross-account or service-to-service access.

**Attribute-Based Access Control (ABAC)**: Use `aws:PrincipalTag/*` and `aws:ResourceTag/*` condition keys to make access decisions based on tags. Example: allow users tagged `Department=Engineering` to access tables tagged `Department=Engineering`.

**Fine-Grained Access Control (FGAC)**: Use `dynamodb:LeadingKeys` to restrict access to specific partition key values. Use `dynamodb:Attributes` to restrict which attributes can be read or written. Example: allow a user to access only items where the partition key matches their user ID.

`dynamodb:LeadingKeys` is evaluated on `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems`. A `Query` on a global secondary index binds the key to the index's partition key value (its first HASH attribute). `Scan` and `SearchVectors` target no single partition and carry no `dynamodb:LeadingKeys` value, so a `ForAllValues` condition does not fence them; fence a full-table read with `ForAnyValue:StringEquals` or a `Null` guard on the statement. A numeric partition key is compared as the normalized number on `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query` and `BatchGetItem`, and as Java's `BigDecimal` prints it (`10.0` stays `10.0`, `1E1` is `1E+1`) on `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems`, which is what DynamoDB does.

### Resource ARNs

Resources are identified by ARN: `arn:aws:dynamodb:<region>:<account-id>:table/<table-name>`. Wildcard matching (`*`, `?`) in policy Resource fields follows AWS IAM conventions. `NotResource` is also supported.

### Supported Policy Elements

- `Effect`: Allow, Deny
- `Action` / `NotAction`: DynamoDB actions (e.g., `dynamodb:PutItem`, `dynamodb:*`)
- `Resource` / `NotResource`: ARN patterns with wildcards. In a `2012-10-17` document, `${key}` policy variables in the pattern are expanded from the request before matching (for example `arn:aws:dynamodb:*:*:table/${aws:username}`); a wildcard written after a variable still matches; `${key, 'default'}` falls back to `default`; `${*}`, `${?}` and `${$}` stand for literal characters; a variable whose key the request does not carry, or that names a multivalued key such as `dynamodb:LeadingKeys` that the request carries, makes the pattern match nothing (a written default is used only when the key is absent); a `:` inside a substituted value does not separate ARN fields. A `2008-10-17` document, or one with no `Version`, matches the pattern as written, as IAM does. Code points up to U+0020 around the key name, around the comma of the default form and before the closing brace are ignored, as in IAM, and a space inside the quotes of a default is kept. A tag whose value is the empty string expands to no characters, as in IAM: `table/${aws:PrincipalTag/team}*` matches every table for a caller tagged `team=""`, while with no `team` tag it matches nothing. To refuse such a caller, add a condition such as `StringLike` on `aws:PrincipalTag/team` with the value `?*` to the `Allow` statement (measured with the IAM policy simulator). A `Deny NotResource` statement on such a pattern spares every table for that caller (also measured), and that condition on it does not change this.
- `Condition`: Full condition block support (see operators and keys above)

## Transport Security

### TLS Configuration

TLS is mandatory. The server refuses to start with `tls.enabled = false`. extenddb uses rustls (no OpenSSL dependency).

- `extenddb init` generates a self-signed certificate and private key at `~/.extenddb/tls/`
- Production deployments should replace with CA-signed certificates
- When TLS is enabled, HSTS headers (`Strict-Transport-Security`) are sent automatically

```toml
[server.tls]
cert_path = "/etc/extenddb/tls/cert.pem"
key_path = "/etc/extenddb/tls/key.pem"
```

### Certificate Rotation

Replace the certificate and key files, then restart extenddb. There is no hot-reload for TLS certificates.

## Web Console Security

An admin session can read and change every account. Every page and form under `/console/accounts/{account_id}` serves an IAM-user session its own account only. When `{account_id}` is not the session's own account, the handler redirects the session to `/console`. The handler makes this comparison before it looks up the account in the path, so another account's ID and an ID that no account has get the same answer. The accounts list shows an IAM-user session its own account alone, and the console home page shows the deployment's account count and admin count to admin sessions only. In its own account an IAM-user session currently reads every user, group and role page, including other users' access key IDs, policy names and tags, which the management API refuses the same user. Issue #731 tracks this. An IAM-user session can create and delete access keys for its own user, and every other change in the console needs an admin session. Until pull request #727 merges, `/console/metrics-data` serves every console session the table names, index names and traffic metrics of every account, because the metrics table has no account column.

The management web console (`/console/*`) implements:

- **CSRF protection**: Unique tokens generated per session, injected into all forms via JavaScript, validated on every POST handler
- **Session management**: Server-side sessions with 8-hour expiry, HttpOnly cookies with `SameSite=Strict` and `Path=/console`
- **Security headers**: X-Content-Type-Options (nosniff), X-Frame-Options (DENY), Referrer-Policy (strict-origin-when-cross-origin)
- **HSTS**: Sent automatically (TLS is always enabled)
- **Login rate limiting**: Failed login attempts are counted per user and source address, and per source address, as described under [Login Rate Limiting](#login-rate-limiting).

## Login Rate Limiting

The console login form, the management API (HTTP Basic auth) and the `/metrics` endpoint share one set of limits on failed logins. Attempts are stored in the catalog's `login_attempts` table, so every surface sees the same history.

- **5 failures per user and source address in 15 minutes.** After the fifth wrong password for a user from one address, further attempts for that user from that address are answered `429 Too Many Requests` until the oldest failure is 15 minutes old. Attempts for the same user from other addresses are not affected, so a third party who knows an admin's name cannot lock the admin out.
- **20 failures per source address in 15 minutes**, counted over all users. This caps guessing from one address at 20 passwords per 15 minutes however many names it tries.
- **Attempts are reserved before the password is checked.** An attempt counts against both limits from the moment it is admitted until its failure is recorded, so a burst of concurrent wrong passwords is admitted up to the cap and the rest are refused without a password check.
- **Bounded verification.** At most 4 bcrypt operations (password checks and password hashes) run at once per process. A login that waits more than 10 seconds for a slot is answered `500` and is not counted as a failure, since no password was checked.

A successful login is not recorded. A storage error before or during the password check is answered `500` and is not counted against the caller.

The source address is the peer address of the TCP connection, with an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) counted as the IPv4 address. Any other IPv6 address is counted by its /64 prefix, because a /64 is the block usually assigned to one host or site and a client can send from any address in it; all addresses in one /64 share one set of limits. ExtendDB does not read `X-Forwarded-For` or any other forwarded-address header (they are caller-supplied).

Callers that reach ExtendDB from one address share one set of limits. Five failures for a user from any of them refuse that user to all of them, and 20 failures in total refuse every user, until the oldest failure is 15 minutes old. Callers share an address in these cases:

- **A reverse proxy or load balancer** that opens its own connections to ExtendDB, so every request arrives from the proxy's address. Put a proxy or load balancer in front of ExtendDB only when it preserves the client address, or when callers behind it are trusted not to lock each other out.
- **The server host itself.** `extenddb manage` and a `/metrics` scraper running on the server host connect from the same address. A scraper with a stale password for an admin refuses that admin on the host after 5 failures, `extenddb manage` included.
- **A NAT gateway**, whose egress address is shared by every client behind it.

To recover an address that is refused, wait until its oldest failure is 15 minutes old, or delete the address's rows from the catalog's `login_attempts` table.

A password can be guessed against one user from many addresses at 5 attempts per address per 15 minutes; only the source-address limit and the strength of the password bound that. Use long passwords for admin users and restrict which networks can reach the management and console ports.

## Provisioned Throughput Throttling

extenddb includes a token bucket rate limiter for provisioned throughput enforcement. When `server.throttling_enabled = true` in `extenddb.toml`, read and write requests are throttled against the table's provisioned RCU/WCU limits. Requests that exceed the limit receive `ProvisionedThroughputExceededException` (HTTP 400), matching real DynamoDB behavior.

Token buckets are purely in-memory operational state — not cached database state. They are recreated on server restart.

## Input Validation

### Defense in Depth

Input validation is layered:

1. **Server layer**: Request size limits, header validation, content-type checks
2. **Engine layer**: All user-supplied strings validated before reaching storage — table names, attribute names, expression strings, policy documents
3. **Storage layer**: Parameterized queries only — no dynamic SQL construction from user input

### Expression Limits

| Limit | Default | Description |
|-------|---------|-------------|
| Max expression tokens | 4,096 | Maximum tokens in a parsed expression |
| Max expression depth | 150 | Maximum nesting depth in expressions |
| Max policy document size | 6,144 bytes | Maximum size of an IAM policy document |

### Path Traversal Protection

Import/export file paths are validated:
- `..` components rejected
- Paths namespaced per account: a caller in account `A` may only resolve paths
  under `<root>/A` for each configured root. Containment in the bare root is not
  sufficient, so tenants sharing an instance cannot read or overwrite each
  other's files. The account id comes from the authenticated identity, never
  from the request
- Containment is decided before any filesystem access, so a path belonging to
  another account answers identically whether or not the file exists
- Paths canonicalized, and containment re-checked afterwards so a symlinked
  ancestor cannot redirect out of the subtree
- Symlinks detected and rejected, including one planted at the export filename
- Export refuses to overwrite an existing file, so it cannot truncate a file it
  did not create
- An export that fails removes the file it created. On Unix it first checks
  that the path still names that file (the same device and inode), and leaves
  the path alone when it names another file, such as one a second export
  created after the first file was removed. The check and the unlink are two
  calls, so a file put at the path between them is removed. If the server
  cannot read the file's device and inode (it has no spare file descriptor),
  the export fails before it writes a line and unlinks the path without the
  check. On other platforms the export unlinks the path without the check.
  The unlink never follows a symlink, and a file the server cannot remove
  stays, with a warning in the server log
- Both operations record the acting account, the operation and the resolved path
- Error messages use generic text (no raw paths leaked)

## Error Message Security

- DynamoDB-fidelity error messages reproduce real DynamoDB exactly (no additional information)
- Internal errors (database connection failures, SQL errors, I/O errors) are logged server-side but return generic messages to clients
- Stack traces, file paths, and SQL text are never exposed to clients

## Operational Security

### Credential Handling

- `extenddb init` prints admin credentials once; they are not stored in plaintext
- The `--password` flag accepts passwords via environment variable (`EXTENDDB_ADMIN_PASSWORD`) to avoid process listing exposure
- Access key secrets are shown once on creation and cannot be retrieved afterward

### Logging

- All logging goes to syslog (no log files with sensitive data on disk)
- Some management operations are audit-logged at WARN, with target `extenddb::audit::manage` or `extenddb::audit::settings`: DeleteAdmin, user and role tag changes, AssumeRole (granted and denied), and settings changes. Other management changes, such as creating or deleting users, roles, access keys and policies, are not audit-logged
- Cache flushes (`cache invalidate`) are logged at INFO with the admin name, the scope and its selectors, so they are not recorded when the log level (`logging.level`, or the `log_level` setting, which overrides it) is `warn` or `error`
- Log messages do not contain secret keys, passwords, or item data. Access key IDs can appear, for example in cache flush records

### Default Configuration

- Binds to `127.0.0.1` (localhost only) by default
- Auth provider defaults to `builtin` (SigV4 + IAM policies). The server refuses to start with `auth.provider = "none"`.
- TLS is mandatory. `extenddb init` generates a self-signed certificate; production deployments should use CA-signed certificates.

### Backup and Recovery

extenddb stores all state in PostgreSQL. Use standard PostgreSQL tools for backup and recovery:

```bash
pg_dump extenddb_catalog > catalog_backup.sql
pg_dump extenddb_catalog_data > data_backup.sql
```

Encryption keys are stored in the catalog database. A catalog backup includes the encryption key needed to decrypt access key secrets.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
