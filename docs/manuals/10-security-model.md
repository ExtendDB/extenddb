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

The steps run in this order; every rejection is HTTP 400.

1. Extract the `Authorization` header (AWS4-HMAC-SHA256 scheme); absent: `MissingAuthenticationToken`
2. Parse credential scope, signed headers, and signature; malformed: `IncompleteSignatureException`. A request with more than one `host` or `authorization` header is refused before this point with a plain 400, as the service's front end does
3. Look up the access key in the credential store (through the credential cache, see below); unknown: `UnrecognizedClientException`
4. Check `X-Amz-Date` against the server clock: more than 15 minutes of skew is `UnrecognizedClientException` (signature expired)
5. Check the credential scope: the region must be this server's `[server] region` and the service must be `dynamodb`; otherwise `InvalidSignatureException` carrying one sentence per failing part, region first, as the service reports it
6. Compare the session token: a temporary key must present its stored token, and a long-term key must present none; the decision is applied at step 9
7. Reconstruct the canonical request and the string-to-sign. `host` must be among the signed headers. Repeated headers contribute their values comma-joined. The hashed-payload line is the SHA-256 of the body the server received; a client-supplied `x-amz-content-sha256` header is not used for it, so a request whose body was altered after signing, or signed with a literal such as `UNSIGNED-PAYLOAD`, fails at step 8
8. Derive the signing key (HMAC-SHA256 chain over date, region, service, "aws4_request") and compare the computed signature with the provided one in constant time; mismatch: `InvalidSignatureException`
9. Apply the deferred decisions from steps 3 and 6, after the signature check so that failure paths take the same time: an inactive key or a wrong or missing token is `UnrecognizedClientException`

Request bodies are parsed before authentication (the service does the same), so a malformed body returns `SerializationException` whatever the signature. Headers not named in `SignedHeaders` do not take part in verification; the service behaves the same way, and clients sign every `x-amz-*` header they send.

#### Credential Types

| Prefix | Type | Lifetime |
|--------|------|----------|
| `AKIAEXTENDDB` | Long-term access key | Until deleted |
| `ASIAEXTENDDB` | Temporary (AssumeRole) | Configurable, default 1 hour |

#### Credential Storage

- Secret keys encrypted with AES-256-GCM using a per-catalog encryption key
- Encryption key generated during `extenddb init` and stored in the catalog database
- Console passwords hashed with bcrypt (cost factor 12)
- Credentials, policies, tags, and table key information are cached in process (`[auth.cache]`, enabled by default, 60 second hard TTL, 30 second stale-while-revalidate). A change made through this instance's admin API or console invalidates its own cache at once; a change made on another instance sharing the catalog is visible here within the TTL, so a key revoked elsewhere can be accepted by this instance for up to 60 seconds. See the admin guide for the settings and the kill switch

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
- **Concurrent policy fetching**: Identity policies, group policies, and permissions boundaries are fetched concurrently from PostgreSQL. All must succeed for evaluation to proceed.
- **Constant-time rejection for inactive keys**: Inactive or expired access keys are rejected without timing differences that could reveal key existence.
- **Policy document validation on write**: Policy documents are validated for JSON structure and size-capped (6,144 bytes) when attached via the management API. Invalid documents are rejected before storage.
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
| `dynamodb:ResourceTag/<key>` | String | Tag on the target DynamoDB table |
| `dynamodb:LeadingKeys` | Multi-valued string | Partition key values being accessed |
| `dynamodb:Attributes` | Multi-valued string | Attribute names being read or written |
| `dynamodb:Select` | String | The `Select` parameter value |
| `dynamodb:ReturnValues` | String | The `ReturnValues` parameter value |
| `dynamodb:ReturnConsumedCapacity` | String | The `ReturnConsumedCapacity` parameter value |
| `dynamodb:FullTableScan` | Boolean | `true` for Scan operations |
| `dynamodb:EnclosingOperation` | String | The enclosing operation for batch/transact sub-operations |

Policy variables (e.g., `${aws:PrincipalTag/Team}`) are expanded in condition values.

### Access Control Patterns

extenddb supports the same access control patterns as real AWS IAM:

**Identity-Based Access Control (IBAC)**: Attach policies directly to IAM users. Each user's policies define what they can do.

**Role-Based Access Control (RBAC)**: Create IAM groups with policies, add users to groups. Users inherit group policies. Create IAM roles for cross-account or service-to-service access.

**Attribute-Based Access Control (ABAC)**: Use `aws:PrincipalTag/*` and `dynamodb:ResourceTag/*` condition keys to make access decisions based on tags. Example: allow users tagged `Department=Engineering` to access tables tagged `Department=Engineering`.

**Fine-Grained Access Control (FGAC)**: Use `dynamodb:LeadingKeys` to restrict access to specific partition key values. Use `dynamodb:Attributes` to restrict which attributes can be read or written. Example: allow a user to access only items where the partition key matches their user ID.

### Resource ARNs

Resources are identified by ARN: `arn:aws:dynamodb:<region>:<account-id>:table/<table-name>`. Wildcard matching (`*`, `?`) in policy Resource fields follows AWS IAM conventions. `NotResource` is also supported.

### Supported Policy Elements

- `Effect`: Allow, Deny
- `Action` / `NotAction`: DynamoDB actions (e.g., `dynamodb:PutItem`, `dynamodb:*`)
- `Resource` / `NotResource`: ARN patterns with wildcards
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

The management web console (`/console/*`) implements:

- **CSRF protection**: Unique tokens generated per session, injected into all forms via JavaScript, validated on every POST handler
- **Session management**: Server-side sessions with 8-hour expiry, HttpOnly cookies with `SameSite=Strict` and `Path=/console`
- **Security headers**: X-Content-Type-Options (nosniff), X-Frame-Options (DENY), Referrer-Policy (strict-origin-when-cross-origin)
- **HSTS**: Sent automatically (TLS is always enabled)
- **Login rate limiting**: Failed login attempts are tracked per user. Excessive failures trigger account lockout.

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
- Management operations are audit-logged at WARN level
- Log messages do not contain credentials, access keys, or item data

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
