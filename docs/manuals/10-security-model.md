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
6. Compute the string-to-sign from the canonical request
7. Compare computed signature against the provided signature (constant-time comparison)
8. On mismatch: return `SignatureDoesNotMatch` (HTTP 403)
9. On unknown key: return `UnrecognizedClientException` (HTTP 403)

#### Credential Types

| Prefix | Type | Lifetime |
|--------|------|----------|
| `AKIAEXTENDDB` | Long-term access key | Until deleted |
| `ASIAEXTENDDB` | Temporary (AssumeRole) | Configurable, default 1 hour |

#### Credential Storage

- Secret keys encrypted with AES-256-GCM using a per-catalog encryption key
- Encryption key generated during `extenddb init` and stored in the catalog database
- Console passwords hashed with bcrypt (cost factor 12)
- No in-process credential cache — every request reads directly from PostgreSQL

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
