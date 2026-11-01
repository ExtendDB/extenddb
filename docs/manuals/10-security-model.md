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
| `aws:ResourceTag/<key>` | String | Tag on the target DynamoDB table |
| `dynamodb:LeadingKeys` | Multi-valued string | Partition key values being accessed |
| `dynamodb:Attributes` | Multi-valued string | Top-level names of the attributes the request names (see below) |
| `dynamodb:Select` | String | The `Select` parameter value, or the value the read uses without one (see below) |
| `dynamodb:ReturnValues` | String | The `ReturnValues` parameter value, or `NONE` on a `PutItem`, `UpdateItem`, `DeleteItem` or transaction write item without one (see below) |
| `dynamodb:ReturnConsumedCapacity` | String | The `ReturnConsumedCapacity` parameter value, or `NONE` on the item, batch and transaction requests the section below lists |
| `dynamodb:FullTableScan` | Boolean | `true` for Scan operations |
| `dynamodb:EnclosingOperation` | String | The enclosing operation for batch/transact sub-operations |

Policy variables (e.g., `${aws:PrincipalTag/Team}`) are expanded in condition values.

### Access Control Patterns

extenddb supports the same access control patterns as real AWS IAM:

**Identity-Based Access Control (IBAC)**: Attach policies directly to IAM users. Each user's policies define what they can do.

**Role-Based Access Control (RBAC)**: Create IAM groups with policies, add users to groups. Users inherit group policies. Create IAM roles for cross-account or service-to-service access.

**Attribute-Based Access Control (ABAC)**: Use `aws:PrincipalTag/*` and `aws:ResourceTag/*` condition keys to make access decisions based on tags. Example: allow users tagged `Department=Engineering` to access tables tagged `Department=Engineering`.

**Fine-Grained Access Control (FGAC)**: Use `dynamodb:LeadingKeys` to restrict access to specific partition key values. Use `dynamodb:Attributes` to restrict which attributes can be read or written. Example: allow a user to access only items where the partition key matches their user ID.

`dynamodb:LeadingKeys` is evaluated on `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems`. A `Query` on a global secondary index binds the key to the index's partition key value (its first HASH attribute). `Scan` and `SearchVectors` target no single partition and carry no `dynamodb:LeadingKeys` value, so a `ForAllValues` condition does not fence them; fence a full-table read with `ForAnyValue:StringEquals` or a `Null` guard on the statement. A numeric partition key is compared as the normalized number on `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query` and `BatchGetItem`, and as Java's `BigDecimal` prints it (`10.0` stays `10.0`, `1E1` is `1E+1`) on `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems`, which is what DynamoDB does.

`dynamodb:Attributes` holds the top-level names of the attributes a request names, with `ExpressionAttributeNames` placeholders resolved. A document path contributes its first name: a projection of `address.city` or `#a.city` gives `address`, and `tags[0]` gives `tags`. An entry of `AttributesToGet` is one name as written, dot included. The key holds:

| Operation | `dynamodb:Attributes` holds |
|-----------|-----------------------------|
| `GetItem` | The table's key attributes, `AttributesToGet`, and the attributes in `ProjectionExpression` |
| `PutItem` | The table's key attributes, the item's attributes, `Expected`, and the attributes in `ConditionExpression` |
| `UpdateItem` | The `Key` attributes, the table's key attributes, `AttributeUpdates`, `Expected`, the attributes in `ConditionExpression`, and the target of every `SET`, `REMOVE`, `ADD` and `DELETE` action in `UpdateExpression`; not the attributes a `SET` value reads |
| `DeleteItem` | The `Key` attributes, the table's key attributes, `Expected`, and the attributes in `ConditionExpression` |
| `Query` | The key attributes of the table and of the queried index, `AttributesToGet`, `QueryFilter`, `ExclusiveStartKey`, and the attributes in `FilterExpression` and `ProjectionExpression` |
| `Scan` | The key attributes of the table and of the scanned index, `AttributesToGet`, `ScanFilter`, `ExclusiveStartKey`, and the attributes in `FilterExpression` and `ProjectionExpression` |
| `SearchVectors` | The attributes in `ProjectionExpression`; the key is absent when there is none |
| `BatchGetItem` | Per table: the attributes of every key, the table's key attributes, `AttributesToGet`, and the attributes in `ProjectionExpression` |
| `BatchWriteItem` | Per table: the attributes of every `PutRequest` item and `DeleteRequest` key, and the table's key attributes, evaluated once with the union of the table's items |
| `TransactGetItems`, `TransactWriteItems` | Per item, what the single-item operation holds: `Get` as `GetItem`, `Put` as `PutItem`, `Update` as `UpdateItem`, `Delete` as `DeleteItem`; a `ConditionCheck` holds the table's key attributes and the attributes in its `ConditionExpression` |

The names come from the request alone. A `DeleteItem` holds the key attributes of the item it deletes, not the item's other attributes, and a read with no projection holds the key attributes while it returns every attribute of the item, so a `Deny` on `ForAnyValue:StringEquals` over an attribute blocks neither (measured on DynamoDB 2026-10-31). As in DynamoDB, a policy on `dynamodb:Attributes` does not stop an `UpdateItem` from copying a denied attribute's value into another attribute through a `SET` value: under a `Deny` on `ssn`, `SET #n = ssn` with `#n` for `name` is allowed, and a projection of `name` then returns the value (measured on DynamoDB 2026-11-01). A `Scan` holds the table's key attributes when it names no attribute, a `Select COUNT` `Scan` included, so a `Null` condition on the key does not match it: a `Deny` of `Scan` gated on `Null` with value `true` refuses neither a `Scan` with no projection nor a projected one, and an `Allow` of `Scan` gated on it grants neither a `Scan` with no projection nor a `Select COUNT` `Scan` (measured on DynamoDB 2026-11-01). A bare `StringNotEquals` on the key does not match whether the key is present or absent, so an `Allow` gated on it grants nothing; on DynamoDB it granted no `Scan`, whether the `Scan` named an attribute or not (measured 2026-11-01). An allowlist written as an `Allow` on `ForAllValues:StringEquals` must list the key attributes of the table and of every index the statement covers, because every item request other than `SearchVectors` holds them. The key is absent on a `SearchVectors` with no `ProjectionExpression` and, on a table that does not exist, on a `GetItem` or transaction `Get` that names no attribute outside its `Key`, a `Query` that names none outside its key condition and a `Scan` that names none.

A `Deny` on `dynamodb:Attributes` does not stop `ReturnValues` `ALL_OLD` or `ReturnValuesOnConditionCheckFailure` `ALL_OLD` from returning the whole stored item, the denied attribute included. Under a `Deny` on `ssn`, an `UpdateItem` of `name` with `ReturnValues` `ALL_OLD` returns the old item with `ssn` in it, and a `PutItem` whose condition fails and a `TransactWriteItems` `ConditionCheck` that fails, each with `ReturnValuesOnConditionCheckFailure` `ALL_OLD`, return the stored item with `ssn` in the error. DynamoDB does the same (measured 2026-11-01). ExtendDB returns the whole stored item in each of these cases, and on a `PutItem` with `ReturnValues` `ALL_OLD`, whose answer on DynamoDB was not measured. The IAM guide's guard against `ReturnValues` returning attributes is a condition on `dynamodb:ReturnValues`, which holds `NONE` on a write that carries no `ReturnValues` (see below). That key reflects the `ReturnValues` parameter alone, not `ReturnValuesOnConditionCheckFailure`, so an `Allow` gated on `dynamodb:ReturnValues` `NONE` grants a write that carries no `ReturnValues`, and such a write whose condition fails still returns the stored item in the error when it sets `ReturnValuesOnConditionCheckFailure` `ALL_OLD`.

These entries were measured on DynamoDB on 2026-10-31 and 2026-11-01: the item attributes of `PutItem`; the target of a `SET`, `REMOVE`, `ADD` or `DELETE` action and the attributes a `SET` value reads; `ConditionExpression` on `PutItem`, `DeleteItem`, a `ConditionCheck` and a transaction `Delete`; `FilterExpression` on `Query` and `Scan`; `Expected` on `PutItem`, `AttributeUpdates`, `ScanFilter` and `QueryFilter`; `AttributesToGet` on `GetItem`, `Query`, `Scan` and `BatchGetItem`; the table's key attributes on `GetItem`, `Query`, `Scan`, `UpdateItem`, `DeleteItem` and a `DeleteRequest`; projected document paths; reads with no projection; `DeleteItem`; a transaction `Update`; the `BatchGetItem`, `TransactGetItems`, `BatchWriteItem` and `TransactWriteItems` forms of a projection and a put; and the evaluation of a `BatchWriteItem` once per table. `ProjectionExpression` on `Query` and `Scan`, `Expected` and `ConditionExpression` on `UpdateItem`, and `Expected` on `DeleteItem` follow the rule measured for the same parameter on the other operations. The key attributes of the queried or scanned index follow the IAM guide's rule that a policy on `dynamodb:Attributes` lists the primary key and index key attributes of the table and of every secondary index the policy covers; they were not measured. Whether `ExclusiveStartKey` adds names of its own cannot be told on DynamoDB, because a valid start key holds only key attributes, which the key already holds.

`dynamodb:Select`, `dynamodb:ReturnValues` and `dynamodb:ReturnConsumedCapacity` hold the request's `Select`, `ReturnValues` and `ReturnConsumedCapacity`. When a request leaves the parameter out, the key holds the value the operation then uses, as on DynamoDB, so a condition decides the same way whether or not the caller writes that value out:

| Key | Requests | Value when the request has no such parameter |
|-----|----------|----------------------------------------------|
| `dynamodb:Select` | `GetItem`, `Query` and `Scan`, each table of a `BatchGetItem`, and each `Get` of a `TransactGetItems` | `SPECIFIC_ATTRIBUTES` with a `ProjectionExpression` or `AttributesToGet`, and `ALL_ATTRIBUTES` without one |
| `dynamodb:Select` | A `Query` or `Scan` with `IndexName` and no projection | `ALL_PROJECTED_ATTRIBUTES` |
| `dynamodb:ReturnValues` | `PutItem`, `UpdateItem` and `DeleteItem`, and the `Put`, `Update` and `Delete` items of a `TransactWriteItems` | `NONE` |
| `dynamodb:ReturnConsumedCapacity` | `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query` and `Scan`, and each table or item of a `BatchGetItem`, `BatchWriteItem`, `TransactGetItems` or `TransactWriteItems` | `NONE` |

A `Query` or `Scan` that carries `Select` holds it as given. `GetItem`, `BatchGetItem` and `TransactGetItems` take no `Select` parameter, so their value comes from the projection alone, and a `Select` member in one of them is not read. The `Put`, `Update` and `Delete` items of a transaction take no `ReturnValues` parameter and always hold `NONE`. A `BatchWriteItem` leaves `dynamodb:ReturnValues` absent; a `ConditionCheck` is treated the same, which was not measured. A batch or transaction request carries one `ReturnConsumedCapacity`, and each of its tables or items is evaluated with it.

So a `Deny` on `dynamodb:Select` `ALL_ATTRIBUTES` refuses a read with no projection, which returns the whole item, and an `Allow` gated on `StringEquals` with `dynamodb:ReturnValues` `NONE` grants a write that carries no `ReturnValues`. A `Query` or `Scan` of an index with no projection holds `ALL_PROJECTED_ATTRIBUTES`, even when the index projects every attribute, so a statement meant to refuse whole-item reads of a table and its indexes lists both `ALL_ATTRIBUTES` and `ALL_PROJECTED_ATTRIBUTES`.

These values were measured on DynamoDB on 2026-11-01: `dynamodb:Select` on `GetItem` and `Query` with and without a projection, on `Scan` without one and with `Select` `COUNT`, on `Query` with `Select` `COUNT`, on a `BatchGetItem` with and without a projection, and on a `TransactGetItems` `Get` without one; `dynamodb:ReturnValues` on `PutItem`, `UpdateItem` and `DeleteItem` with and without the parameter, on a `BatchWriteItem`, and on the `Put`, `Update` and `Delete` items of a `TransactWriteItems`; and `dynamodb:ReturnConsumedCapacity` on `GetItem` with and without the parameter, on a `BatchWriteItem` and on a `TransactGetItems` `Get`. `dynamodb:ReturnConsumedCapacity` on the other requests follows those, and a projection written as `AttributesToGet` follows the rule measured for `ProjectionExpression`; neither was measured directly. `SPECIFIC_ATTRIBUTES` on a `Scan` with a projection follows the `Query` and `GetItem` rows and was not measured, and a `TransactGetItems` `Get` with a projection was measured only against a `Deny` on `ALL_ATTRIBUTES`, which it passed, so its `SPECIFIC_ATTRIBUTES` value follows the `BatchGetItem` and `GetItem` rows. A `ConditionCheck` leaving `dynamodb:ReturnValues` absent was not measured. `ALL_PROJECTED_ATTRIBUTES` on a read of an index follows the DynamoDB API reference, which gives it as the default when a `Query` or `Scan` of an index has neither `Select` nor `ProjectionExpression`; it was not measured.

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
