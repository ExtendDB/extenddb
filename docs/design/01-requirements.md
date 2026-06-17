# extenddb — Requirements Document

**Version:** 1.0
**Date:** 2026-04-03
**Status:** Draft

## 1. Overview

extenddb (ExtendDB) is a standalone, stateless Rust application that provides a 100% DynamoDB-compatible API backed by pluggable storage engines. Any AWS SDK client (Java, Python, Go, Node.js, Rust, .NET, etc.) connects directly with zero code changes. The system runs as its own process, completely independent of any specific database, and supports pluggable authentication providers and pluggable storage backends.

### 1.1 Goals

- **Full DynamoDB API compatibility** — byte-for-byte compatible wire protocol, error responses, and SDK behavior
- **Pluggable storage** — clean trait-based abstraction allowing any database as a backend (PostgreSQL first)
- **Pluggable authentication** — support local SigV4, AWS IAM, Azure AD, and custom providers
- **Standalone & stateless** — single async Rust binary, no embedded database, horizontally scalable
- **Production-ready** — TLS, observability, graceful shutdown, configurable limits, VM and Kubernetes deployment

### 1.2 Non-Goals / Out of Scope

| Feature | Reason |
|---------|--------|
| PartiQL (ExecuteStatement, BatchExecuteStatement, ExecuteTransaction) | Deferred — focus on JSON wire protocol first |
| Backup/Restore (CreateBackup, DeleteBackup, DescribeBackup, ListBackups, RestoreTableFromBackup, DescribeContinuousBackups, UpdateContinuousBackups, RestoreTableToPointInTime) | Excluded — backup is a storage-layer concern, not an API concern |
| Auto-scaling APIs (DescribeTableReplicaAutoScaling, UpdateTableReplicaAutoScaling) | Excluded — auto-scaling is an infrastructure concern |
| Contributor Insights (DescribeContributorInsights, ListContributorInsights, UpdateContributorInsights) | Excluded — analytics feature, not core API |
| Kinesis Streaming Destination (DescribeKinesisStreamingDestination, EnableKinesisStreamingDestination, DisableKinesisStreamingDestination, UpdateKinesisStreamingDestination) | Excluded — AWS-specific integration |
| Resource Policies (GetResourcePolicy, PutResourcePolicy, DeleteResourcePolicy) | Deferred — resource-based policies are a future enhancement |
| IAM Policy Variables (`${aws:PrincipalTag/key}`, `${aws:username}`, etc.) | Deferred — variable substitution in Resource ARNs and Condition values is a future enhancement (see REQ-ABAC-006) |
| Federated Role Assumption (AssumeRoleWithSAML, AssumeRoleWithWebIdentity) | Deferred — v1 supports basic AssumeRole only (see REQ-IDENT-008) |
| Role Chaining (role assumes role) | Deferred — v1 requires CallerArn to be a user; SourceIdentity and TransitiveTagKeys are deferred alongside (see REQ-IDENT-009) |
| Role MaxSessionDuration | Deferred — v1 accepts any DurationSeconds with no per-role cap (see REQ-IDENT-010) |

## 2. Wire Protocol Compatibility

### 2.1 HTTP Request Format

DynamoDB and DynamoDB Streams are two separate services in every AWS SDK, but both use the same wire protocol format: HTTP POST to `/` with a JSON body. The services are distinguished by the `X-Amz-Target` header prefix:

| Service | Target Prefix | Example |
|---------|--------------|---------|
| DynamoDB | `DynamoDB_20120810` | `DynamoDB_20120810.PutItem` |
| DynamoDB Streams | `DynamoDBStreams_20120810` | `DynamoDBStreams_20120810.GetRecords` |

Both services use `dynamodb` as the SigV4 signing name, so authentication is identical.

```
POST / HTTP/1.1
Host: <host>:<port>
Content-Type: application/x-amz-json-1.0
X-Amz-Target: DynamoDB_20120810.<OperationName>
X-Amz-Date: <ISO8601>
Authorization: AWS4-HMAC-SHA256 Credential=<...>, SignedHeaders=<...>, Signature=<...>
Content-Length: <n>

<JSON request body>
```

**Requirements:**
- REQ-WIRE-001: Accept HTTP POST to `/` for all DynamoDB and DynamoDB Streams operations
- REQ-WIRE-002: Parse `X-Amz-Target` header to extract operation name. Accept both prefixes: `DynamoDB_20120810.<Op>` and `DynamoDBStreams_20120810.<Op>`
- REQ-WIRE-003: Accept `Content-Type: application/x-amz-json-1.0`
- REQ-WIRE-004: Parse `Authorization` header for SigV4 authentication
- REQ-WIRE-005: Parse `X-Amz-Date` header for request timestamp validation
- REQ-WIRE-006: Support `Accept-Encoding: gzip` and return compressed responses
- REQ-WIRE-007: Return `UnknownOperationException` (HTTP 404) for unrecognized operations

### 2.2 HTTP Response Format

```
HTTP/1.1 200 OK
x-amzn-RequestId: <UUID>
x-amz-crc32: <CRC32>
Content-Type: application/x-amz-json-1.0
Content-Length: <n>

<JSON response body>
```

**Requirements:**
- REQ-WIRE-010: Return `x-amzn-RequestId` header on every response (UUID v4)
- REQ-WIRE-011: Return `x-amz-crc32` header with CRC32 checksum of the response body
- REQ-WIRE-012: Return `Content-Type: application/x-amz-json-1.0`
- REQ-WIRE-013: Support gzip response compression when client sends `Accept-Encoding: gzip`
- REQ-WIRE-014: Return HTTP 200 for successful operations
- REQ-WIRE-015: Return appropriate HTTP status codes for errors (see §2.3)

### 2.3 Error Response Format

Error responses must be byte-for-byte compatible with DynamoDB:

```json
{
    "__type": "com.amazonaws.dynamodb.v20120810#<ErrorType>",
    "message": "<human-readable message>"
}
```

**Requirements:**
- REQ-ERR-001: Error JSON must contain `__type` field with `com.amazonaws.dynamodb.v20120810#` prefix
- REQ-ERR-002: Error JSON must contain `message` field with descriptive text
- REQ-ERR-003: HTTP status codes must match DynamoDB exactly (see error catalog below)
- REQ-ERR-004: SDK retry behavior must work identically (throttling errors return 400, not 429)

### 2.4 Error Catalog

| Error Type | HTTP Status | When Returned |
|------------|-------------|---------------|
| `AccessDeniedException` | 400 | IAM policy denies the action |
| `ConditionalCheckFailedException` | 400 | Condition expression evaluates to false |
| `IdempotentParameterMismatchException` | 400 | TransactWriteItems with same client token but different params |
| `IncompleteSignature` | 403 | SigV4 Authorization header is malformed |
| `InternalServerError` | 500 | Unhandled server error |
| `ItemCollectionSizeLimitExceededException` | 400 | Item collection exceeds 10 GB (tables with LSIs) |
| `MalformedHttpRequestException` | 400 | Request body cannot be decompressed or parsed |
| `MissingAuthenticationToken` | 403 | No Authorization header present |
| `ProvisionedThroughputExceededException` | 400 | Table/partition throughput limit exceeded |
| `RequestEntityTooLargeException` | 413 | Request body exceeds maximum size |
| `RequestLimitExceeded` | 400 | Account-level throughput quota exceeded |
| `RequestTimeoutException` | 408 | Request processing timed out |
| `ResourceInUseException` | 400 | Table is being created/deleted/updated |
| `ResourceNotFoundException` | 400 | Table or index does not exist |
| `SerializationException` | 400 | Request JSON is malformed or has invalid types |
| `ServiceUnavailable` | 503 | Server is temporarily unavailable |
| `ThrottlingException` | 400 | General throttling (rate limiting) |
| `TransactionCanceledException` | 400 | Transaction cancelled due to condition failure or conflict |
| `TransactionConflictException` | 400 | Concurrent transaction conflict on same item |
| `TransactionInProgressException` | 400 | Transaction with same client token already in progress |
| `UnknownOperationException` | 404 | Unrecognized operation in X-Amz-Target |
| `UnrecognizedClientException` | 403 | Invalid access key or signature mismatch |
| `ValidationException` | 400 | Input validation failure (bad table name, missing required field, etc.) |

## 3. Functional Requirements — Operations

### 3.1 Data Plane Operations (In Scope)

These are the core CRUD operations. All must be fully implemented.

| # | Operation | Description |
|---|-----------|-------------|
| 1 | `PutItem` | Create or replace an item |
| 2 | `GetItem` | Read a single item by primary key |
| 3 | `UpdateItem` | Modify attributes of an existing item (SET/REMOVE/ADD/DELETE) |
| 4 | `DeleteItem` | Delete a single item by primary key |
| 5 | `Query` | Read items matching a key condition, with optional filter |
| 6 | `Scan` | Read all items in a table/index, with optional filter |
| 7 | `BatchGetItem` | Read up to 100 items across multiple tables |
| 8 | `BatchWriteItem` | Write/delete up to 25 items across multiple tables |
| 9 | `TransactGetItems` | Atomic read of up to 100 items |
| 10 | `TransactWriteItems` | Atomic write of up to 100 items (put/update/delete/condition_check) |

**Key requirements for data plane:**
- REQ-DATA-001: PutItem must support `ConditionExpression`, `ReturnValues` (NONE, ALL_OLD), `ReturnConsumedCapacity`, `ReturnItemCollectionMetrics`, `ReturnValuesOnConditionCheckFailure`
- REQ-DATA-002: GetItem must support `ConsistentRead`, `ProjectionExpression`, `ExpressionAttributeNames`
- REQ-DATA-003: UpdateItem must support all four update actions: SET, REMOVE, ADD, DELETE
- REQ-DATA-004: UpdateItem must support `ReturnValues` (NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW)
- REQ-DATA-005: Query must support `KeyConditionExpression`, `FilterExpression`, `ProjectionExpression`, `ScanIndexForward`, `Limit`, `ExclusiveStartKey`, pagination via `LastEvaluatedKey`
- REQ-DATA-006: Query must support querying Global Secondary Indexes and Local Secondary Indexes via `IndexName`
- REQ-DATA-007: Scan must support `FilterExpression`, `ProjectionExpression`, `Limit`, `ExclusiveStartKey`, `Segment`/`TotalSegments` for parallel scan
- REQ-DATA-008: Scan response size must not exceed 1 MB; return `LastEvaluatedKey` for pagination
- REQ-DATA-009: BatchGetItem must support up to 100 items, 16 MB total response; return `UnprocessedKeys` for partial failures
- REQ-DATA-010: BatchWriteItem must support up to 25 items (put or delete); return `UnprocessedItems` for partial failures
- REQ-DATA-011: TransactWriteItems must be atomic — all succeed or all fail; support `ClientRequestToken` for idempotency
- REQ-DATA-012: TransactGetItems must be atomic — consistent snapshot of up to 100 items
- REQ-DATA-013: All data operations must support legacy `Expected` and `ConditionalOperator` parameters (converted internally to expressions)
- REQ-DATA-014: All write operations must trigger stream event capture when streams are enabled on the table
- REQ-DATA-015: `ConsistentRead` parameter must be accepted on GetItem, Query, Scan, and BatchGetItem. When a read replica is configured, `ConsistentRead=false` reads must route to the replica; `ConsistentRead=true` reads must route to the primary. When no replica is configured, all reads are strongly consistent. Capacity calculations must reflect the consistency mode regardless of replica configuration (0.5 RCU for eventually consistent, 1.0 RCU for strongly consistent).

### 3.2 Control Plane Operations (In Scope)

| # | Operation | Description |
|---|-----------|-------------|
| 11 | `CreateTable` | Create a new table with key schema, optional GSIs/LSIs |
| 12 | `DeleteTable` | Delete a table and all its data |
| 13 | `DescribeTable` | Return table metadata, status, size, item count |
| 14 | `UpdateTable` | Modify throughput, add/remove GSIs, change billing mode |
| 15 | `ListTables` | List table names with pagination |
| 16 | `DescribeTimeToLive` | Return TTL configuration for a table |
| 17 | `UpdateTimeToLive` | Enable/disable TTL on a table attribute |
| 18 | `DescribeEndpoints` | Return service endpoint information |
| 19 | `DescribeLimits` | Return account-level throughput limits |
| 20 | `TagResource` | Add tags to a table |
| 21 | `UntagResource` | Remove tags from a table |
| 22 | `ListTagsOfResource` | List tags for a table |

**Key requirements for control plane:**
- REQ-CTRL-001: CreateTable must support `KeySchema`, `AttributeDefinitions`, `BillingMode` (PROVISIONED, PAY_PER_REQUEST), `ProvisionedThroughput`, `GlobalSecondaryIndexes`, `LocalSecondaryIndexes`, `StreamSpecification`, `SSESpecification`, `Tags`
- REQ-CTRL-002: CreateTable must validate all naming rules and limits (see §5)
- REQ-CTRL-003: UpdateTable must support adding/removing GSIs, changing billing mode, and modifying provisioned throughput
- REQ-CTRL-004: DescribeTable must return accurate `TableSizeBytes` and `ItemCount` (may be approximate)
- REQ-CTRL-005: ListTables must support `Limit` and `ExclusiveStartTableName` for pagination
- REQ-CTRL-006: TTL worker must run as a background task, using an indexed sweep (expression index on TTL attribute) to efficiently find and delete expired items. Staleness metrics are recorded per deletion.

### 3.3 Import/Export Operations (In Scope)

| # | Operation | Description |
|---|-----------|-------------|
| 23 | `ImportTable` | Import data from S3 or local filesystem (DYNAMODB_JSON, ION, CSV) |
| 24 | `DescribeImport` | Describe an import job |
| 25 | `ListImports` | List import jobs |
| 26 | `ExportTableToPointInTime` | Export table data to filesystem (DYNAMODB_JSON, ION) |
| 27 | `DescribeExport` | Describe an export job |
| 28 | `ListExports` | List export jobs |

**Key requirements:**
- REQ-IMPEXP-001: Import must support DYNAMODB_JSON, ION, and CSV input formats
- REQ-IMPEXP-002: Export must support DYNAMODB_JSON and ION output formats
- REQ-IMPEXP-003: Import/export must work with local filesystem paths (S3 support is a future enhancement)
- REQ-IMPEXP-004: Import/export jobs must be tracked with status (IN_PROGRESS, COMPLETED, FAILED, CANCELLED)

### 3.4 DynamoDB Streams Operations (In Scope — Deferred Detail)

| # | Operation | Description |
|---|-----------|-------------|
| 29 | `DescribeStream` | Return stream metadata and shard information |
| 30 | `ListStreams` | List streams, optionally filtered by table |
| 31 | `GetShardIterator` | Get an iterator for reading stream records from a shard |
| 32 | `GetRecords` | Read stream records using a shard iterator |

**Key requirements:**
- REQ-STREAM-001: Support all four stream view types: KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES
- REQ-STREAM-002: Stream records must capture changes from PutItem, UpdateItem, DeleteItem, BatchWriteItem, TransactWriteItems
- REQ-STREAM-003: Support shard iterator types: TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER
- REQ-STREAM-004: Stream records must be ordered by sequence number within a shard
- REQ-STREAM-005: Detailed shard management and retention design is deferred

### 3.5 Global Tables Operations (In Scope — Deferred Detail)

| # | Operation | Description |
|---|-----------|-------------|
| 33 | `CreateGlobalTable` | Create a global table with replicas |
| 34 | `DescribeGlobalTable` | Describe global table configuration |
| 35 | `DescribeGlobalTableSettings` | Describe per-replica settings |
| 36 | `ListGlobalTables` | List global tables |
| 37 | `UpdateGlobalTable` | Add/remove replicas |
| 38 | `UpdateGlobalTableSettings` | Update per-replica settings |

**Key requirements:**
- REQ-GT-001: Include Global Tables operations in the API surface (accept requests, return valid responses)
- REQ-GT-002: Detailed multi-region replication design is deferred
- REQ-GT-003: Single-instance mode must accept Global Table operations and treat the local instance as the sole replica

## 4. Authentication & Authorization Requirements

### 4.1 Pluggable Authentication

- REQ-AUTH-001: Authentication must be pluggable via a provider trait
- REQ-AUTH-002: The built-in provider must implement full AWS SigV4 signature validation
- REQ-AUTH-003: SigV4 validation must include: canonical request construction, signing key derivation (HMAC-SHA256 chain: secret → date → region → service → signing), constant-time signature comparison
- REQ-AUTH-004: Support configurable clock skew tolerance (default: ±300 seconds)
- REQ-AUTH-005: Credential storage must encrypt secret keys at rest (AES-256-GCM)
- REQ-AUTH-006: Credential lookup must be cached with configurable TTL
- REQ-AUTH-007: Future providers must be addable without modifying core code:
  - **Azure AD**: Validate OAuth2/OIDC bearer token, map Azure AD identity to local identity
  - **Custom**: User-provided authentication via shared library or gRPC sidecar

### 4.1.1 Auth Modes

extenddb supports three authentication modes, selectable by configuration. The mode determines how credentials are validated and how authorization decisions are made.

**Mode 1: No auth** (removed — `auth.provider = "none"` is no longer accepted)
Previously accepted any request regardless of the Authorization header. This mode has been removed. The server refuses to start with `provider = "none"`.

**Mode 2: Local credential store** (`auth.provider = "builtin"`)
extenddb maintains its own credential store and policy documents. SigV4 signatures are validated against locally stored secret keys. Authorization is evaluated against locally stored IAM-style policies. Credentials and policies are managed via the management API or seed configuration. This is the default for production in disconnected or semi-connected environments.

**Mode 3: AWS IAM** (`auth.provider = "aws_iam"`)
When extenddb has network connectivity to AWS (e.g., running on EC2), it delegates authentication and authorization to real AWS IAM. This mode has three sub-capabilities:

- REQ-AUTH-008: **STS authentication via pre-signed GetCallerIdentity token.** The client generates a pre-signed STS `GetCallerIdentity` URL (signed for the `sts` service) and sends it as an `X-Extenddb-Auth-Token` header alongside the normal DynamoDB request. extenddb calls STS using this pre-signed URL to validate the credential and resolve the caller's IAM ARN, account ID, and user ID. Cache validated identities with configurable TTL to avoid per-request STS calls. This is the same pattern used by EKS (`aws-iam-authenticator`) and Vault's AWS auth method.
- REQ-AUTH-009: **IAM policy retrieval.** Fetch the caller's attached policies, group policies, and permissions boundaries from IAM using `iam:GetUserPolicy`, `iam:ListAttachedUserPolicies`, `iam:GetPolicy`, `iam:GetPolicyVersion`, `iam:ListGroupsForUser`, `iam:GetRolePolicy`, `iam:ListAttachedRolePolicies`, and `iam:GetUserPermissionsBoundary` / `iam:GetRolePermissionsBoundary`. Retrieved policies are evaluated by the same local policy engine used in Mode 2. Cache retrieved policies with configurable TTL.
- REQ-AUTH-010: **Near-seamless SDK experience with thin client wrapper.** A lightweight extenddb client wrapper generates the pre-signed STS token and attaches it as `X-Extenddb-Auth-Token` before each request. The wrapper uses the same AWS credentials and SDK configuration as normal DynamoDB calls. Switching between extenddb and real DynamoDB requires changing `endpoint_url` and adding/removing the wrapper. The wrapper is provided for Python (boto3 plugin), Rust, and Java.
- REQ-AUTH-011: **Graceful degradation.** If STS or IAM is unreachable (network partition, transient failure), extenddb must return a clear error (`ServiceUnavailable` with a message indicating the auth backend is unreachable) rather than silently allowing or denying requests. Cached identities and policies remain valid for their TTL during outages.
- REQ-AUTH-012: **IAM permissions required.** The EC2 instance (or IAM principal) running extenddb must have permissions to call `sts:GetCallerIdentity` (always allowed, no policy needed) and the `iam:Get*`/`iam:List*` actions above. The required IAM policy for the extenddb host is documented in the deployment guide.

> **Design note:** Mode 3 enables a powerful workflow: develop locally with Mode 2 (local credentials + policies), then deploy to EC2 with Mode 3 (real IAM) using the same application code and SDK configuration (only `endpoint_url` changes, plus adding the extenddb client wrapper). The same policy documents work in both modes because the policy engine is shared. The pre-signed token approach is a proven pattern used by EKS and Vault — it works because the token is signed for the `sts` service by the client's own credentials, avoiding the SigV4 service-binding problem.

### 4.2 Identity Model

extenddb implements a local IAM identity model that mirrors the structure of AWS IAM for DynamoDB access control. This enables testing of IAM policies, role-based access, and tag-based access control (ABAC) against extenddb with the same policy documents used in production.

- REQ-IDENT-001: Support IAM-style principals: users and roles. A **user** has long-term credentials (access key + secret key). A **role** has a trust policy and is assumed by users to obtain temporary credentials.
- REQ-IDENT-002: Support **groups**. A group contains zero or more users. Policies attached to a group apply to all member users. A user can belong to multiple groups.
- REQ-IDENT-003: Each principal (user, role) has an ARN: `arn:aws:iam::{account_id}:user/{name}`, `arn:aws:iam::{account_id}:role/{name}`. Groups: `arn:aws:iam::{account_id}:group/{name}`.
- REQ-IDENT-004: Support **principal tags** (key-value pairs attached to users and roles). Principal tags are available as `aws:PrincipalTag/{key}` in policy condition evaluation.
- REQ-IDENT-005: Support **session tags** passed during role assumption. Session tags override principal tags for the duration of the session and are available as `aws:PrincipalTag/{key}`.
- REQ-IDENT-006: Support **AssumeRole** — a user assumes a role to obtain temporary session credentials (access key, secret key, session token). The session has an expiration time. The role's trust policy controls who can assume it.
- REQ-IDENT-007: Session credentials include an `aws:PrincipalTag/*` namespace populated from the role's tags, the user's tags, and any session tags passed at assumption time (session tags take precedence).
- REQ-IDENT-008 (Deferred): **Federated role assumption** (`AssumeRoleWithSAML`, `AssumeRoleWithWebIdentity`) is not supported in v1. Applications that use web identity federation (e.g., Lambda functions, EKS pods) to access DynamoDB cannot replicate that auth path against extenddb. Workaround: use basic AssumeRole with equivalent policies.
- REQ-IDENT-009 (Deferred): **Role chaining** (a role assuming another role) is not supported in v1. `CallerArn` in AssumeRole must reference a user. `SourceIdentity` (tracks the original caller through a chain) and `TransitiveTagKeys` (session tags that persist through chains) are deferred alongside role chaining since they only apply in chained scenarios.
- REQ-IDENT-010 (Deferred): **MaxSessionDuration** per role is not enforced in v1. AWS IAM roles have a configurable maximum session duration (default 1 hour, up to 12 hours) that caps the `DurationSeconds` parameter in AssumeRole. extenddb v1 accepts any `DurationSeconds` value with no per-role validation.

### 4.3 Authorization — Identity-Based Policies

- REQ-AUTHZ-001: Authorization must evaluate IAM-style policy documents
- REQ-AUTHZ-002: Support `Allow` and `Deny` effects with explicit Deny precedence
- REQ-AUTHZ-003: Support action matching with wildcards (`dynamodb:*`, `dynamodb:Get*`)
- REQ-AUTHZ-004: Support resource matching with wildcards (`arn:aws:dynamodb:*:*:table/Prefix*`)
- REQ-AUTHZ-005: Authorization must be decoupled from authentication — any auth provider can feed into the same policy engine
- REQ-AUTHZ-006: Support **identity-based policies** attached to users, groups, and roles. When a request is made:
  - Collect all policies attached directly to the user
  - Collect all policies from groups the user belongs to
  - If the user assumed a role: collect all policies attached to the role, plus any session policy passed during assumption
  - Evaluate all collected policies together using the standard IAM evaluation algorithm
- REQ-AUTHZ-007: Support **permissions boundaries** on users and roles. A permissions boundary is a managed policy that sets the maximum permissions. The effective permissions are the intersection of identity-based policies and the permissions boundary.
- REQ-AUTHZ-008: Policy evaluation order follows AWS IAM semantics: (1) explicit Deny in any policy → Deny; (2) if permissions boundary exists, action must be allowed by both the boundary and an identity policy; (3) explicit Allow in any identity policy → Allow; (4) implicit Deny.

### 4.4 Authorization — Tag-Based Access Control (ABAC)

- REQ-ABAC-001: Support **resource tag conditions** in policies via `dynamodb:ResourceTag/{key}`. The policy engine resolves resource tags by querying the tag store for the target table's ARN.
- REQ-ABAC-002: Support **principal tag conditions** via `aws:PrincipalTag/{key}`. Principal tags come from the authenticated identity (user tags, role tags, or session tags).
- REQ-ABAC-003: Support the following condition operators for ABAC:
  - String operators: `StringEquals`, `StringNotEquals`, `StringLike`, `StringNotLike`, `StringEqualsIgnoreCase`
  - Numeric operators: `NumericEquals`, `NumericNotEquals`, `NumericLessThan`, `NumericGreaterThan`, `NumericLessThanEquals`, `NumericGreaterThanEquals`
  - Date operators: `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateGreaterThan`, `DateLessThanEquals`, `DateGreaterThanEquals`
  - Boolean: `Bool`
  - Null check: `Null`
  - ARN operators: `ArnLike`, `ArnNotLike`, `ArnEquals`, `ArnNotEquals`
  - Set operators: `ForAllValues:StringEquals`, `ForAnyValue:StringEquals` (and other `ForAllValues`/`ForAnyValue` prefixes with any string/numeric operator)
- REQ-ABAC-004: Support the following DynamoDB-specific condition context keys:
  - `dynamodb:LeadingKeys` — restricts access based on the partition key value of items being accessed. Used with `ForAllValues:StringEquals` to limit a user to their own items.
  - `dynamodb:Attributes` — restricts which attributes can be read or written. Used with `ForAllValues:StringEquals`.
  - `dynamodb:Select` — restricts the `Select` parameter in Query/Scan (e.g., force `SPECIFIC_ATTRIBUTES` to prevent full-item reads).
  - `dynamodb:ReturnValues` — restricts the `ReturnValues` parameter.
  - `dynamodb:ReturnConsumedCapacity` — restricts the `ReturnConsumedCapacity` parameter.
  - `dynamodb:FullTableScan` — restricts whether Scan operations are allowed (`Bool` condition).
  - `dynamodb:EnclosingOperation` — identifies the parent operation for batch/transact sub-operations.
- REQ-ABAC-005: The policy engine must build a **request context** for each operation containing all applicable context keys. The request context is populated by the server middleware before policy evaluation.
- REQ-ABAC-006 (Deferred): **Policy variables** (`${aws:PrincipalTag/key}`, `${aws:username}`, etc.) in Resource ARNs and Condition values are not supported in v1. In AWS IAM, policy variables enable a single policy to grant access to resources that match the caller's own tags — e.g., `"Resource": "arn:aws:dynamodb:*:*:table/${aws:PrincipalTag/Team}-*"`. Without policy variables, each team or department requires a separate policy with hardcoded resource ARNs, which reduces the ABAC testing value. This is a high-priority enhancement for a future version.

### 4.5 Authorization — Caching

- REQ-CACHE-001: Policy evaluation results must be cached with configurable TTL to avoid repeated database lookups on every request
- REQ-CACHE-002: Identity resolution (user → groups → policies, role → policies) must be cached with configurable TTL
- REQ-CACHE-003: Resource tag lookups for ABAC must be cached with configurable TTL
- REQ-CACHE-004: Cache invalidation must occur when policies, group memberships, tags, or credentials are modified via the management API

## 5. DynamoDB Limits & Constraints

All limits must be enforced by default with DynamoDB-compatible values. All limits must be configurable via the configuration system.

### 5.1 Item & Attribute Limits

| Limit | DynamoDB Default | Configurable | Requirement ID |
|-------|-----------------|--------------|----------------|
| Maximum item size | 400 KB | Yes | REQ-LIM-001 |
| Partition key value max size | 2048 bytes | Yes | REQ-LIM-002 |
| Sort key value max size | 1024 bytes | Yes | REQ-LIM-003 |
| Attribute name max size | 64 KB (65535 bytes) | Yes | REQ-LIM-004 |
| Index key attribute name max size | 255 characters | Yes | REQ-LIM-005 |
| Nesting depth for document types | 32 levels | Yes | REQ-LIM-006 |
| Expression length max | 4 KB | Yes | REQ-LIM-007 |
| Expression attribute names max | 100 per expression | Yes | REQ-LIM-008 |
| Expression attribute values max | 100 per expression | Yes | REQ-LIM-009 |
| Projected attributes per index (INCLUDE) | 100 combined across all indexes | Yes | REQ-LIM-010 |

### 5.2 Table & Index Limits

| Limit | DynamoDB Default | Configurable | Requirement ID |
|-------|-----------------|--------------|----------------|
| Table name length | 3–255 characters | Yes | REQ-LIM-020 |
| Table name characters | `[a-zA-Z0-9_.-]` | No | REQ-LIM-021 |
| Tables per account | 2,500 | Yes | REQ-LIM-022 |
| GSIs per table | 20 | Yes | REQ-LIM-023 |
| LSIs per table | 5 | Yes | REQ-LIM-024 |
| Item collection size (tables with LSIs) | 10 GB | Yes | REQ-LIM-025 |

### 5.3 Throughput Limits

| Limit | DynamoDB Default | Configurable | Requirement ID |
|-------|-----------------|--------------|----------------|
| Per-table RCU (on-demand) | 40,000 | Yes | REQ-LIM-030 |
| Per-table WCU (on-demand) | 40,000 | Yes | REQ-LIM-031 |
| Per-table RCU (provisioned) | 40,000 | Yes | REQ-LIM-032 |
| Per-table WCU (provisioned) | 40,000 | Yes | REQ-LIM-033 |
| Per-account RCU (provisioned) | 80,000 | Yes | REQ-LIM-034 |
| Per-account WCU (provisioned) | 80,000 | Yes | REQ-LIM-035 |
| Per-partition RCU | 3,000 | Yes | REQ-LIM-036 |
| Per-partition WCU | 1,000 | Yes | REQ-LIM-037 |

### 5.4 Operation Limits

| Limit | DynamoDB Default | Configurable | Requirement ID |
|-------|-----------------|--------------|----------------|
| BatchGetItem — max items | 100 | Yes | REQ-LIM-040 |
| BatchGetItem — max response size | 16 MB | Yes | REQ-LIM-041 |
| BatchWriteItem — max items | 25 | Yes | REQ-LIM-042 |
| TransactGetItems — max items | 100 | Yes | REQ-LIM-043 |
| TransactWriteItems — max items | 100 | Yes | REQ-LIM-044 |
| Query/Scan — max response size | 1 MB | Yes | REQ-LIM-045 |
| ListTables — max per page | 100 | Yes | REQ-LIM-046 |
| Max request body size | 16 MB | Yes | REQ-LIM-047 |

## 6. Capacity & Throughput Emulation

- REQ-CAP-001: Support both `PROVISIONED` and `PAY_PER_REQUEST` billing modes
- REQ-CAP-002: Calculate Read Capacity Units: 1 RCU = one strongly consistent read up to 4 KB; 0.5 RCU for eventually consistent; 2 RCU for transactional
- REQ-CAP-003: Calculate Write Capacity Units: 1 WCU = one write up to 1 KB; 2 WCU for transactional
- REQ-CAP-004: Enforce per-partition throughput limits (default: 3,000 RCU/s, 1,000 WCU/s) using token bucket / leaky bucket
- REQ-CAP-005: Enforce per-table provisioned throughput limits for PROVISIONED billing mode
- REQ-CAP-006: Return `ConsumedCapacity` in responses when `ReturnConsumedCapacity` is TOTAL or INDEXES
- REQ-CAP-007: Return per-table and per-index capacity breakdown when `ReturnConsumedCapacity` is INDEXES
- REQ-CAP-008: Return `ProvisionedThroughputExceededException` when throughput limits are exceeded
- REQ-CAP-009: Throughput tracking must be per-instance (stateless — no cross-instance coordination required)

## 7. Catalog & Data Separation Requirements

### 7.1 Catalog Database

The catalog database stores extenddb metadata: table definitions, indexes, tags, and the data database connection info. The data database stores user item data. This follows the PostgreSQL/MySQL pattern of system catalog vs. user databases.

- REQ-CAT-001: extenddb metadata (table definitions, indexes, tags) must reside in a catalog database, separate from user item data
- REQ-CAT-002: User item data must reside in a data database whose connection info is recorded in the catalog
- REQ-CAT-003: The `extenddb.toml` connection string points to the catalog database; the catalog stores the data database location
- REQ-CAT-004: The catalog database name is user-chosen via the connection string — never hardcoded. Multiple independent catalogs can coexist on one PostgreSQL instance

### 7.2 Catalog Versioning

- REQ-CAT-005: The catalog carries a semver version (`major.minor.patch`). Major = breaking schema changes, minor = additive, patch = non-structural
- REQ-CAT-006: The expected catalog version is a build-time constant compiled into the binary — not configurable at runtime
- REQ-CAT-007: On startup, the server validates the catalog version matches the version compiled into the binary. Mismatch → refuse to start with a clear error directing the user to run `extenddb migrate`
- REQ-CAT-008: `extenddb --version` prints both binary version and expected catalog version: `extenddb 0.1.0 (catalog 1.0.0)`. Binary version and catalog version are independent
- REQ-CAT-009: Any PR that modifies catalog schema or migration files must bump the catalog version. This is a hard review gate for developer, reviewer, and principal reviewer

### 7.3 Lifecycle Tools

- REQ-CAT-010: The server does not run migrations on startup. Migrations are an explicit step via `extenddb init` or `extenddb migrate`. Server startup is read-only against the catalog schema
- REQ-CAT-011: `extenddb init` — subcommand that initializes a new deployment: creates (or connects to) the catalog database, runs initial migrations, creates (or connects to) the data database, records the data database connection in the catalog, generates `extenddb.toml` if needed. Documented in `docs/`
- REQ-CAT-012: `extenddb destroy` — subcommand that tears down a deployment: reads config, connects to catalog, enumerates all user tables, prints what will be destroyed, requires `--yes` to confirm, drops data database, drops catalog database. Documented in `docs/`
- REQ-CAT-013: `extenddb verify` — subcommand that validates a deployment: connects to catalog, checks version, enumerates all tables and indexes in catalog, connects to data database, verifies corresponding storage structures exist. Reports healthy/missing/inconsistent. Documented in `docs/`
- REQ-CAT-014: `extenddb migrate` — subcommand that applies schema migrations to an existing catalog: reads current version, determines required migrations, shows plan, requires `--yes` to confirm, runs migrations, updates catalog version. Documented in `docs/`

## 8. Storage Backend Requirements

### 8.1 Pluggable Storage Trait

- REQ-STOR-001: All data access must go through a `StorageEngine` trait (async, `Send + Sync`)
- REQ-STOR-002: The trait must cover: table CRUD, item CRUD, query/scan, batch operations, transaction operations, metadata, stream events, import/export job tracking
- REQ-STOR-003: The trait must support transactions with serializable isolation for TransactWriteItems/TransactGetItems
- REQ-STOR-004: The trait must not leak backend-specific types — all inputs and outputs use core DynamoDB types
- REQ-STOR-005: Adding a new backend must not require changes to any existing crate

### 8.2 PostgreSQL Backend (First Implementation)

- REQ-PG-001: Use async PostgreSQL driver (sqlx with tokio runtime)
- REQ-PG-002: Connection pooling with configurable pool size
- REQ-PG-003: Each DynamoDB table maps to a PostgreSQL table with typed key columns and `item_data JSONB`
- REQ-PG-004: GSIs map to separate PostgreSQL tables with automatic backfill on write
- REQ-PG-005: Use parameterized queries for all read operations to prevent SQL injection
- REQ-PG-006: Schema migrations managed via embedded migration files
- REQ-PG-007: Support PostgreSQL 14+
- REQ-PG-008: Support optional read replica connection for eventually consistent reads. When `read_replica_url` is configured, `ConsistentRead=false` reads (GetItem, Query, Scan, BatchGetItem) route to the replica pool. All writes and `ConsistentRead=true` reads always use the primary pool.

## 9. Expression Engine Requirements

- REQ-EXPR-001: Parse and evaluate `ConditionExpression` (comparisons, functions, logical operators)
- REQ-EXPR-002: Parse and evaluate `FilterExpression` (same grammar as ConditionExpression, applied post-query)
- REQ-EXPR-003: Parse and evaluate `UpdateExpression` with all four actions: SET, REMOVE, ADD, DELETE
- REQ-EXPR-004: Parse and evaluate `ProjectionExpression` (attribute selection, nested paths)
- REQ-EXPR-005: Parse and evaluate `KeyConditionExpression` (partition key equality + sort key conditions)
- REQ-EXPR-006: Resolve `ExpressionAttributeNames` (`#name` → actual attribute name)
- REQ-EXPR-007: Resolve `ExpressionAttributeValues` (`:value` → AttributeValue)
- REQ-EXPR-008: Support nested document paths (`address.city`, `tags[0].name`)
- REQ-EXPR-009: Support all DynamoDB functions: `attribute_exists`, `attribute_not_exists`, `attribute_type`, `begins_with`, `contains`, `size`
- REQ-EXPR-010: Support `BETWEEN` and `IN` operators
- REQ-EXPR-011: SET action must support `if_not_exists()` and `list_append()` functions
- REQ-EXPR-012: Expression parsing must produce clear error messages matching DynamoDB's validation errors

## 10. Non-Functional Requirements

### 10.1 Performance

- REQ-PERF-001: Single-item read latency < 5ms p99 (excluding network, with warm cache)
- REQ-PERF-002: Single-item write latency < 10ms p99 (excluding network)
- REQ-PERF-003: Support at least 10,000 concurrent connections
- REQ-PERF-004: Request parsing overhead < 100μs (JSON deserialization + validation)
- REQ-PERF-005: CRC32 computation < 10μs for typical response sizes (< 10 KB)

### 10.2 Observability

- REQ-OBS-001: Structured logging via `tracing` crate with configurable log levels
- REQ-OBS-002: Per-operation latency metrics (p50, p95, p99)
- REQ-OBS-003: Per-operation request count and error count metrics
- REQ-OBS-004: Per-table metrics (request count, consumed capacity)
- REQ-OBS-005: JSON metrics endpoint (`/metrics`) using DynamoDB CloudWatch-style metric names and dimensions
- REQ-OBS-006: Health check endpoint (`/health`) returning 200 when the server is ready
- REQ-OBS-007: Request ID propagation through all log entries

### 10.3 Security

- REQ-SEC-001: TLS support via rustls (no OpenSSL dependency)
- REQ-SEC-002: Secret keys encrypted at rest with AES-256-GCM
- REQ-SEC-003: Constant-time signature comparison to prevent timing attacks
- REQ-SEC-004: Request size limits enforced before body parsing
- REQ-SEC-005: No secret material in log output
- REQ-SEC-006: Configurable rate limiting (global and per-table)

### 10.4 Deployment

- REQ-DEPLOY-001: Single static binary (no runtime dependencies beyond libc)
- REQ-DEPLOY-002: Configuration via TOML file + environment variable overrides + CLI flags
- REQ-DEPLOY-003: Graceful shutdown on SIGTERM (drain in-flight requests, close connections)
- REQ-DEPLOY-004: Kubernetes-ready: health/readiness probes, configurable bind address, env var config
- REQ-DEPLOY-005: VM-ready: systemd-compatible, config file based, log to stdout/file
- REQ-DEPLOY-006: Container image < 50 MB (static musl build)

### 10.5 Rust-Specific Requirements

- REQ-RUST-001: Minimum Supported Rust Version (MSRV): stable channel, latest - 2 releases
- REQ-RUST-002: No `unsafe` code in application crates (dependencies may use unsafe)
- REQ-RUST-003: All public APIs documented with rustdoc
- REQ-RUST-004: Clippy clean with default lints
- REQ-RUST-005: Cargo workspace with independent crate versioning

## 11. Data Type Requirements

The system must support all 10 DynamoDB attribute value types:

| Type Descriptor | Type | Rust Representation |
|----------------|------|---------------------|
| `S` | String | `String` |
| `N` | Number | `String` (arbitrary precision, up to 38 digits) |
| `B` | Binary | `Vec<u8>` (base64 encoded on wire) |
| `SS` | String Set | `BTreeSet<String>` (unique, unordered) |
| `NS` | Number Set | `BTreeSet<String>` (unique, unordered) |
| `BS` | Binary Set | `BTreeSet<Vec<u8>>` (unique, unordered) |
| `BOOL` | Boolean | `bool` |
| `NULL` | Null | `bool` (always `true`) |
| `L` | List | `Vec<AttributeValue>` (ordered, heterogeneous) |
| `M` | Map | `BTreeMap<String, AttributeValue>` (unordered, heterogeneous) |

**Requirements:**
- REQ-TYPE-001: Each AttributeValue must have exactly one type descriptor set; reject with `SerializationException` otherwise
- REQ-TYPE-002: Empty strings and binary values are allowed for non-key attributes
- REQ-TYPE-003: Empty sets (SS, NS, BS) are not allowed; reject with `ValidationException`
- REQ-TYPE-004: Number type must preserve up to 38 digits of precision
- REQ-TYPE-005: Number range: 1E-130 to 9.9999999999999999999999999999999999999E+125 (positive and negative)
- REQ-TYPE-006: Binary values must be base64-encoded on the wire
- REQ-TYPE-007: Primary key attributes must be S, N, or B type only
- REQ-TYPE-008: Document types (L, M) support nesting up to 32 levels deep

## 12. SDK Client Configuration Requirements

An unmodified application that works against AWS DynamoDB must work against extenddb by changing only endpoint configuration — no code changes, no SDK patches, no custom plugins.

### 12.1 Endpoint Override Mechanism

AWS SDKs (Python/boto3, Java v2, Rust, Go v2, .NET, Node.js) all support the same endpoint override hierarchy, standardized in late 2023:

1. **Service-specific environment variable** (highest priority)
2. **Global endpoint environment variable** (`AWS_ENDPOINT_URL`)
3. **Service-specific setting in `~/.aws/config`** (via `services` section)
4. **Global setting in `~/.aws/config`**

The service-specific environment variables for DynamoDB and DynamoDB Streams are:

| Service | SDK Client Name | Environment Variable |
|---------|----------------|---------------------|
| DynamoDB | `dynamodb` | `AWS_ENDPOINT_URL_DYNAMODB` |
| DynamoDB Streams | `dynamodbstreams` | `AWS_ENDPOINT_URL_DYNAMODB_STREAMS` |

Both must point to the same extenddb address (extenddb serves both services on a single port).

**Requirements:**
- REQ-SDK-001: extenddb serves both DynamoDB and DynamoDB Streams on a single `host:port`. No separate Streams port or path prefix is needed — the `X-Amz-Target` prefix distinguishes the two services.
- REQ-SDK-002: `DescribeEndpoints` must return the server's own `host:port` as the `Address` with a `CachePeriodInMinutes` of 10. This is a no-op for SDK clients that set `endpoint_url` (endpoint discovery is disabled when a custom endpoint is set), but must return a valid response if called directly.
- REQ-SDK-003: SigV4 signing name is `dynamodb` for both DynamoDB and DynamoDB Streams requests. The auth layer does not need to distinguish between the two services.

### 12.2 AWS Configuration Files

extenddb requires credentials for SigV4 authentication. The standard `~/.aws/credentials` and `~/.aws/config` files are used by the SDK client, not by extenddb itself. extenddb has its own credential store (see §4). The credentials configured in `~/.aws/credentials` must match a credential pair registered in extenddb's credential store.

**Minimal setup — environment variables only (simplest):**

```bash
# Point both services at extenddb
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:8000
export AWS_ENDPOINT_URL_DYNAMODB_STREAMS=https://127.0.0.1:8000

# Credentials must match a pair registered in extenddb's credential store
export AWS_ACCESS_KEY_ID=local-dev-key
export AWS_SECRET_ACCESS_KEY=local-dev-secret
export AWS_DEFAULT_REGION=us-east-1
```

With this setup, any application using `boto3.client('dynamodb')` or `boto3.client('dynamodbstreams')` — or the equivalent in any other SDK — automatically talks to extenddb with zero code changes.

**Alternative — `~/.aws/config` with services section:**

```ini
[profile extenddb]
region = us-east-1
services = extenddb-services

[services extenddb-services]
dynamodb =
  endpoint_url = https://127.0.0.1:8000
dynamodb_streams =
  endpoint_url = https://127.0.0.1:8000
```

```ini
# ~/.aws/credentials
[extenddb]
aws_access_key_id = local-dev-key
aws_secret_access_key = local-dev-secret
```

Usage: `export AWS_PROFILE=extenddb` — then all SDK clients in the process use extenddb.

**Alternative — global endpoint override (all AWS services go to one place):**

```bash
export AWS_ENDPOINT_URL=https://127.0.0.1:8000
```

This sends every AWS SDK call to extenddb. Only useful when the application exclusively uses DynamoDB.

### 12.3 Endpoint Discovery Behavior

DynamoDB SDKs support optional endpoint discovery via the `DescribeEndpoints` operation. The behavior varies by SDK:

- **When `endpoint_url` is set:** Endpoint discovery is completely disabled. The SDK sends all requests to the configured URL. This is the normal extenddb usage path.
- **When `endpoint_url` is NOT set:** The SDK may call `DescribeEndpoints` to discover the optimal endpoint. This only happens against real AWS.

**Requirements:**
- REQ-SDK-004: extenddb must handle `DescribeEndpoints` requests and return a valid response pointing to itself. This ensures correctness if an application explicitly calls `DescribeEndpoints`, even though SDKs skip it when `endpoint_url` is set.
- REQ-SDK-005: Endpoint discovery must not redirect clients away from extenddb. The returned `Address` must always be the server's own listen address.

## 13. Testing Requirements

- REQ-TEST-001: A minimal test suite must exist for every phase, covering all implemented operations. No phase exits without a passing test suite
- REQ-TEST-002: Tests are written in Python (boto3) and AWS CLI, covering all Phase 1 operations: CreateTable, DescribeTable, ListTables, DeleteTable, and key error paths
- REQ-TEST-003: Tests must be dual-target — run against both real DynamoDB and extenddb with identical assertions, controlled by an endpoint parameter. No target-specific branching
- REQ-TEST-004: If behavior differs between real DynamoDB and extenddb, that is a bug in extenddb
- REQ-TEST-005: Tests must clean up after themselves — all created resources are deleted on exit, even on failure

## 14. Documentation Requirements

### 14.1 Getting Started

- REQ-DOC-001: A getting-started document that a human can follow end-to-end: init a deployment, start the server, hit it with AWS CLI commands
- REQ-DOC-002: The getting-started doc must document both `--endpoint-url` and the `AWS_ENDPOINT_URL_DYNAMODB` environment variable / `~/.aws/config` profile approach
- REQ-DOC-003: If a human has to ask how to do something, the docs are broken

## 15. Logging & Startup Requirements

### 15.1 Startup Banner

- REQ-LOG-001: On launch, extenddb logs a summary of its effective configuration: binary version, catalog version, bind address, region, account ID, auth provider, catalog database, data database, log level
- REQ-LOG-002: Connection strings must redact passwords in log output

### 15.2 Syslog Integration

- REQ-LOG-003: extenddb logs to syslog with ident `extenddb` and facility `daemon`. Read logs with `journalctl -t extenddb`
- REQ-LOG-004: Default log level is `debug`
- REQ-LOG-005: Supported levels: `debug`, `info`, `error`, `critical`
- REQ-LOG-006: Log format follows syslog convention: ISO 8601 timestamp, level, message

### 15.3 Log Hygiene

- REQ-LOG-007: Reviewers verify that log statements use appropriate levels. Every `error` and `critical` log message must have a corresponding entry in `docs/troubleshooting.md`
- REQ-LOG-008: Any PR that adds or changes error/critical messages must update the troubleshooting doc

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
