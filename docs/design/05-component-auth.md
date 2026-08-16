# extenddb — Component Design: Authentication & Authorization

**Version:** 2.0
**Date:** 2026-04-06
**Status:** Draft
**Crate:** `extenddb-auth`

## 1. Purpose

The `auth` crate provides pluggable authentication and authorization. It defines the `AuthProvider` trait, implements the built-in SigV4 provider, and contains the IAM policy evaluation engine with support for identity-based policies, role-based access control, and tag-based access control (ABAC). The crate depends on `extenddb-core` (for types and errors) and `async_trait` (for object-safe async trait dispatch). It has no HTTP framework or storage dependencies.

> **Note on `CredentialStore`:** The `auth` crate defines its own `CredentialStore` trait for credential, identity, and policy lookup. The `bin` crate provides a thin adapter (`StorageCredentialAdapter`) that implements `CredentialStore` by delegating to the storage engine's `CredentialEngine` trait. This keeps `auth` independent of the `storage` crate while allowing identity data to live in the same database.

## 2. Module Structure

```
crates/auth/src/
├── lib.rs
├── provider.rs           # AuthProvider trait definition
├── identity.rs           # AuthIdentity, ResolvedPrincipal, Session types
├── sigv4/
│   ├── mod.rs
│   ├── canonical.rs      # Canonical request construction
│   ├── signing_key.rs    # HMAC-SHA256 signing key derivation
│   ├── verify.rs         # Signature verification (constant-time)
│   └── parse.rs          # Authorization header parsing
├── policy/
│   ├── mod.rs
│   ├── document.rs       # PolicyDocument, Statement types
│   ├── evaluator.rs      # Policy evaluation engine (identity-based + boundary)
│   ├── matcher.rs        # Action/resource/ARN wildcard matching
│   ├── condition.rs      # Condition operator evaluation (String*, Numeric*, Date*, Arn*, Bool, Null, ForAllValues, ForAnyValue)
│   └── context.rs        # RequestContext builder (aws:PrincipalTag/*, dynamodb:LeadingKeys, etc.)
├── credential/
│   ├── mod.rs
│   ├── store.rs          # CredentialStore trait + cached implementation
│   └── encryption.rs     # AES-256-GCM encrypt/decrypt for secret keys
└── builtin.rs            # BuiltinAuthProvider (SigV4 + local identities + policies)
```

## 3. Identity Model

extenddb implements a local IAM identity model that mirrors AWS IAM structure for DynamoDB access control.

### 3.1 Principal Types

```rust
/// A resolved principal — any entity that can make requests.
/// Named `ResolvedPrincipal` to distinguish from `PrincipalMatch` in policy
/// statements, which represents ARN strings in Principal/NotPrincipal fields.
#[derive(Debug, Clone)]
pub enum ResolvedPrincipal {
    User(UserIdentity),
    AssumedRole(RoleSession),
}

#[derive(Debug, Clone)]
pub struct UserIdentity {
    pub user_name: String,
    pub user_arn: String,           // arn:aws:iam::{account}:user/{name}
    pub account_id: String,
    pub tags: HashMap<String, String>,  // principal tags
    pub groups: Vec<String>,        // group names this user belongs to
}

#[derive(Debug, Clone)]
pub struct RoleSession {
    pub role_name: String,
    pub role_arn: String,           // arn:aws:iam::{account}:role/{name}
    pub session_name: String,
    pub account_id: String,
    /// Effective tags: role tags merged with session tags (session tags win).
    pub tags: HashMap<String, String>,
    pub session_policy: Option<PolicyDocument>,
    pub expires_at: time::OffsetDateTime,  // `time` crate — already in dependency tree via `axum`
}
```

### 3.2 AuthIdentity (Authentication Result)

```rust
/// Authenticated identity returned by a provider.
#[derive(Debug, Clone)]
pub struct AuthIdentity {
    /// The resolved principal (user or assumed-role session).
    pub principal: ResolvedPrincipal,
    /// Provider name (e.g., "builtin", "aws_iam", "azure_ad").
    pub provider: &'static str,
}

impl AuthIdentity {
    /// Returns the principal ARN for policy evaluation.
    pub fn principal_arn(&self) -> &str {
        match &self.principal {
            ResolvedPrincipal::User(u) => &u.user_arn,
            ResolvedPrincipal::AssumedRole(r) => &r.role_arn,
        }
    }

    /// Returns principal tags for condition evaluation.
    pub fn principal_tags(&self) -> &HashMap<String, String> {
        match &self.principal {
            ResolvedPrincipal::User(u) => &u.tags,
            ResolvedPrincipal::AssumedRole(r) => &r.tags,
        }
    }

    pub fn account_id(&self) -> &str {
        match &self.principal {
            ResolvedPrincipal::User(u) => &u.account_id,
            ResolvedPrincipal::AssumedRole(r) => &r.account_id,
        }
    }
}
```

## 4. AuthProvider Trait

```rust
use extenddb_core::error::DynamoDbError;

/// Request context passed to the auth provider.
pub struct AuthRequest<'a> {
    pub headers: &'a HashMap<String, String>,
    pub body: &'a [u8],
    pub operation: &'a str,
    pub resource_arn: &'a str,
}

/// Authorization decision.
pub enum AuthzDecision {
    Allow,
    Deny { reason: String },
}

/// Request context for policy condition evaluation.
/// Built by the server middleware before calling authorize().
///
/// Multi-value keys (`leading_keys`, `attributes`) use `Option<Vec<String>>`
/// to distinguish "key not applicable to this operation" (`None`) from
/// "applicable but empty set" (`Some(vec![])`). This matters for the `Null`
/// condition operator: `None` means the key is absent (Null=true passes),
/// while `Some(vec![])` means the key is present (Null=true fails).
pub struct RequestContext {
    /// aws:PrincipalTag/* — from the authenticated identity's tags.
    pub principal_tags: HashMap<String, String>,
    /// dynamodb:ResourceTag/* — from the target table's tags.
    pub resource_tags: HashMap<String, String>,
    /// dynamodb:LeadingKeys — partition key values being accessed.
    /// `None` for table-level operations (CreateTable, DeleteTable, etc.)
    /// where the key is not applicable. `Some(vec![])` for item-level
    /// operations where no keys were extracted (shouldn't happen in practice).
    pub leading_keys: Option<Vec<String>>,
    /// dynamodb:Attributes — attribute names being read or written.
    /// `None` for operations that don't specify attributes.
    pub attributes: Option<Vec<String>>,
    /// dynamodb:Select — the Select parameter value (if applicable).
    pub select: Option<String>,
    /// dynamodb:ReturnValues — the ReturnValues parameter value.
    pub return_values: Option<String>,
    /// dynamodb:ReturnConsumedCapacity — the ReturnConsumedCapacity value.
    pub return_consumed_capacity: Option<String>,
    /// dynamodb:FullTableScan — true if this is a Scan operation.
    pub full_table_scan: Option<bool>,
    /// dynamodb:EnclosingOperation — parent operation for batch/transact sub-ops.
    pub enclosing_operation: Option<String>,
}

/// DynamoDB request parameters extracted by the server before authorization.
/// Populated from the parsed request body; used to build `RequestContext`.
pub struct RequestParams {
    /// Partition key values being accessed (item-level operations).
    /// `None` for table-level operations (CreateTable, DescribeTable, etc.).
    pub leading_keys: Option<Vec<String>>,
    /// Attribute names being read or written (ProjectionExpression, etc.).
    /// `None` when the request doesn't constrain attributes.
    pub attributes: Option<Vec<String>>,
    /// The Select parameter (e.g., "ALL_ATTRIBUTES", "COUNT").
    pub select: Option<String>,
    /// The ReturnValues parameter (e.g., "ALL_OLD", "NONE").
    pub return_values: Option<String>,
    /// The ReturnConsumedCapacity parameter.
    pub return_consumed_capacity: Option<String>,
    /// Parent operation name for sub-operations in batch/transact requests.
    pub enclosing_operation: Option<String>,
}

/// Pluggable authentication and authorization provider.
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    /// Authenticate a request. Returns the identity or an error.
    async fn authenticate(&self, request: &AuthRequest<'_>) -> Result<AuthIdentity, DynamoDbError>;

    /// Authorize an authenticated identity for an action on a resource.
    async fn authorize(
        &self,
        identity: &AuthIdentity,
        action: &str,
        resource_arn: &str,
        context: &RequestContext,
    ) -> Result<AuthzDecision, DynamoDbError>;

    /// Provider name for logging and configuration.
    fn name(&self) -> &'static str;
}
```

## 5. Built-in SigV4 Provider

### 5.1 Authentication Flow

```
1. Parse Authorization header:
   AWS4-HMAC-SHA256 Credential=<access_key>/<date>/<region>/dynamodb/aws4_request,
   SignedHeaders=<headers>, Signature=<hex_signature>

2. Extract access_key_id from Credential field

3. Look up credential: access_key_id → StoredCredential
   - Check credential cache first (moka async cache with TTL)
   - On miss: query storage backend via CredentialStore
   - StoredCredential includes: encrypted secret, principal_type, principal_name

4. Decrypt secret key (AES-256-GCM)

5. Derive signing key:
   date_key    = HMAC-SHA256(secret_key,       date_stamp)
   region_key  = HMAC-SHA256(date_key,          region)
   service_key = HMAC-SHA256(region_key,         "dynamodb")
   signing_key = HMAC-SHA256(service_key,        "aws4_request")

6. Construct canonical request and string to sign (unchanged from v1)

7. Constant-time compare client signature vs expected signature
   - If mismatch: return UnrecognizedClientException

8. Resolve full identity based on principal_type:
   a. If PrincipalType::User:
      - principal_name is the user_name
      - Load UserIdentity via get_user(principal_name): tags, groups
      - Return AuthIdentity with ResolvedPrincipal::User
   b. If PrincipalType::Session:
      - principal_name is the session_token
      - Load RoleSession via get_session(principal_name)
      - Check session expiration: if expired, return ExpiredTokenException
      - Return AuthIdentity with ResolvedPrincipal::AssumedRole

   The join path for session credentials:
   _dynamodb_credentials.principal_name = _dynamodb_sessions.session_token
   This allows the auth flow to go from access_key_id → credential →
   session_token → full RoleSession in two lookups.
```

### 5.2 Authorization Flow

```
1. Collect applicable policies for the principal:
   a. If ResolvedPrincipal::User:
      - User's directly attached policies
      - Policies from all groups the user belongs to
      - User's permissions boundary (if set)
   b. If ResolvedPrincipal::AssumedRole:
      - Role's attached policies
      - Session policy (if passed during AssumeRole)
      - Role's permissions boundary (if set)

2. Build effective policy set:
   - Identity policies = union of all collected policies
   - Boundary policies = permissions boundary (if any)
   - Session policies = session policy from AssumeRole (if any)

3. Evaluate using IAM algorithm:
   a. Check all statements across all policies for explicit Deny
      - If any statement matches (action + resource + conditions) with Effect=Deny → DENY
   b. If permissions boundary exists:
      - Action must be allowed by at least one boundary statement → else DENY
   c. If session policy exists:
      - Action must be allowed by at least one session policy statement → else DENY
   d. Check identity policies for explicit Allow
      - If any statement matches with Effect=Allow → ALLOW
   e. Implicit deny → DENY
```

### 5.3 Credential Store

```rust
/// Abstraction for credential and identity storage.
#[async_trait::async_trait]
pub trait CredentialStore: Send + Sync {
    /// Look up a credential by access key ID.
    async fn get_credential(&self, access_key_id: &str) -> Result<Option<StoredCredential>, DynamoDbError>;

    /// Resolve a user's full identity: tags, group memberships.
    async fn get_user(&self, user_name: &str) -> Result<Option<UserRecord>, DynamoDbError>;

    /// Resolve a role's definition: tags, trust policy, permissions boundary.
    async fn get_role(&self, role_name: &str) -> Result<Option<RoleRecord>, DynamoDbError>;

    /// Get all policies for a principal (user or role) including group policies.
    async fn get_effective_policies(&self, principal: &ResolvedPrincipal) -> Result<Vec<PolicyDocument>, DynamoDbError>;

    /// Get the permissions boundary for a principal (if set).
    async fn get_permissions_boundary(&self, principal_arn: &str) -> Result<Option<PolicyDocument>, DynamoDbError>;

    /// Resolve a session credential to its RoleSession.
    async fn get_session(&self, session_token: &str) -> Result<Option<RoleSession>, DynamoDbError>;
}

pub struct StoredCredential {
    pub access_key_id: String,
    pub secret_key_encrypted: Vec<u8>,
    pub principal_type: PrincipalType,  // User or Session
    pub principal_name: String,
    pub is_active: bool,
}

pub enum PrincipalType { User, Session }

pub struct UserRecord {
    pub user_name: String,
    pub user_arn: String,
    pub account_id: String,
    pub tags: HashMap<String, String>,
    pub groups: Vec<String>,
    pub permissions_boundary_arn: Option<String>,
}

pub struct RoleRecord {
    pub role_name: String,
    pub role_arn: String,
    pub account_id: String,
    pub tags: HashMap<String, String>,
    pub trust_policy: PolicyDocument,
    pub permissions_boundary_arn: Option<String>,
}

/// Cached wrapper around any CredentialStore.
///
/// ### CredentialStore ↔ CredentialEngine Mapping
///
/// The `StorageCredentialAdapter` (in `bin`) bridges these two traits:
///
/// | `CredentialStore` method       | Delegates to `CredentialEngine` method(s)                          |
/// |-------------------------------|--------------------------------------------------------------------|
/// | `get_credential(access_key_id)` | `get_credential(access_key_id)`                                  |
/// | `get_user(user_name)`          | `get_user(user_name)` + `get_user_groups(user_name)` (merged)    |
/// | `get_role(role_name)`          | `get_role(role_name)`                                             |
/// | `get_effective_policies(principal)` | For User: `get_policies_for_principal(user_arn)` + for each group in `UserIdentity.groups`: construct group ARN as `arn:aws:iam::{UserIdentity.account_id}:group/{group_name}`, then `get_policies_for_principal(group_arn)`. For AssumedRole: `get_policies_for_principal(role_arn)` |
/// | `get_permissions_boundary(arn)` | `get_permissions_boundary(arn)`                                  |
/// | `get_session(session_token)`   | `get_session(session_token)`                                      |
///
/// Write-side `CredentialEngine` methods (`create_user`, `create_role`, `create_group`,
/// `add_user_to_group`, `store_credential`, `store_policy`, `create_session`,
/// `delete_user`, `delete_role`, `deactivate_credential`, `cleanup_expired_sessions`)
/// are called by the management API handlers in the `server` crate
/// (see `POST /management/*` routes in 06-component-server.md).
pub struct CachedCredentialStore {
    inner: Arc<dyn CredentialStore>,
    credential_cache: moka::future::Cache<String, StoredCredential>,
    user_cache: moka::future::Cache<String, UserRecord>,
    role_cache: moka::future::Cache<String, RoleRecord>,
    /// Caches individual per-ARN policy lookups, NOT composed effective policies.
    /// `get_effective_policies` composes results at query time by looking up
    /// each principal ARN (user + groups) separately. This ensures
    /// `invalidate_policies(group_arn)` correctly evicts stale group policies
    /// without requiring group→user propagation.
    policy_cache: moka::future::Cache<String, Vec<PolicyDocument>>,
    session_cache: moka::future::Cache<String, RoleSession>,
    boundary_cache: moka::future::Cache<String, Option<PolicyDocument>>,
}

impl CachedCredentialStore {
    /// Invalidate a cached credential. Called by management API after
    /// store_credential or deactivate_credential.
    pub fn invalidate_credential(&self, access_key_id: &str) {
        self.credential_cache.invalidate(access_key_id);
    }

    /// Invalidate a cached user. Called by management API after
    /// create_user, delete_user, set_user_tags, add_user_to_group,
    /// or remove_user_from_group.
    pub fn invalidate_user(&self, user_name: &str) {
        self.user_cache.invalidate(user_name);
    }

    /// Invalidate a cached role. Called by management API after
    /// create_role, delete_role, or set_role_tags.
    pub fn invalidate_role(&self, role_name: &str) {
        self.role_cache.invalidate(role_name);
    }

    /// Invalidate cached policies for a principal. Called by management API
    /// after store_policy, detach_policy, or any group membership change
    /// (which affects effective policies for users in that group).
    pub fn invalidate_policies(&self, principal_arn: &str) {
        self.policy_cache.invalidate(principal_arn);
    }

    /// Invalidate a cached session. Called by management API after
    /// revoke_session.
    pub fn invalidate_session(&self, session_token: &str) {
        self.session_cache.invalidate(session_token);
    }

    /// Invalidate a cached permissions boundary. Called by management API
    /// after set_permissions_boundary.
    pub fn invalidate_boundary(&self, principal_arn: &str) {
        self.boundary_cache.invalidate(principal_arn);
    }
}
```

## 6. IAM Policy Evaluation

### 6.1 Policy Document Structure

```rust
pub struct PolicyDocument {
    pub version: String,  // "2012-10-17"
    pub statements: Vec<Statement>,
}

/// Action matching: a statement uses either Action or NotAction, never both.
/// This prevents invalid states at the type level (tenet 4: Rust-safe by default).
pub enum ActionMatch {
    /// Action — matches listed actions.
    Actions(Vec<String>),       // "dynamodb:PutItem", "dynamodb:*"
    /// NotAction — matches everything except listed actions.
    NotActions(Vec<String>),
}

/// Resource matching: a statement uses either Resource or NotResource, never both.
pub enum ResourceMatch {
    /// Resource — matches listed resources.
    Resources(Vec<String>),     // "arn:aws:dynamodb:*:*:table/Users"
    /// NotResource — matches everything except listed resources.
    NotResources(Vec<String>),
}

/// Principal matching: used in trust policies. Identity-based policies omit this
/// (the principal is implicit — the authenticated caller).
pub enum PrincipalMatch {
    Principals(Vec<String>),
    NotPrincipals(Vec<String>),
}

pub struct Statement {
    pub sid: Option<String>,
    pub effect: Effect,
    pub action_match: ActionMatch,
    pub resource_match: ResourceMatch,
    pub conditions: Vec<Condition>,
    /// Used in trust policies (role assumption). None for identity-based policies.
    pub principal_match: Option<PrincipalMatch>,
}

pub enum Effect { Allow, Deny }

pub struct Condition {
    pub operator: ConditionOperator,
    pub key: String,              // "aws:PrincipalTag/Department", "dynamodb:LeadingKeys"
    pub values: Vec<String>,
}

/// All IAM condition operators relevant to DynamoDB access control.
pub enum ConditionOperator {
    // String
    StringEquals,
    StringNotEquals,
    StringEqualsIgnoreCase,
    StringLike,             // supports * and ? wildcards
    StringNotLike,
    // Numeric
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanEquals,
    // Date
    DateEquals,
    DateNotEquals,
    DateLessThan,
    DateLessThanEquals,
    DateGreaterThan,
    DateGreaterThanEquals,
    // Boolean
    Bool,
    // Null check
    Null,
    // ARN
    ArnEquals,
    ArnNotEquals,
    ArnLike,
    ArnNotLike,
    // Set operators (prefix applied to any of the above)
    ForAllValues(Box<ConditionOperator>),
    ForAnyValue(Box<ConditionOperator>),
    // IfExists suffix (condition passes if key is absent)
    IfExists(Box<ConditionOperator>),
}
```

> **Deferred: Policy Variables.** AWS IAM supports variable substitution in `Resource` ARNs and `Condition` values — e.g., `"Resource": "arn:aws:dynamodb:*:*:table/${aws:PrincipalTag/Team}-*"` grants access to tables matching the caller's own team tag. extenddb v1 does not support policy variables; all Resource and Condition values are treated as literal strings (with wildcard matching where applicable). This means ABAC policies that use variables to generalize across principals must be written as separate per-principal policies. Policy variable support is tracked as REQ-ABAC-006.

**JSON deserialization:**

IAM policy documents are JSON. Both Mode 2 (management API) and Mode 3 (IAM retrieval) receive policies as JSON strings. Deserialization uses `serde_json` with the following rules:

- `Effect`: case-insensitive match to `"Allow"` or `"Deny"`. Any other value is a parse error.
- `Action` / `NotAction`: a single string or array of strings. Mutually exclusive — a statement with both is a parse error. Mapped to `ActionMatch::Actions` or `ActionMatch::NotActions`.
- `Resource` / `NotResource`: same pattern as Action. Mapped to `ResourceMatch`.
- `Principal` / `NotPrincipal`: same pattern. Only present in trust policies; absent in identity-based policies. Mapped to `Option<PrincipalMatch>`.
- `Condition`: a nested object `{ "OperatorName": { "key": "value" | ["values"] } }`. The operator name is parsed by splitting on `:` to detect `ForAllValues:` / `ForAnyValue:` prefixes and `IfExists` suffix, then matching the base operator name (e.g., `StringEquals`, `ArnLike`). Unknown operator names are a parse error.
- `Sid`: optional string, preserved but not used in evaluation.
- `Version`: must be `"2012-10-17"`. Other versions are accepted with a warning log but evaluated identically.

```rust
impl PolicyDocument {
    pub fn from_json(json: &str) -> Result<Self, PolicyParseError> {
        let raw: serde_json::Value = serde_json::from_str(json)?;
        let version = raw["Version"].as_str().unwrap_or("2012-10-17").to_owned();
        let statements = parse_statements(&raw["Statement"])?;
        Ok(Self { version, statements })
    }
}
```

### 6.2 Condition Context Trait

Both `RequestContext` (DynamoDB operations) and `AssumeRoleContext` (trust policy evaluation) need to resolve condition keys for the policy evaluator. This shared trait avoids duplicating the condition evaluation logic.

```rust
/// Trait for resolving condition keys during policy evaluation.
/// Implemented by `RequestContext` (DynamoDB operations) and
/// `AssumeRoleContext` (trust policy / AssumeRole).
pub trait ConditionContext {
    /// Resolve a condition key to its value(s).
    /// Returns `None` when the key is absent or not applicable.
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>>;

    /// Returns `true` if `key` is a multivalued (set-valued) condition key.
    /// Arity is a property of the key, not the number of values present.
    /// `RequestContext` reports `dynamodb:Attributes` and `dynamodb:LeadingKeys`
    /// as multivalued; all other keys (and all trust-policy keys) are single-valued.
    fn is_multivalued_key(&self, key: &str) -> bool;
}
```

**Condition-key arity (single-valued vs multivalued).** AWS classifies each
condition key as single-valued or multivalued (`ArrayOfString`) in the service
authorization reference. For DynamoDB, `dynamodb:Attributes` and
`dynamodb:LeadingKeys` are multivalued; everything else extenddb resolves
(`dynamodb:Select`, `dynamodb:ReturnValues`, tag keys, etc.) is single-valued.
A **bare** operator (one without a `ForAllValues:`/`ForAnyValue:` qualifier)
applied to a multivalued key **never matches** — verified against real AWS IAM
(BR-7085), and independent of how many values are present. Only the set-operator
qualifiers evaluate multivalued keys. This is fail-safe: a bare `Deny` on such a
key is a no-op (the author must use `ForAnyValue:` to build a denylist), and a
bare `Allow` allowlist grants nothing (implicit deny).

### 6.3 Request Context

The request context is built by the server middleware before policy evaluation. It contains all condition keys that policies can reference.

```rust
impl ConditionContext for RequestContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        self.resolve_key_inner(key)
    }
}

impl RequestContext {
    /// Build context for a DynamoDB operation.
    pub fn build(
        identity: &AuthIdentity,
        operation: &str,
        resource_tags: &HashMap<String, String>,
        request_params: &RequestParams,
    ) -> Self {
        RequestContext {
            principal_tags: identity.principal_tags().clone(),
            resource_tags: resource_tags.clone(),
            // Only set leading_keys for item-level operations
            leading_keys: request_params.leading_keys.clone(),
            // Only set attributes when the request specifies them
            attributes: request_params.attributes.clone(),
            select: request_params.select.clone(),
            return_values: request_params.return_values.clone(),
            return_consumed_capacity: request_params.return_consumed_capacity.clone(),
            full_table_scan: if operation == "Scan" { Some(true) } else { None },
            enclosing_operation: request_params.enclosing_operation.clone(),
        }
    }

    /// Resolve a condition key to its value(s) for evaluation.
    /// Returns `None` when the key is absent from the context (not applicable
    /// to this operation or not recognized). Returns `Some(vec![])` when the
    /// key is present but has an empty value set.
    fn resolve_key_inner(&self, key: &str) -> Option<Vec<&str>> {
        if let Some(tag_key) = key.strip_prefix("aws:PrincipalTag/") {
            self.principal_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else if let Some(tag_key) = key.strip_prefix("dynamodb:ResourceTag/") {
            self.resource_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else {
            match key {
                "dynamodb:LeadingKeys" => self.leading_keys.as_ref()
                    .map(|v| v.iter().map(|s| s.as_str()).collect()),
                "dynamodb:Attributes" => self.attributes.as_ref()
                    .map(|v| v.iter().map(|s| s.as_str()).collect()),
                "dynamodb:Select" => self.select.as_deref().map(|v| vec![v]),
                "dynamodb:ReturnValues" => self.return_values.as_deref().map(|v| vec![v]),
                "dynamodb:ReturnConsumedCapacity" => self.return_consumed_capacity.as_deref().map(|v| vec![v]),
                "dynamodb:FullTableScan" => self.full_table_scan.map(|v| vec![if v { "true" } else { "false" }]),
                "dynamodb:EnclosingOperation" => self.enclosing_operation.as_deref().map(|v| vec![v]),
                _ => None,
            }
        }
    }
}
```

### 6.4 Evaluation Algorithm

```
1. Collect all applicable policies:
   - For User: user policies + group policies
   - For AssumedRole: role policies + session policy (if any)

2. Collect boundary policies:
   - Permissions boundary (if set on user or role)

3. Phase 1 — Explicit Deny:
   For each statement in ALL policies (identity + boundary + session):
     If effect == Deny AND action_matches AND resource_matches AND conditions_match:
       → Return DENY (explicit deny is final, no override)

4. Phase 2 — Permissions Boundary check (if boundary exists):
   Must find at least one Allow in boundary policies where
   action_matches AND resource_matches AND conditions_match.
   If no Allow found in boundary → Return DENY (implicit deny from boundary)

5. Phase 3 — Session Policy check (if session policy exists):
   Must find at least one Allow in session policy where
   action_matches AND resource_matches AND conditions_match.
   If no Allow found in session policy → Return DENY

6. Phase 4 — Identity Policy Allow:
   For each statement in identity policies:
     If effect == Allow AND action_matches AND resource_matches AND conditions_match:
       → Return ALLOW

7. Implicit Deny → Return DENY
```

**Action and resource matching with negation variants:**

```rust
/// Returns true if the request action matches this statement's action constraint.
fn action_matches(statement: &Statement, request_action: &str) -> bool {
    match &statement.action_match {
        ActionMatch::Actions(patterns) =>
            patterns.iter().any(|p| wildcard_match(p, request_action)),
        ActionMatch::NotActions(patterns) =>
            !patterns.iter().any(|p| wildcard_match(p, request_action)),
    }
}

/// Returns true if the request resource matches this statement's resource constraint.
fn resource_matches(statement: &Statement, request_resource: &str) -> bool {
    match &statement.resource_match {
        ResourceMatch::Resources(patterns) =>
            patterns.iter().any(|p| arn_match(p, request_resource)),
        ResourceMatch::NotResources(patterns) =>
            !patterns.iter().any(|p| arn_match(p, request_resource)),
    }
}
```

`NotAction` inverts the action match: the statement applies to every action *except* those listed. A `Deny` with `NotAction: ["dynamodb:GetItem"]` denies all DynamoDB actions except `GetItem`. An `Allow` with `NotAction: ["dynamodb:DeleteTable"]` allows everything except `DeleteTable`.

`NotResource` inverts the resource match: the statement applies to every resource *except* those listed. Combined with `Deny`, this is commonly used to deny access to all tables except a specific one.

**Top-level entry point:**

```rust
/// Top-level policy evaluation. Implements the 4-phase IAM evaluation algorithm
/// described above: explicit deny → permissions boundary → session policy → identity allow.
/// Returns Allow only if no explicit Deny is found, boundary and session policies
/// (if present) allow the action, and at least one identity policy explicitly allows it.
pub fn evaluate_policies(
    identity_policies: &[PolicyDocument],
    permissions_boundary: Option<&PolicyDocument>,
    session_policy: Option<&PolicyDocument>,
    action: &str,
    resource_arn: &str,
    context: &impl ConditionContext,
) -> Result<AuthzDecision, DynamoDbError>
```

### 6.5 Condition Evaluation

**Nesting rules for set operators and IfExists:**
- Valid nestings: `ForAllValues(IfExists(base))`, `ForAnyValue(IfExists(base))`, `ForAllValues(base)`, `ForAnyValue(base)`, `IfExists(base)`, or a bare `base` operator.
- `IfExists` is never nested inside another `IfExists`. `ForAllValues`/`ForAnyValue` are never nested inside each other.
- When a set operator wraps `IfExists(base)`, the IfExists semantics apply at the set level (key-absent → pass), and the base operator is used for per-value comparison.

```rust
/// Evaluate a single condition against any context that implements ConditionContext.
/// This is used for both DynamoDB request authorization (RequestContext) and
/// trust policy evaluation during AssumeRole (AssumeRoleContext).
pub fn evaluate_condition(condition: &Condition, context: &impl ConditionContext) -> bool {
    let context_values = context.resolve_key(&condition.key);
    let is_multivalued = context.is_multivalued_key(&condition.key);

    match &condition.operator {
        ConditionOperator::Null => {
            // Null: true means key must be absent, false means key must be present
            let key_absent = context_values.is_none();
            condition.values.first().map_or(false, |v| {
                (v == "true" && key_absent) || (v == "false" && !key_absent)
            })
        }
        ConditionOperator::ForAllValues(inner) => {
            // Unwrap IfExists if nested: ForAllValues(IfExists(base))
            let (absent_passes, base_op) = unwrap_if_exists(inner);
            match context_values {
                None => true,  // ForAllValues with absent key is vacuously true
                               // (same result whether or not IfExists is present)
                Some(vals) => vals.iter().all(|cv| {
                    condition.values.iter().any(|pv| compare_single(base_op, cv, pv))
                })
            }
        }
        ConditionOperator::ForAnyValue(inner) => {
            // Unwrap IfExists if nested: ForAnyValue(IfExists(base))
            let (absent_passes, base_op) = unwrap_if_exists(inner);
            match context_values {
                None => absent_passes,  // false normally, true if IfExists
                Some(vals) => vals.iter().any(|cv| {
                    condition.values.iter().any(|pv| compare_single(base_op, cv, pv))
                })
            }
        }
        ConditionOperator::IfExists(inner) => {
            match context_values {
                None => true,  // key absent → condition passes
                // bare operator on a multivalued key never matches (BR-7085)
                Some(_) if is_multivalued => false,
                Some(vals) => evaluate_single_value_condition(inner, &vals, &condition.values)
            }
        }
        other => {
            match context_values {
                None => false,  // key absent → condition fails (unless IfExists)
                // bare operator on a multivalued key never matches (BR-7085)
                Some(_) if is_multivalued => false,
                Some(vals) => evaluate_single_value_condition(other, &vals, &condition.values)
            }
        }
    }
}

/// Helper: unwrap an IfExists wrapper if present, returning whether
/// absent-key should pass and the base operator for per-value comparison.
fn unwrap_if_exists(op: &ConditionOperator) -> (bool, &ConditionOperator) {
    match op {
        ConditionOperator::IfExists(base) => (true, base),
        other => (false, other),
    }
}

/// Evaluate a non-set, non-IfExists condition operator against a single-valued key.
/// Bare operators on multivalued keys are rejected by the caller before reaching
/// here (see condition-key arity above), so `context_values` holds one element in
/// practice.
///
/// The context value must match at least one policy value (implicit OR
/// across policy values within a single condition key).
fn evaluate_single_value_condition(
    op: &ConditionOperator,
    context_values: &[&str],
    policy_values: &[String],
) -> bool {
    context_values.iter().all(|cv| {
        policy_values.iter().any(|pv| compare_single(op, cv, pv))
    })
}

/// Compare a single context value against a single policy value using
/// the given base operator. Returns true if the comparison holds.
fn compare_single(op: &ConditionOperator, context_value: &str, policy_value: &str) -> bool {
    match op {
        ConditionOperator::StringEquals => context_value == policy_value,
        ConditionOperator::StringNotEquals => context_value != policy_value,
        ConditionOperator::StringEqualsIgnoreCase =>
            context_value.eq_ignore_ascii_case(policy_value),
        ConditionOperator::StringLike => wildcard_match(policy_value, context_value),
        ConditionOperator::StringNotLike => !wildcard_match(policy_value, context_value),
        ConditionOperator::NumericEquals =>
            parse_f64(context_value) == parse_f64(policy_value),
        ConditionOperator::NumericNotEquals =>
            parse_f64(context_value) != parse_f64(policy_value),
        ConditionOperator::NumericLessThan =>
            parse_f64_cmp(context_value, policy_value, |a, b| a < b),
        ConditionOperator::NumericLessThanEquals =>
            parse_f64_cmp(context_value, policy_value, |a, b| a <= b),
        ConditionOperator::NumericGreaterThan =>
            parse_f64_cmp(context_value, policy_value, |a, b| a > b),
        ConditionOperator::NumericGreaterThanEquals =>
            parse_f64_cmp(context_value, policy_value, |a, b| a >= b),
        ConditionOperator::DateEquals | ConditionOperator::DateNotEquals
        | ConditionOperator::DateLessThan | ConditionOperator::DateLessThanEquals
        | ConditionOperator::DateGreaterThan | ConditionOperator::DateGreaterThanEquals =>
            compare_dates(op, context_value, policy_value),
        ConditionOperator::Bool =>
            context_value.eq_ignore_ascii_case(policy_value),
        ConditionOperator::ArnEquals => context_value == policy_value,
        ConditionOperator::ArnNotEquals => context_value != policy_value,
        ConditionOperator::ArnLike => arn_match(policy_value, context_value),
        ConditionOperator::ArnNotLike => !arn_match(policy_value, context_value),
        // Null, ForAllValues, ForAnyValue, IfExists are handled by the caller
        _ => false,
    }
}
```

### 6.6 Wildcard Matching

```rust
/// Match a pattern against a value. Supports `*` and `?` wildcards.
/// Used for Action, Resource, and StringLike/ArnLike condition matching.
pub fn wildcard_match(pattern: &str, value: &str) -> bool;

/// Match an ARN pattern against an ARN value.
/// ARN matching is segment-aware: each colon-separated segment is matched independently.
/// `*` in a segment matches any value for that segment.
/// `arn:aws:dynamodb:*:*:table/User*` matches `arn:aws:dynamodb:us-east-1:123:table/Users`
pub fn arn_match(pattern: &str, value: &str) -> bool;
```

## 7. Role Assumption (AssumeRole)

extenddb provides a local AssumeRole mechanism for testing role-based access patterns. This is not exposed as an STS API — it's a extenddb management API.

### 7.1 AssumeRole Flow

```
1. Client calls extenddb management API: POST /management/assume-role
   {
     "CallerArn": "arn:aws:iam::<account-id>:user/developer",
     "RoleName": "data-reader",
     "SessionName": "test-session",
     "SessionTags": [{"Key": "Project", "Value": "Alpha"}],
     "SessionPolicy": { ... },  // optional inline session policy
     "DurationSeconds": 3600,
     "ExternalId": "optional-external-id"
   }

2. Load role via get_role(role_name). Extract trust policy.

3. Validate CallerArn format: must match arn:aws:iam::{account_id}:user/{user_name}.
   Return 400 ValidationError if malformed or if the principal type is not "user"
   (role-chaining is deferred to a future version — see REQ-IDENT-009).

4. Load caller's tags via get_user(caller_user_name) (parsed from CallerArn).

5. Build AssumeRoleContext from caller tags + SessionTags + ExternalId.

6. Evaluate trust policy: caller ARN must match a Principal in an Allow
   statement, and all Condition blocks must pass against AssumeRoleContext.
   If denied, return 403 AccessDenied.

7. Generate temporary credentials:
   - New access_key_id (prefixed "ASIA" to match AWS convention)
   - New secret_access_key
   - Session token
   - Expiration timestamp

8. Merge tags: role tags + session tags (session tags override on conflict)

9. Store session in _dynamodb_sessions table

10. Return credentials to caller
```

> **Deferred role features (v1):**
> - **Federated assumption** (`AssumeRoleWithSAML`, `AssumeRoleWithWebIdentity`) is not supported. Applications using web identity federation should use basic AssumeRole with equivalent policies. See REQ-IDENT-008.
> - **Role chaining** (role assumes role), **SourceIdentity**, and **TransitiveTagKeys** are not supported. `CallerArn` must reference a user. See REQ-IDENT-009.
> - **MaxSessionDuration** is not enforced. `DurationSeconds` is accepted without per-role validation. See REQ-IDENT-010.

### 7.2 Trust Policy

A role's trust policy controls who can assume it:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "arn:aws:iam::<account-id>:user/developer"},
    "Action": "sts:AssumeRole"
  }]
}
```

The trust policy is evaluated when AssumeRole is called. Only the `Principal` and `Condition` fields are checked — `Action` must be `sts:AssumeRole` and `Resource` is implicit (the role itself).

> **Note:** Trust policies use `"Action": "sts:AssumeRole"` for compatibility with real IAM policy documents, even though extenddb exposes AssumeRole via `/management/assume-role` rather than an STS endpoint. The policy engine matches this action string during trust policy evaluation regardless of the actual HTTP endpoint used.

Trust policy conditions are evaluated against an `AssumeRoleContext` (not the DynamoDB `RequestContext`), which contains:
- `aws:PrincipalTag/*` — tags of the calling user (the entity assuming the role)
- `aws:RequestTag/*` — session tags passed in the AssumeRole request
- `sts:ExternalId` — the ExternalId parameter (if provided in the AssumeRole request)

```rust
/// Context for trust policy condition evaluation.
/// Separate from RequestContext because trust policies reference
/// STS-specific keys, not DynamoDB-specific keys.
pub struct AssumeRoleContext {
    pub principal_tags: HashMap<String, String>,
    pub request_tags: HashMap<String, String>,
    pub external_id: Option<String>,
}

impl ConditionContext for AssumeRoleContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        if let Some(tag_key) = key.strip_prefix("aws:PrincipalTag/") {
            self.principal_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else if let Some(tag_key) = key.strip_prefix("aws:RequestTag/") {
            self.request_tags.get(tag_key).map(|v| vec![v.as_str()])
        } else if key == "sts:ExternalId" {
            self.external_id.as_deref().map(|v| vec![v])
        } else {
            None
        }
    }
}
```

## 8. Auth Providers

### 8.1 No-Auth Provider (Mode 1) — REMOVED

> **Historical note (P63b):** The `NoopAuthProvider` and `AuthIdentity::Anonymous`
> variant were removed in v0.0.67. Authentication is now mandatory — the server
> refuses to start with `auth.provider = "none"`. The code below is preserved
> for historical reference only.

```rust
/// [REMOVED] Accepted all requests without validating credentials.
/// For environments where authentication is not under test.
///
/// Returns a synthetic identity with account_id "000000000000".
/// This value is never used in policy evaluation because authorize()
/// returns Allow unconditionally — no policy engine runs in Mode 1.
/// If code constructs resource ARNs using this account_id, they will
/// contain "000000000000" which won't match real account IDs in policies,
/// but that's irrelevant since policies are never evaluated.
pub struct NoopAuthProvider;

impl AuthProvider for NoopAuthProvider {
    async fn authenticate(&self, _request: &AuthRequest<'_>) -> Result<AuthIdentity, DynamoDbError> {
        Ok(AuthIdentity {
            principal: ResolvedPrincipal::User(UserIdentity {
                user_name: "anonymous".into(),
                user_arn: "arn:aws:iam::000000000000:user/anonymous".into(),
                account_id: "000000000000".into(),
                tags: HashMap::new(),
                groups: vec![],
            }),
            provider: "none",
        })
    }

    async fn authorize(&self, _identity: &AuthIdentity, _action: &str, _resource_arn: &str, _context: &RequestContext) -> Result<AuthzDecision, DynamoDbError> {
        Ok(AuthzDecision::Allow)
    }

    fn name(&self) -> &'static str { "none" }
}
```

### 8.2 Built-in SigV4 Provider (Mode 2)

The default provider for local testing with managed identities and policies. Composes the SigV4 verification flow (§5.1), credential store (§5.3), and policy evaluation engine (§6.4) into a single `AuthProvider` implementation.

```rust
/// SigV4 authentication with local credential store and IAM policy evaluation.
/// This is the default auth mode — identities and policies are managed via the
/// extenddb management API and stored in the pluggable storage backend.
pub struct BuiltinAuthProvider {
    credential_store: CachedCredentialStore,
    encryption_key: [u8; 32],
}

impl AuthProvider for BuiltinAuthProvider {
    async fn authenticate(
        &self,
        request: &AuthRequest<'_>,
    ) -> Result<AuthIdentity, DynamoDbError> {
        // 1. Parse Authorization header → access_key_id, signed_headers, signature
        // 2. credential_store.get_credential(access_key_id)
        // 3. Decrypt secret key with self.encryption_key
        // 4. Derive signing key and verify signature (§5.1 steps 5–7)
        // 5. Resolve identity:
        //    - PrincipalType::User → credential_store.get_user(principal_name)
        //    - PrincipalType::Session → credential_store.get_session(principal_name),
        //      check expiration
        // 6. Return AuthIdentity with ResolvedPrincipal
    }

    async fn authorize(
        &self,
        identity: &AuthIdentity,
        action: &str,
        resource_arn: &str,
        context: &RequestContext,
    ) -> Result<AuthzDecision, DynamoDbError> {
        let policies = self.credential_store
            .get_effective_policies(&identity.principal).await?;
        let boundary = self.credential_store
            .get_permissions_boundary(&identity.principal_arn()).await?;
        let session_policy = match &identity.principal {
            ResolvedPrincipal::AssumedRole(session) => session.session_policy.as_ref(),
            _ => None,
        };

        evaluate_policies(&policies, boundary.as_ref(), session_policy, action, resource_arn, context)
    }

    fn name(&self) -> &'static str { "builtin" }
}
```

### 8.3 AWS IAM Provider (Mode 3)

When extenddb runs in an environment with AWS connectivity (EC2, ECS, Lambda, corporate network with VPN), the AWS IAM provider delegates authentication to STS and retrieves real IAM policies for local evaluation. This gives full IAM fidelity without maintaining a local credential store.

#### 8.3.1 Architecture

```
Client (with extenddb wrapper)
    │
    ├─ 1. Pre-sign STS GetCallerIdentity URL
    │     (signed for service=sts, host=sts.amazonaws.com)
    │
    ├─ 2. Attach as X-Extenddb-Auth-Token header
    │
    ├─ 3. Send normal DynamoDB request to extenddb
    │     (SigV4 signature for service=dynamodb is ignored in Mode 3)
    │
    ▼
┌─────────────────────────────────────────────┐
│  AwsIamProvider                             │
│                                             │
│  1. authenticate()                          │
│     ├─ Extract token from X-Extenddb-Auth-Token │
│     ├─ Hash token → cache key               │
│     ├─ Cache miss: HTTP GET the pre-signed  │
│     │   URL → STS validates the signature   │
│     ├─ STS returns: ARN, Account, UserId    │
│     ├─ Cache identity (TTL configurable)    │
│     └─ Return AuthIdentity                  │
│                                             │
│  2. authorize()                             │
│     ├─ Check policy cache (by principal ARN)│
│     ├─ Cache miss: call IAM to fetch        │
│     │   policies for the principal          │
│     ├─ Evaluate fetched policies using      │
│     │   the same local policy engine        │
│     ├─ Cache policies (TTL configurable)    │
│     └─ Return AuthzDecision                 │
└─────────────────────────────────────────────┘
```

This is the same pattern used by EKS (`aws-iam-authenticator`) and Vault's AWS auth method. The client generates a pre-signed STS `GetCallerIdentity` request using its own credentials (signed for the `sts` service), and extenddb calls STS with that pre-signed request to validate the caller's identity.

> **Why pre-signed tokens instead of header forwarding?** SigV4 signatures are bound to the service name, host, and request body they were generated for. A request signed for `dynamodb` / `localhost:18443` cannot be replayed against `sts` / `sts.amazonaws.com` — STS will reject the signature because the credential scope, signed host, and body hash all mismatch. Pre-signed URLs solve this: the client signs a separate request specifically for STS, and extenddb uses that to authenticate.

#### 8.3.2 Authentication: Pre-Signed STS GetCallerIdentity

STS `GetCallerIdentity` is unique among AWS APIs: it validates the caller's credentials and returns their identity without requiring any IAM permissions on the caller. Any valid AWS credential can call it.

**Client-side token generation (performed by the extenddb wrapper):**

```
1. Using the caller's AWS credentials, generate a pre-signed URL for:
   - Service: sts
   - Action: GetCallerIdentity
   - Method: GET (query-string signed, not POST)
   - Expiry: 60 seconds (short-lived to limit replay window)

2. The resulting URL looks like:
   https://sts.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15
   &X-Amz-Algorithm=AWS4-HMAC-SHA256
   &X-Amz-Credential=AKIA.../20260406/us-east-1/sts/aws4_request
   &X-Amz-Date=20260406T201600Z
   &X-Amz-Expires=60
   &X-Amz-SignedHeaders=host
   &X-Amz-Signature=...

3. Base64-encode the URL and set it as the X-Extenddb-Auth-Token header.
```

**Server-side authentication flow:**

```rust
pub struct AwsIamProvider {
    http_client: reqwest::Client,
    iam_client: aws_sdk_iam::Client,
    identity_cache: moka::future::Cache<String, CachedIdentity>,
    policy_cache: moka::future::Cache<String, CachedPolicies>,
    config: AwsIamConfig,
}

pub struct AwsIamConfig {
    /// AWS region for IAM calls. Defaults to the instance's region
    /// (from IMDS / AWS_REGION env var) when running on EC2.
    pub region: Option<String>,
    /// TTL for cached identities (token hash → identity mapping).
    /// Default: 60 seconds. Kept short because pre-signed URLs expire.
    /// A rotated-away access key's identity remains cached (and accepted)
    /// for up to one TTL window — acceptable for most use cases.
    /// Lower this for security-sensitive deployments.
    pub identity_cache_ttl_seconds: u64,
    /// TTL for cached policies (principal ARN → policies).
    /// Default: 300 seconds. IAM policy changes during this window
    /// are not reflected — this is eventual consistency, similar to
    /// real DynamoDB's IAM propagation delay.
    pub policy_cache_ttl_seconds: u64,
}

struct CachedIdentity {
    pub arn: String,
    pub account_id: String,
    pub user_id: String,
    pub principal_type: IamPrincipalType,
}

enum IamPrincipalType {
    User { user_name: String },
    AssumedRole { role_name: String, session_name: String },
}

struct CachedPolicies {
    pub identity_policies: Vec<PolicyDocument>,
    pub permissions_boundary: Option<PolicyDocument>,
}
```

```
1. Extract the X-Extenddb-Auth-Token header from the request.
   - Missing header → return IncompleteSignature error.

2. Base64-decode the token to get the pre-signed STS URL.
   - Invalid base64 → return IncompleteSignature error.

3. Validate the URL:
   - Host must be sts.amazonaws.com (or regional STS endpoint).
   - Action must be GetCallerIdentity.
   - Reject URLs pointing to other services or actions
     (prevents token confusion attacks).

4. Hash the token (SHA-256) → use as identity cache key.
   - Cache hit: return cached identity. Skip STS call.

5. Cache miss: HTTP GET the pre-signed URL.
   - Use a plain HTTP client (reqwest), NOT the AWS SDK STS client.
     The SDK client would re-sign the request with the instance's
     credentials, overwriting the client's pre-signed signature.
   - Success: parse XML response for ARN, Account, UserId.
   - Failure (InvalidClientTokenId, SignatureDoesNotMatch, ExpiredToken):
     return the corresponding DynamoDB error to the client.
   - Failure (network/timeout): return ServiceUnavailable.

6. Parse the ARN to determine principal type:
   - arn:aws:iam::{account}:user/{name} → IamPrincipalType::User
   - arn:aws:sts::{account}:assumed-role/{role}/{session}
     → IamPrincipalType::AssumedRole

7. Cache the identity (keyed by token hash, TTL from config).

8. Build and return AuthIdentity with ResolvedPrincipal.
   - For User: construct UserIdentity. Tags are fetched during
     authorization (policy retrieval phase), not here.
   - For AssumedRole: construct RoleSession. Session tags are
     fetched during authorization.
```

#### 8.3.3 Authorization: IAM Policy Retrieval

After authentication, extenddb needs the caller's policies to make authorization decisions. Rather than trusting IAM blindly (which would mean no fine-grained access control), extenddb fetches the actual policies and evaluates them locally using the same policy engine as Mode 2.

**Policy retrieval flow for a User principal:**

```
1. Check policy_cache for this principal ARN.
   - Cache hit: use cached policies. Skip IAM calls.

2. Cache miss: fetch policies from IAM.

   a. Get inline policies:
      - iam:ListUserPolicies(UserName) → policy names
      - For each: iam:GetUserPolicy(UserName, PolicyName) → policy document

   b. Get attached managed policies:
      - iam:ListAttachedUserPolicies(UserName) → policy ARNs
      - For each: iam:GetPolicy(PolicyArn) → default version ID
      - For each: iam:GetPolicyVersion(PolicyArn, VersionId) → policy document

   c. Get group policies:
      - iam:ListGroupsForUser(UserName) → group names
      - For each group:
        - iam:ListGroupPolicies(GroupName) → inline policy names
        - For each: iam:GetGroupPolicy(GroupName, PolicyName) → document
        - iam:ListAttachedGroupPolicies(GroupName) → managed policy ARNs
        - For each: fetch via GetPolicy + GetPolicyVersion (as above)

   d. Get permissions boundary:
      - iam:GetUser(UserName) → PermissionsBoundary ARN (if set)
      - If set: iam:GetPolicy + GetPolicyVersion for the boundary

3. Parse all policy documents into PolicyDocument structs.
   (Same JSON → Rust deserialization as Mode 2 management API.)

4. Cache the result (keyed by principal ARN, TTL from config).

5. Evaluate using the standard policy engine (§6.4):
   - Explicit Deny check across all policies
   - Permissions boundary check (if present)
   - Identity policy Allow check
   - Implicit Deny
```

**Policy retrieval flow for an AssumedRole principal:**

```
1. Parse role name from ARN:
   arn:aws:sts::{account}:assumed-role/{role_name}/{session} → role_name

2. Check policy_cache for arn:aws:iam::{account}:role/{role_name}.

3. Cache miss: fetch role policies from IAM.

   a. Inline policies: iam:ListRolePolicies + iam:GetRolePolicy
   b. Attached policies: iam:ListAttachedRolePolicies + GetPolicy + GetPolicyVersion
   c. Permissions boundary: iam:GetRole → PermissionsBoundary ARN

   Note: session policies (passed during AssumeRole) are NOT retrievable
   from IAM after the fact. If the caller used a session policy, extenddb
   cannot enforce it. This is a known limitation — document it.

4. Cache, parse, evaluate as above.
```

**Principal tags retrieval:**

```
For User: iam:ListUserTags(UserName) → tags
For Role: iam:ListRoleTags(RoleName) → tags

Tags are fetched alongside policies and cached together.
These populate aws:PrincipalTag/* for ABAC condition evaluation.
```

#### 8.3.4 IAM Permissions Required by extenddb Host

The EC2 instance (or IAM role) running extenddb in Mode 3 needs the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ExtenddbAuthPolicyRetrieval",
      "Effect": "Allow",
      "Action": [
        "iam:GetUser",
        "iam:GetRole",
        "iam:ListUserPolicies",
        "iam:GetUserPolicy",
        "iam:ListAttachedUserPolicies",
        "iam:ListGroupsForUser",
        "iam:ListGroupPolicies",
        "iam:GetGroupPolicy",
        "iam:ListAttachedGroupPolicies",
        "iam:ListRolePolicies",
        "iam:GetRolePolicy",
        "iam:ListAttachedRolePolicies",
        "iam:GetPolicy",
        "iam:GetPolicyVersion",
        "iam:ListUserTags",
        "iam:ListRoleTags"
      ],
      "Resource": "*"
    }
  ]
}
```

> `sts:GetCallerIdentity` requires no permissions — any valid AWS credential can call it.

#### 8.3.5 Known Limitations (Mode 3)

1. **Session policies are not enforceable.** When a caller assumed a role with an inline session policy, that policy is not retrievable from IAM after the fact. extenddb evaluates only the role's attached policies and permissions boundary. This means Mode 3 is slightly more permissive than real DynamoDB for callers using session policies.

2. **Resource-based policies are not evaluated.** Consistent with the overall deferral of resource-based policies (see Non-Goals table).

3. **Service control policies (SCPs) are not evaluated.** SCPs are an Organizations-level feature. extenddb does not query Organizations APIs.

4. **Policy variable substitution is not supported.** Consistent with REQ-ABAC-006 deferral. Fetched policies containing `${aws:PrincipalTag/key}` will treat the variable syntax as a literal string.

5. **Cross-account access is not evaluated.** extenddb assumes all callers are in the same account. Cross-account role assumption and resource policies are not supported.

6. **Latency on cache miss.** Policy retrieval for a user with multiple groups and managed policies can require 10+ IAM API calls. With caching (default 300s TTL), this only happens once per principal per TTL window. First-request latency for a new principal may be 500ms–2s.

7. **Identity cache eventual consistency.** If a user rotates their access key, the old key's identity remains cached for up to one `identity_cache_ttl_seconds` window. During this window, a pre-signed token generated with the old key (before deletion) that hasn't expired could still produce a cache hit. In practice, the short token expiry (60s) and short default identity cache TTL (60s) limit this window. For security-sensitive deployments, lower the identity cache TTL.

8. **Policy cache eventual consistency.** If an admin detaches or modifies a policy in IAM, extenddb continues using the cached (now-stale) policies for up to `policy_cache_ttl_seconds`. This is analogous to real DynamoDB's IAM propagation delay (typically seconds). This is a conscious design choice — the TTL trades freshness for reduced IAM API call volume.

9. **Client wrapper required.** Unlike Modes 1 and 2, Mode 3 requires a thin client-side wrapper to generate the pre-signed STS token. The wrapper is provided for Python, Rust, and Java. Applications that cannot use the wrapper cannot use Mode 3.

#### 8.3.6 Configuration

```toml
[auth]
provider = "aws_iam"

[auth.aws_iam]
# AWS region for IAM API calls. Optional — defaults to the instance's
# region (from IMDS / AWS_REGION / AWS_DEFAULT_REGION env var).
# Required only when running outside EC2 or when targeting a different region.
# region = "us-east-1"
identity_cache_ttl_seconds = 60    # short: tokens expire in 60s
policy_cache_ttl_seconds = 300
```

#### 8.3.7 Implementation

```rust
impl AwsIamProvider {
    pub fn new(
        aws_config: &aws_config::SdkConfig,
        config: AwsIamConfig,
    ) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            iam_client: aws_sdk_iam::Client::new(aws_config),
            identity_cache: moka::future::Cache::builder()
                .time_to_live(Duration::from_secs(config.identity_cache_ttl_seconds))
                .build(),
            policy_cache: moka::future::Cache::builder()
                .time_to_live(Duration::from_secs(config.policy_cache_ttl_seconds))
                .build(),
            config,
        }
    }
}

/// Validate that a decoded pre-signed URL is a legitimate GetCallerIdentity
/// request and not a token confusion attack (e.g., a pre-signed URL for a
/// different service or action).
///
/// Checks:
/// - URL scheme is `https`
/// - Host is `sts.amazonaws.com`, `sts.<region>.amazonaws.com`, or
///   `sts-fips.<region>.amazonaws.com` (commercial and GovCloud regions).
///   China (`amazonaws.com.cn`) is not supported in v1.
/// - Query parameter `Action` equals `GetCallerIdentity`
///
/// Returns `Err(DynamoDbError::IncompleteSignature)` on any validation failure.
fn validate_presigned_url(url: &str) -> Result<(), DynamoDbError> {
    let parsed = url::Url::parse(url)
        .map_err(|_| DynamoDbError::incomplete_signature("Malformed auth token URL"))?;

    if parsed.scheme() != "https" {
        return Err(DynamoDbError::incomplete_signature("Auth token must use HTTPS"));
    }

    let host = parsed.host_str().unwrap_or_default();
    // Exact match for global endpoint, or sts.<region>.amazonaws.com /
    // sts-fips.<region>.amazonaws.com.  The dot-count check (== 3) ensures
    // exactly one region segment — no extra subdomains like sts.evil.com or
    // evil.amazonaws.com can slip through.
    // Known limitation: China partition (amazonaws.com.cn) is not supported in v1.
    let valid_host = host == "sts.amazonaws.com"
        || (host.starts_with("sts.")
            && host.ends_with(".amazonaws.com")
            && host.matches('.').count() == 3)
        || (host.starts_with("sts-fips.")
            && host.ends_with(".amazonaws.com")
            && host.matches('.').count() == 3);
    if !valid_host {
        return Err(DynamoDbError::incomplete_signature("Auth token host must be STS"));
    }

    let action = parsed.query_pairs()
        .find(|(k, _)| k == "Action")
        .map(|(_, v)| v);
    if action.as_deref() != Some("GetCallerIdentity") {
        return Err(DynamoDbError::incomplete_signature("Auth token action must be GetCallerIdentity"));
    }

    Ok(())
}

impl AuthProvider for AwsIamProvider {
    async fn authenticate(&self, request: &AuthRequest<'_>) -> Result<AuthIdentity, DynamoDbError> {
        let token = request.headers.get("x-extenddb-auth-token")
            .ok_or_else(|| DynamoDbError::incomplete_signature(
                "Missing X-Extenddb-Auth-Token header (required for aws_iam auth mode)"
            ))?;

        let presigned_url = base64_decode(token.as_bytes())
            .map_err(|_| DynamoDbError::incomplete_signature("Invalid auth token encoding"))?;

        validate_presigned_url(&presigned_url)?;  // host, action checks

        // Cache key is the SHA-256 of the raw base64 token (not the decoded URL).
        // The same client produces identical base64 for the same pre-signed URL,
        // so this is a stable cache key within the token's validity window.
        let cache_key = sha256_hex(&token);

        if let Some(cached) = self.identity_cache.get(&cache_key).await {
            return Ok(self.build_auth_identity(&cached));
        }

        // Network errors (DNS, TCP, TLS) are caught here at the transport layer.
        // parse_get_caller_identity_response handles only STS-level errors
        // (non-2xx responses with error codes in the XML body).
        let response = self.http_client.get(&presigned_url).send()
            .await
            .map_err(|_| DynamoDbError::service_unavailable("Auth backend unreachable"))?;

        let identity = parse_get_caller_identity_response(response)
            .await
            .map_err(|e| match e {
                StsError::InvalidClientTokenId => DynamoDbError::unrecognized_client("Invalid access key"),
                StsError::SignatureDoesNotMatch => DynamoDbError::unrecognized_client("Signature mismatch"),
                StsError::ExpiredToken => DynamoDbError::expired_token("Security token expired"),
                StsError::MalformedResponse => DynamoDbError::service_unavailable("Unexpected STS response"),
            })?;

        self.identity_cache.insert(cache_key, identity.clone()).await;
        Ok(self.build_auth_identity(&identity))
    }

    async fn authorize(
        &self,
        identity: &AuthIdentity,
        action: &str,
        resource_arn: &str,
        context: &RequestContext,
    ) -> Result<AuthzDecision, DynamoDbError> {
        let principal_arn = identity.principal_arn();

        let policies = if let Some(cached) = self.policy_cache.get(principal_arn).await {
            cached
        } else {
            let fetched = self.fetch_policies(identity)
                .await
                .map_err(|_| DynamoDbError::service_unavailable("Policy retrieval failed"))?;
            self.policy_cache.insert(principal_arn.to_owned(), fetched.clone()).await;
            fetched
        };

        evaluate_policies(
            &policies.identity_policies,
            policies.permissions_boundary.as_ref(),
            None, // session policy — not retrievable in Mode 3
            action,
            resource_arn,
            context,
        )
    }

    fn name(&self) -> &'static str { "aws_iam" }
}
```

### 8.4 Azure AD Provider (Sketch — Deferred)

```rust
pub struct AzureAdProvider {
    tenant_id: String,
    jwks_uri: String,
    role_mapping: HashMap<String, String>,  // Azure AD group → local role name
}

impl AuthProvider for AzureAdProvider {
    // Extract Bearer token, validate JWT, map to local Principal
    async fn authenticate(&self, request: &AuthRequest<'_>) -> Result<AuthIdentity, DynamoDbError> { todo!() }

    // Use local policy engine keyed by mapped principal
    async fn authorize(&self, identity: &AuthIdentity, action: &str, resource_arn: &str, context: &RequestContext) -> Result<AuthzDecision, DynamoDbError> { todo!() }

    fn name(&self) -> &'static str { "azure_ad" }
}
```

## 9. Integration with Middleware

The auth provider is injected into the server's middleware pipeline as an `Arc<dyn AuthProvider>`. The middleware builds the `RequestContext` before calling `authorize()`:

```rust
pub struct AuthLayer {
    provider: Arc<dyn AuthProvider>,
}

impl AuthLayer {
    pub async fn authenticate_and_authorize(
        &self,
        headers: &HashMap<String, String>,
        body: &[u8],
        operation: &str,
        table_name: Option<&str>,
        resource_tags: &HashMap<String, String>,
        request_params: &RequestParams,
    ) -> Result<AuthIdentity, DynamoDbError> {
        let resource_arn = build_resource_arn(table_name);
        let request = AuthRequest { headers, body, operation, resource_arn: &resource_arn };

        let identity = self.provider.authenticate(&request).await?;

        let context = RequestContext::build(
            &identity,
            operation,
            resource_tags,
            request_params,
        );

        let action = format!("dynamodb:{operation}");
        let decision = self.provider.authorize(&identity, &action, &resource_arn, &context).await?;

        match decision {
            AuthzDecision::Allow => Ok(identity),
            AuthzDecision::Deny { reason } => Err(DynamoDbError::access_denied(reason)),
        }
    }
}
```

## 10. Secret Key Encryption

```rust
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

pub fn encrypt_secret(plaintext: &[u8], key: &[u8; 32]) -> Vec<u8> {
    // Generate random 12-byte nonce
    // Encrypt with AES-256-GCM
    // Return: nonce || ciphertext || tag
}

pub fn decrypt_secret(ciphertext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, AuthError> {
    // Extract nonce (first 12 bytes)
    // Decrypt with AES-256-GCM
    // Return plaintext
}
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
