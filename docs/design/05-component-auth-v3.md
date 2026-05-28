# extenddb — Component Design: Authentication & Authorization

**Version:** 3.0
**Date:** 2026-04-15
**Status:** Approved (interactive review session 2026-04-15)
**Crate:** `extenddb-auth`
**Supersedes:** 05-component-auth.md (v2.0)

## 1. Purpose

The `auth` crate provides pluggable authentication and authorization for extenddb. It defines the `AuthProvider` trait, implements the built-in SigV4 provider with local credential management, and contains the IAM policy evaluation engine supporting identity-based access control (IBAC), role-based access control (RBAC), and fine-grained/tag-based access control (ABAC/FGAC).

The crate depends on `extenddb-core` (for types and errors) and `async_trait` (for object-safe async trait dispatch). It has no HTTP framework or storage dependencies.

> **No caching.** All credential lookups, policy fetches, and identity resolution read directly from the catalog store on every request. The latency is sub-millisecond for indexed lookups, and this approach eliminates stale-cache bugs and cache invalidation complexity entirely. Multiple extenddb instances sharing the same catalog see consistent data without coordination. If profiling under production load shows database round-trips are a bottleneck, caching can be added as a transparent layer without changing any interfaces.

## 2. Key Design Decisions (from 2026-04-15 review session)

1. **Builtin SigV4 auth (Mode 2) is the primary target.** Full SigV4 signature verification with local credential store and IAM policy evaluation.

2. **All identity/policy management goes through a Management API.** HTTP endpoints under `/management/*` on the same server. The CLI is a thin client that calls these endpoints.

3. **Management API is authenticated via admin users.** Admin users have username/password credentials (bcrypt-hashed). `extenddb init` creates the first admin user and prints the password once.

4. **IAM users self-service their own access keys.** An admin creates the IAM user with a console password. The IAM user authenticates to the management API and creates their own access key/secret key pair. The admin never sees the secret key.

5. **Multi-account support.** The catalog schema is scoped by `account_id`. Different accounts can have tables with the same name. The `account_id` is resolved from the authenticated identity, not from config.

6. **Auth provider is config-only.** Set in `extenddb.toml` as `auth.provider = "builtin"`. The `"none"` value is no longer accepted — the server refuses to start with it. Cannot be changed at runtime via settings.

7. **No catalog migration.** `extenddb init` is required. The schema includes auth tables from the start.

8. **Policy-controlled management.** Admin vs IAM user permissions are enforced via default policies seeded at account/user creation time, not hard-coded in application logic. This allows future extension where management operations can be delegated to non-admin users via policy.

9. **Future management console.** The management API is designed so a web console can be built as a frontend client. The console can bind to a routable interface. Multiple admin users are supported, all equivalent (organizational admins).

## 3. Auth Modes

### 3.1 Mode 1: No Auth (removed)

Previously `auth.provider = "none"`. This mode has been removed. The server refuses to start with `provider = "none"`. All deployments use builtin auth.

### 3.2 Mode 2: Builtin SigV4 (`auth.provider = "builtin"`)

Full SigV4 signature verification with local credential store and IAM policy evaluation. Multi-account. All identity/policy management via CLI and management API.

### 3.3 Mode 3: Federated AWS IAM (deferred)

Pre-signed STS `GetCallerIdentity` token flow. Delegates authn to real AWS STS, fetches real IAM policies. Requires AWS connectivity. Design unchanged from v2.0 §8.3.

### 3.4 Mode 4: Azure AD (deferred)

JWT-based auth with Azure AD group-to-role mapping. Design unchanged from v2.0 §8.4.

## 4. Admin User Model

Admin users are organizational administrators who manage accounts, IAM users, roles, groups, and policies. They are separate from IAM users — they do not make DynamoDB requests.

### 4.1 Bootstrap

```
extenddb init --catalog-db extenddb ...
  → creates catalog schema (including admin_users table and all IAM tables)
  → generates random password for first admin user "admin"
  → stores bcrypt hash in admin_users
  → prints: "Admin user created. Username: admin, Password: <random>"
  → password shown exactly once
```

### 4.2 Admin Operations

Admins authenticate to the management API with username/password (HTTP Basic auth). Admins can:

- Create/delete other admin users
- Change their own password
- Create/delete accounts
- Create/delete IAM users (with initial console password)
- Create/delete groups, roles
- Attach/detach policies to users, groups, roles
- Tag users, roles
- Manage permissions boundaries

### 4.3 Storage

```sql
CREATE TABLE admin_users (
    admin_name TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,   -- bcrypt (salt embedded in hash)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## 5. IAM Identity Model

### 5.1 Accounts

```sql
CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,       -- 12-digit numeric string
    account_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Account IDs are generated as 12-digit zero-padded random numbers, matching AWS format.

### 5.2 IAM Users

```sql
CREATE TABLE iam_users (
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    user_name TEXT NOT NULL,
    user_arn TEXT NOT NULL UNIQUE,
    password_hash TEXT,                -- bcrypt, NULL if console access not enabled
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, user_name)
);

CREATE TABLE iam_user_tags (
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, user_name, tag_key),
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);
```

IAM users authenticate to the management API to self-service their own access keys and change their own password. They authenticate with `account_id/user_name` as the username and their console password.

### 5.3 Access Keys

```sql
CREATE TABLE access_keys (
    access_key_id TEXT PRIMARY KEY,    -- AKIA* for long-lived, ASIA* for session
    secret_key_encrypted BYTEA NOT NULL,
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);
```

- Access key ID: 20-character string, `AKIA` prefix for long-lived keys
- Secret key: 40-character random string, encrypted with AES-256-GCM at rest
- Secret key shown exactly once at creation time, never retrievable after
- Created by the IAM user themselves via management API self-service

### 5.4 Groups

```sql
CREATE TABLE iam_groups (
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    group_name TEXT NOT NULL,
    group_arn TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, group_name)
);

CREATE TABLE iam_group_members (
    account_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    user_name TEXT NOT NULL,
    PRIMARY KEY (account_id, group_name, user_name),
    FOREIGN KEY (account_id, group_name) REFERENCES iam_groups(account_id, group_name) ON DELETE CASCADE,
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);
```

### 5.5 Roles

```sql
CREATE TABLE iam_roles (
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    role_name TEXT NOT NULL,
    role_arn TEXT NOT NULL UNIQUE,
    trust_policy JSONB NOT NULL,
    permissions_boundary_arn TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, role_name)
);

CREATE TABLE iam_role_tags (
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, role_name, tag_key),
    FOREIGN KEY (account_id, role_name) REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);
```

### 5.6 Sessions (Temporary Credentials from AssumeRole)

```sql
CREATE TABLE iam_sessions (
    session_token TEXT PRIMARY KEY,
    access_key_id TEXT NOT NULL UNIQUE,
    secret_key_encrypted BYTEA NOT NULL,
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    session_name TEXT NOT NULL,
    session_tags JSONB,
    session_policy JSONB,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (account_id, role_name) REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);
```

### 5.7 Policies

```sql
CREATE TABLE iam_policies (
    account_id TEXT NOT NULL,
    principal_type TEXT NOT NULL,       -- 'user', 'group', or 'role'
    principal_name TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    policy_document JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, principal_type, principal_name, policy_name)
);

CREATE TABLE iam_permissions_boundaries (
    account_id TEXT NOT NULL,
    principal_type TEXT NOT NULL,       -- 'user' or 'role'
    principal_name TEXT NOT NULL,
    policy_document JSONB NOT NULL,
    PRIMARY KEY (account_id, principal_type, principal_name)
);
```

### 5.8 Default Policies

When `extenddb init` creates the first admin user, it seeds a default policy. When an admin creates an IAM user, a default self-service policy is attached.

**Admin default policy** (seeded at init, attached to admin operations):
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "iam:*",
    "Resource": "*"
  }]
}
```

**IAM user default self-service policy** (attached at user creation):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateAccessKey",
        "iam:DeleteAccessKey",
        "iam:ListAccessKeys",
        "iam:ChangePassword"
      ],
      "Resource": "arn:aws:iam::${account_id}:user/${user_name}"
    }
  ]
}
```

Note: the `${account_id}` and `${user_name}` in the default policy are literal substitutions performed at user creation time, not IAM policy variables. Policy variable support (REQ-ABAC-006) remains deferred.

## 6. Catalog Schema Changes for Multi-Account

### 6.1 Tables

```sql
CREATE TABLE tables (
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    table_name TEXT NOT NULL,
    key_schema JSONB NOT NULL,
    attribute_definitions JSONB NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput JSONB,
    stream_specification JSONB,
    table_status TEXT NOT NULL DEFAULT 'CREATING',
    creation_date_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    table_size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    table_arn TEXT NOT NULL,
    table_id TEXT NOT NULL,
    ttl_attribute TEXT,
    deletion_protection_enabled BOOLEAN NOT NULL DEFAULT false,
    status_transition_at TIMESTAMPTZ,
    PRIMARY KEY (account_id, table_name),
    CONSTRAINT tables_table_id_unique UNIQUE (table_id)
);
```

### 6.2 Data Table Naming

Per-table backend data tables change from `_ddb_{table_name}` to `_ddb_{account_id}_{table_name}` to avoid collisions between accounts.

### 6.3 Indexes, Streams, Tags

- `indexes` FK is on `table_id` (UUID) — no change needed, already account-independent.
- `stream_shards` and `stream_records` reference `table_name` — must add `account_id` or switch to `table_id` FK.
- `tags` keyed by `resource_arn` — ARNs embed account_id, so no change needed.

### 6.4 Account ID Flow

In Mode 2 (builtin), `account_id` is resolved from the authenticated identity:
```
Authorization header → access_key_id → access_keys table → account_id
```

In Mode 1 (none), `account_id` comes from `extenddb.toml` config (backward compatible).

The `OperationContext` carries `account_id` from auth resolution. Every engine dispatch passes it to storage. Every storage query is scoped by `account_id`.

## 7. SigV4 Verification

Unchanged from v2.0 §5.1. The server-side flow:

1. Parse `Authorization: AWS4-HMAC-SHA256 Credential=<access_key>/<date>/<region>/dynamodb/aws4_request, SignedHeaders=<headers>, Signature=<hex>`
2. Look up `access_key_id` → `StoredCredential` (encrypted secret, account_id, user_name)
3. Decrypt secret key (AES-256-GCM)
4. Validate timestamp (X-Amz-Date within ±15 minutes)
5. Reconstruct canonical request from HTTP method, URI, signed headers, body hash
6. Derive signing key: 4-step HMAC-SHA256 chain (date → region → service → aws4_request)
7. Compute expected signature
8. Constant-time compare
9. On match: resolve full identity (user or role session)
10. On mismatch: return appropriate DynamoDB error

For temporary credentials (ASIA* keys), the `X-Amz-Security-Token` header contains the session token. The server looks up the session, checks expiration, and resolves to the role's identity.

## 8. Policy Evaluation Engine

Unchanged from v2.0 §6. The engine evaluates all three access control patterns through the same algorithm:

| Pattern | What's Checked |
|---------|---------------|
| IBAC | Action + Resource match against user/group policies |
| RBAC | Action + Resource match against role policies (after AssumeRole) |
| ABAC/FGAC | Action + Resource + Condition match using principal/resource tags |

### 8.1 Evaluation Algorithm

1. Collect applicable policies (user + group policies, or role policies)
2. Collect permissions boundary (if set)
3. Collect session policy (if AssumeRole with inline policy)
4. **Explicit Deny scan** — any Deny statement that matches → DENY
5. **Permissions boundary check** — must find Allow in boundary → else DENY
6. **Session policy check** — must find Allow in session policy → else DENY
7. **Identity policy Allow** — find Allow in identity policies → ALLOW
8. **Implicit Deny** → DENY

### 8.2 Condition Operators

Full set from v2.0 §6.5: StringEquals, StringLike, NumericEquals, DateEquals, Bool, Null, ArnLike, ForAllValues, ForAnyValue, IfExists, and all variants.

### 8.3 DynamoDB Condition Keys

- `aws:PrincipalTag/*` — from authenticated identity's tags
- `dynamodb:ResourceTag/*` — from target table's tags
- `dynamodb:LeadingKeys` — partition key values being accessed
- `dynamodb:Attributes` — attribute names being read/written
- `dynamodb:Select`, `dynamodb:ReturnValues`, `dynamodb:ReturnConsumedCapacity`
- `dynamodb:FullTableScan` — true for Scan operations
- `dynamodb:EnclosingOperation` — parent operation for batch/transact sub-ops

### 8.4 Management API Policy Evaluation

Management API calls are treated as IAM actions (`iam:CreateUser`, `iam:PutUserPolicy`, etc.) and evaluated against the caller's policies using the same engine. This is how admin vs IAM user permissions are enforced — via default policies, not hard-coded logic.

The only exception is the bootstrap admin created by `extenddb init`: if no policies exist for this principal, the system allows the operation. Once the seed policy is in place, normal evaluation takes over.

## 9. AssumeRole

AssumeRole is a CLI command (and management API endpoint) that creates temporary credentials for a role.

```
extenddb manage --user admin --password <pw> assume-role \
    --account-id 123456789012 \
    --role-name data-reader \
    --caller-arn arn:aws:iam::123456789012:user/alice \
    --session-name test-session \
    [--session-tags '{"Project":"Alpha"}'] \
    [--session-policy policy.json] \
    [--duration-seconds 3600]
```

Flow:
1. Load role, extract trust policy
2. Evaluate trust policy: caller ARN must match a Principal in an Allow statement, conditions must pass
3. Generate temporary credentials (ASIA* access key + secret + session token)
4. Merge tags: role tags + session tags (session wins on conflict)
5. Store session in `iam_sessions`
6. Print access key ID, secret key, session token (shown once)

## 10. Encryption Key Management

Secret keys are encrypted at rest with AES-256-GCM. The encryption key is generated at `extenddb init` time and stored in the `settings` table. The threat model is: the catalog database is the trust boundary. If an attacker can read the settings table, they already have access to all data.

```sql
INSERT INTO settings (key, value) VALUES ('encryption_key', '<base64-encoded-32-byte-key>');
```

## 11. Management API Endpoints

All under `/management/*`. Authenticated via HTTP Basic auth (admin users use `admin_name:password`, IAM users use `account_id/user_name:password`).

### Admin endpoints:
- `POST /management/admins` — create admin user
- `PUT /management/admins/{name}/password` — change admin password
- `DELETE /management/admins/{name}` — delete admin user
- `POST /management/accounts` — create account
- `DELETE /management/accounts/{id}` — delete account
- `POST /management/accounts/{id}/users` — create IAM user
- `DELETE /management/accounts/{id}/users/{name}` — delete IAM user
- `POST /management/accounts/{id}/groups` — create group
- `DELETE /management/accounts/{id}/groups/{name}` — delete group
- `POST /management/accounts/{id}/groups/{name}/members` — add user to group
- `DELETE /management/accounts/{id}/groups/{name}/members/{user}` — remove user from group
- `POST /management/accounts/{id}/roles` — create role
- `DELETE /management/accounts/{id}/roles/{name}` — delete role
- `PUT /management/accounts/{id}/users/{name}/policy/{policy}` — put user policy
- `PUT /management/accounts/{id}/groups/{name}/policy/{policy}` — put group policy
- `PUT /management/accounts/{id}/roles/{name}/policy/{policy}` — put role policy
- `DELETE /management/accounts/{id}/users/{name}/policy/{policy}` — delete user policy
- `PUT /management/accounts/{id}/users/{name}/tags` — tag user
- `PUT /management/accounts/{id}/roles/{name}/tags` — tag role
- `PUT /management/accounts/{id}/users/{name}/permissions-boundary` — set boundary
- `PUT /management/accounts/{id}/roles/{name}/permissions-boundary` — set boundary
- `POST /management/accounts/{id}/roles/{name}/assume` — assume role

### IAM user self-service endpoints:
- `POST /management/accounts/{id}/users/{name}/access-keys` — create access key (self only)
- `DELETE /management/accounts/{id}/users/{name}/access-keys/{key_id}` — delete access key (self only)
- `GET /management/accounts/{id}/users/{name}/access-keys` — list access keys (self only)
- `PUT /management/accounts/{id}/users/{name}/password` — change own password

### List/describe endpoints (admin only):
- `GET /management/accounts` — list accounts
- `GET /management/accounts/{id}/users` — list users
- `GET /management/accounts/{id}/groups` — list groups
- `GET /management/accounts/{id}/roles` — list roles
- `GET /management/accounts/{id}/users/{name}/policies` — list user policies
- `GET /management/accounts/{id}/groups/{name}/policies` — list group policies
- `GET /management/accounts/{id}/roles/{name}/policies` — list role policies

## 12. CLI Commands

The CLI is a thin client that calls the management API. All commands require `--config extenddb.toml` to discover the server address, or `--endpoint https://127.0.0.1:8000`.

```
extenddb manage --user <admin|account_id/user_name> --password <pw> <subcommand>
```

Subcommands mirror the management API endpoints listed in §11.

## 13. Configuration

```toml
[auth]
provider = "builtin"    # "none" or "builtin"
```

When `provider = "none"`: current behavior, no auth, single-tenant.
When `provider = "builtin"`: SigV4 verification, multi-account, policy evaluation.

The `[server].account_id` config key is used only when `provider = "none"`. When `provider = "builtin"`, account_id comes from the authenticated identity.

## 14. Error Fidelity

Auth-related DynamoDB errors:
- `UnrecognizedClientException` — invalid access key or signature mismatch
- `IncompleteSignatureException` — malformed Authorization header
- `MissingAuthenticationTokenException` — no Authorization header
- `ExpiredTokenException` — expired session credentials
- `AccessDeniedException` — policy evaluation denied the request

These must match real DynamoDB error format (HTTP status, error code, message structure).

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
