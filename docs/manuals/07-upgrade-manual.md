# Upgrade Manual

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Current Status

Catalog 0.0.3 is current. The 0.0.2 to 0.0.3 upgrade is the first in-place catalog upgrade ExtendDB has, and **every existing PostgreSQL deployment must run it**, including deployments that never use vector indexes: the server refuses to start against a catalog version it was not built for.

See [Catalog 0.0.3](#catalog-003-current) below for what changes and the exact sequence.

## How Catalog Upgrades Work

### The Migration System

Migrations are SQL files in `crates/storage-postgres/migrations/`, applied in filename order:

```
001_schema.sql            ← the complete initial schema
002_vector_indexes.sql    ← vector index metadata, catalog 0.0.3
```

The `schema_history` table tracks which files have been applied. When `extenddb migrate` runs, it:

1. Reads all migration files embedded in the binary (via `include_str!`)
2. Checks `schema_history` for each filename
3. Applies any unapplied migrations in order
4. Records each applied filename in `schema_history`

Running `extenddb migrate` on an up-to-date catalog is a no-op.

### The Catalog Version

A single row in the `settings` table stores the catalog version:

```sql
SELECT value FROM settings WHERE key = 'catalog_version';
-- '0.0.2'
```

The binary embeds an expected catalog version (`CATALOG_VERSION` constant in `crates/storage-postgres/src/lib.rs`). At startup, the server compares the database value against the binary's expectation. If they don't match, the server refuses to start and directs the operator to run `extenddb migrate`.

### Version Semantics

The catalog version follows semantic versioning:

- **MAJOR**: Breaking schema changes that may require data migration or downtime
- **MINOR**: New tables or columns (backward-compatible, additive)
- **PATCH**: Index changes, constraint fixes, seed data updates

## Writing a New Migration

When you need to change the catalog schema, here's the process:

### 1. Create the migration file

Add a new SQL file with the next sequence number:

```
crates/storage-postgres/migrations/002_your_feature.sql
```

The file should be a single transaction:

```sql
-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 002: Brief description of what this adds/changes.

BEGIN;

-- Your DDL here.
ALTER TABLE tables ADD COLUMN IF NOT EXISTS new_column TEXT;

-- Bump the catalog version.
UPDATE settings SET value = '0.1.0' WHERE key = 'catalog_version';

COMMIT;
```

### 2. Register it in the migration runner

Add the file to `CATALOG_MIGRATIONS` in `crates/storage-postgres/src/migrations.rs`:

```rust
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-postgres/migrations/001_schema.sql"),
    ),
    (
        "002_your_feature.sql",
        include_str!("../../storage-postgres/migrations/002_your_feature.sql"),
    ),
];
```

### 3. Bump the catalog version constant

In `crates/storage-postgres/src/lib.rs`:

```rust
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 1, 0);
```

This must match the version written by your migration's `UPDATE settings` statement.

### 4. Update 001_schema.sql

The consolidated schema file is what fresh installs get. Add your new column/table/index to `001_schema.sql` as well, and update its `INSERT INTO settings` to seed the new version. This way fresh installs get the final schema in one pass, while existing deployments get there via the incremental migration.

### Design Considerations

**Idempotency.** Use `IF NOT EXISTS`, `IF EXISTS`, and `ADD COLUMN IF NOT EXISTS` so migrations can be safely re-run.

**Backward compatibility.** Prefer additive changes (new columns with defaults, new tables) over destructive ones (dropping columns, renaming tables). A running server on the old binary should survive the schema change until it's restarted with the new binary.

**Transaction boundaries.** Wrap each migration in `BEGIN`/`COMMIT`. If any statement fails, the entire migration rolls back and the catalog stays at the previous version.

**No data migrations in DDL files.** If a schema change requires backfilling data, do it in Rust code triggered by `extenddb migrate`, not in raw SQL. This gives you error handling, progress reporting, and the ability to batch large updates.

**Test both paths.** Every migration must be tested two ways:
1. Fresh install (`extenddb init`) — verifies `001_schema.sql` is correct
2. Upgrade (`extenddb migrate` on a catalog at the previous version) — verifies the incremental migration works

## General Upgrade Procedure

For future releases that include catalog changes:

1. **Stop the server**

```bash
extenddb stop --config extenddb.toml
```

2. **Back up databases**

```bash
pg_dump extenddb_catalog > catalog_backup_$(date +%Y%m%d).sql
pg_dump extenddb > data_backup_$(date +%Y%m%d).sql
```

3. **Build the new version**

```bash
git pull
cargo build --release
```

4. **Run migrations**

```bash
extenddb migrate --config extenddb.toml
```

5. **Verify**

```bash
extenddb verify --config extenddb.toml
```

6. **Start the server**

```bash
extenddb serve --config extenddb.toml
```

## Rollback Procedure

If an upgrade fails:

1. Stop the server
2. Restore from backup:

```bash
psql -c "DROP DATABASE extenddb_catalog;"
psql -c "CREATE DATABASE extenddb_catalog OWNER extenddb;"
psql -d extenddb_catalog -f catalog_backup_YYYYMMDD.sql
```

3. Rebuild the previous version and start it

## Version History

### Catalog 0.0.3 (Current)

Adds vector index metadata:

- New `vector_indexes` table: one row per vector index, holding its dimensions, distance function, vector attribute, search schema, projection, and build state.
- New `vector_indexes` column on `backups`: a snapshot of the source table's vector index configuration, taken when the backup is created.

**Every PostgreSQL deployment must apply this**, whether or not it uses vector indexes, because the server refuses to start against a catalog version it was not built for.

Upgrade sequence:

```bash
extenddb stop --config extenddb.toml
extenddb migrate --yes --config extenddb.toml
extenddb serve --config extenddb.toml
```

Run `extenddb migrate` without `--yes` first to see what is pending; it reports `catalog 0.0.2 -> 0.0.3` and changes nothing.

The upgrade is not reversible in place: a 0.0.2 binary refuses to start against a 0.0.3 catalog, by the same check in the other direction. Roll back by restoring the catalog backup taken before the upgrade, as described above. Downgrading is safe for data written before the upgrade; vector indexes created afterwards are not representable in 0.0.2 and are lost with the restore.

During the upgrade the migration also attempts to install the pgvector extension on the data database. Failure is reported as a notice and does not stop the upgrade: vector indexes are then refused at request time, and every other operation is unaffected. See the Admin Guide for what the notice means and what to do about it.

### Catalog 0.0.2 (Initial Release)

Complete schema: accounts, tables, indexes, tags, streams, IAM (users, groups, roles, policies, access keys, sessions, permissions boundaries), idempotency tokens, metrics, login attempts, backups, continuous backups, TTL support, settings.

The first release, so all 0.0.2 deployments were fresh installs.

---

## Behavior Changes by Release

Catalog upgrades change the schema; behavior changes alter how the running server
interprets existing data or configuration. Review these before upgrading a live
deployment, even when no catalog migration is required.

### v0.1.6 — IAM: bare operators on multivalued condition keys are no-ops (BR-7085)

**What changed.** A **bare** condition operator (no `ForAnyValue:` / `ForAllValues:`
qualifier) applied to a *multivalued* condition key — `dynamodb:Attributes` or
`dynamodb:LeadingKeys` — now **never matches**, matching real AWS IAM exactly. Before
v0.1.6, extenddb evaluated a bare operator as an implicit AND across the request's values,
so such a condition could match and appear to enforce a restriction.

**Who is affected.** Any deployment with an IAM policy that uses a bare operator (e.g.
`StringEquals`) on `dynamodb:Attributes` or `dynamodb:LeadingKeys`. A common pattern is a
denylist that *appeared* to protect an attribute:

```json
{ "Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
  "Condition": { "StringEquals": { "dynamodb:Attributes": ["ssn"] } } }
```

Under the old behavior this could deny a single-attribute request; after the upgrade it is a
no-op (as it always was on real AWS). **This is a security-relevant change: policies relying
on the old semantics no longer restrict access.**

**Migration.** Rewrite affected policies to use a set qualifier before upgrading:

- Denylist (block if the request touches *any* listed attribute):
  `"ForAnyValue:StringEquals": { "dynamodb:Attributes": ["ssn"] }`
- Allowlist (allow only when *every* requested attribute is listed):
  `"ForAllValues:StringEquals": { "dynamodb:Attributes": ["pk", "sk"] }` on an `Allow`.

The same applies to `dynamodb:LeadingKeys`. Audit existing policies with
`extenddb manage list-user-policies` / `list-role-policies` and update any that use bare
operators on these keys. No catalog migration is required for this change.

### IAM: `aws:TokenIssueTime` on MongoDB role sessions created before the upgrade

**What changed.** ExtendDB now resolves `aws:TokenIssueTime` for a role session from
the session's recorded creation time. On the MongoDB backend the session store did not
record a creation time before this release.

**Who is affected.** MongoDB deployments only. A role session established before the
upgrade has no stored creation time, so `aws:TokenIssueTime` is absent for that session
until it expires and is re-established. The three SQL and the Cassandra backends already
recorded the session creation time and are not affected.

**Impact.** The direction is over-deny only and self-heals: a policy that gates on
`aws:TokenIssueTime` (a presence check or a `Date` comparison) denies a pre-upgrade
MongoDB session that it would grant, and resolves correctly once the session is
re-established. No catalog migration is required; new sessions record the creation time
automatically.

### IAM: a malformed CIDR in an identity policy is rejected at write

**What changed.** A condition value for `IpAddress` or `NotIpAddress` that is not a
valid CIDR or address (for example `10.0.0.0/33`, `999.0.0.0/8`, `not-a-cidr`,
`10.0.0.0/`) is now rejected when an identity policy or a permissions boundary is
written, with a `MalformedPolicyDocument`-class error, matching the service. Before
this release the value was accepted and the statement silently did nothing (an
`IpAddress` or `NotIpAddress` `Deny` never blocked).

**Who is affected.** Any deployment that stored an identity policy or permissions
boundary with a malformed CIDR. New writes with a malformed value now fail. A policy
already stored with a malformed value evaluates fail closed: it no longer parses, so the
request is denied rather than the statement being ignored. Audit stored policies with
`extenddb manage list-user-policies` / `list-role-policies` and correct any malformed CIDR
before upgrading so a legitimate request is not denied.

**What a stored malformed-CIDR policy does after upgrade.** The failure is
whole-document and scoped to the principal that owns the policy. Every request by
that principal is denied until the policy is corrected, including a request that a
different, valid policy of the same principal would allow (the malformed document
fails the request closed before the valid policy can grant). The caller receives
`AccessDeniedException` with the message "Not authorized to perform this action
(policy evaluation error)", not a server error: a parse failure on the authorization
path is mapped to a deny, not to `InternalServerError`. A different principal whose
policies are all well formed is unaffected.

**Trust policies are unchanged.** The live service accepts a malformed CIDR in a trust
policy at write and drops the statement at evaluation; ExtendDB matches this, so a trust
policy is not rejected for a malformed CIDR. No catalog migration is required.

### IAM: condition key names and tag keys, and more CIDR forms

**Condition key names and tag keys fold under Unicode on the request path.** A
stored identity policy whose condition key name or tag key contains a non-ASCII
letter that folds to a resolvable key (for example the long s folding to `s`, the
st ligature to `st`, or the sharp s to `ss`) now resolves that key on a DynamoDB
request, matching the service. For such a stored policy a `Deny` gated on the
Unicode-spelled key now applies where it previously did not, and a negated `Allow`
gated on it no longer grants. A policy using only ASCII key names is unaffected.
The trust (`AssumeRole`) path matches key names and tag keys ASCII-case only and
does not fold, as the service does; a trust policy whose condition key name or tag
key contains a non-ASCII letter has that key read as absent on the trust path.

**Tag keys that fold to the same string resolve deterministically.** If a
principal carries two tag keys that fold to the same string (for example `mass`
and `ma\u{00df}`), `aws:PrincipalTag/<key>` now resolves to the byte-exact stored
key, then to the smallest fold-matching key by code point, every time, rather
than at random. A policy naming one of the keys exactly is unaffected.

**Tag write rules match the service.** `TagUser`, `TagRole` and `AssumeRole`
session tags now reject a tag key or value outside IAM's grammar
`[\p{L}\p{Z}\p{N}_.:/=+\-@]`: any Unicode Letter (Lu, Ll, Lt, Lm, Lo), Number
(Nd, Nl, No) or Separator (Zs, Zl, Zp) by general category, plus the listed
symbols. A combining mark (such as U+0345, category Mn), `#`, `!` and other
punctuation, a key over 128 or value over 256 characters, the reserved `aws:`
key prefix, and two keys that differ only in ASCII case are all rejected. Only
the tags named in a given request are validated, so a tag already stored that
the new rule would reject stays in place and keeps evaluating, and does not
block setting other tags on the same principal; it is rejected only when a later
request includes it again. Live IAM behaves the same way: a tag request
validates only its own tags.

**Policy Sid charset.** A policy `Sid` is now accepted only when it is empty or
entirely ASCII letters and digits, on both the trust write (`CreateRole`) and
the identity-policy write (`PutUserPolicy`), matching the service, which rejects
any other `Sid` (a hyphen, underscore, space, dot or non-ASCII character) with
`MalformedPolicyDocument`. The `Sid` is a statement label and does not affect
evaluation, so a policy already stored with a non-conforming `Sid` keeps
evaluating unchanged; only a new write is checked.

**More CIDR forms.** `IpAddress`/`NotIpAddress` now accept a leading-zero octet
and read it as decimal (`010.0.0.0/8` is `10.0.0.0/8`, not octal), matching the
service, and reject an IPv4-mapped IPv6 literal (`::ffff:a.b.c.d`) at the identity
and boundary write (the service rejects it; accepting it would store an
`IpAddress` `Deny` that silently never matched). No catalog migration is required
for any of these changes.

---

### v0.1.14: IAM resource tags evaluate only as `aws:ResourceTag/`

**What changed.** ExtendDB now resolves resource tags only as `aws:ResourceTag/<key>`, matching the live service. `dynamodb:ResourceTag/<key>` is left absent (the live service never populates it). Before v0.1.14, ExtendDB filled `dynamodb:ResourceTag/<key>` from the target table's tags, so a condition on that key evaluated against the tag value instead of being treated as absent. `aws:ResourceTag/<key>` is unaffected and keeps resolving from the table's tags, case-insensitively on the key.

**Who is affected.** Any deployment with an IAM policy that uses a `dynamodb:ResourceTag/` condition key or a `${dynamodb:ResourceTag/...}` policy variable: inline user, group and role policies, user and role permissions boundaries, and session policies. A policy that uses `aws:ResourceTag/` is not affected. Role trust policies are not affected: the AssumeRole path never resolved `dynamodb:ResourceTag/`, so a trust policy that names it behaved then as it does now.

**Migration is required.** `extenddb serve` refuses to start on a catalog that has not been migrated, with an error that names `extenddb migrate --yes`. Run `extenddb migrate --yes` once per deployment before starting the new binary. On PostgreSQL, `migrate` connects as the admin user, so pass `--pg-user <admin>` and set `EXTENDDB_PG_PASSWORD`, as for `init`. The migration rewrites every stored `dynamodb:ResourceTag/<key>` condition key and `${dynamodb:ResourceTag/...}` policy variable to the `aws:ResourceTag/` form, keeping the operator, the tag key and the value unchanged. The rewrite preserves each policy's decision, because `aws:ResourceTag/` resolves from the same table tags the old `dynamodb:ResourceTag/` key read. The one difference is a `${dynamodb:ResourceTag/<key>}` policy variable on a table without that tag: neither form expands, and the literal text in the comparison changes from `${dynamodb:ResourceTag/<key>}` to `${aws:ResourceTag/<key>}`. The decision changes only for a request value equal to one of those literals. A freshly initialised deployment needs no migrate step; its catalog is already recorded as migrated.

**What `migrate` prints.** Without `--yes`, `migrate` lists each policy it would rewrite, one line per policy with the account, the principal, the policy name and the number of condition keys, and writes nothing. With `--yes`, it rewrites and records that the migration is complete. It runs on every `migrate`, so an interrupted run is finished by the next one. If a policy lookup fails, or a policy needs attention (below), `migrate` reports it, exits non-zero, and does not record completion, so a later run retries. Policy bodies are never printed.

**Both forms on one key.** If one operator block in a policy holds both `aws:ResourceTag/<key>` and `dynamodb:ResourceTag/<key>` for the same tag key with the same value, the two were one condition before the upgrade, and `migrate` drops the `dynamodb:` key. With different values they cannot share one JSON key. `migrate` then leaves the policy unchanged, lists it as `NEEDS ATTENTION` with the account, the principal and the policy name, exits non-zero, and does not record the migration as complete, so `serve` keeps refusing the catalog. Fix the policy with the previous version still serving the catalog (keep one of the two forms), then run `migrate --yes` with the new binary. A session policy that does not parse is reported in the log and skipped; it cannot be evaluated under either form.

**What each statement shape does for a policy that was not migrated.** An operator who writes a policy by hand after the upgrade, for example from an old template, needs to know that the `dynamodb:ResourceTag/` form has no effect. With the key absent, standard IAM absent-key semantics apply:

| Statement shape on `dynamodb:ResourceTag/env` | Outcome for an unmigrated policy |
|---|---|
| `StringEquals` (bare positive) | Never matches; an `Allow` stops granting, a `Deny` stops blocking |
| `StringNotEquals` (bare negated) | Always matches on the absent single-valued key; an `Allow` grants, a `Deny` blocks |
| `StringEqualsIfExists` | Passes (the key is absent), so the statement applies on the key alone |
| `ForAllValues:StringEquals` | Vacuously true on the absent key, so the statement applies on the key alone |
| `ForAnyValue:StringEquals` | False on the absent key, so an `Allow` does not grant |
| `Null` with value `true` | Matches (the key is absent) |
| `Null` with value `false` | Does not match (the key is absent) |
| `${dynamodb:ResourceTag/env}` policy variable in a value | Does not expand; the literal text `${dynamodb:ResourceTag/env}` is compared, which no request value equals |

To get the intended behaviour, write the condition on `aws:ResourceTag/<key>`.

**Sessions issued before the migration.** Session policies are stored in the catalog, so `migrate` rewrites them along with the identity policies. A session issued before the upgrade is evaluated with its rewritten policy once `migrate --yes` has run, and it does not need to be issued again. The new binary does not serve an unmigrated catalog, so no request is evaluated against the old policy text under the new rules.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
