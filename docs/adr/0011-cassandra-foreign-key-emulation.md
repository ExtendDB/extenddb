# ADR-0001: Foreign Key Constraint Emulation

- Status: Accepted
- Date: 2026-06-04
- Deciders: ExtendDB Cassandra plugin contributors

## Context

The PostgreSQL storage implementation relies on database-enforced foreign key constraints to maintain referential integrity in the IAM catalog. For example, creating an IAM user requires a valid account_id, and the PostgreSQL schema declares a foreign key from `iam_users.account_id` to `accounts.account_id`. The database automatically rejects any insert that violates this constraint.

Apache Cassandra does not support foreign key constraints. The database will accept any write to any table regardless of whether referenced parent entities exist. This creates a risk of orphaned records and data inconsistencies if not handled explicitly in application code.

## Options Considered

1. **Application-layer pre-checks** — Before inserting a child entity, execute a SELECT query to verify the parent entity exists. Return `OpError::NotFound` if the parent is missing, matching PostgreSQL FK violation behavior.

2. **Eventual consistency checks** — Allow writes to proceed without validation, then run periodic background jobs to detect and repair orphaned records.

3. **Denormalization** — Embed parent entity data in child records to eliminate the need for referential integrity checks.

## Decision

Application-layer pre-checks for all child entity creation operations.

## Rationale

- Matches PostgreSQL behavior exactly: Operations fail fast with `OpError::NotFound` when the parent entity is missing, providing consistent error semantics across backends.
- IAM operations are infrequent and admin-driven. The extra SELECT overhead (typically <10ms in a local datacenter) is negligible compared to the operation's overall latency budget.
- Eventual consistency checks would allow invalid states to persist temporarily, violating ExtendDB's expectation that IAM operations are immediately consistent.
- Denormalization would break the normalized schema design shared with PostgreSQL and complicate updates when parent entities change.
- Idiomatic Cassandra practice: Pre-checks are the standard pattern for emulating constraints in Cassandra applications (see DataStax documentation on modeling best practices).

## Consequences

- Every child entity creation (users, access keys, policy attachments) includes a pre-check SELECT before the main write.
- Helper methods `account_exists()` and `user_exists()` centralize this pattern in `CassandraCatalogStore`.
- Small race condition window: A parent entity could be deleted between the pre-check and the child insert. This is acceptable because:
  - IAM deletions are rare
  - The race window is milliseconds
  - Worst case is an orphaned record, which can be cleaned up via account deletion
- Code duplication: Each foreign key relationship requires explicit pre-check logic, unlike PostgreSQL where the database handles this declaratively.
- Performance impact is minimal: Pre-checks use indexed queries on primary keys, which Cassandra serves from a single replica with sub-10ms latency.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
