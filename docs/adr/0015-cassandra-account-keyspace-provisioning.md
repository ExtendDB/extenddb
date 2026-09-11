# ADR-0005: Account Keyspace Provisioning Timing

- Status: Accepted
- Date: 2026-06-05
- Deciders: ExtendDB Cassandra plugin contributors

## Context

The Cassandra backend uses a keyspace-per-account isolation model ([cassandra-key-decisions.md](../../notes/cassandra-key-decisions.md)). Each ExtendDB account gets its own Cassandra keyspace (e.g., `extenddb_account_123456789012`) to provide operational benefits: storage isolation, noisy neighbor protection, per-account monitoring, independent backups, and configurable replication strategies.

The PostgreSQL backend uses a single database with all accounts sharing the same schema. Account creation in PostgreSQL is a simple catalog INSERT with no storage initialization. This raises the question: when should Cassandra create the account-specific keyspace?

Two natural timing points exist:
1. **During account creation** — Create keyspace in `ManagementStore::create_account()`
2. **On first table creation** — Lazy keyspace creation in `TableEngine::create_table()`

Existing code in `create_table_impl` validates that the account keyspace exists and returns an error if it doesn't, requiring an explicit choice.

## Options Considered

1. **Keyspace creation during account creation** — `ManagementStore::create_account_impl()` creates both the catalog entry and the account keyspace in a single operation.

2. **Lazy keyspace creation on first table** — `TableEngine::create_table_impl()` checks if the account keyspace exists and creates it if missing (idempotent).

3. **Manual provisioning** — Require operators to explicitly create account keyspaces via separate API or CLI command before creating tables.

## Decision

Create account keyspace during account creation (`ManagementStore::create_account_impl`).

## Rationale

- The operational benefits of per-account keyspaces (monitoring, backups, replication strategy, storage tiering) require the keyspace to exist as part of complete account provisioning. An account without its keyspace is incomplete from an operational perspective.

- Account creation is already an administrative control plane operation with relaxed latency requirements (seconds are acceptable). Keyspace creation overhead (schema agreement across cluster) fits naturally here.

- Matches the architectural intent: The keyspace-per-account design was chosen specifically for operational isolation. Deferring keyspace creation until first table use would delay achieving this isolation.

- Simplifies table operations: `create_table` can assume the account keyspace exists, keeping data plane operations fast and simple. The existing validation in `create_table_impl` catches operational misconfigurations early.

- Consistent with "account provisioning" semantics: Creating an account in a multi-tenant system includes allocating the account's isolated storage namespace, not just catalog metadata.

- Lazy creation would hide keyspace creation costs in the critical path of the first `CreateTable` API call, causing unexpected latency spikes for end users.

## Consequences

- `create_account_impl()` must call `ensure_keyspace()` after successfully inserting the account into the catalog. Keyspace creation failure requires rollback of the account catalog entry.

- Account creation latency increases by the keyspace creation time (~100-500ms for local cluster, up to several seconds for geo-distributed clusters with schema agreement).

- Empty accounts (created but never used) consume minimal resources: a keyspace with no tables has negligible overhead in Cassandra.

- Keyspace creation is idempotent (`CREATE KEYSPACE IF NOT EXISTS`), making retry logic simple and safe.

- Account deletion must include `DROP KEYSPACE` to fully clean up account resources.

- Operators can configure per-account replication strategies at account creation time, enabling immediate operational controls.

- Testing simplified: Test fixtures can create accounts and immediately verify keyspace existence without creating tables first.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
