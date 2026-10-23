# ADR-0006: Index Data Table PRIMARY KEY Structure

- Status: Accepted
- Date: 2026-06-16
- Deciders: ExtendDB Cassandra plugin contributors

## Context

Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI) require separate physical data tables to store indexed items with alternate keys. The PostgreSQL backend uses a PRIMARY KEY constraint of `(pk, base_pk, base_sk_*)` where all columns are part of a composite uniqueness constraint. Cassandra's PRIMARY KEY serves a fundamentally different purpose: it defines both the partition key (for data distribution) and clustering keys (for ordering within a partition). A direct translation of the PostgreSQL schema results in incorrect data distribution and query patterns.

The index table stores:
- `pk` — index partition key (computed from index key attributes)
- `sk_*` — index sort keys (e.g., `sk_s`, `sk_n`, `sk_b` for different types)
- `base_pk` — base table partition key (for uniqueness)
- `base_sk_*` — base table sort keys (for uniqueness)
- `item_data` — projected item attributes (JSON)

DynamoDB semantics require that:
1. Queries against an index use the index keys (not base table keys)
2. Index keys are not unique — multiple base items can have the same index keys
3. Each base item appears at most once in the index

## Options Considered

1. **PostgreSQL-style ordering: `PRIMARY KEY (pk, base_pk, base_sk_*)`** — Partition by index PK, cluster by base keys only, with index sort keys as regular columns.

2. **Cassandra-optimized ordering: `PRIMARY KEY ((pk), sk_*, base_pk, base_sk_*)`** — Partition by index PK, cluster first by index sort keys, then by base keys.

3. **Composite partition key: `PRIMARY KEY ((pk, base_pk), sk_*, base_sk_*)`** — Include base PK in partition key for uniqueness.

## Decision

Cassandra-optimized ordering: `PRIMARY KEY ((pk), sk_*, base_pk, base_sk_*)`

## Rationale

- **Enables efficient range queries**: DynamoDB GSI queries with `KeyConditionExpression` like `pk = 'value' AND sk BETWEEN 'a' AND 'z'` require that index sort keys be clustering keys. Cassandra's clustering keys provide ordered storage within a partition, making range scans efficient.

- **Matches DynamoDB query semantics**: Index queries specify index keys (not base keys) in the WHERE clause. With index sort keys as clustering columns, Cassandra can use them directly in queries without requiring secondary indexes or ALLOW FILTERING.

- **Preserves uniqueness**: Base table keys (`base_pk`, `base_sk_*`) as trailing clustering keys ensure that each base item appears at most once, even when multiple items have identical index keys.

- **Correct data distribution**: Partition key `(pk)` distributes index data by index partition key, matching DynamoDB's behavior where items with the same GSI partition key are co-located.

- **Option 1 fails for queries**: Placing index sort keys as regular columns prevents using them in WHERE clauses for ordering, forcing full partition scans or expensive secondary indexes.

- **Option 3 breaks distribution**: Including `base_pk` in the partition key distributes data by `(pk, base_pk)` combination, destroying co-location of items with the same index key and making range queries impossible.

## Consequences

- **Code divergence from PostgreSQL**: The PRIMARY KEY structure differs between backends. DELETE statements in Cassandra must include ALL PRIMARY KEY columns (index keys + base keys), while PostgreSQL DELETE uses only base keys (relying on the uniqueness constraint). This requires Cassandra-specific implementations of `delete_index_row_multi` that include index key parameters.

- **Query compatibility**: Index queries work naturally in Cassandra using `WHERE pk = ? AND sk_s > ?` patterns, matching DynamoDB's KeyConditionExpression semantics.

- **Write path complexity**: Index maintenance functions (`sync_indexes`, `delete_index_row_multi`, `insert_index_row_multi`) must be aware of the key ordering difference. The Cassandra version needs both index keys and base keys for DELETE operations.

- **Testing challenges**: Integration tests must verify PRIMARY KEY structure explicitly by querying Cassandra system schema tables, as incorrect key ordering produces runtime errors ("Some partition key parts are missing") rather than compile-time failures.

- **Hash-only indexes**: For indexes with no sort key, PRIMARY KEY becomes `((pk), base_pk, base_sk_*)`. The double parentheses `((pk))` syntax is required to distinguish the partition key from clustering keys.

- **Documentation burden**: The difference in PRIMARY KEY structure must be clearly documented in code comments and architecture documents to prevent confusion when comparing implementations.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
