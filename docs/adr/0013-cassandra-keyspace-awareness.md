# ADR-0003: Dynamic Keyspace Construction

- Status: Accepted
- Date: 2026-06-04
- Deciders: ExtendDB Cassandra plugin contributors

## Context

PostgreSQL and Cassandra handle schema namespacing differently:

- **PostgreSQL**: Database selection happens at connection time via the connection pool URL. SQL queries reference tables without database prefix (`SELECT * FROM accounts`). The application configures which database to use externally to the SQL language.

- **Cassandra**: Keyspaces are part of the CQL language. Every query must explicitly specify the keyspace (`SELECT * FROM extenddb_catalog.accounts`). There is no implicit "current keyspace" selection in the connection.

Early implementations hardcoded keyspace names like `extenddb_catalog` directly in query strings. This breaks configurability: test environments, multi-tenant deployments, and custom installations cannot override the keyspace prefix.

## Options Considered

1. **Dynamic keyspace construction** — Store `keyspace_prefix` in config, construct keyspace names dynamically via helper methods like `catalog_keyspace()`. Pass keyspace prefix through initialization chain.

2. **USE statement per connection** — Execute `USE extenddb_catalog` on each session, then write queries without keyspace prefix. Mimics PostgreSQL behavior.

3. **Query-time keyspace override** — Allow each query to specify keyspace as a parameter, defaulting to config value.

## Decision

Dynamic keyspace construction with JDBC-style connection strings.

## Rationale

- CQL `USE` statement is fragile: Session pooling and reconnection logic can lose the `USE` context. Cassandra best practice is to always qualify table names with keyspace.
- Explicit keyspace in every query prevents ambiguity: No hidden state. Code review can verify which keyspace is being queried.
- JDBC-style connection strings are familiar: `host1:9042,host2:9042/keyspace_prefix` matches developer expectations from other database drivers.
- Centralized helper methods: `catalog_keyspace()` and `data_keyspace()` ensure consistent keyspace name construction. Changes to naming conventions require updates in only one place.
- Testability: Test fixtures can instantiate storage with custom keyspace prefixes, enabling parallel test execution without keyspace collisions.
- PostgreSQL compatibility: Config structure remains similar. Both backends have a connection string and a schema/keyspace namespace concept.

## Consequences

- Config structure includes `keyspace_prefix` field (default: `"extenddb"`).
- Connection string format: `host1:9042,host2:9042/keyspace_prefix`. Parser splits on `/` to extract hosts and keyspace.
- All query strings use helper methods:
  ```rust
  let query = format!(
      "SELECT * FROM {}.accounts WHERE account_id = ?",
      self.catalog_keyspace()
  );
  ```
- Helper methods added to `CassandraCatalogStore`:
  - `catalog_keyspace()` returns `"{prefix}_catalog"`
  - `data_keyspace(account_id)` returns `"{prefix}_data_{account_id}"`
- Migration scripts must support variable keyspace prefixes: Use environment variable substitution or templating.
- No hardcoded keyspace names anywhere in code: Violations break configurability and are caught in code review.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
