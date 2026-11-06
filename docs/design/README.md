# extenddb — Architecture & Design

This directory contains the authoritative design documents for ExtendDB (extenddb). All implementation work references these docs.

## Document Index

| #  | Document | Scope |
|----|----------|-------|
| 01 | [Requirements](01-requirements.md) | Wire protocol, operations, limits, data types, non-functional requirements |
| 02 | [High-Level Design](02-high-level-design.md) | Architecture overview, crate structure, request lifecycle, key design decisions, technology choices |
| 03 | [Core](03-component-core.md) | Types, expression engine, validation, capacity calculation, errors (`extenddb-core` crate) |
| 04 | [Storage](04-component-storage.md) | StorageEngine traits, input/output types, PostgreSQL backend, schema design, pagination, GSI consistency (`extenddb-storage`, `extenddb-storage-postgres` crates) |
| 05 | [Auth](05-component-auth.md) | AuthProvider trait, SigV4 validation, IAM policy engine, credential encryption (`extenddb-auth` crate) |
| 06 | [Server](06-component-server.md) | HTTP server, routing, middleware pipeline, response formatting, TLS, rate limiting, throughput tracking (`extenddb-server` crate) |
| 07 | [Streams](07-component-streams.md) | DynamoDB Streams design space — capture mechanism, shard management, retention (high-level; detailed design deferred) |
| 08 | [Configuration](08-component-config.md) | TOML config, env vars, CLI, logging, metrics, health checks, deployment (VM, Kubernetes, Docker) |
| 09 | [Testing](09-testing.md) | Test strategy, reference suites, golden files, multi-language test suites, coverage tracking |
| 10 | [Licenses](10-dependency-licenses.md) | Licensing summary for third-party dependencies |
| 11 | [Scaling](11-high-availability.md) | DRAFT document on scaling ExtendDB |
| 12 | [Auth / Authz Cache](12-auth-authz-cache.md) | In-process cache for IAM data (credentials, policies, tags, table key info); companion to 05 and 06 |
| 13 | [MongoDB Storage](13-storage-mongodb.md) | MongoDB storage backend: engine and catalog traits over the official driver, replica-set transactions |
| 15 | [Backup and Restore](15-backup-restore.md) | Overview of the off-host backup series: the invariants, the per-operation contracts and a short section per later PR; the format, store and snapshot documentation lives with their crates (`crates/core/src/backup_format`, `crates/backup-store`, `crates/storage/src/snapshot.rs`) |
| 15b | [Backup memory and duration record, PR 8](15-backup-restore-bench-pr8.md) | The measurements section 7 and revision item 31 of the design cite: peak RSS and duration of the 100k, 1M, worst-expansion and incompressible backups per backend, the allocator series, the PostgreSQL page's cost |
| 15c | [Restore memory and duration record, PR 9](15-backup-restore-bench-pr9.md) | The measurements section 7 and revision item 33 of the design cite for the restore: peak RSS and duration of the 100k, 1M, worst-expansion and large-item restores on SQLite and PostgreSQL, with and without one allocator arena, the terms the code bounds and the sizing sentence |
| 15d | [Backup and restore record, PR 11](15-backup-restore-bench-pr11.md) | The measurements the testing summary cites for the MongoDB, Cassandra and vector cells: peak RSS and duration of the 100k and 1M restores on MongoDB and Cassandra and of the backup and restore of 100k- and 1M-item tables with a 128-dimension vector index on PostgreSQL and SQLite, with and without one allocator arena; the 100k and 1M MongoDB backups; later runs that attribute SQLite's restore memory to the write batch, which grows with the item's size whatever the attribute types (the same items with no vector index, items of a 2,290-character string, 20,000 items of 1,536 numbers, two restores at once, the peak's spread over nine runs), give the PostgreSQL vector index's share from five same-harness pairs and MongoDB's restore duration with the backfill worker's cycle; SQLite's WAL growth beside a backup with and without two GSIs; and the figures measured for the backup manual (PostgreSQL dead row versions and SQLite WAL growth during a backup) with their reproductions |


## How to Use These Docs

- **Before implementing a feature:** read the relevant component doc and the requirements doc for the operation.
- **Requirement IDs** (e.g., `REQ-WIRE-001`, `REQ-DATA-005`) are stable references — use them in code comments and commit messages.
- **Deferred items** are explicitly marked. Do not implement deferred features without updating the design doc first.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
