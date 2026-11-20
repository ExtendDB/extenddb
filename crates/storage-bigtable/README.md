# extenddb-storage-bigtable

Google Cloud Bigtable storage backend for [ExtendDB](https://github.com/ExtendDB/extenddb), an in-tree workspace crate selected by Cargo feature.

> **Experimental.** The Cloud Bigtable backend is in the **Experimental** tier (`README.md`) and is intended for evaluation rather than production use. Behavior and configuration may change across releases without a deprecation period.

## Design

Maps ExtendDB's storage, catalog, IAM, stream, and TTL traits onto Cloud Bigtable's wide-column, single-row-atomic data model:

- **Single-row OCC & multi-row 2PC**: Single-item writes execute guarded `CheckAndMutateRow` CAS with deterministic millisecond-aligned cell timestamps (`occ_version * 1_000`) derived from `m:v`. Multi-item `TransactWriteItems` and stream-capturing single-item writes coordinate through `__extenddb_txn_log__` using sorted intent locking (`m:intent:<txn_id>`), incarnation-fenced CAS commits, cooperative read-through on `GetItem` and double-read snapshot validation on `TransactGetItems`.
- **Ordered secondary-index shadow tables**: Each GSI (`t<id>_g<hash>`) and LSI (`t<id>_l<hash>`) uses a dedicated Bigtable shadow table keyed by 32-byte SHA-256 partition digests and escape-encoded `RANGE` segments (`<= 4,096` bytes), preserving `ConsistentRead = true` on LSIs and multi-attribute `HASH`/`RANGE` queries on GSIs.
- **Catalog & stream ledger**: Account, table, IAM, encryption-key ring (`SecretKeys`), operational metrics, server registrations, migration locks, and 4-shard DynamoDB Streams records live in `__extenddb_catalog__` (with separate data-instance support via `data_instance_id` and `__extenddb_data_ledger__`).
- **Sharded TTL & tombstone reclamation**: `__extenddb_ttl_index__` partitions expiration entries across 16 shards, backfills pre-existing items while reporting `ENABLING`, and queues `tomb:` entries so `sweeper` reclaims version-only deleted rows after the `intent_timeout_secs` (60 s default) grace window.

## Building

Backends are mutually exclusive; disable the default `postgres` feature when building `extenddb` for Bigtable:

```bash
cargo build --release -p extenddb --no-default-features --features bigtable
```

## Configuration

```toml
[storage]
backend = "bigtable"

[storage.bigtable]
project_id = "my-gcp-project"
instance_id = "my-bigtable-instance"
app_profile_id = "single-cluster-tx" # Single-cluster routing with single-row transactions enabled
```

For local development against `cbtemulator`, IAM requirements, split catalog/data instances (`data_instance_id`), and known limitations, see [Bigtable Setup](https://github.com/ExtendDB/extenddb/blob/main/docs/bigtable-setup.md) and [Differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
