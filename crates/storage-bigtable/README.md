# extenddb-storage-bigtable

Google Cloud Bigtable storage backend for [ExtendDB](https://github.com/ExtendDB/extenddb), an in-tree workspace crate selected by Cargo feature.

> **Experimental.** The Cloud Bigtable backend is in the **Experimental** tier (`README.md`) and is intended for evaluation rather than production use. Behavior and configuration may change across releases without a deprecation period.

## Design

Maps ExtendDB's storage, catalog, IAM, stream, and TTL traits onto Cloud Bigtable's wide-column, single-row-atomic data model:

- **Single-row OCC & multi-row 2PC**: Single-item writes execute guarded `CheckAndMutateRow` CAS with deterministic millisecond-aligned cell timestamps (`occ_version * 1_000`) derived from `m:v`. Multi-item `TransactWriteItems`, `> 90,000`-mutation writes, and stream-capturing single-item writes coordinate through `__extenddb_txn_log__` using sorted intent locking (`m:intent:<txn_id>`), incarnation-fenced CAS commits, cooperative `COMMITTED` intent resolution across `GetItem`, `Query`, `Scan` (read-committed across rows), and `TransactGetItems` (double-read snapshot validation).
- **Ordered secondary-index shadow tables**: Each GSI (`t<id>_g<hash>`) and LSI (`t<id>_l<hash>`) uses a dedicated Bigtable shadow table keyed by 32-byte SHA-256 partition digests and escape-encoded `RANGE` segments (`<= 4,096` bytes), maintaining LSIs inline on the write path (with `ensure_lsi_read_after_write` flushing queued recoveries and rolling forward `COMMITTED`/`post-*` entries before LSI reads, plus `sweeper` replay across transient blips) and supporting multi-attribute `HASH`/`RANGE` queries on GSIs.
- **Catalog & stream ledger**: Account, table (`table_id_index`), IAM, encryption-key ring (`SecretKeys`), operational metrics, server registrations, migration locks, and 4-shard DynamoDB Streams records live in `__extenddb_catalog__` (with separate data-instance support via `data_instance_id` and `__extenddb_data_ledger__`). System table names are fixed, so one Bigtable instance (or catalog/data instance pair) holds one catalog.
- **Sharded TTL & tombstone reclamation**: `__extenddb_ttl_index__` partitions `<= 4,096`-byte `table_id`-tagged expiration entries across 16 shards, backfills pre-existing items while reporting `ENABLING`, preserves due entries while TTL is `Disabled`/`Enabling`/`Disabling` while lazily retiring orphaned entries from dropped or recreated tables (`table_id` mismatch) as they come due, and queues `tomb:` entries so `sweeper` reclaims version-only deleted rows after the `intent_timeout_secs` (60 s default) grace window without starving under continuous traffic.

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

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) and [NOTICE](https://github.com/ExtendDB/extenddb/blob/main/NOTICE).
