# Google Cloud Bigtable Setup

> See [NOTICE](NOTICE.md) for important disclaimers.

> **Experimental.** The Google Cloud Bigtable backend is experimental (`README.md`) and is not for production use. Its behavior or configuration may change across releases without a deprecation period.

## Prerequisites

- Rust 1.91.1 or later
- For local development: the Google Cloud SDK `cbtemulator` binary (`gcloud components install bigtable`) or Docker (`gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators`)
- For Google Cloud deployments:
  - A Cloud Bigtable instance (and optional second instance when separating catalog and data via `data_instance_id`)
  - An **application profile configured with single-cluster routing and single-row transactions enabled** (`allow_transactional_writes = true`)

## Building with Bigtable Support

Storage backends are mutually exclusive at compile time. Disable the default `postgres` feature and enable `bigtable`:

```bash
cargo build --release --no-default-features --features bigtable
```

Verify the compiled backend:

```bash
./target/release/extenddb version
# extenddb 1.0.0 (bigtable)
```

## Local Setup (`cbtemulator`)

1. Start `cbtemulator` bound to loopback only:

   ```bash
   # Using an installed Google Cloud SDK cbtemulator binary:
   cbtemulator -host=127.0.0.1 -port=8086
   ```

   Or with Docker:

   ```bash
   docker run -d --name extenddb-cbtemulator \
       -p 127.0.0.1:8086:8086 \
       gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
       gcloud beta emulators bigtable start --host-port=0.0.0.0:8086
   ```

   > **Authentication warning.** When `emulator_host` is configured in `extenddb.toml` or `BIGTABLE_EMULATOR_HOST` is set in the environment, `extenddb` connects over plaintext gRPC without Google Cloud authentication and emits a startup warning. Keep emulator endpoints restricted to loopback in local development and CI.

2. Write a minimal `extenddb.toml` (`chmod 600 extenddb.toml`):

   ```toml
   [storage]
   backend = "bigtable"

   [storage.bigtable]
   project_id = "local-dev"
   instance_id = "local-dev"
   emulator_host = "127.0.0.1:8086"
   ```

3. Initialize the catalog, data ledger, TLS certificates, default account, and admin user:

   ```bash
   ./target/release/extenddb init --backend bigtable --config extenddb.toml --overwrite
   ```

4. Start the server:

   ```bash
   ./target/release/extenddb serve --foreground --config extenddb.toml
   ```

To run the automated emulator test harness end to end:

```bash
devtools/run-bigtable-tests -- --pytest --comprehensive
```

## Google Cloud Deployment Requirements

### Application Profile (Single-Cluster Routing Required)

ExtendDB relies on Cloud Bigtable `CheckAndMutateRow` for optimistic concurrency control, 2PC intent locking, and catalog compare-and-swap mutations. Cloud Bigtable supports single-row transactions (`CheckAndMutateRow`) **only on application profiles that use single-cluster routing with single-row transactions enabled** (`SingleClusterRouting` with `allow_transactional_writes = true`). Multi-cluster routing profiles do not offer single-row transactions and reject `CheckAndMutateRow` with `FAILED_PRECONDITION`.

Create a dedicated single-cluster application profile:

```bash
gcloud bigtable app-profiles create extenddb-single-cluster \
    --project=my-gcp-project \
    --instance=my-bigtable-instance \
    --route-to=my-primary-cluster \
    --transactional-writes \
    --description="ExtendDB single-cluster transactional routing"
```

When connecting to Google Cloud Bigtable (no emulator active), `extenddb serve` calls `GetAppProfile` on the configured `app_profile_id` (or `"default"` when unset) at startup and refuses to start if the profile uses multi-cluster routing or disables single-row transactions.

### IAM Permissions

ExtendDB authenticates using `credentials_path` (a service-account JSON key file) when set, or Application Default Credentials (ADC) otherwise. Grant the service account permissions on the target instance(s) covering:

- **Data plane**: `bigtable.tables.readRows`, `bigtable.tables.mutateRows`, `bigtable.tables.checkAndMutateRow`
- **Table lifecycle (`init`, `migrate`, `CreateTable`, `UpdateTable`, `DeleteTable`, `destroy`)**: `bigtable.tables.create`, `bigtable.tables.update`, `bigtable.tables.delete`, `bigtable.tables.get`, `bigtable.tables.list`
- **Startup app-profile validation**: `bigtable.appProfiles.get`

Predefined role `roles/bigtable.admin` (scoped to the Bigtable instance) includes all of the above.

### Configuration Example

```toml
[server]
bind_addr = "127.0.0.1"
port = 18443
region = "us-east-1"

[server.tls]
cert_path = "/etc/extenddb/tls/cert.pem"
key_path = "/etc/extenddb/tls/key.pem"

[storage]
backend = "bigtable"

[storage.bigtable]
project_id = "my-gcp-project"
instance_id = "my-catalog-instance"
data_instance_id = "my-data-instance" # Optional: isolate data, index, txn-log, and TTL tables
app_profile_id = "extenddb-single-cluster"
credentials_path = "/etc/extenddb/service-account.json"
pool_size = 20
catalog_pool_size = 8

[auth]
provider = "builtin"
```

### Migrations and Teardown

- **Schema & ledger initialization / upgrades**: `extenddb init` and `extenddb migrate --yes` acquire and renew the `lock:catalog_migrations` lease, verify no serving instances hold active `server_registration:*` leases, provision `__extenddb_catalog__` on `instance_id`, and provision `__extenddb_data_ledger__`, `__extenddb_txn_log__`, and `__extenddb_ttl_index__` on `data_instance_id` (or `instance_id` when shared).
- **Safe teardown (`destroy`)**: `extenddb destroy` deletes only ExtendDB's four system tables (`__extenddb_catalog__`, `__extenddb_data_ledger__`, `__extenddb_txn_log__`, `__extenddb_ttl_index__`), tables recorded in the catalog, and orphaned physical tables matching ExtendDB's exact hex naming pattern (`t<16hex>`, `t<16hex>_g<8hex>`, `t<16hex>_l<8hex>`). Non-ExtendDB tables sharing the Bigtable instance are left untouched.

## Known Limitations

- **Experimental tier**: Not supported for production workloads (`README.md`).
- **One catalog per Bigtable instance**: ExtendDB's four system tables (`__extenddb_catalog__` on `instance_id`, and `__extenddb_data_ledger__`, `__extenddb_txn_log__`, and `__extenddb_ttl_index__` on `data_instance_id` or `instance_id`) use fixed names, so one Bigtable instance (or `instance_id` / `data_instance_id` pair) holds one ExtendDB catalog. Run one `extenddb serve` process per catalog.
- **On-demand backups (`CreateBackup`)**: Unsupported (`ValidationException: CreateBackup is not supported by this storage backend`). `ListBackups` returns an empty list, and `DescribeBackup`, `DeleteBackup`, and `RestoreTableFromBackup` return `BackupNotFoundException`. Use Cloud Bigtable's native cluster backups for disaster recovery.
- **Point-in-time export (`ExportTableToPointInTime`)**: As on the other non-MVCC backends, exports run as a synchronous paged `Scan` of the live table without a point-in-time snapshot.
- **Cross-row `Query`/`Scan` snapshot & post-commit index replay**: `Query` and `Scan` resolve `COMMITTED` 2PC intents per row (read-committed across rows rather than a single cross-row snapshot). Secondary-index shadow tables are updated inline immediately after the base-row commit; if a transient Bigtable error interrupts inline post-commit shadow or TTL maintenance, `post-*` recovery records (buffered in memory if `__extenddb_txn_log__` is momentarily unreachable) are flushed and rolled forward before LSI `Query`/`Scan` calls (`ensure_lsi_read_after_write`) and replayed asynchronously by `sweeper` (meaning an LSI `ConsistentRead = true` read can observe pre-write state only if the LSI shadow table remains unreachable during the read or `__extenddb_txn_log__` was unreachable on a peer node).
- **Multi-`RANGE` GSI row-key ceiling (`4,096` bytes)**: Single-`RANGE` GSIs and LSIs always fit within `3,276` bytes; on a GSI with `2..=4` `RANGE` attributes, writes whose combined escape-encoded `RANGE` values exceed Cloud Bigtable's `4,096`-byte row-key limit are rejected before the base write commits with `ValidationException`.
- **Vector search**: `VectorIndexes` and `SearchVectors` are not supported.
- **Table size statistics**: `DescribeTable` always reports `ItemCount: 0` and `TableSizeBytes: 0` because Cloud Bigtable exposes no per-table row count or logical item-byte total without scanning every row.
