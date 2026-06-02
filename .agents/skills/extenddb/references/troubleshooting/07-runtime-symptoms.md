# Runtime Symptoms

This file holds verbatim Cause and Fix entries for runtime performance and eventual-consistency symptoms that appear while the extenddb server is running. Entries are copied from `docs/troubleshooting.md` and the "Source" line at the end of each entry records the section and last sync date for drift detection.

## Connection pool exhausted

### HTTP 500 on all requests under heavy load

<a name="connection-pool-exhausted"></a>

**Error text:**
```
HTTP 500 on all requests under heavy load
```

**Cause:** The active storage backend connection pool is exhausted. All connections are in use and new requests cannot acquire a connection within the timeout. extenddb currently returns HTTP 500 (Internal Server Error) instead of the more appropriate 503 (Service Unavailable).

**Fix:** Increase the pool size in `extenddb.toml`:
```toml
[storage.tidb]
pool_size = 50
catalog_pool_size = 50

# PostgreSQL alternate backend:
[storage.postgres]
pool_size = 50
```

For TiDB, each frontend opens strong-data, default-read-data, engine-catalog, and catalog-store/auth pools. If the problem persists, inspect TiDB sessions, slow queries, DDL jobs, and Resource Control throttling with TiDB's cluster diagnostics. PostgreSQL alternate deployments should inspect `pg_stat_activity`.

**Known limitation:** The HTTP status code should be 503 with a `Retry-After` header. This is tracked as technical debt.

**Source:** `docs/troubleshooting.md`, section "Connection Pool Exhaustion", last synced 2026-05-12.

## Streams capture delay

### `Stream capture: failed to assign shard for <table>: <error>`

<a name="streams-capture-delay"></a>

**Error text:**
```
Stream capture: failed to assign shard for <table>: <error>
```

**Cause:** After a successful write (PutItem, DeleteItem, UpdateItem), extenddb tried to capture a stream record but could not determine which shard to assign it to. The data write succeeded — only the stream record is missing.

**Fix:** Check storage backend connectivity and stream metadata. TiDB stores stream records in the shared data database table and uses native TTL for retention. PostgreSQL alternate deployments should also verify the table's stream shard metadata.

**Source:** `docs/troubleshooting.md`, section "DynamoDB Streams", last synced 2026-05-12.

### `Stream capture: failed to write record for <table>: <error>`

**Error text:**
```
Stream capture: failed to write record for <table>: <error>
```

**Cause:** A stream record was constructed but could not be persisted to the `stream_records` table. The data write succeeded — only the stream record is missing.

**Fix:** Check storage backend connectivity and disk space. For TiDB, inspect the data database `stream_records` table and TiDB slow/query diagnostics. PostgreSQL alternate deployments should also check for backend-specific unique constraint errors.

**Source:** `docs/troubleshooting.md`, section "DynamoDB Streams", last synced 2026-05-12.

### `Stream capture: failed to get sequence number: <error>`

**Error text:**
```
Stream capture: failed to get sequence number: <error>
```

**Cause:** extenddb could not generate a sequence number for a stream record. The data write succeeded — only the stream record is missing.

**Fix:** Check PostgreSQL connectivity.

**Source:** `docs/troubleshooting.md`, section "DynamoDB Streams", last synced 2026-05-12.

### `Stream cleanup worker: <error>`

**Error text:**
```
Stream cleanup worker: <error>
```

**Cause:** The background worker that deletes stream records older than 24 hours encountered a database error. Expired records will accumulate until the worker succeeds.

**Fix:** Check PostgreSQL connectivity. The worker retries every hour automatically.

**Source:** `docs/troubleshooting.md`, section "DynamoDB Streams", last synced 2026-05-12.

## GSI propagation delay

### GSI query returns stale data after a write

<a name="gsi-propagation-delay"></a>

**Error text:**
```
GSI query returns stale data after a write
```

**Cause:** GSI updates are applied asynchronously with a configurable propagation delay (default 10ms). This matches real DynamoDB's eventually consistent GSI behavior. Each GSI can have its own `propagation_delay_ms` setting; the system-wide default is controlled by the `gsi_propagation_delay_ms` runtime setting.

**Fix:** This is expected behavior. For tests that query GSIs after writes, poll/retry the GSI query until the expected data appears. To make all GSIs synchronous for testing, set `extenddb settings set gsi_propagation_delay_ms 0`. For production-like testing, keep the default async delay.

**Source:** `docs/troubleshooting.md`, section "GSI Async Update Behavior", last synced 2026-05-12.
