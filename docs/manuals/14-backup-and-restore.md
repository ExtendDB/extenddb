# Backup and Restore

> See [NOTICE](../NOTICE.md) for important disclaimers.

A filesystem backup store keeps every backup in the same failure domain as the host that holds it, so S3 is
the production recommendation: a lost host or disk loses a filesystem store's backups with it unless
something copies the root off the host, while an S3 bucket (or an S3-compatible server on other hardware)
survives the loss of the ExtendDB host and of its database.

This manual is for operators who plan, run and test backups. The admin guide is the reference for every
configuration key and command named here; the
[differences document](../differences-from-dynamodb.md#backup-and-restore) records how each backup operation
compares with Amazon DynamoDB's.

## Strategy

What a table backup is. `CreateBackup` takes one table's items and definition at one instant and writes
them to the configured backup store, the `[backup.store]` section: a directory (`kind = "filesystem"`) or
an S3 bucket and prefix (`kind = "s3"`). The request answers `CREATING` and a job of the server uploads the
backup; it becomes `AVAILABLE` when the upload ends. The backup is a consistent snapshot of the table on
PostgreSQL, SQLite and MongoDB. The catalog keeps a row per backup as the index that `ListBackups` and
`DescribeBackup` read, and the store holds the bytes, so a lost catalog loses the index, not the backups
([Recovering from catalog loss](#recovering-from-catalog-loss)).

Cassandra has no table snapshot. `CreateBackup` there writes the in-catalog format (inside the Cassandra
cluster, in the catalog keyspace and the account's keyspace) whatever the store, and that copy pages the live
table, so it is not point-in-time. A configured store serves restores into Cassandra of backups that another
backend wrote to the store. For off-host copies of a Cassandra deployment use the cluster's own snapshot
tooling.

What a table backup is not:

- It does not hold the catalog: accounts, IAM users, groups, roles, policies, access keys, runtime settings
  and the encryption key live in the catalog database, which no table backup contains. Back the catalog up
  with the database's own tools (the admin guide's [Data Recovery](05-admin-guide.md#data-recovery) entry
  names the databases and gives the PostgreSQL commands), and keep that copy off the host as well. On
  PostgreSQL the catalog and data databases are one copy: dump both with the server stopped, or take a base
  backup of the PostgreSQL server that holds both. A catalog backup holds the encryption key that decrypts the
  access key secrets, and on PostgreSQL the application role's password: protect it as a secret, and create
  the dump files with `umask 077`.
- It is not scheduled. ExtendDB has no backup scheduler: run `CreateBackup` from cron or your scheduler. As a
  crontab line, where every `%` must be escaped because cron turns an unescaped `%` into a newline:

  ```
  0 * * * * AWS_CA_BUNDLE=/path/to/cert.pem AWS_DEFAULT_REGION=us-east-1 aws dynamodb create-backup --table-name orders --backup-name orders-$(date +\%Y\%m\%d\%H\%M) --endpoint-url https://<host>:18443
  ```

  or put the command, with its `%` unescaped, in a script that cron runs. `AWS_CA_BUNDLE` (or the CLI's
  `--ca-bundle`) names the self-signed certificate `init` generates, `~/.extenddb/tls/cert.pem` of the user
  who ran it; without it the CLI fails certificate verification. Cron's environment names no region;
  `AWS_DEFAULT_REGION` gives the CLI one, which it otherwise needs from the user's `~/.aws/config`.
- It has no retention. A backup stays until `DeleteBackup` removes it. Your scheduler deletes the backups it
  no longer wants.
- It is not point-in-time recovery, which ExtendDB does not provide (below).

A plan that covers host loss, database loss and operator error therefore has three parts: table backups on a
schedule to an S3 store in another failure domain, database-level backups of the catalog, and a restore drill
run on a schedule ([Restore drill](#restore-drill)).

## RPO and RTO

### Recovery point

The recovery point objective equals the interval between two backups of a table. There is no point-in-time
recovery: `RestoreTableToPointInTime` answers `PointInTimeRecoveryUnavailableException`, and
`UpdateContinuousBackups` with `PointInTimeRecoveryEnabled` true answers
`ContinuousBackupsUnavailableException`. A restore brings the table back as it was at the backup's
`BackupCreationDateTime`, the instant the request opened the table's snapshot; every write acknowledged after
that instant is lost.

Per store kind:

| Store | Loss it survives | Recovery point |
|---|---|---|
| S3, a bucket outside the ExtendDB host and the database host | the ExtendDB host, the database, the catalog | the backup interval |
| filesystem, a root on the ExtendDB host | the database and the catalog, while the host's disk survives | the backup interval for database loss; for host loss, the interval of whatever copies the root off the host |
| filesystem, a root on a shared mount | as the mount's own storage | as the mount's own storage |
| in-catalog, inside the database (Cassandra always; PostgreSQL, MongoDB and in-memory SQLite without `[backup.store]`; `kind = "disabled"`) | the loss of a table, not of the database or its host | the backup interval while the database survives; none once it is lost, which for in-memory SQLite is at every restart |

### Recovery time

The recovery time is the restore's duration plus the time to bring a server back, and, after catalog loss,
the time to rebuild the catalog and run `extenddb backup sync`. A restore answers `CREATING` at once and the
table accepts reads and writes when it reports `ACTIVE`; every index, vector indexes included, is rebuilt from
the items during the restore, so the table is complete when it is `ACTIVE`.

Measured restore durations, from the request to `ACTIVE` as the client saw it. Host: 16 cores (Xeon
Platinum 8124M), 30 GiB, Linux 5.10; PostgreSQL 16 (with pgvector for the vector rows), MongoDB 7.0 as a
single-node replica set and Cassandra 4.1 as a single node, each in a container on loopback; SQLite as the
binary bundles it; release builds; a filesystem store on local disk; items of a 200-byte string under a
composite key with no secondary index unless the row says otherwise. Each figure is one run (a range is
several) on a host that ran other work, so the durations are upper bounds. The figures come from the
measurement records under `docs/design/` (`15-backup-restore-bench-*.md`), each on the release build it
names:

| Table | PostgreSQL | SQLite | MongoDB | Cassandra |
|---|---|---|---|---|
| 100k items | 1.1 s | 2.9 s | 3.9 to 8.9 s (unordered) | 6.1 s |
| 1M items, backup in key order | 9.5 s | 26.8 to 56.3 s | | 49.0 s |
| 1M items, unordered backup | 17.5 s | 66 to 90 s | 33.8 s (up to 5 s more in another run) | |
| 100k items, two GSIs | 6.2 to 9.9 s | 10.2 to 13.9 s | | |
| 300k items, two GSIs | 19.7 to 21.4 s | 51.7 to 51.9 s | | |
| 100k items with a 128-dimension vector and a vector index | 25 s | 22 s (28 s under `MALLOC_ARENA_MAX=1`) | | |
| 1M items with a 128-dimension vector and a vector index | 246 s | 214 s (283 s under `MALLOC_ARENA_MAX=1`) | | |

A server writes its own backups in key order for hash-only and single-range-key tables on PostgreSQL and
SQLite; MongoDB's backups and those of multi-part-key tables are unordered. On MongoDB the table becomes
`ACTIVE` at the next cycle of the backend's backfill worker after the restore's load ends, and the worker
runs every 5 s, so a MongoDB restore takes the load's time plus up to 5 s: 3.9 to 8.9 s at 100k items over
five runs, and at 1M items 33.8 s in one run, which another run may exceed by up to one cycle. The Cassandra rows restore a
SQLite backup copied into the Cassandra deployment's store, since Cassandra's own backups are in-catalog. The
vector rows' data files carry about 2.5 KB of line text per item, 9.5 times the plain rows' 265 bytes (the
plain rows' pad is one repeated character, so their files compress far better: 90 to 176 times smaller than
the vector rows' compressed files, 176 for the 1M key-ordered backup); their duration is not the index's cost
alone. In the runs paired with an indexed restore, the same 100k items without the vector index restored in
18.0 to 18.3 s on SQLite and 15.6 to 18.1 s on PostgreSQL: the index is about 20% of the 100k restore's
duration on SQLite (18 to 22% in two sets of runs) and 33 to 47% on PostgreSQL (five pairs), the runs of each
set or pair from one measurement harness (the measurement record's "Later runs").

Measured backup durations, `CreateBackup` to `AVAILABLE`, on the same host class: 1M items with two GSIs
36.1 s on PostgreSQL (the host loaded by two other 1M-item loads) and 12.45 s on SQLite; 500k items with two
GSIs 8.81 s on MongoDB; 1M items without a secondary index 13.5 s on MongoDB; 1M items with a 128-dimension
vector 243 s on PostgreSQL and 210 s on SQLite (222 s under `MALLOC_ARENA_MAX=1`).

Not measured: a database on another host, an S3 store's upload and download, the spool's disk, and the restore
of a Cassandra deployment's own in-catalog backups beyond 60,000 items (11.2 s for 60,000 on an earlier build,
the differences document's [Backup and Restore](../differences-from-dynamodb.md#backup-and-restore) section).
Time a restore drill on your own hardware and store; that figure, not this table, is your recovery time.

### Memory while a job runs

Every backup and restore job is bounded per server by `[backup] max_concurrent_jobs` (default 4); a request
over it waits up to 30 s for a slot and is then refused with `LimitExceededException`. Size the host for the
jobs it runs at once. The admin guide's `max_concurrent_jobs` row has the figures for tables of large items.
For the tables above and two more shapes, the process's peak over its idle figure while one job ran:

| Job | PostgreSQL | SQLite | MongoDB | Cassandra |
|---|---|---|---|---|
| restore, 1M items, key order | 16 MiB | 87 to 98 MiB | | 33 MiB |
| restore, 1M items, unordered | 16 MiB | 144 to 164 MiB | 52 MiB | |
| restore, 1M items with a 128-number vector and a vector index | 111 MiB | 552 MiB; 204 MiB under `MALLOC_ARENA_MAX=1` | | |
| backup, 1M items with a 128-number vector and a vector index | 104 MiB | 138 MiB; 58 MiB under `MALLOC_ARENA_MAX=1` | | |
| restore, 100k items with a 128-number list and no vector index | 60 to 71 MiB | 397 to 424 MiB; 184 to 186 MiB under `MALLOC_ARENA_MAX=1` | | |
| restore, 100k items with a 2,290-character string and no index | | 253 MiB; 130 MiB under `MALLOC_ARENA_MAX=1` | | |
| restore, 20,000 items with a 1,536-number vector and a vector index | | 669 MiB; 301 to 316 MiB under `MALLOC_ARENA_MAX=1` | | |
| backup, 20,000 items with a 1,536-number vector and a vector index | | 521 MiB | | |

On SQLite a restore holds its write batch, up to 10,000 items or 32 MiB of item text, and the decode chunks in
front of it, so for items of more than a few hundred bytes its memory grows with the item's size up to that
bound, whatever the attribute types, and the vector index is not what it belongs to. 100k items of about
2.5 KB of strings with no index held 253 MiB over idle, and 130 MiB under `MALLOC_ARENA_MAX=1` (two runs each),
against 38 MiB for 100k items of 200 bytes. Lists of numbers decode larger at the same text size: 100k items
of 128 numbers held 345 to 424 MiB over the measured runs with or without a vector index, and 184 to 195 MiB
under one arena; the figure also grows with the numbers per item (the 1,536-number row). On PostgreSQL the
vector index adds 27 to 53 MiB at 100k items (five pairs of runs with and without it). Under glibc's default allocator the SQLite vector restore's
peak is higher at 1M items than at 100k (552 against 354 MiB), and the 1M run levelled off after about 150 s;
no other size was measured. Under `MALLOC_ARENA_MAX=1` the same restores held 195 MiB at 100k and 204 MiB at
1M, at a cost in duration: 32% for one 1M restore (283 s against 214 s), and for two 100k restores running at
once 57.5 s against 24.9 s under the default arenas (279 against 697 MiB over idle for the pair). The
deployment guide's service unit carries `MALLOC_ARENA_MAX=1` commented out, because one allocator arena costs
the request path a few percent (3.5 to 9.2% on GetItem, Query and Scan on SQLite). Set it on a SQLite host
that restores tables of items of more than a few hundred bytes, or backs up tables of large items, and size
such a host per running restore from the points measured under it: 130 MiB for 100k items of 2.5 KB strings,
184 to 208 MiB for items of 128 numbers (100k and 1M), 301 to 316 MiB for 20,000 items of 1,536 numbers, and
more for larger items (the admin guide's `max_concurrent_jobs` row has the large-item figures). Without it,
size for the default-allocator figures, which these measurements do not bound. The peaks varied by up to about
20% between runs of one cell, idle or loaded (345 to 409 MiB over nine runs of the SQLite 100k vector restore).

## Configuration

The `[backup]` section's keys (the job bound, the heartbeat and reconciler intervals, `spool_path`) and the
`[backup.store]` section are documented in the admin guide's [`[backup]`](05-admin-guide.md#backup) and
[`[backup.store]`](05-admin-guide.md#backupstore) sections. The store's keys, defaults and refusals are the
backup store crate's, `crates/backup-store/src/config.rs`.

A filesystem store:

```toml
[backup.store]
kind = "filesystem"

[backup.store.filesystem]
path = "/var/lib/extenddb/backups"
```

The configured root must exist: the server does not create it and refuses to start, "backup root <path> is
not usable: No such file or directory (os error 2)". Create it owned by the server's user with mode `0700`, for example
`install -d -m 0700 -o <server user> /var/lib/extenddb/backups` ([A filesystem root](#a-filesystem-root) has
the rules for the directories above it).

An S3 store; a restore from S3 downloads to `spool_path`, which needs disk for the compressed size of the
backups being restored at once (on file-backed SQLite it defaults to `backup-spool/` beside the database file):

```toml
[backup]
spool_path = "/var/lib/extenddb/backup-spool"

[backup.store]
kind = "s3"

[backup.store.s3]
bucket = "my-extenddb-backups"
prefix = "extenddb/"
region = "us-east-1"
```

The spool is created `0700` when it is absent. An existing one must be owned by the server's user with mode
`0700`, under directories that follow the root's rules ([A filesystem root](#a-filesystem-root)), or the
server refuses to start, naming it (a spool left `0755` by `mkdir -p` under a `022` umask is refused,
"backup.spool_path ... is not private").

Credentials for S3 come from the AWS SDK's default chain (environment, shared config, instance or container
role) and are never in the file. An S3-compatible server takes `endpoint`, and an `http://` endpoint needs
`force_path_style = true` and a loopback host unless `allow_insecure_endpoint = true`. The S3-compatible
material (the request checksums some servers reject, TLS trust, conditional writes) is in
[`crates/backup-store/README.md`](../../crates/backup-store/README.md).

Defaults: on file-backed SQLite an absent section is a filesystem store at `backups/` beside the database
file. On PostgreSQL, MongoDB, Cassandra and an in-memory SQLite database there is no default, and without a
store `CreateBackup` writes the in-catalog format with a deprecation warning ([Legacy
backups](#legacy-backups)). `kind = "disabled"` keeps a deployment on the in-catalog format on purpose.

Instances that share a catalog must share one store root: one S3 bucket and prefix, or one shared mount. A
host-local directory configured at one path on each host is not one root: once the catalog has backups there,
a second instance pointed at its own directory under that path starts with the store unhealthy. Before the
first backup nothing tells the directories apart: each instance creates its own identity file and store id,
the first backups split the rows between the two ids, and the next check reports an identity mismatch that
only `backup purge` clears (the deployment guide's [Multi-Instance
Considerations](11-deployment-guide.md#multi-instance-considerations)); with an `extenddb-store.json` copied
from one directory to the other nothing detects it, and a restart can delete the only copy of a backup (the
admin guide's [`[backup.store]`](05-admin-guide.md#backupstore) section). Point every instance at the shared
root before the first backup, and never copy `extenddb-store.json` between roots.

`extenddb destroy` removes the databases and leaves the store: the store is the copy that survives it.
`extenddb backup purge --confirm` is what deletes a store's backups. A catalog re-created with `init` finds
the old root's `extenddb-store.json` naming the old catalog; `extenddb backup sync --adopt-store` makes the
new catalog its owner, and until then the store is unhealthy, or, on file-backed SQLite with the default root,
unconfigured, with `CreateBackup` writing in-catalog backups ([The catalog is lost and
re-created](#the-catalog-is-lost-and-re-created)). Or configure a new root.

`/health` reports the store as the `backup_store` component, for example
`{"components":{"backup_store":{"kind":"filesystem","status":"healthy"}},"status":"healthy"}` (on MongoDB
`status` comes first). An unhealthy store (unreachable, or failing its identity check) is `status` `degraded`
with HTTP 200 kept, so a store outage never takes the data plane out of rotation; `CreateBackup`, and the
restore and delete of a backup in that store, are refused with a `ValidationException` until the next check
passes.

## Securing the store

### Encryption and integrity

Backup contents are DynamoDB JSON in gzip, in plaintext. Encryption at rest is the store's job: SSE-S3 or
SSE-KMS on the bucket (SSE-KMS needs `kms:GenerateDataKey` and `kms:Decrypt` on the key for the server's
role), or an encrypted filesystem for a filesystem root. Bucket versioning and Object Lock add protection
against deletion and ransomware; ExtendDB depends on neither. A restore verifies every data file's checksums
and the manifest's before it writes any item, so a corrupted file fails the restore, naming the file, and
leaves no table behind.

### A filesystem root

The root must be owned by the server's user with mode `0700`, and every directory above it must be owned by
root or by that user and not writable by group or others unless it carries the sticky bit. The server
refuses anything else at startup, naming the path, for example:

```
[backup.store] (filesystem path /srv/backups/extenddb) cannot be opened: permission denied: /srv/backups (an
ancestor of the backup root) has mode 0775 and is writable by group or others without the sticky bit; another
user could replace the backup root. chmod it to remove group and world write, or move the backup root
```

So a root under a directory a storage administrator owns, under an NFS mount owned by `nobody`, or under a
group-writable parent without the sticky bit (a `root:staff 0775` `/srv`) is refused; put the root under a
directory owned by root or by the server's user. A root of mode `0755` is refused with "chmod it to 0700".
The root itself must exist before the server starts; the directories the store creates under it are `0700`
and its files `0600`.

A copy tool that takes the root off the host (rsync, restic, borg) cannot read the tree as a member of a
group: run it as the server's user or as root, and have it copy the root as a whole while no backup job runs,
or copy whole backup prefixes whose `extenddb-manifest.json` is present (a prefix without that file is a
backup still being written).

The owner-only checks see the tree as the kernel reports it. They cannot observe a bind mount of the root or
of a directory under it, nor a filesystem without Unix permission semantics: a root on such a filesystem
mounted with a mask that makes it report `0700` passes the check while enforcing nothing. Both are the
operator's responsibility. The root also needs hard links, which the store's create-if-absent uses; some CIFS
and FUSE mounts refuse `link(2)`, and the server then reports the store as having failed validation.

### An S3 bucket

The least-privilege IAM policy for the server's role, its actions and the resources each applies to, is in
[`crates/backup-store/README.md`](../../crates/backup-store/README.md) (the IAM item of its Behavior section).
The store's check probes each grant the policy lists and names a missing one. On an SSE-KMS bucket it does not
probe `kms:Decrypt`, which an upload's parts (`UploadPart`) and a restore's download (`GetObject`) use, so a
role without it passes the check and fails at the first job that needs it. Run the restore drill after any
change to the key policy.

Lifecycle rules for the bucket:

- `AbortIncompleteMultipartUpload` (one day): a process killed during an upload leaves a multipart upload
  that only a later deletion of the same prefix would sweep; the rule removes the rest.
- On a versioned bucket (an Object Lock bucket always is), deletions add delete markers and keep the
  noncurrent versions, which are billed: add `NoncurrentVersionExpiration` and `Expiration` with
  `ExpiredObjectDeleteMarker: true`, or use an unversioned bucket.

Objects are keyed `<prefix><account id>/<table name>/<backup id>/`, so each account's backups sit under a
prefix of their own. Write access to an account's prefix is backup-administrator authority for that account:
whoever can write there can delete the account's backups, or replace one with a backup of other items whose
checksums and manifest agree, which a restore then accepts. Grant the server's role the whole prefix, and
grant any other principal (a tenant's tooling, a copy job) at most its own account's prefix,
`arn:aws:s3:::<bucket>/<prefix><account id>/*`, with IAM prefix policies.

The AWS SDK's `trace` logs carry credentials: at `trace`, `aws_sigv4` logs the canonical request with the
session token and `aws_smithy_runtime` the `authorization` header. Do not enable those targets at `trace` in
production.

## What a backup costs the database

A backup holds its snapshot open from the request until its last data file is written, and each backend pays
for that while it runs. The figures below were measured for this manual; the measurement record
`docs/design/15-backup-restore-bench-pr11.md` ("Figures measured for the manual") has their method, build,
load and repeat runs.

- PostgreSQL. The snapshot is a read-only `REPEATABLE READ` transaction on the data database. While it is
  open, `VACUUM` cannot remove any row version that became dead after it started, in any table of the data
  database: every update and delete during the backup leaves its old row version in place until the backup
  ends. Measured on the host above: during a 25 s backup (100k items with a 128-dimension vector), 12,156
  `PutItem` overwrites of 1,000 200-byte items in another table grew that table from 0.4 to 4.4 MiB, about
  345 bytes per write, and a `VACUUM` run 10 s into the backup reported its 4,948 dead row versions as "dead
  but not yet removable"; the same writer for the same time with no backup left the table at 0.5 MiB. The
  first `VACUUM` after the backup removed every dead version, and the table keeps the space for later writes.
  Size the data database's disk for the update and delete volume of the longest backup.
- SQLite. The snapshot is a read transaction, which stops the WAL from being checkpointed past it, so the
  `-wal` file grows by the pages written during the backup, and how many pages a write changes depends on the
  table's indexes. Measured on the host above, beside a 100k-item backup (items with a 128-dimension vector),
  with `PutItem` overwrites of 200-byte items in another table:
  - a table with no secondary index: 15,472 writes during a 22.8 s backup grew the `-wal` file from 4.0 to
    61.1 MiB, about 3.9 KB per write;
  - a table with two GSIs (each overwrite changing both index keys, `ALL` projection): 13,996 writes during
    a 25.6 s backup grew it from 4.1 to 1,294 MiB, about 82 to 97 KB per write over four runs, some 20
    to 24 pages per write. A GSI is asynchronous at the default `index_propagation_delay_ms`: the put queues each
    index write in `gsi_pending` inside the put's transaction, and later transactions apply and remove it,
    each appending every page it touched.

  The same writers for the same time with no backup left the file at 4.0 and 4.1 MiB. The file keeps its size
  after the backup, and after the server stops; later writes reuse it. Size the disk for the pages each write
  changes, about one 4 KiB page for a table with no index and about 24 for a table with two GSIs, times the
  writes during the longest backup.
- MongoDB. The server keeps snapshot history for `minSnapshotHistoryWindowInSeconds` (default 300). A backup
  whose read stays open longer may fail with `SnapshotTooOld`; the backup is then `FAILED` and must be taken
  again. A table that takes longer than the window to back up needs the parameter raised on the replica set
  (`setParameter`), which costs the server memory for the longer history. On the host above, 1M items of 200
  bytes backed up in 13.5 s.

On PostgreSQL each running job also holds a dedicated connection outside the pool, whose session advisory
lock is the job's claim; startup raises `pool_size` and `catalog_pool_size` to at least
`2 * max_concurrent_jobs + 10`, and `max_connections` must leave room for it (the deployment guide's
connection pool section). The claim's session sets TCP keepalives (30 s idle, then three probes 10 s apart),
so after the loss of the owner's host the database ends the session about a minute later. Another instance's
reconciler takes the job over only once that session has ended and the job's heartbeat is older than
`[backup] stale_job_after_secs` (default 300 s). An owner whose host still answers but whose process is
stopped (a paused VM, `SIGSTOP`) keeps its session, and with it the job, until it resumes. To end the
session, find it by its lock key, `backup:<backup id>` for a backup (the last component of the backup ARN)
and `<account id>/<table name>` for a restore into that table, and terminate it:

```sql
-- in the catalog database
SELECT l.pid FROM pg_locks l
 WHERE l.locktype = 'advisory' AND l.objsubid = 1 AND l.granted
   AND (l.classid::bigint << 32 | l.objid::bigint) = hashtextextended('backup:<backup id>', 4539474)
   AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database());
SELECT pg_terminate_backend(<pid>);
```

The job keeps writing until its next heartbeat (within 15 s), then stops with no further store or catalog
access, and once its heartbeat is older than `stale_job_after_secs` the reconciler resolves it: a backup with
no complete manifest becomes `FAILED` (`DescribeBackup` answers `BackupNotFoundException`) and its partial
objects are deleted; a store-backed restore is resumed from its recorded progress (the differences document's
restore-interruption row). Do not terminate the session whose `application_name` is `extenddb serve`: that is
the server's own registration. The server then refuses requests with `ServiceUnavailable` and stops its
background work while it registers again, and every store-backed backup it is running fails (reason
`shutdown`) and must be taken again.

A transaction-mode pooler (PgBouncer with `pool_mode = transaction`) is unsupported for the
claim: the lock lands on a pooled server connection and is not released with the job. Reach the catalog
database directly or through a session-mode pooler.

## Restore drill

Run a drill on a schedule and after every change to the store, the hosts or the release. It proves that the
backups restore, and its duration is your recovery time.

Every `aws` command below is for the ExtendDB server: give each one `--endpoint-url https://<host>:18443`, with
`AWS_CA_BUNDLE` naming the server's certificate, as the crontab line above does. Without the flag the CLI sends
the request to Amazon DynamoDB, with whatever credentials the host's profile holds.

1. Pick a backup: `aws dynamodb list-backups --table-name orders`, or take one with `create-backup` and poll
   `aws dynamodb describe-backup --backup-arn <arn>` until `BackupStatus` is `AVAILABLE`. On the server,
   `extenddb backup list --config extenddb.toml` prints the store-backed backups with their size, item count
   and store; it runs beside a server.
2. Restore it under a new name:
   `aws dynamodb restore-table-from-backup --backup-arn <arn> --target-table-name orders-drill`. The answer is
   `CREATING` at once.
3. Poll `aws dynamodb describe-table --table-name orders-drill` until `TableStatus` is `ACTIVE` and every
   index reports `ACTIVE`. Until then the table answers reads with `ResourceNotFoundException`, and
   `DeleteTable` on it is refused. Note the elapsed time.
4. Compare: `aws dynamodb scan --table-name orders-drill --select COUNT` (follow `LastEvaluatedKey`) against
   the backup's `SourceTableDetails.ItemCount` from `describe-backup`, and read a few known keys with `get-item`.
   `DescribeTable` reports `ItemCount` 0 until the statistics pass next runs, as the service does, so count
   with a scan.
5. Delete the drill table: `aws dynamodb delete-table --table-name orders-drill --endpoint-url https://<host>:18443`.

A drill on the same server proves the backup. To prove the off-host copy, run it on a separate deployment:
copy the backup's prefix (`<account id>/<table name>/<backup id>/`, every object under it) under the drill
deployment's own store root, with the same relative path, as the drill server's user, keeping directories
`0700` and files `0600` (`cp -rp`, `rsync -a`, or `chmod -R u=rwX,go=` on the copy afterwards). Then run
`extenddb backup sync` there (below), and restore. The drill deployment needs the account the backup's ARN
names. A copy that lands readable by other users, as `aws s3 cp --recursive`, `tar` or `rsync` without
permissions under a `022` umask leave it, is refused: `backup sync` exits 1 naming the directory ("permission
denied: <directory> has mode 0755 and is accessible to other users; chmod it to 0700"), a restore of such a
backup is refused (`InternalServerError`; the server log names the directory), and when the copy is the newest backup the store's next check marks the whole store unhealthy.
Run `chmod -R u=rwX,go=` on the copy and `extenddb backup sync` again.

A restore that fails (a checksum mismatch, found before any item is written, or an item the target refuses
during the load) removes its target,
and the server logs the reason with the backup ARN. A store-backed restore stopped by the server stopping is
resumed by a server's reconciler from its recorded progress; an in-catalog one is failed and its target
removed. If a resume finds a target holding more
items than the backup and no recorded progress, it fails as `target_not_empty` and leaves the target
`CREATING`; `extenddb backup remove-target --account <account id> <table>` removes it (the admin guide's
`backup` section).

## Recovering from catalog loss

The store holds every store-backed backup; the catalog holds the rows that index them, the accounts and IAM.
After a catalog is lost, bring the catalog back, then give it a row for each backup with `extenddb backup
sync`.

### The catalog is restored from a database backup

Restore the catalog from its database backup (the admin guide's [Data
Recovery](05-admin-guide.md#data-recovery) entry). On PostgreSQL the catalog and data databases are one copy:
restore both, either from dumps taken together with the server stopped, into new, empty databases owned by the
application role as the entry gives it (restoring over the existing databases restores nothing), or from one
base backup of the PostgreSQL server that held both, with PostgreSQL's own procedure. On a new PostgreSQL
server create the application role before the dumps' restore and, when the server's address changed, set the
data database's address in the restored catalog, as the entry's last paragraph says. On SQLite the database
file holds the catalog and the tables; restore it as the entry's SQLite row says. Either way every table returns to the copy's moment, and a table created after it is gone. The restored catalog has the same catalog id, so it still
owns the store root, and its accounts and IAM are back. Then run, with the servers stopped on SQLite and
beside them on the other backends:

```bash
extenddb backup sync --config extenddb.toml
```

Sync lists the root and inserts an `AVAILABLE` row for each complete backup the catalog has no row for (the
backups taken after the catalog's backup). It reports the prefixes it skipped (an incomplete backup, one the
catalog already has) and the `AVAILABLE` rows whose manifest is no longer in the store (backups deleted after
the catalog's backup); `extenddb backup sync --prune-missing` removes those rows and deletes nothing in the
store. When the newest of those rows is the catalog's newest `AVAILABLE` backup, the store is unhealthy (each
check looks for that backup's manifest, and `/health` reports `"reason":"manifest probe failed"`): both
commands refuse ("sync changes no row while the store is unhealthy"), as do `CreateBackup` and the restore and
delete of every backup in that store, and the state survives a restart. Putting that backup's prefix back from
a copy clears it. This release has no command that clears it without such a copy: `--prune-missing` refuses
while the store is unhealthy. To stay out of this state, take each catalog backup right after the deletions
your scheduler makes, so that the newest backup a catalog backup names is not one deleted later.

Tables backed up after the copy can be restored, under their own names or others, once `backup sync` lists
their backups; their writes after their last backup are lost.

A PostgreSQL catalog database restored alone, beside a data database that survived, is a catalog older than
the tables, and gives this: a table created after the copy is not listed, and creating it again gives an empty
table while its old items stay in a table of the data database that nothing references; a table deleted after
the copy is listed and answers `ResourceNotFoundException` until it is deleted again. Restore the pair
instead.

### The catalog is lost and re-created

When no catalog backup exists, `extenddb init` creates a new catalog with a new catalog id. The store's
`extenddb-store.json` still names the old catalog, so the new server starts with the store unhealthy:

```
{"components":{"backup_store":{"kind":"filesystem","reason":"owned by another catalog","status":"unhealthy"}},"status":"degraded"}
```

On file-backed SQLite with the default root (no `[backup.store]` section) the server instead logs a warning,
"the default root ... failed its check (extenddb-store.json names catalog <old id>; this catalog is <new
id>); the catalog has no backup rows for it, so the store is unconfigured and CreateBackup writes in-catalog
backups", and `/health` shows no `backup_store` component (`{"components":{},"status":"healthy"}`). Until
the adoption below, `CreateBackup` answers `AVAILABLE` at once and writes an in-catalog backup into the
database file, the failure domain this manual's first paragraph warns about; such a backup stays in-catalog
after the adoption ([Legacy backups](#legacy-backups)). Pause scheduled backups until the adoption, or
configure `[backup.store]` explicitly, which gives the unhealthy store above instead.

In both shapes plain `backup sync` refuses (exit 1, "owned by another catalog"). Then:

1. Stop every server of the old catalog, on every host. A server that still runs keeps acting on the root
   until a later check finds it adopted.
2. Start a server of the new catalog: `extenddb manage` works through its management API. Re-create the
   accounts with their old account ids, `extenddb manage --user admin create-account --account-id <old id>
   --account-name <name>` (the admin guide's Accounts section), and their IAM users and policies. A backup is
   reachable only by callers of the account its ARN names. `create-access-key` makes new key pairs; to keep
   the clients' existing ones, import each with `extenddb manage --user admin import-access-key --account-id
   <id> --user-name <user> --access-key-id <key id> --secret-access-key <secret> --yes`. While that command
   runs, the secret is in its arguments, which the process list shows to every user of the host. On a shared
   host send the pair to the management API instead, in a request body read from a file:
   `curl --cacert <cert> --netrc-file <netrc> -H 'Content-Type: application/json' --data @key.json
   https://<host>:18443/management/accounts/<id>/users/<user>/access-keys/import`, where `key.json` holds
   `{"access_key_id": "<key id>", "secret_access_key": "<secret>"}` and the netrc file the admin user's name
   and password, both created under `umask 077`; the answer is `Access key imported`. On SQLite stop the
   server before step 3.
3. Adopt the root and sync it into the new catalog, on SQLite with the server stopped and on the other
   backends beside the running servers:

   ```bash
   extenddb backup sync --adopt-store --config extenddb.toml
   ```

   It rewrites `extenddb-store.json` to name the new catalog (keeping the store id), logs the change under
   `extenddb::audit`, and syncs:

   ```
   Adopted the root: extenddb-store.json named catalog <old id> and now names this catalog (store id <id>).
   ...
   --- Adopted (an AVAILABLE row inserted for each):
     arn:aws:dynamodb:us-east-1:<account>:table/orders/backup/<backup id>
   ...
   === extenddb backup sync complete: 1 backup(s) adopted, 0 prefix(es) skipped, 0 AVAILABLE row(s) whose manifest is gone ===
   ```

4. On SQLite start the server again; on the other backends the running servers find the store adopted at a
   later check. `/health` reports the `backup_store` component healthy (on SQLite's default root as well), the
   backups are `AVAILABLE`, and each restores as in the drill. The new catalog knows none of the old tables; restore each
   from its backup.

On SQLite every `backup sync` needs the server stopped, since it opens the database as the server does. On
PostgreSQL, MongoDB and Cassandra it runs beside servers and holds the catalog's maintenance claim while it
runs, during which `CreateBackup` and `RestoreTableFromBackup` on every instance answer
`LimitExceededException`; `--adopt-store` and `--prune-missing` first wait, as `purge` does, until no backup
or restore job is running and no backup failed within `stale_job_after_secs` (a job that failed may still be
writing to its prefix), at most `2 * stale_job_after_secs`, and then refuse. Sync exits 0 when it completed
and 1 when it refused or skipped a prefix for a store or catalog error (run it again). It exits 6 when it
cannot open the catalog: beside a running server on SQLite ("another extenddb process is already using ..."),
or on the other backends while a migration runs. On a catalog `serve` would refuse it exits with `serve`'s
code: 3 with migrations pending (run `extenddb migrate`), 4 on a catalog a later release migrated with a
breaking migration, and 5 on one that needs attention (the upgrade manual's [catalog
status](07-upgrade-manual.md#the-catalog-status)).
The admin guide's `backup sync` paragraph has its options and output in full.

### An in-memory SQLite server

A server on an in-memory SQLite database (`[storage.sqlite] path = ":memory:"`) starts with a new, empty
catalog every time. With `[backup.store]` configured it adopts, at startup, a root that an earlier in-memory
server owned, and syncs the root's backups into its new catalog before it serves (on a filesystem store; an
S3 store in the reconciler's first cycle). The log line

```
backup store (filesystem) startup sync of the in-memory catalog: 1 backup(s) adopted, 0 prefix(es) skipped (0 of them reserved for exports or the change-log archive)
```

ends the window in which the earlier backups answer `BackupNotFoundException`. Tables, accounts and IAM do not
survive the restart; only the backups do. A backup is reachable only by callers of the account its ARN names,
and the in-memory server creates its default account with a new id at every start, so until that account
exists again the adopted backups answer `AccessDeniedException` and `ListBackups` lists none of them. After
each start, re-create the accounts with their old ids (`extenddb manage --user admin create-account
--account-id <old id> --account-name <name>`, as the admin user, whose password is the
`EXTENDDB_ADMIN_PASSWORD` the server started with or, without it, the one it printed at start), then their
users, policies and keys; the backups then answer `DescribeBackup` and restore. Start a new in-memory server
only after the old one has exited. A root that a durable catalog owns is never adopted by an in-memory server.
Every `extenddb backup` command refuses an in-memory database (exit 1), since it would open a new, empty
catalog that ends with the command.

## Legacy backups

A backup written in the in-catalog format (by an earlier release, by a server without a configured store or
with `kind = "disabled"`, and on Cassandra always) lives in the database the server uses: on PostgreSQL in
the catalog database, on SQLite in the database file, on MongoDB in the catalog and data databases, on
Cassandra in the catalog keyspace and the account's keyspace. It shares the database's failure domain, and a
lost database loses it.

Legacy backups stay listable, describable, restorable and deletable whether or not a store is configured:
`ListBackups` lists both kinds side by side. `extenddb backup list` lists only the store's rows. The
in-catalog format is deprecated: without a store `CreateBackup` logs a warning on each call ("is writing the
in-catalog backup format, which a later release stops creating; configure [backup.store] ..."), and a later
release stops creating it.

This release has no command that converts a legacy backup to the store format (`backup migrate-legacy` is
planned, #821). To move one into the store today, with the store configured:

1. Restore it under a new name: `aws dynamodb restore-table-from-backup --backup-arn <legacy arn>
   --target-table-name orders-moved --endpoint-url https://<host>:18443`, and wait for `ACTIVE`.
2. `aws dynamodb create-backup --table-name orders-moved --backup-name <name> --endpoint-url
   https://<host>:18443` and wait for `AVAILABLE`. This is a store-backed backup with a new ARN and a new
   `BackupCreationDateTime`, of a table named `orders-moved`.
3. Check the new backup with a drill, then `aws dynamodb delete-backup` the legacy one and `aws dynamodb
   delete-table` the interim table, each with `--endpoint-url https://<host>:18443`.

To keep the original table name, restore into it only once that table is gone: deleting a live table to make
room loses every write since the legacy backup. While the table exists, `create-backup` of it is a
store-backed backup of that name with its current items; keep the legacy backup only for its older contents.
A backup taken by an earlier release may restore without its secondary indexes
(the upgrade manual's Current Status section); take a new backup of each table after an upgrade rather than
moving the old ones. A table with a vector index has no legacy backup: the in-catalog format refuses it.

## What a restore preserves

A restore reproduces what Amazon DynamoDB's does, on every backend, with one gap (a GSI's on-demand throughput):

| Carried across | Not carried across |
|---|---|
| items, with every number's 38 digits | table class (a restore is `STANDARD`) |
| key schema and attribute definitions | stream settings |
| global and local secondary indexes, rebuilt from the items | TTL settings |
| billing mode and provisioned throughput, table and GSI | tags |
| the table's on-demand throughput | a GSI's on-demand throughput (ExtendDB records none) |
| SSE settings | deletion protection |
| vector indexes, into PostgreSQL and SQLite | auto scaling, IAM policies, CloudWatch settings |

Reapply what is not carried by hand after the restore. The overrides of `RestoreTableFromBackup`
(`BillingModeOverride`, `GlobalSecondaryIndexOverride`, `LocalSecondaryIndexOverride`,
`SSESpecificationOverride`, `OnDemandThroughputOverride`, `ProvisionedThroughputOverride`) apply as on the
service; `VectorIndexOverride` is refused.

Across backends: a store-backed backup restores into any backend, with these refusals, each before any table
is created or with the target removed:

- A backup with vector indexes is refused into MongoDB and Cassandra, which have no vector support.
- Into MongoDB, an `N` sort-key value of more than 34 significant digits fails the restore (the sort-key
  column is Decimal128); other numbers keep their 38 digits.

`DescribeTable` on a restored table reports `ItemCount` and `TableSizeBytes` 0 until the statistics pass
rewrites them. The differences document's [Backup and Restore](../differences-from-dynamodb.md#backup-and-restore)
section has each of these rows in full, with the multi-part-key preview and the restore's error texts.

## Moving data to and from Amazon DynamoDB

The backup format is the DynamoDB S3 export layout: each backup's `data/` directory holds gzip files of
DynamoDB JSON, one `{"Item": {...}}` object per line, beside the export's `manifest-summary.json` and
`manifest-files.json`, plus ExtendDB's own `extenddb-manifest.json` and its checksum file.

From ExtendDB to Amazon DynamoDB: upload a backup's `data/` files to an S3 bucket and run the service's
`ImportTable` with `InputFormat` `DYNAMODB_JSON`, `InputCompressionType` `GZIP`, an `S3BucketSource` whose
`S3KeyPrefix` is that directory, and `TableCreationParameters` built from the backup: the key schema from
`describe-backup`'s `SourceTableDetails`, the GSIs from its `SourceTableFeatureDetails` (for an on-demand
table drop each GSI's `ProvisionedThroughput`, which the backup reports as 0 and 0 and the SDK's parameter
validation refuses), and the attribute types (`AttributeDefinitions`, which neither carries) from the
`source_table` object of the backup's `extenddb-manifest.json`. `TableCreationParameters` has no member for
local secondary indexes, so the service's import creates none; a table with LSIs arrives without them. That is
the layout the service's import reads; this release has not run the import against the service.

From Amazon DynamoDB to ExtendDB: export the table to S3 with `DYNAMODB_JSON`, download the export's data
files, decompress them and concatenate them into one file under an `[import]` path, in the calling account's
directory (`<import path>/<account id>/`), then call ExtendDB's `ImportTable` with a `FileSource` naming that
file and `InputFormat` `DYNAMODB_JSON`. Name the file by its canonical path in `FileSource.Path`: a path
through a symbolically linked directory is refused, "Path must resolve under one of the configured allowed
paths", whether or not the `[import] paths` entry is itself written through that link, and a file that is
itself a symbolic link is refused with `ValidationException` "Symbolic links are not allowed in import/export
paths". ExtendDB's `ImportTable` reads one uncompressed local file (a gzip file is refused, "I/O error reading
import file at line 1"), creates the table from `TableCreationParameters`, reads the whole file before it
writes the first item, completes before it answers, and skips the items that fail validation (keys that do not
match the key schema, an item over the size limit), counting them in `ErrorCount`; the file is bounded by
`max_import_file_bytes` (10 GiB) and `max_import_item_count` (10 million). The same works for one of
ExtendDB's own backup data files.

ExtendDB's `ExportTableToPointInTime` writes one uncompressed DynamoDB JSON file under an `[export]` path, in
the calling account's directory, at the path the request's `FilePath` member names (an ExtendDB member; a
request without it reads `S3Prefix` as the path, and `S3Bucket` is accepted and ignored, so a request naming
neither path is refused, "Either FilePath or S3Prefix must be provided"). It reads the table as it changes, so it is not a consistent snapshot. Reading a store prefix or a
DynamoDB export directly, with `ImportTable` or a restore, is planned (#748) and not in this release; `backup
sync` adopts only prefixes with an ExtendDB manifest.

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
