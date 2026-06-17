# feat(storage): add S3 Object Annotations backend (named annotations, base64-chunked items, AWS ships the SELECT)

### Forward

This is the second time. [PR #54](https://github.com/ExtendDB/extenddb/pull/54) added a
Route 53 storage backend, on the standing argument that Route 53 is a database. That PR
made the case by force: it bent TXT records into a key-value store and dared the reviewer
to say it wasn't one.

S3 Object Annotations — [launched 2026-06-16](https://aws.amazon.com/blogs/aws/amazon-s3-annotations-attach-rich-queryable-context-directly-to-your-objects/) —
is a more honest fit than Route 53 was, and I want to be clear about why. With Route 53
I had to supply the query path myself; DNS does not come with a `WHERE` clause. With S3
Annotations, AWS shipped the query path. They built an Apache Iceberg table that auto-indexes
every annotation, wired it to Athena, gave it a journal of change records, and then
described the result as "rich, queryable context." They built a database and declined to
call it one. This PR calls it one.

### What's in here

A new `extenddb-storage-s3annotations` crate (behind an `s3annotations` cargo feature)
stores items as named annotations on a sentinel S3 object. The object is the table — the
structural analog of #54's hosted zone. Its default key is `.well-actually`.

- **`encoding`** — the real, round-trip-tested item↔annotation mapping. Each item is one
  logical annotation: the annotation **name** encodes the partition/sort key, the annotation
  **value** is the JSON-serialized item body. Bodies are base64-encoded and chunked into
  ≤ 1 MB pieces, one annotation per chunk, named `<key>#0001`, `<key>#0002`, … and
  reassembled on read. This is the direct analog of #54's 255-byte TXT-string spillover;
  the constraint moved from 255 bytes to 1 MB but the mechanism is identical.
- **`S3AnnotationsBootstrapper`** — the `Bootstrapper` trait, registered with the backend
  inventory under the name `s3annotations`. Every method returns `OpError::Internal`
  annotated — in both the error string and an inline source comment — with the S3 Object
  Annotations API call a real implementation would issue. It is a porting map with a
  non-zero exit code.

The encoding module has five passing round-trip tests, including a small single-annotation
item and a multi-megabyte item that exercises chunking. `cargo test -p extenddb-storage-s3annotations`
is green.

### Why this is not as deranged as it sounds

**Properties that make the substrate look reasonable:**

- AWS provides the `SELECT` path. Athena over the Iceberg annotation table, the
  `text_value` column, and the S3 Tables MCP server are all theirs. The query engine
  ExtendDB would otherwise have to build is simply free.
- Annotations update in place without rewriting the object. An `UpdateItem` does not pay to
  rewrite the whole item the way a DynamoDB write effectively does.
- Annotations move with the object on copy and replication, and are deleted with the object.
  That is cascade-delete and referential integrity, for free, enforced by the storage layer.
- The annotation table is an asynchronously-built secondary index that you did not provision,
  do not manage, and are not billed to maintain as a GSI.
- Objects in S3 Glacier remain queryable through the annotation table without a restore. The
  rows can sit in cold storage while the index stays hot.

**Properties that make it indefensible:**

- Annotation tables refresh within an hour and backfill takes "hours to days," so the
  queryable index has an eventual-consistency window measured in business days.
- The point API (`GetObjectAnnotation`) is strongly consistent, but the SQL path lags. Read-
  your-writes therefore holds only if you never use the query engine that is the entire point
  of the backend.
- Annotation storage bills at S3 Standard rates regardless of the parent object's storage
  class. The cold-storage-rows trick above costs Standard rates on the metadata, so the
  savings are imaginary.
- 1,000 annotations per object caps items per table at 1,000.
- Every Athena query is a full table scan billed per TB scanned. There is no point-read price;
  there is only the scan.

I am genuinely unsure which list is more interesting. I have included both for the reviewer's
enjoyment.

### Pricing

| Component | DynamoDB on-demand | S3 Annotations backend |
|-----------|--------------------|------------------------|
| Storage | Per GB-month, by storage class | Per GB-month at **S3 Standard**, regardless of the object's class |
| Writes | Per WCU-second | Per `PutObjectAnnotation` call |
| Reads (consistent point) | Per RCU | Per `GetObjectAnnotation` call |
| Reads (analytical) | Per RCU (Query/Scan) | Per **TB scanned** in Athena — every query is a full table scan |

Point-read-heavy workloads map cleanly onto `GetObjectAnnotation`. Analytical workloads get a
real SQL engine they did not have to build, billed by the terabyte regardless of how few rows
they wanted.

### Streams

ExtendDB streams map onto the S3 Metadata **journal table**, which is a change log AWS already
maintains in near real time. The streams implementation tails `CREATE_ANNOTATION` and
`DELETE_ANNOTATION` records:

- `record_timestamp` → `ApproximateCreationDateTime`
- `CREATE_ANNOTATION` → `INSERT`
- `DELETE_ANNOTATION` → `REMOVE`

This is cleaner than #54's streams. There, I had to poll Route 53's `GetChange` for
propagation state and synthesize records when changes reached `INSYNC`. Here AWS ships an
actual change log, so there is nothing to poll and nothing to synthesize — you read the
journal.

### Build matrix

| Build | Behavior |
|-------|----------|
| `cargo build` | Unchanged; `s3annotations` not registered |
| `cargo build --features s3annotations` | `extenddb init --backend s3annotations` reaches the bootstrapper with an S3 Annotations error |
| `cargo test -p extenddb-storage-s3annotations` | Five encoding round-trip tests; all pass |

### Before / after

**Before:**

```
$ extenddb init --backend s3annotations
Error: Internal("Unknown backend: s3annotations. Available backends: postgres")
```

**After** (built with `--features s3annotations`):

```
$ extenddb init --backend s3annotations
Error: Internal("ensure_app_user: S3 Annotations backend is registered but the
relevant operation is not yet implemented. Use --backend postgres, or wire this
method to the corresponding S3 Annotations API call (referenced inline below).
Maps to S3 Annotations CreateBucketMetadataConfiguration.")
```

Users now receive both an error and a porting map to the AWS API call. The full map, one row
per `Bootstrapper` method:

| `Bootstrapper` method | Maps to S3 Annotations call |
|-----------------------|-----------------------------|
| `ensure_app_user`, `grant_app_role_to_admin` | `CreateBucketMetadataConfiguration` |
| `create_catalog_db`, `create_data_db` | `CreateBucketMetadataConfiguration` |
| `run_catalog_migrations`, `run_data_migrations` | `UpdateBucketMetadataAnnotationTableConfiguration` |
| `record_data_connection`, `bootstrap_encryption_key`, `bootstrap_default_account`, `bootstrap_admin_user` | `PutObjectAnnotation` |
| `is_catalog_initialized`, `list_table_names` | `ListObjectAnnotations` |
| `get_data_db_name`, `read_catalog_version` | `GetObjectAnnotation` |
| `drop_databases` | `DeleteObjectAnnotation` |

### What's still missing

| Piece | State |
|-------|-------|
| Cargo crate + workspace member | Done |
| `Bootstrapper` impl (registered, stubbed with sourced errors) | Done |
| `encoding` module (chunking, base64, round-trip tests) | Done |
| `OperationsEngineRegistration` | Not in this PR |
| `StorageConfigRegistration` | Not in this PR |
| `SettingsStoreRegistration` | Not in this PR |
| `DiagnosticsStoreRegistration` | Not in this PR |
| `ServerComponentsRegistration` | Not in this PR |
| `crates/bin/src/config.rs` (hard-references postgres) | Not modified |

### Organizational note

With this PR, two of ExtendDB's pluggable backends are covered AWS services. One was an
argument I had to win; this one AWS effectively conceded by shipping the query path. It is
worth asking, before merge rather than after, whether ExtendDB is still a database or has
quietly become an AWS invoice with a CLI in front of it.

If the project would rather not answer that question, the alternative is the same one I
offered in #54: I will withdraw this PR and instead submit a one-line edit to the
`--backend` help text in `crates/bin/src/cmd_init.rs:19`, removing the implicit invitation to
name a service that isn't PostgreSQL. I leave the choice of which is funnier to the maintainer.

**I do declare that S3 is, in fact, a database. I dare you to prove me wrong.**
