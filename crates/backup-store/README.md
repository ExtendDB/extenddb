# extenddb-backup-store

Backup object store abstraction for ExtendDB: one `BackupStore` trait with a
filesystem implementation and an S3 implementation built on the official
`aws-sdk-s3` crate. The engine writes portable backups through this trait; see
`docs/design/15-backup-restore.md` for the full design.

## Behavior

- Keys are `/`-separated component paths. Both stores reject empty
  components, `.` and `..`, backslashes, control characters, and keys over
  1024 bytes, so the two stores accept the same key space.
- The filesystem store canonicalizes its root at construction, refuses any
  operation whose resolved target escapes the root (symlink defense), and
  writes through a temporary sibling file renamed into place so a crash never
  leaves a truncated object under its final name.
- The S3 store takes credentials from the standard AWS SDK chain, uses
  multipart upload for bodies larger than the configured part size (default
  8 MiB, minimum 5 MiB), aborts the multipart upload when a put fails,
  paginates listings, and deletes prefixes in DeleteObjects batches of 1000.

## Tests

`cargo test -p extenddb-backup-store` runs the key validation and
configuration unit tests, the filesystem conformance suite against a
temporary directory, and the S3 conformance suite.

The S3 suite needs a local S3-compatible endpoint and is skipped with a
printed reason when `EXTENDDB_TEST_S3_ENDPOINT` is unset. To run it against a
local MinIO:

```sh
docker run --rm -d -p 9000:9000 --name extenddb-backup-store-minio \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

EXTENDDB_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
cargo test -p extenddb-backup-store
```

A bare `minio server /data-dir` binary works the same way; only the endpoint
and the two credential variables matter.

`EXTENDDB_TEST_S3_BUCKET` (default `extenddb-backup-store-tests`) and
`EXTENDDB_TEST_S3_REGION` (default `us-east-1`) can override the bucket and
region. The suite creates the bucket when it does not exist and isolates each
test under a unique key prefix. CI wiring for the MinIO service is tracked as
its own task in the design doc.
