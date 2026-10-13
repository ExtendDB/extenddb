# extenddb-backup-store

Backup object store abstraction for ExtendDB: one `BackupStore` trait with a
filesystem implementation and an S3 implementation built on the official
`aws-sdk-s3` crate. The engine writes portable backups through this trait; see
`docs/design/15-backup-restore.md` (#396) for the full design.

## Behavior

- Keys are `/`-separated component paths. Both stores reject empty
  components, `.` and `..`, backslashes, control characters, and keys over
  1024 bytes, so the two stores accept the same key space.
- The filesystem store canonicalizes its root at construction and resolves
  every key one component at a time without following symlinks: a symlink
  anywhere in a key's path, pointing inside or outside the root, is refused
  by `put`, `get` and `head`; `list` and `delete_prefix` skip symlinks
  rather than follow them (a symlink below the prefix is removed as a link,
  never followed; a symlink at the prefix path itself is treated as nothing
  there). Those checks are path-based and can be raced by a process that can
  write under the root, so the store requires that nobody but the owner can
  reach the tree and enforces it on Unix: `open` and `validate` refuse a
  root that grants any permission to group or others or is owned by another
  user, and refuse a root whose ancestor is owned by a third user or is
  writable by others without the sticky bit; every operation re-checks the
  root and refuses any existing directory it descends into that fails the
  same owner-only rule; directories are created `0700` and objects `0600` at
  creation. Writes go through a temporary sibling file, `fsync`, rename into
  place and `fsync` of the directory; a failed put removes the temporary,
  any directories it created, and the renamed file if the directory sync
  fails, so nothing is observable under the key.
- Consequence of the owner-only rule: a backup pickup agent (rsync, restic,
  borg) cannot read the tree as a member of the extenddb group; it has to
  run as the extenddb user or as root. A root under `/srv/backups` owned by
  a storage administrator, or under an NFS mount owned by `nobody`, is
  refused by the ancestor rule; put the root under a directory owned by root
  or by the extenddb user.
- `put` returns the stored object's metadata (bytes written and, on S3, the
  ETag with quotes stripped; a multipart ETag is not an MD5) so a manifest
  can be written from the put alone.
- The S3 store takes credentials from the standard AWS SDK chain, uses
  multipart upload for bodies larger than the configured part size (default
  8 MiB, minimum 5 MiB; S3's 10,000-part limit caps one object at about
  78 GiB at the default), aborts the multipart upload when a put fails,
  paginates listings, and deletes prefixes in DeleteObjects batches of 1000.

## Tests

`cargo test -p extenddb-backup-store` runs the key validation and
configuration unit tests, the filesystem conformance suite against a
temporary directory, and the S3 conformance suite.

The S3 suite needs a local S3-compatible endpoint and is skipped with a
printed reason when `EXTENDDB_TEST_S3_ENDPOINT` is unset. CI uses
[moto](https://github.com/getmoto/moto) in server mode; the pinned version
is in `.github/workflows/test.yml`:

```sh
pip install "moto[server]==<version from test.yml>"
moto_server -p 9000 &

EXTENDDB_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=testing \
AWS_SECRET_ACCESS_KEY=testing \
AWS_REGION=us-east-1 \
cargo test -p extenddb-backup-store
```

Any S3-compatible server works the same way (a MinIO binary, Garage,
LocalStack); only the endpoint and credential variables matter. Set
`EXTENDDB_TEST_S3_REQUIRED=1` to turn a missing endpoint into a test failure
instead of a skip; `.github/workflows/test.yml` sets it so the S3 suite
cannot be skipped silently in CI.

`EXTENDDB_TEST_S3_BUCKET` (default `extenddb-backup-store-tests`) and
`EXTENDDB_TEST_S3_REGION` (default `us-east-1`) can override the bucket and
region. The suite creates the bucket when it does not exist and isolates each
test under a unique key prefix.
