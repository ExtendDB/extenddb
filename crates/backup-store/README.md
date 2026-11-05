# extenddb-backup-store

Backup object store abstraction for ExtendDB: one `BackupStore` trait with a
filesystem implementation and an S3 implementation built on the official
`aws-sdk-s3` crate. The engine writes portable backups through this trait. The
design overview (`docs/design/15-backup-restore.md`, #396) states the rules the
store's callers honour; this README is the record of what the store does.

## Behavior

- Keys are `/`-separated component paths. Both stores reject empty
  components, `.` and `..`, backslashes, control characters, U+FFFE and
  U+FFFF (S3 stores them, but its XML listings cannot carry them, so `list`
  and `delete_prefix` over the key would fail), components over 255 bytes
  (the common filesystem name limit), and keys over 1024 bytes. That rule
  is the upper bound both stores accept; each store narrows it, and the
  `key` module doc (`cargo doc -p extenddb-backup-store`) is the one list
  of those narrowings.
- The filesystem store canonicalizes its root at construction and resolves
  every key one component at a time without following symlinks: a symlink
  anywhere in a key's path, pointing inside or outside the root, is refused
  by `put`, `get` and `head`; `list` and `delete_prefix` resolve their
  prefix the same way and refuse a symlink above the final prefix component,
  treat a symlink at the prefix path itself as nothing there, and below the
  prefix skip symlinks (`list`) or remove them as links without following
  them (`delete_prefix`). Those checks are path-based and can be raced by a process that can
  write under the root, so the store requires that nobody but the owner can
  reach the tree and enforces it on Unix: `open` and `validate` refuse a
  root that grants any permission to group or others or is owned by another
  user, and refuse a root whose ancestor is owned by a third user or is
  writable by group or others without the sticky bit; every operation
  re-checks the root, refuses a root whose canonical path has changed since
  `open` (an ancestor moved or replaced by a symlink), and refuses any
  existing directory it descends into that fails the same owner-only rule,
  on the way to the prefix as well as below it;
  directories are created `0700` and objects `0600` at creation. Writes go
  through a temporary sibling file, which is flushed (so a full disk fails
  the put instead of storing a short object) and synced, then renamed into
  place, and the directory is synced; a put that does not reach the rename
  (an error, its future dropped by a caller's timeout or a cancelled job at
  any point before the rename, the temporary's own open included, a
  panicking body) removes the temporary from a drop guard and leaves an
  object that was already under the key untouched, and a failure after the
  rename (the directory sync) leaves the new, fully written object in
  place. A put never leaves a partial object under its key; only a process
  that ends mid-put without running the guard (killed, or exiting without
  waiting for the runtime's blocking threads) leaves a `.put-*.tmp`.
  Directories a failed put created stay, empty and invisible to `list`;
  `delete_prefix` of the prefix removes them. `list` reports only names that
  pass the key rule; anything else under the root (a non-UTF-8 name, a
  backslash, an in-flight `.put-*.tmp`) is skipped by `list` and removed by
  `delete_prefix`. The filesystem store's `list` walks the prefix and returns
  the whole listing as one `Vec` before the stream yields its first entry
  (about 72 bytes plus the key per object), so a caller that lists a large
  root holds the whole listing at once; the S3 store's listing is paged.
  `delete_prefix` checks every directory under the prefix
  against the owner-only rule before it removes anything, removes the
  prefix directory and everything under it but leaves the directories above
  the prefix, and treats an entry another deleter removed first as already
  gone (not counted; the trait doc defines the count). A `delete_prefix`
  whose future is dropped (a caller's timeout) stops before its next entry
  removal: its walk runs on the blocking pool and checks a flag the dropped
  future sets. `validate` counts a probe a concurrent `delete_prefix("")`
  removed first as removed. Paths that only a
  race or a crash reaches (a file removed between the walk and the open,
  `O_NOFOLLOW` on the open, re-checks of directories a concurrent process
  could have created or changed, the canonical check before a directory is
  created, the fsyncs) are kept as defence in depth and have no test; the
  `fs` module doc lists them.
- Consequence of the owner-only rule: a backup pickup agent (rsync, restic,
  borg) cannot read the tree as a member of the extenddb group; it has to
  run as the extenddb user or as root. A root under `/srv/backups` owned by
  a storage administrator, under an NFS mount owned by `nobody`, or under a
  group-writable parent such as a `root:staff 0775` `/srv`, is refused by
  the ancestor rule; put the root under a directory owned by root or by the
  extenddb user and not writable by group or others.
- `put` returns the stored object's metadata (bytes written and, on S3, the
  ETag with quotes stripped; a multipart ETag is not an MD5) so a manifest
  can be written from the put alone.
- The S3 store takes credentials from the AWS SDK's default chain (the
  `s3` module doc names its members, among them `credential_process`,
  which runs a command named in the shared config file, and the SSO
  provider, which rewrites its cached token file under `$HOME/.aws/sso`;
  without `region`, `AWS_REGION`, `AWS_DEFAULT_REGION` or a profile region,
  `open` queries IMDS for the region), uses
  multipart upload for bodies larger than the configured part size (default
  8 MiB, minimum 5 MiB; S3's 10,000-part limit caps one object at about
  78 GiB at the default; a larger body is refused before part 10,001),
  aborts the multipart upload when a put fails or its future is dropped
  (including while `CreateMultipartUpload` or the abort after a failure is
  in flight), except at runtime shutdown (below), paginates
  listings, skips listed keys that fail the key rule (console folder
  markers), deletes prefixes in DeleteObjects batches of 1000, and aborts
  any in-progress multipart upload under a deleted prefix (component-aligned,
  like the object deletion). A process killed mid-put leaves an upload only
  the next `delete_prefix` of that prefix sweeps; a bucket lifecycle rule
  `AbortIncompleteMultipartUpload` (one day) is the backstop for uploads
  under a prefix nothing deletes, for a `delete_prefix` whose
  credentials lack `s3:ListBucketMultipartUploads` or
  `s3:AbortMultipartUpload` (it logs a warning and succeeds once the
  objects are gone; `validate` reports either missing grant at startup),
  and for parts that were still in
  flight when a dropped put's abort ran: S3 documents that an
  `AbortMultipartUpload` may have to be repeated to free every part, and
  the guard sends one. Runtime shutdown sends no abort: the guard's abort
  is a task on the put's runtime, and a runtime that shuts down with puts
  in flight (a server stopping on SIGTERM without awaiting its jobs) drops
  those tasks unpolled, an in-flight `CreateMultipartUpload` included. Those
  uploads, parts included, stay until a `delete_prefix` covering the key
  sweeps them or the lifecycle rule removes them, so a caller that must
  leave nothing awaits its puts, or runs its failure path's
  `delete_prefix`, before the runtime shuts down. One case leaves an empty
  upload (no parts, so nothing billed) that only the lifecycle rule
  removes: a `CreateMultipartUpload` the SDK retried after the server had
  created the first upload, whose id the store never sees. (A `validate`
  dropped while its probe `CreateMultipartUpload` is in flight aborts the
  probe upload, as a put does.) Every request has a 60 s attempt timeout
  and a 300 s operation timeout; the attempt includes sending a part, so each part must
  upload within 60 s and `upload_part_size_bytes` should be at most about
  the link's bytes per second times 60 (the default 8 MiB needs about
  140 KB/s, 5 GiB about 90 MB/s). Those timeouts end with a `GetObject`
  response's headers; the body is bounded per chunk, not per request: a
  chunk that does not arrive within 60 s fails the read with a transport
  error (the SDK's own stalled-stream protection fails a body under 1 B/s
  after 5 s first). Resolving credentials (the whole provider chain, every
  `AssumeRole` hop and an SSO refresh included) is bounded separately by
  the SDK's 5 s identity-cache load timeout: a slow credential path fails
  the request with `identity resolver timed out after 5s`, not after 60 s.
  The store targets general-purpose buckets: S3 Express
  One Zone directory buckets return listings in no defined order, and the
  trait promises key order. `open` refuses the two bucket names the SDK
  routes to S3 Express, a directory bucket (the `--x-s3` suffix) and a
  directory-bucket access point (`--xa-s3`), and the client is built with
  S3 Express session authentication disabled, so even a client that reached
  such a name would send no `CreateSession`, which the SDK sends from a
  client without the store's request check.
- A put that overlaps a `delete_prefix` covering its key may fail or may
  leave its complete object after the deletion returns (on both stores);
  the store gives no ordering between them, and callers must not put under
  a prefix they are deleting. A `delete_prefix` whose future is dropped
  stops at its next step: the S3 store at its next request, the filesystem
  store before its next entry removal; what it removed stays removed.
- On a bucket with versioning enabled (an Object Lock bucket always is),
  `delete_prefix` adds delete markers: the noncurrent versions of every
  deleted object stay and are billed, and the markers stay after those
  versions expire. Each `validate` also leaves one delete marker, under
  `<prefix>.extenddb-validate-<uuid>`. Add `NoncurrentVersionExpiration`
  and `Expiration { ExpiredObjectDeleteMarker: true }` lifecycle actions
  (alongside `AbortIncompleteMultipartUpload`) to such a bucket, or use an
  unversioned bucket.
- `validate` on S3 lists one key under the prefix rather than calling
  `HeadBucket`: the error body names the problem (`NoSuchBucket`,
  `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch`, a redirect
  to another region). `head` on a denied or missing bucket cannot: HEAD
  responses have no body, so a denial reads as `HTTP 403 (no error code in
  the response)` and a missing bucket as an absent key; any response
  without an S3 error code (an HTML page from a proxy) is reported by its
  status the same way. It then probes each other grant the
  store uses and names the missing one on a denial; the probe list, and
  what it leaves (no object or upload; on a versioned bucket, one delete
  marker), are documented once, on `validate` in `impl BackupStore for
  S3Store` (`src/s3.rs`). Denials keep S3's code and message, which names
  the action and the policy or KMS key that refused it. Transport failures
  (no region, no credentials, an unreachable endpoint) are reported with
  their cause. `InvalidToken`, `ExpiredToken` and `TokenRefreshRequired`
  are reported as permission errors with their code.
- IAM: two resources. `s3:ListBucket` and `s3:ListBucketMultipartUploads`
  are bucket actions and go on the bucket ARN (a statement on
  `bucket/<prefix>*` alone grants neither, and `validate` then fails at its
  first request with "lack s3:ListBucket"); `s3:ListBucket` can be limited
  to the prefix with an `s3:prefix` condition, `s3:ListBucketMultipartUploads`
  has no prefix condition key. `s3:GetObject`, `s3:PutObject`,
  `s3:DeleteObject` and `s3:AbortMultipartUpload` are object actions and go
  on `bucket/<prefix>*`. A minimal policy for prefix `extenddb/`:

  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": "arn:aws:s3:::my-extenddb-backups",
        "Condition": { "StringLike": { "s3:prefix": "extenddb/*" } }
      },
      {
        "Effect": "Allow",
        "Action": "s3:ListBucketMultipartUploads",
        "Resource": "arn:aws:s3:::my-extenddb-backups"
      },
      {
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
        "Resource": "arn:aws:s3:::my-extenddb-backups/extenddb/*"
      }
    ]
  }
  ```

  With an empty `prefix` the example drops the `s3:prefix` condition.

  An SSE-KMS bucket also needs `kms:GenerateDataKey` and `kms:Decrypt` on
  its key; `validate` checks those only in
  part (the probe `CreateMultipartUpload` needs `kms:GenerateDataKey`;
  nothing it sends needs `kms:Decrypt`, which `UploadPart` and `GetObject`
  do).
- S3-compatible servers. Only moto has been run against this crate. The
  SDK sends a CRC32 request checksum (as an `x-amz-checksum-crc32` request
  header) on every `PutObject`, `UploadPart` and
  `DeleteObjects` by default and validates response checksums; some
  S3-compatible servers reject or mishandle those (`NotImplemented`,
  `XAmzContentSHA256Mismatch`). The SDK's own knobs apply, no store setting
  exists: `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED` and
  `AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED` in the environment, or
  `request_checksum_calculation` / `response_checksum_validation` in the
  shared config profile. TLS trust is the system certificate store unless
  `SSL_CERT_FILE` or `SSL_CERT_DIR` is set, in which case only those are
  loaded (the SDK does not read `AWS_CA_BUNDLE`); a server with a private
  CA needs its CA in the system store or in one of them. A file or
  directory without a parsable certificate makes every `https://` request
  fail, and aborts `open` in a debug build.
- The AWS SDK is built with aws-config's `sso` feature (an `sso_session`
  profile in the shared config works); besides the SSO clients that
  feature links `ring`, `hex` and `zeroize`, a second cryptography library
  beside the `aws-lc-rs` the TLS stack uses.

## Configuration

`BackupStoreConfig` models the `[backup.store]` table of the server
configuration (the server adopts it when the backup job lands; see the
design doc's PR plan). The table's shape, keys and defaults are documented
once, in the `config` module doc (`cargo doc -p extenddb-backup-store`).
Only the section matching `kind` is read; `open` fails at startup when it is
missing or a value is unusable (an empty bucket, an S3 Express bucket
name, a part size under 5 MiB or over 5 GiB, zero concurrency, a malformed
or over-long prefix, an unusable filesystem root). Every endpoint setting
the AWS SDK reads for a client that sends requests for the store, with one
exception below, is held at `open` to the syntax rule and the clear-text
rule (loopback hosts only, and `force_path_style = true` for an `http://`
S3 endpoint, unless `allow_insecure_endpoint = true`), both stated once in
the `config` module doc: the S3 client's (`endpoint`, or
`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL` or the shared config); the STS,
SSO and SSO OIDC endpoints the SDK's credential providers send to
(`AWS_ENDPOINT_URL_STS`, `AWS_ENDPOINT_URL_SSO`, `AWS_ENDPOINT_URL_SSO_OIDC`,
`AWS_ENDPOINT_URL`, the shared config), whether or not the configured
credentials use them; and the IMDS endpoint
(`AWS_EC2_METADATA_SERVICE_ENDPOINT`, or the profile's
`ec2_metadata_service_endpoint`), which may also name IMDS's link-local
address, 169.254.169.254 or `[fd00:ec2::254]`. The exception is
`AWS_CONTAINER_CREDENTIALS_FULL_URI`, which keeps the SDK's own rule
(`http://` to loopback or the container addresses only, or `https://`). A
refusal names the client and the setting the endpoint came from and never
the value, and a refused configuration sends no request. Every request the
S3 client sends is checked again before it is sent; the credential
providers' requests are checked only through their endpoints at `open`. An
`http://` S3 endpoint is reached without the environment's proxy, and so
is every request of the credential chain when any of its endpoint settings
(the STS, SSO, SSO OIDC or general setting, the container URI or the IMDS
endpoint) is `http://`; a local credential emulator or sidecar therefore
needs no `NO_PROXY` entry. Without such a setting the chain uses the SDK's
default client, and its two `http://` requests, IMDS at its link-local
address and the container credentials client at 169.254.170.2 (when
`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` is set), go through `HTTP_PROXY`
or `ALL_PROXY` unless `NO_PROXY` lists those addresses. A SOCKS proxy
(`ALL_PROXY=socks5://...`) makes every `https://` request fail: the
connector speaks only HTTP `CONNECT`. The AWS SDK is built with SSO
support, so an `sso_session` profile in the shared config works.

Logging. The store's own messages (its errors and warnings) carry no
credential and never repeat an endpoint value. The SDK's `trace` level does:
`aws_sigv4` logs the canonical request with the session token,
`aws_smithy_runtime` the request with its `authorization` header and
signature, and `aws_runtime` the configured endpoint, user information
included. Do not enable those targets at `trace` in production.

## Tests

`cargo test -p extenddb-backup-store` runs the key validation,
configuration and S3 error-mapping unit tests, the filesystem conformance
suite against a temporary directory, and the S3 conformance suite.

A few filesystem tests (another user owning the root, a subdirectory, or an
ancestor of the root) need to chown and run only as root; elsewhere they
print a skip reason, or fail when `EXTENDDB_TEST_REQUIRE_ROOT` is set. CI
builds the conformance binary as the normal user and runs it as root with
that variable set, so nothing in `target/` ends up owned by root and a run
that is not root fails:

```sh
cargo test --locked -p extenddb-backup-store --test conformance --no-run
sudo env EXTENDDB_TEST_REQUIRE_ROOT=1 target/debug/deps/conformance-<hash> filesystem::
```

Most of the S3 suite needs a local S3-compatible endpoint and is skipped
with a printed reason when `EXTENDDB_TEST_S3_ENDPOINT` is unset. Thirteen
S3 tests run without one: the two `open` refusals of an over-long prefix
and an over-long key, the client configuration read-back, the transport
failure against a closed port, the SSO profile, the endpoints from the SDK
environment, the credential providers' endpoints and the IMDS endpoint (a
listener on a non-loopback address of the host must receive nothing; on a
host without one the two tests check the refusals only and say so), the
credential providers' requests reaching their loopback endpoint and not
the environment's proxy, the two S3 Express bucket names refused at
`open`, the client sending no `CreateSession` to such a name, the
request-time clear-text refusal, and an `http://` endpoint reached without
the environment's proxy. None of them needs
anything from the environment: every test that sends a request, or opens a
store whose credentials matter, runs in a child process with its own
credentials (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` `testing`, a web
identity token file, an SSO token cache or a container authorization
token), `AWS_REGION` and `AWS_EC2_METADATA_DISABLED` (except the cells
whose IMDS endpoint is a loopback listener), and without the parent's
proxy variables, so the whole crate passes with no
endpoint, no credentials and an empty `HOME`. The five tests that put a
TCP proxy in front of the endpoint need an `http://` one and skip with a
printed reason against `https://`. CI uses
[moto](https://github.com/getmoto/moto) in server mode; its version and
those of its dependencies are pinned in `requirements-moto.txt` at the
repository root:

```sh
pip install -r requirements-moto.txt
moto_server -p 9000 &

EXTENDDB_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=testing \
AWS_SECRET_ACCESS_KEY=testing \
AWS_REGION=us-east-1 \
cargo test -p extenddb-backup-store
```

Another S3-compatible server (MinIO built from source, Garage, LocalStack)
should work the same way through the endpoint and credential variables,
but none has been run; see the checksum note in "Behavior". What moto
cannot produce is synthesised in the suite (a 301, a 403 with its error
body, a per-key `DeleteObjects` error, `ListMultipartUploads` truncation,
which moto never does, and responses held, swallowed or cut after their
headers through a TCP proxy); the test file's module doc lists each. Set
`EXTENDDB_TEST_S3_REQUIRED` to any value to turn a missing endpoint into a
test failure instead of a skip; `.github/workflows/test.yml` sets it so the
S3 suite cannot be skipped silently in CI.

`EXTENDDB_TEST_S3_BUCKET` (default `extenddb-backup-store-tests`) and
`EXTENDDB_TEST_S3_REGION` (default `us-east-1`) can override the bucket and
region. The suite checks for the bucket once per process with `HeadBucket`,
creates it only when it is missing, and isolates each test under a unique
key prefix.
