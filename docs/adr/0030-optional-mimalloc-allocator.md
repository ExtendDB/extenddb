# ADR-0030: mimalloc is an opt-in global allocator for the server binary

- Status: Accepted
- Date: 2026-11-10
- Deciders: @parsnips

## Context

The `extenddb` binary uses the system allocator. On Linux that is the malloc
of the C library that the binary links. The container images and a
`cargo build` on a glibc host use glibc malloc. The Linux binaries of the npm
packages are static musl builds, and they use the musl malloc. This ADR
measured glibc only.

The request path allocates often. A request body becomes a `serde_json::Value`,
then `AttributeValue`s. A PartiQL statement becomes a `PutItem` body, which is
parsed again. The item becomes JSONB for the storage backend. Each step
allocates strings, maps and vectors, and frees them on one of many tokio worker
threads.

We measured the PostgreSQL build of `main` at 9c3fbfb, with and without the
feature, under conditional inserts with unique keys. The method below gives
the load, the settings and how each figure was read.

In a `perf` profile of the server under the `PutItem` load with 32 requests in
flight, code in `libc.so.6` took 16% of the server's samples, which is 22% of
the samples in user space.

### Method

- **Builds.** `cargo build --release --locked -p extenddb`, and the same
  command with `--features mimalloc`, in the `rust:1.97-bookworm` image (rustc
  1.97.1, glibc 2.36) with `RUSTFLAGS=-Cforce-frame-pointers=yes`. The two
  builds differ in the feature only.
- **Host.** One machine ran the load generator, the server and PostgreSQL: an
  AMD Ryzen 9 9900X (12 cores, 24 threads) with 32 GB of memory, on Linux
  x86_64. The containers had no CPU or memory limit. The host also ran other
  work.
- **PostgreSQL.** The `postgres:16-alpine` image, with `--shm-size 2g`, a data
  directory made with `initdb --no-sync`, and these settings:
  `max_connections=400`, `fsync=off`, `synchronous_commit=off`,
  `full_page_writes=off`, `checkpoint_timeout=1h`, `max_wal_size=2GB`,
  `shared_buffers=2GB`, `effective_cache_size=6GB`, `work_mem=64MB`,
  `maintenance_work_mem=512MB`, `wal_buffers=64MB`.
- **Server.** The binary in a `debian:bookworm-slim` container on the host
  network, after `extenddb init`, with TLS on. `[storage.postgres]` has
  `pool_size = 20` and `catalog_pool_size = 20`. The settings
  `control_plane_delay_seconds` and `gsi_propagation_delay_ms` are 0, and
  `throttling_enabled` is false. The tokio runtime has its default number of
  worker threads, which is one for each of the 24 logical CPUs.
- **Load generator.** A Go program that uses the AWS SDK for Go v2. Its
  source and the script that runs one measurement are in the description of
  pull request #954. It creates a table with a binary hash key and a binary
  range key, on-demand billing, no index and no stream. Each request inserts
  an item with a new 16-byte hash key, a 1-byte range key and a 256-byte
  binary value. The `put` operation sends `PutItem` with
  `attribute_not_exists(#pk)`. The `partiql` operation sends
  `ExecuteStatement` with `INSERT INTO "<table>" VALUE {...}` and three
  parameters. For each request in flight, one goroutine sends requests one
  after another on its own HTTPS connection. The requests have SigV4
  signatures and are not sent again after an error. The commands:

  ```
  extenddb-load -endpoint https://127.0.0.1:28443 -ca <cert.pem> \
    -levels 1,8,32,128 -duration 10s -op put
  extenddb-load -endpoint https://127.0.0.1:28443 -ca <cert.pem> \
    -levels 1,8,32,128 -duration 10s -op partiql
  ```

  Each level is one step of 10 seconds, after one request on each connection
  to open it. Each command deletes its table when it ends.
- **One measurement.** A new PostgreSQL container, a new deployment and a new
  server process, then the two commands: eight steps. We made three
  measurements of each build and alternated between the builds.
- **Server CPU per request.** A shell loop read `utime + stime` of the server
  process from `/proc/<pid>/stat` every 100 ms. The CPU time of a step is the
  value at the end of the step minus the value 10 seconds before, at 100 ticks
  for each second. That time divided by the number of requests that the step
  completed is the figure.
- **Peak resident memory.** `VmHWM` of the server process from
  `/proc/<pid>/status`, read one time, after the eighth step. It is the peak
  of the whole measurement and not of one step.
- **Profile.** One more run of each build, with one step of 40 seconds at 32
  requests in flight for each operation. `perf record -F 999 -g -p <pid> --
  sleep 25` started 6 seconds into the `PutItem` step, and
  `perf report --no-children --sort dso` gave the share of each shared object.

## Options Considered

1. **Keep the system allocator.** No change.
2. **mimalloc as an opt-in feature.** A `mimalloc` feature on the bin crate
   sets `mimalloc::MiMalloc` as the `#[global_allocator]`. Default builds do
   not change.
3. **mimalloc by default.** The same change with the feature on in the default
   feature set, so every release binary uses it.
4. **jemalloc (`tikv-jemallocator`).** Not measured here. Its build needs
   `make` and runs a configure script, and it does not build for MSVC
   targets.

## Decision

The bin crate has an opt-in `mimalloc` feature that makes mimalloc the global
allocator (Option 2).

## Rationale

- The steps with 32 requests in flight, from the three measurements of each
  build. Other work on the host disturbed one step (marked \*).

  | Operation | Allocator | Throughput (ops/s), per measurement | Server CPU per request (µs) |
  |---|---|---|---|
  | `PutItem` | glibc malloc | 15,092, 15,099, 14,992 | 257, 257, 251 |
  | `PutItem` | mimalloc | 15,679, 15,065, 15,415 | 226, 222, 227 |
  | `INSERT` | glibc malloc | 14,613, 6,616\*, 14,058 | 297, 319\*, 287 |
  | `INSERT` | mimalloc | 14,909, 14,595, 15,134 | 259, 263, 253 |

  Server CPU for each request fell by about 12% for both operations.
  Throughput rose by about 2% to 4%, which is close to the variation between
  measurements. With mimalloc, `libc.so.6` took 5% of the server's samples.
- Resident memory did not go in one direction, so this ADR makes no claim
  about it. `VmHWM` at the end of each measurement, with the number of inserts
  that its eight steps completed, to the nearest thousand:

  | Allocator | Inserts, per measurement | `VmHWM` (MiB) |
  |---|---|---|
  | glibc malloc | 839,000, 630,000, 774,000 | 351, 302, 327 |
  | mimalloc | 862,000, 804,000, 809,000 | 418, 389, 399 |

  The review of this change used a different client (32 processes, with one
  connection each). After 62,000 to 82,000 inserts, a server ended at 92.5 to
  100.5 MB with glibc malloc and at 121.0 to 126.5 MB with mimalloc. After
  about 500,000 inserts, it ended at 231.2 MB with glibc malloc and at
  221.5 MB with mimalloc. In the two sets of figures, memory with mimalloc was
  higher in some runs and lower in one.
- The `mimalloc` crate (0.1.52, which builds mimalloc v3) compiles its C
  sources with the `cc` crate. The workspace already builds C through `cc` for
  `aws-lc-sys`. The crate's `secure` feature (guard pages, encoded free lists)
  stays off.
- An opt-in feature does not change memory behaviour for existing
  deployments. mimalloc holds and returns memory differently from glibc, and
  the figures above do not show which allocator holds less. Operators can
  measure it on their own workload before the default changes.
- A `#[global_allocator]` in the bin crate serves the allocations of all Rust
  code in the process, and no library crate changes. It does not replace
  `malloc` for C code: the `mimalloc` crate's `override` feature is off, so C
  libraries in the binary, such as AWS-LC, keep the system allocator. A
  third-party thin bin can choose its own allocator.

## Consequences

- A build that spends less CPU in the allocator is one flag away:
  `cargo build --release --features mimalloc`.
- Release images and packages keep the system allocator until a later decision
  turns the feature on by default. That decision needs measurements from more
  workloads (read-heavy, Scan, the other backends), of resident memory in long
  runs, and of a musl build.
- `MALLOC_ARENA_MAX` is a setting of glibc malloc. The backup and restore
  manual and the deployment guide describe it for hosts that run backups and
  restores, and their memory figures are for the system allocator. In a build
  with the feature, Rust allocations do not go through glibc malloc, so the
  variable does not bound them.
- The feature adds a C dependency to builds that turn it on.
- Reverting is a one-line change to the feature set.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
