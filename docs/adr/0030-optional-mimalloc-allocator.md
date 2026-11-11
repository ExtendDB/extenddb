# ADR-0030: mimalloc is an opt-in global allocator for the server binary

- Status: Proposed
- Date: 2026-11-10
- Deciders: @parsnips

## Context

The `extenddb` binary uses the system allocator. On Linux that is glibc
malloc.

The request path allocates often. A request body becomes a `serde_json::Value`,
then `AttributeValue`s. A PartiQL statement becomes a `PutItem` body, which is
parsed again. The item becomes JSONB for the storage backend. Each step
allocates strings, maps and vectors, and frees them on one of many tokio worker
threads.

We measured the PostgreSQL build of `main` at 9c3fbfb under a load generator
that sends conditional inserts with unique keys to a table with a hash and a
range key and no indexes. It sends them as `PutItem` with
`attribute_not_exists` on the partition key, and as `ExecuteStatement` with an
`INSERT` statement. Each item has a 256-byte value. The host was a 24-core
x86_64 Linux machine. The server ran in a Debian 12 container (glibc 2.36),
with PostgreSQL 16 in a second container and connection pools of 20.

We profiled the server with `perf` (frame pointers on) under the `PutItem`
load with 32 requests in flight. Code in `libc.so.6` took 16% of the server's
samples, which is 22% of the samples in user space.

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

- Measured on the workload above, with 32 requests in flight for 10 seconds
  for each operation. Each measurement used a fresh database, and the two
  builds were the same apart from the `mimalloc` feature. The runs alternated
  between the builds. The host also ran other work, which disturbed one
  window (marked \*).

  | Operation | Allocator | Throughput (ops/s), per measurement | Server CPU per request (µs) |
  |---|---|---|---|
  | `PutItem` | glibc malloc | 15,092, 15,099, 14,992 | 257, 257, 251 |
  | `PutItem` | mimalloc | 15,679, 15,065, 15,415 | 226, 222, 227 |
  | `INSERT` | glibc malloc | 14,613, 6,616\*, 14,058 | 297, 319\*, 287 |
  | `INSERT` | mimalloc | 14,909, 14,595, 15,134 | 259, 263, 253 |

  Server CPU for each request fell by about 12% for both operations.
  Throughput rose by about 2% to 4%, which is close to the variation between
  measurements. With mimalloc, `libc.so.6` took 5% of the server's samples.
- Peak resident memory of the server after each measurement (about 600,000
  inserts) was 309 to 359 MB with glibc malloc and 398 to 428 MB with
  mimalloc.
- The `mimalloc` crate (0.1.52, which builds mimalloc v3) compiles its C
  sources with the `cc` crate. The workspace already builds C through `cc` for
  `aws-lc-sys`. The crate's `secure` feature (guard pages, encoded free lists)
  stays off.
- An opt-in feature does not change memory behaviour for existing
  deployments. mimalloc holds and returns memory differently from glibc, and
  resident memory was higher in the measurement above. Operators can measure
  it on their own workload before the default changes.
- A `#[global_allocator]` in the bin crate applies to the whole process and
  does not touch any library crate. A third-party thin bin can choose its own
  allocator.

## Consequences

- A build that spends less CPU in the allocator is one flag away:
  `cargo build --release --features mimalloc`.
- Release images and packages keep the system allocator until a later decision
  turns the feature on by default. That decision needs measurements from more
  workloads (read-heavy, Scan, the other backends) and of resident memory.
- The feature adds a C dependency to builds that turn it on.
- Reverting is a one-line change to the feature set.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.
