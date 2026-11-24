# PR 9 restore memory and duration record (2026-11-02; the index, 60,000-item, input-order and 3M-row runs added the same day)

This is the record the design cites for the restore (section 7, revision item
30 of `15-backup-restore.md`) and the admin guide's `max_concurrent_jobs` row
quotes. The scripts (`bench-restore-rss.sh`, `kill-points.py`) and the raw
samples (`/proc/<pid>/status` every 200 ms per run, the servers' logs) are
kept beside the PR outside the repository, on the measuring host; the figures
below are what they produced. The backup side's record is
`15-backup-restore-bench-pr8.md`.

## Method

The release binary (`cargo build --release -p extenddb --no-default-features
--features sqlite` and `--features postgres`, built from the PR's final tree)
running `serve --foreground` with a filesystem backup store on local disk.
Each run: a fresh deployment (`init`, the port patched, `[backup]
max_concurrent_jobs = 2`), the table loaded through the wire
(`kill-points.py create-table`: BatchWriteItem from 16 threads, composite
key, no GSI; or `put-worst` for the worst shape), `CreateBackup` and the wait
for `AVAILABLE`, then the server stopped and started again so the process's
high-water mark covers the restore alone; `RestoreTableFromBackup` into a new
table; `VmHWM` read right before the request (the idle figure) and once
`DescribeTable` reports `ACTIVE` (the peak), sampled every 200 ms in between;
"over idle" is the difference. The duration is from the request to `ACTIVE`
as the client saw it; the job's own line gives the same figure within 0.2 s.

The worst shape is #394's worst-expansion item (`data_file.rs`'s line-cap
table): a list of 2,262 number sets whose members are `1.0E-130`, 409 KB as
the service sizes an item, 27.7 MB as a data-file line, about 34 MB as a
decoded `Item`; each `worst<N>` table holds 1,000 small items and then `N`
such items. `big1200` is 1,200 items of a 350,000-byte string under one
hash key, written into one data file (`data_file_target_bytes` raised past
the table), the shape on which the SQLite batch was measured on the PR's first
build, before it had a byte bound. The 100k and 1M tables are 200-byte `S` pads under a
composite key and have no secondary index, and their data files are in key
order (the server's own backup of a table loaded through the wire is
`key_ordered`); every figure in the first table below is for that shape, a
table with secondary indexes restores in more memory on SQLite (the second
table), and an unordered backup of the same items restores in more memory and
time on SQLite (the input-order table). The 60,000-item runs are the same items
and key, no index: the shape the differences document's in-catalog restore
timings were measured on.

Host: 16 cores (Xeon Platinum 8124M), 30 GiB, Linux 5.10 (amzn2int), glibc
2.26; PostgreSQL 16.15 in a container on loopback (55432); the SQLite the
binary bundles. What was not held constant: the host ran other work during
the runs (the four hooks builds during the first six runs, with the load
average between 8 and 13 on 16 cores; a MongoDB kill-point run and the
matrix cells beside the later ones, load 2 to 7), so the durations are upper
bounds and comparable only within one back-to-back pair; the peaks are the
process's own and do not depend on the host's load. Everything is on
loopback and on a filesystem store: nothing here says what a network database
or an S3 store's download does to the duration, and the spool's disk is not
in these figures.

## The figures

| backend | shape | data files | idle `VmHWM` | peak `VmHWM` | peak − idle | `ACTIVE` after | downloaded |
|---|---|---|---|---|---|---|---|
| SQLite | worst 12 | 4 | 22.1 MiB | 870.5 MiB | **848 MiB** | 17.7 s | 3,793,555 B |
| SQLite | worst 24 | 8 | 20.4 MiB | 869.2 MiB | **848 MiB** | 26.8 s | 7,567,821 B |
| SQLite | worst 48 | 16 | 22.1 MiB | 1,035.1 MiB | **1,013 MiB** | 57.0 s | 15,116,357 B |
| SQLite | worst 24, `MALLOC_ARENA_MAX=1` | 8 | 20.2 MiB | 412.9 MiB | **392 MiB** | 20.5 s | 7,567,821 B |
| PostgreSQL | worst 24 | 8 | 21.4 MiB | 799.8 MiB | **778 MiB** | 22.5 s | 7,567,821 B |
| PostgreSQL | worst 24, `MALLOC_ARENA_MAX=1` | 8 | 21.4 MiB | 448.8 MiB | **427 MiB** | 22.7 s | 7,567,821 B |
| SQLite | big1200 (1,200 × 350 KB, one file) | 1 | 20.8 MiB | 291.5 MiB | **270 MiB** | 3.1 s | 450,922 B |
| PostgreSQL | big1200 | 1 | 21.4 MiB | 177.8 MiB | **156 MiB** | 3.3 s | 450,922 B |
| SQLite | 100k × 200 B | 1 | 21.9 MiB | 60.2 MiB | **38 MiB** | 2.9 s | 366,631 B |
| SQLite | 1M × 200 B, key-ordered lines | 4 | 21.2 MiB | 110.4 MiB | **89 MiB** | 26.8 s | 3,017,864 B |
| PostgreSQL | 100k × 200 B | 1 | 21.4 MiB | 32.0 MiB | **10.7 MiB** | 1.1 s | 366,631 B |
| PostgreSQL | 1M × 200 B, key-ordered lines | 4 | 21.5 MiB | 37.8 MiB | **16.2 MiB** | 9.5 s | 3,017,864 B |

The SQLite 1M run's high-water mark over time: 60 MiB at 2.7 s (the 100k
run's peak), rising to 87 MiB by 20.8 s as the load went on, then 110 MiB in
the last second, at the count and the flip. The growth during the load is the
write connection's page cache filling (`PRAGMA cache_size = -32000`,
`crates/storage-sqlite/src/sqlite_util.rs`: 31 MiB per connection, bounded by
the pool, as the backup record says of its 45 MiB); the final 23 MiB is the
`COUNT(*)` over the base table: the counting connection's page cache filling
with the table's b-tree pages, bounded by the same `cache_size` (32 MiB) and
measured at the same size at 3M rows (22 to 28 MiB, the input-order table
below), so it does not grow with the table past the cache. On PostgreSQL the 1M run's high-water mark reached 90% of its final
value within 3 s and the count added nothing visible. 1M − 100k: +51 MiB on
SQLite (+27 of them during the load), +5.5 MiB on PostgreSQL. Neither
materializes the table (1M items of 200 bytes are over 200 MB of items).

## The terms the code bounds

Per running restore, times `max_concurrent_jobs`:

- the parsed manifest, for the whole job (one entry per data file);
- the decoder's line, at most `MAX_LINE_BYTES` (32 MiB, `data_file.rs`), and
  the deserializer's intermediate value for it (the backup record's "one
  row's parse", `r + 2d`: 109 MiB at the line cap);
- the decoded items of up to `LOAD_CHUNK_BYTES` (32 MiB,
  `crates/engine/src/backup/restore/load.rs`) of data-file line text per
  chunk, the chunk closing at the first item that takes it to the bound (so at
  most the bound plus one line, 64 MiB of text at the cap) or at
  `LOAD_CHUNK_ITEMS` (1,000) items, at four chunk positions: two in the
  channel to `bulk_load`, the one being filled and the one being drained. The
  bound counts the bytes the decoder read for the chunk's items, the unit the
  backup side budgets in, not the service's `item_size_bytes` (which is 68
  times smaller for the worst shape and bounded nothing). `bulk_load` is one
  data file at a time, so the chunks in flight never hold more than one file's
  items: at most the source's `data_file_target_bytes` plus one line of text
  (64 + 32 MiB at the defaults), a knob the restoring host does not set;
- the backend's write batch: on SQLite and Cassandra at most
  `RESTORE_BATCH_BYTES` (32 MiB, `crates/storage/src/restore_engine.rs`) of
  serialized item text plus one item, or `RESTORE_BATCH_ITEMS` (10,000)
  items when they are smaller, held beside the decoded items of that text
  (SQLite collects `(Item, item_json)`; Cassandra serializes once to measure
  and again to write); on PostgreSQL at most `COPY_BATCH_BYTES` (4 MiB,
  `crates/storage/src/backup_definition.rs`) of item text plus one item, or
  `COPY_BATCH_ITEMS` (500) rows, as `Vec<(Item, String)>` (the item and its
  JSON text, two copies), one transaction per file; on MongoDB 1,000
  documents per `insert_many`, bounded in bytes only by the data file
  (`bulk_load` is per file), since the driver splits a call by the server's
  message bound on its own;
- on SQLite, the `COUNT(*)` after the last file: the counting connection's
  page cache (`PRAGMA cache_size = -32000`, 32 MiB), which the count fills
  with the base table's b-tree pages and which stops at that setting whatever
  the table holds (22 to 28 MiB measured at 1M and at 3M rows, the input-order
  table below).

None of the terms grows with the table: the chunk and batch terms grow with
the item, which `max_item_size_bytes` bounds, the line cap bounds the line,
and `cache_size` bounds the count's pages. These are the restore's own terms; a table with
secondary or vector indexes adds, on SQLite, the backend's own per-connection
page cache for the index b-trees (below), which is not a buffer of the
restore and which the batch bound does not cover. The decoded `Item` of the worst shape is about 1.2 times
its line, so at the line cap the chunk positions hold up to 4 × 64 MiB × 1.2 ≈
307 MiB of decoded items when a file holds that much, the SQLite or Cassandra
batch up to 32 MiB of text plus about 77 MiB of decoded items, and the
PostgreSQL batch 4 MiB plus one 32 MiB item twice. The measured worst-shape
peaks are above those terms under glibc's default allocator for the reason
the backup record gives: the items' strings are allocated on one thread (the
decoder's blocking thread) and freed on another (the backend's write), and
the per-thread arenas keep that memory, so the high-water mark grows with the
number of large items a restore reads (848 MiB at 12 and 24 items, 1,013 MiB
at 48 on SQLite, with no plateau measured) and varies between runs. Under
`MALLOC_ARENA_MAX=1` the peak is the terms' and did not grow from 12 to 24
items on the backup side; here it is 392 MiB (SQLite) and 427 MiB
(PostgreSQL) at 24 items.

Before the bounds (the figures measured the same way on the PR's first
build, kept beside this record): SQLite worst 12,
24 and 48 at 616, 891 and 925 MiB, worst 24 under one arena 370 MiB;
PostgreSQL worst 24 at 861 MiB, under one arena 428 MiB; the 1,200 × 350 KB
table on SQLite 435 MiB over idle (`VmRSS`, sampled every 100 ms in the
process that had taken the backup) against 127 MiB on PostgreSQL, the whole
file's items in one batch. With the batch bounded at 32 MiB of text the same
table holds 270 MiB on SQLite (`VmHWM` from a fresh start, so not the same
figure's method; the one-file table of 420 MB of items now loads in batches
of about 95 items) and 156 MiB on PostgreSQL. The worst-shape SQLite figures
with the default arenas moved with the allocator between the two series (616
to 848 at 12 items, 925 to 1,013 at 48), as the backup record's series did;
the one-arena figures, which measure the terms, are 370 to 392 MiB on SQLite
and 428 to 427 MiB on PostgreSQL.

## Tables with secondary indexes

The same 200-byte items under the same composite key with two GSIs
(`by-g0`, `by-g1`, hash key `g<i>` with 97 values, range key `sk`, `ALL`
projection), measured the same way (a fresh start, `VmHWM`), after every GSI
reported `ACTIVE` and with each GSI's `Scan` count checked against the item
count (equal in every run). "head" is the build of the tree before the
spool-sweep order, the `finish_restore` re-read and the `remove-target`
command were added, "final" the build with them; nothing in the restore's
load changed between the two, and the runs differ as the allocator's runs do.

| backend | shape | build | idle `VmHWM` | peak `VmHWM` | peak − idle | `ACTIVE` after | load average at start |
|---|---|---|---|---|---|---|---|
| SQLite | 100k × 200 B, two GSIs | head | 21.4 MiB | 164.8 MiB | **143 MiB** | 10.2 s | 2.9 |
| SQLite | 100k × 200 B, two GSIs | final | 21.3 MiB | 246.4 MiB | **225 MiB** | 12.3 s | 12.1 (release builds running) |
| SQLite | 100k × 200 B, two GSIs | final, repeated | 21.1 MiB | 164.1 MiB | **143 MiB** | 13.9 s | 1.8 |
| SQLite | 300k × 200 B, two GSIs | head | 22.0 MiB | 187.1 MiB | **165 MiB** | 51.7 s | 8.1 |
| SQLite | 300k × 200 B, two GSIs | final | 21.7 MiB | 193.5 MiB | **172 MiB** | 51.9 s | 6.1 |
| PostgreSQL | 100k × 200 B, two GSIs | head | 21.4 MiB | 50.8 MiB | **29.3 MiB** | 9.9 s | 8.4 |
| PostgreSQL | 100k × 200 B, two GSIs | final | 23.3 MiB | 46.2 MiB | **22.9 MiB** | 6.2 s | 4.6 |
| PostgreSQL | 300k × 200 B, two GSIs | head | 21.3 MiB | 52.1 MiB | **30.8 MiB** | 19.7 s | 4.8 |
| PostgreSQL | 300k × 200 B, two GSIs | final | 24.0 MiB | 57.6 MiB | **33.5 MiB** | 21.4 s | 9.7 |

With two GSIs the SQLite restore holds three to six times the no-index
figure at 100k (143 to 225 MiB in three runs against 38 and 43 MiB) and
grows from 100k to 300k (143 to 165 and 172 MiB); PostgreSQL holds 23 to
34 MiB against 10.7 MiB and grows little. In the SQLite runs the load itself
climbs to about 105 to 125 MiB of `VmRSS` before the last batch and the final
`COUNT(*)` and flip add the rest (50 MiB in the 143 MiB runs, 120 MiB in the
225 MiB run). The term is SQLite's, not the restore's: the restore's own
buffer is the one batch of `(Item, item_json)` bounded by `RESTORE_BATCH_BYTES`
whatever the indexes are, and the extra resident memory is the write
connection's page cache (`PRAGMA cache_size = -32000`, 32 MiB per connection,
`crates/storage-sqlite/src/sqlite_util.rs`) holding the index tables' b-tree
pages, which a load of scattered index keys dirties across the whole tree,
plus glibc's per-arena free lists (the figure falls by about a third under
`MALLOC_ARENA_MAX=1`: 72 MiB against 119 MiB at 100k in another run on the
same shape). It is bounded by `cache_size` per connection and grows
sub-linearly with the table, not with the restore's buffers; a smaller batch
for an indexed table was built and measured (91.7 MiB against 92.7 MiB over
idle at 100k in that pair of runs: the cache persists across batches, so
smaller transactions do not lower the peak) and not kept. The published
no-index figures stand for the no-index shape; size a SQLite host that
restores indexed tables for the no-index figure plus about 32 MiB of page
cache per running restore, and read the run-to-run spread above as the
allocator's.

## Input order and the count term

The 1M figures above are for the server's own backup, whose data files are
in key order. The same items (200-byte `S` pads, composite key, no index)
as an unordered backup, the prefix built by hand with the hash keys cycling
(`pk = i % 1000`, the shape of every MongoDB-sourced backup and every
multi-part-key table), and once more in key order (`pk = i // 1000`, 1,000
items per hash key), at 1M and at 3M rows, on two builds of this PR:
"committed", the pushed head's, and "final", the build after the last
corrections (the PostgreSQL vector-relation drop on a removed target, the
Cassandra progress re-read, the job's first beat before the reply; nothing in
the SQLite load changed between the two). Sampled every 50 ms.
"count + flip" is the resident set's growth in the restore's last stretch
(the peak less the `VmRSS` one second before `ACTIVE`), which is the final
`COUNT(*)` and the flip; the host's load average at the start is beside each
run (the hooks builds and other suites ran beside the SQLite runs).

| backend | shape | build | peak − idle | count + flip | `ACTIVE` after | load |
|---|---|---|---|---|---|---|
| SQLite | 1M, key-ordered lines | final | **98.1 MiB** | 21.9 MiB | 56.3 s | 16.2 |
| SQLite | 1M, unordered lines | final | **163.5 MiB** | 13.6 MiB | 90.3 s | 18.0 |
| SQLite | 1M, unordered lines | committed | **144.4 MiB** | 13.4 MiB | 66.2 s | 3.1 |
| SQLite | 3M, key-ordered lines | final | **109.3 MiB** | 27.6 MiB | 102.7 s | 19.1 |
| SQLite | 3M, key-ordered lines | committed | **116.4 MiB** | 22.9 MiB | 84.9 s | 4.5 |
| SQLite | 3M, unordered lines | final | **173.5 MiB** | 12.8 MiB | 264.9 s | 10.9 |
| SQLite | 3M, unordered lines | committed | **170.8 MiB** | 14.4 MiB | 274.7 s | 6.0 |
| PostgreSQL | 1M, unordered lines | final | **16.4 MiB** | 0.1 MiB | 17.5 s | 5.6 |

Two things the table shows. The input order is a term of SQLite's, not of
the restore's: an unordered backup of 1M items holds 1.5 to 1.8 times the
key-ordered figure (144 to 164 MiB against 87 to 98) and takes two to three
times as long, because scattered inserts dirty pages across the whole base
b-tree where ordered inserts append to its right edge, and the write
connection's page cache (`cache_size`, 32 MiB) plus the allocator's arenas
hold the difference; the restore's own buffers (the chunk and the batch) are
the same in both runs, and PostgreSQL does not depend on the order (16.2 MiB
ordered, 16.4 unordered). The ordered 1M run on this binary reproduced the
first table's figure (98 against 89 MiB over idle; 86.9 MiB in another run
of the same shape on the committed build). And the count's term does not grow
with the table: 22 to 28 MiB at 1M and at 3M rows in key order, 13 to 14 MiB
unordered at both sizes, every figure under the counting connection's
`cache_size` (32 MiB), which is what bounds it; from 1M to 3M rows the whole
peak moved by +11 MiB key-ordered and +7 to +10 MiB unordered while the
table tripled. The durations are upper bounds: the SQLite runs on the final
build ran under a load average of 11 to 19 (the ordered 1M run took 56 s
against 26.8 s on the quieter host of the first table).

## The 60,000-item shape, store-backed

The differences document's "How `RestoreTableFromBackup` runs" row quotes
response and `ACTIVE` times measured on an earlier build's in-catalog restore
of a 60,000-item table with no secondary index. The same shape (60,000 items
of 200-byte `S` pads under a composite key, no index) restored from a
filesystem store on the final binaries, the host's load average 5.4 to 7.6
with other runs beside it:

| backend | response | `ACTIVE` after | peak − idle | data files / bytes |
|---|---|---|---|---|
| SQLite | 0.016 s | 1.85 s | 38.3 MiB | 1 / 360,532 |
| PostgreSQL | 0.051 s | 0.67 s | 9.3 MiB | 1 / 360,532 |
| MongoDB | 0.055 s | 3.74 s (the job's DONE at 1.8 s; the backfill worker's flip after) | 30.1 MiB | 1 / 360,532 |

Cassandra was not measured on this shape (its own CreateBackup is in-catalog,
so a store-backed Cassandra restore needs a backup taken elsewhere and
adopted, which the kill-point and matrix runs do at 12,000 items). The
in-catalog figures the differences document quotes (response 0.032, 0.012,
0.053, 0.151 s; `ACTIVE` after 1.5, 2.1, 3.6, 11.2 s on PostgreSQL, SQLite,
MongoDB, Cassandra) were measured on another build and host and are not
comparable run for run; the store-backed response includes the sidecar and
manifest reads, and its `ACTIVE` time the download and the per-file
verification.

## The sizing sentence

Size a host for about 450 MiB per running restore of a table of large items
under `MALLOC_ARENA_MAX=1` on SQLite or PostgreSQL (392 and 427 MiB measured
at 24 items of 27.7 MB), times `max_concurrent_jobs`; under the default
arenas the peak grows with the number of large items a restore reads and no
bound was measured (1,013 MiB over idle at 48 such items on SQLite is the
largest seen; 778 MiB at 24 on PostgreSQL). A restore of 200-byte items holds
under 100 MiB on SQLite and under 20 MiB on PostgreSQL at 1M items when the
table has no secondary index and the backup's lines are in key order (the
server's own backups of hash-only and single-range-key tables); an unordered
backup of the same items (every MongoDB-sourced backup and every
multi-part-key table) held 144 to 164 MiB over idle on SQLite at 1M and 171 to
174 MiB at 3M (the input-order section), PostgreSQL 16 MiB either way; with two GSIs SQLite held 143 to 225 MiB at
100k and 165 to 172 MiB at 300k (the section above), PostgreSQL 23 to 34 MiB. A
restore is not "much less" than a backup of the same table: the backup
record's worst-shape figures are 466 to 1,281 MiB under the default arenas
and 452 to 481 MiB under one arena.

## Durations (the RTO baseline the testing summary names)

On this host, loopback, filesystem store: 1M items of 200 bytes in key order
restore in 9.5 s on PostgreSQL and 26.8 s on SQLite (100k: 1.1 s and 2.9 s;
60k: 0.67 s and 1.85 s, and 3.74 s on MongoDB); the same 1M items as an
unordered backup in 17.5 s on PostgreSQL and 66 to 90 s on SQLite, and 3M
items on SQLite in 85 to 103 s key-ordered and 265 to 275 s unordered (the
input-order section, on a host whose load average ran between 4 and 19); with two GSIs 100k in 6.2 to 9.9 s on
PostgreSQL and 10.2 to 13.9 s on SQLite and 300k in 19.7 to 21.4 s and 51.7
to 51.9 s, on a host whose load average ran between 1.8 and 12; 24 items of
27.7 MB in 22.5 s on PostgreSQL and 26.8 s on SQLite; 1,200 items of 350 KB in
3.3 s and 3.1 s. The backup side's 1M figures are in the PR 8 record (36 s on
PostgreSQL under a loaded host, 12.5 s on SQLite).

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
