# PR 11 backup and restore record: the open cells (2026-11-05)

This is the record the testing summary of `15-backup-restore.md` cites for
the cells the PR 8 and PR 9 records left open: a 1M-item restore on MongoDB
and on Cassandra, and the backup and restore of a 1M-item table carrying a
vector index on PostgreSQL and on SQLite. The script (`bench-pr11.py`) and
the raw samples (`/proc/<pid>/status` every 200 ms per phase, the servers'
logs, one JSON result per run) are kept beside the PR outside the
repository, on the measuring host; the figures below are what they
produced. The earlier records are `15-backup-restore-bench-pr8.md` (the
backup) and `15-backup-restore-bench-pr9.md` (the restore).

## Method

The PR 9 record's method. The release binaries (`cargo build --release -p
extenddb` for PostgreSQL, `--no-default-features --features sqlite`,
`mongodb` and `cassandra` for the others), built from this PR's tree, whose
Rust sources are PR 10's, running `serve --foreground` with a filesystem
backup store on local disk. Each run: a fresh deployment (`init`, the port
patched, `[backup] max_concurrent_jobs = 2`, and, after provisioning,
`control_plane_delay_seconds = 0.05` and `index_propagation_delay_ms = 0`
set with `extenddb settings set`), the table loaded through the wire, then
for each measured phase the server stopped and started again so the
process's high-water mark covers that phase alone; `VmHWM` read one second
after the start (the idle figure) and one second after the phase ended (the
peak), sampled every 200 ms in between; "over idle" is the difference. A
backup ends when `DescribeBackup` reports `AVAILABLE`, a restore when
`DescribeTable` reports the table and every vector index `ACTIVE`, each
timed from the request as the client saw it; the job's own log line gives
the same figure within 0.3 s (on MongoDB the job's line is its `DONE`,
below). After each restore the target was counted with a paged `Scan`
(`Select = COUNT`), equal to the items written in every run, and on a vector
table searched with `SearchVectors` for item 7's own vector, which came
first in every run.

The table: a 200-byte `S` pad of one repeated character (`"x" * 200`) under
a composite key (`pk` cycling over 1,000 values, `sk` the item number), no
secondary index, loaded with BatchWriteItem from four client processes of
eight threads each (the load is not measured). The vector shape adds an
`emb` attribute to every item, a list of 128 numbers (seeded uniform values
in [-1, 1], six decimals), and a `COSINE` vector index on it with no hash
element; its data files are about 2.5 KB of line text per item, 9.5 times
the plain shape's 265 bytes, and 532 bytes compressed against 3.0 to 5.9
bytes for the plain shape (90 to 176 times; 176 for the 1M key-ordered
backup, 3.0 bytes per item, 145 at 100k, and 90 for MongoDB's unordered 1M
backup: the plain pad is one repeated character, so its 265-byte lines
compress to 3 to 6 bytes). The vector cells' durations are not the index's
cost alone.

MongoDB restores its own backup, whose data files are unordered (the job's
log line says so). Cassandra's own CreateBackup is
in-catalog, so the Cassandra cell restores the SQLite build's store-backed
backup of the same table (key-ordered lines): the prefix is copied under the
root the Cassandra deployment owns and adopted with `extenddb backup sync`,
as `devtools/backup-restore-matrix` does, and the server is started again
before the restore.

Host: 16 cores (Xeon Platinum 8124M), 30 GiB, Linux 5.10 (amzn2int);
PostgreSQL 16.15 with pgvector 0.8.7 in a container on loopback (55432); the
SQLite the binary bundles (libsqlite3-sys 0.30.1); MongoDB 7.0.43,
single-node replica set in a container on loopback; Cassandra 4.1.12, a
single node in a container on loopback started as the Cassandra integration
workflow starts its service (`MAX_HEAP_SIZE=1024M`, `HEAP_NEWSIZE=128M`,
`PasswordAuthenticator`), used by these runs alone. What was not held
constant: the host ran other work; the 1-minute load average at each phase's
start is beside each figure (1.3 to 6.4, and 12.6 at the start of the
PostgreSQL 1M vector backup, which began as its table's load ended and ended
at 1.7), so the durations are upper bounds, and each figure is one run. The
peaks varied by up to about 20% between runs of one cell, idle or loaded:
the SQLite 100k vector restore's peak over idle was 344.6 to 409.0 MiB over
nine runs (this record's cell and the runs below, two of them pinned to two
cores, idle and beside four busy loops), and the PostgreSQL 100k restore of
the same items with no vector index 60.0 to 71.4 MiB over five. Everything is on
loopback and on a filesystem store: nothing here says what a network
database or an S3 store's upload or download does to the duration, and the
spool's disk is not in these figures. Every run is under glibc's default
arenas (glibc 2.26) except the four rows marked `MALLOC_ARENA_MAX=1`.

## Restores

| backend | shape | data files | idle `VmHWM` | peak `VmHWM` | peak − idle | `ACTIVE` after | downloaded | load |
|---|---|---|---|---|---|---|---|---|
| MongoDB | 100k × 200 B, unordered lines | 1 | 23.5 MiB | 58.6 MiB | **35.1 MiB** | 3.92 s | 573,993 B | 2.7 |
| MongoDB | 1M × 200 B, unordered lines | 4 | 23.5 MiB | 75.3 MiB | **51.8 MiB** | 33.81 s | 5,932,752 B | 3.4 |
| Cassandra | 100k × 200 B, key-ordered lines | 1 | 22.5 MiB | 42.4 MiB | **19.9 MiB** | 6.12 s | 366,631 B | 1.3 |
| Cassandra | 1M × 200 B, key-ordered lines | 4 | 23.2 MiB | 56.3 MiB | **33.1 MiB** | 48.99 s | 3,017,864 B | 3.2 |
| PostgreSQL | 100k, 128-dimension vector | 4 | 21.4 MiB | 122.0 MiB | **100.6 MiB** | 25.15 s | 53,202,407 B | 4.9 |
| PostgreSQL | 1M, 128-dimension vector | 38 | 21.5 MiB | 132.6 MiB | **111.1 MiB** | 246.41 s | 531,977,239 B | 1.7 |
| SQLite | 100k, 128-dimension vector | 4 | 22.8 MiB | 376.8 MiB | **354.0 MiB** | 21.78 s | 53,202,407 B | 3.1 |
| SQLite | 1M, 128-dimension vector | 38 | 21.7 MiB | 573.8 MiB | **552.1 MiB** | 214.23 s | 531,977,239 B | 1.9 |
| SQLite | 100k, 128-dimension vector, `MALLOC_ARENA_MAX=1` | 4 | 22.8 MiB | 218.1 MiB | **195.3 MiB** | 27.93 s | 53,202,407 B | 2.8 |
| SQLite | 1M, 128-dimension vector, `MALLOC_ARENA_MAX=1` | 38 | 23.1 MiB | 227.0 MiB | **203.9 MiB** | 282.86 s | 531,977,239 B | 2.4 |

The restore request answered in 0.014 to 0.181 s in every run (0.05 s on
MongoDB, 0.17 to 0.18 s on Cassandra, 0.06 s on PostgreSQL, 0.014 to
0.016 s on SQLite). On MongoDB the job's `DONE` ends the item load (33.6 s
at 1M), and the backend's backfill worker flips the table `ACTIVE` at its
next cycle: it runs every `GSI_BACKFILL_INTERVAL`, 5 s
(`crates/storage-mongodb/src/ttl_worker.rs`), and the end of a restore does
not wake it. A MongoDB restore's duration is therefore the job's plus up to
5 s: 3.92 s here, and 8.94 and 8.92 s in two runs at 100k whose job took
3.9 s (below). On Cassandra the 1M high-water mark reached 90% of its
final value at 11.5 s of 49.9, on MongoDB at 18.9 s of 34.7; those times
include the idle figure, and over idle they are 11.7 s and 21.1 s.

From 100k to 1M the peak over idle moved +16.7 MiB on MongoDB, +13.2 MiB on
Cassandra and +10.5 MiB on PostgreSQL with a vector index. On PostgreSQL the
1M run's mark reached 90% of its final value at 51 s of 247 (57.8 s over
idle) and moved by 1 MiB after 150 s.

SQLite with a vector index holds more, and under the default arenas its peak
is higher at 1M than at 100k (+198 MiB, 354 to 552 MiB over idle); the 1M
run levelled off after about 150 s, and no other size was measured. The 1M
run's `VmHWM` was 363 MiB at 15 s, 484 MiB at 90 s and 574 MiB at 150 s,
flat from there to the end at 215 s, with `VmRSS` 516 MiB at the end. Under
`MALLOC_ARENA_MAX=1` the same restore holds 195 MiB over idle at 100k and
204 MiB at 1M (+8.6 MiB; `VmHWM` 193 MiB at 20 s and 225 MiB at 60 s,
within 2 MiB of that to the end), so the growth is the allocator's, the
pattern the PR 9 record explains for its worst-expansion shape (items
allocated on the decoder's thread and freed on the backend's write, the
per-thread arenas keeping that memory). What stays is the restore's terms
on this shape: a write batch of up to `RESTORE_BATCH_ITEMS` (10,000) items,
here about 25 MB of line text with each item's 128 numbers decoded beside
it, and, as the PR 9 record found for indexed tables, the write
connection's page cache (`cache_size`, 32 MiB per connection) over the base
and index tables. The decoded size of one such item was not measured. One
arena cost this restore 32% in duration (282.9 s against 214.2 s, one run
each); with two 100k restores running at once it cost 131% (57.5 s against
24.9 s for the pair, 278.9 against 696.7 MiB over idle; the later runs
below).

## Backups of the same tables

| backend | shape | idle `VmHWM` | peak `VmHWM` | peak − idle | `AVAILABLE` after | backup size | load |
|---|---|---|---|---|---|---|---|
| MongoDB | 100k × 200 B | 24.5 MiB | 33.8 MiB | **9.3 MiB** | 1.45 s | 573,993 B | 3.0 |
| MongoDB | 1M × 200 B | 24.3 MiB | 40.3 MiB | **16.0 MiB** | 13.49 s | 5,932,752 B | 3.8 |
| PostgreSQL | 100k, 128-dimension vector | 20.8 MiB | 103.8 MiB | **83.0 MiB** | 24.19 s | 53,202,407 B | 6.4 |
| PostgreSQL | 1M, 128-dimension vector | 21.4 MiB | 125.6 MiB | **104.2 MiB** | 243.17 s | 531,977,239 B | 12.6 |
| SQLite | 100k, 128-dimension vector | 21.4 MiB | 119.2 MiB | **97.8 MiB** | 21.42 s | 53,202,407 B | 3.0 |
| SQLite | 1M, 128-dimension vector | 21.5 MiB | 159.1 MiB | **137.6 MiB** | 209.72 s | 531,977,239 B | 3.8 |
| SQLite | 100k, 128-dimension vector, `MALLOC_ARENA_MAX=1` | 22.3 MiB | 75.6 MiB | **53.3 MiB** | 22.19 s | 53,202,407 B | 3.3 |
| SQLite | 1M, 128-dimension vector, `MALLOC_ARENA_MAX=1` | 22.3 MiB | 80.1 MiB | **57.8 MiB** | 222.48 s | 531,977,239 B | 3.1 |

The backup's growth from 100k to 1M is +6.7 MiB on MongoDB, +21.2 MiB on
PostgreSQL and +39.8 MiB on SQLite with a vector index under the default
arenas (+4.5 MiB under one arena), each under the PR 8 record's bound of
112 MiB. The MongoDB rows are of a table without secondary indexes, so they
do not compare with the PR 8 record's MongoDB rows (two GSIs, 500k in the
1M row's place); they are the first 1M-item MongoDB backup measured. A
backup stores no index contents (the design's testing summary), so the
vector rows measure the backup of the items that carry the vectors.

## Durations (the RTO baseline the testing summary names)

On this host, loopback, filesystem store, one run each: 1M items of 200
bytes restore in 33.8 s on MongoDB (its own backup, unordered lines; another
run may take up to one 5 s cycle of the backfill worker more) and in
49.0 s on Cassandra (a key-ordered backup adopted from SQLite); 100k in
3.9 s (3.9 to 8.9 s over five runs: the job's time plus up to one cycle)
and 6.1 s. A 1M-item table whose items carry a 128-dimension vector,
with a vector index, backs up in 243 s on PostgreSQL and 210 s on SQLite
and restores in 246 s and 214 s; under `MALLOC_ARENA_MAX=1` the SQLite
backup took 222 s and the SQLite restore 283 s. At 100k: 24 s and 21 to
22 s to back up, 25 s and 22 to 28 s to restore. The 1M MongoDB backup took 13.5 s. The PR 9 record's 1M
restores of the plain shape were 9.5 s on PostgreSQL and 26.8 s on SQLite
on the same host class under another build and load, so they compare with
these as an order of magnitude only.

After each vector restore one `SearchVectors` over the whole table (no hash
element, so every vector is scored) answered in 0.24 s on PostgreSQL and in
5.5 to 15.8 s on SQLite at 1M over seven runs (14.7 s in this run, 5.8 s in
the one-arena run, 5.5 s in a later run, 6.9 s and 5.7 s, one arena, on
04c29155, and 15.8 and 15.2 s in the last section's runs), 0.04 s and 0.6 s
at 100k: SQLite's vector search is an exact scan
(`crates/storage-sqlite/src/vector_search.rs`), which an index with a hash
element bounds to one partition.

## Later runs on the same host

These runs used release builds of 30553b38 and, for the second 1,536-number
one-arena restore and the third two-GSI WAL run, of 04c29155, two earlier
versions of this change: 04c29155's Rust sources are this commit's, and
30553b38's differ from them only in the help text of two `init` options. On
the host above, with a separate harness: a fresh deployment per
run, a filesystem store, `max_concurrent_jobs = 2`, the item shapes above
loaded from four processes of eight threads, the server's default settings
(no `settings set`), the server restarted before each measured phase,
`VmHWM` read right before the request and at the end, `/proc/<pid>/status`
sampled every 200 ms. Every restore was counted and every vector restore
searched for item 7's vector, as above. The 1-minute load average at each
phase's start is in the last column.

They reproduce the cells above in kind (within 12% on the peak, within 20%
on the durations): the SQLite 100k vector restore 373.3 and 376.5 MiB over
idle in 23.3 and 22.7 s (load 3.0, 2.9), 192.3 MiB in 28.6 s under one arena
(2.8); the SQLite 1M vector backup 141.2 MiB in 210.7 s (3.1), its restore
522.8 MiB in 218.7 s (2.5) and 204.3 MiB in 286.7 s under one arena (1.7);
the PostgreSQL 100k vector backup 83.0 MiB in 26.5 s (10.8) and restore 112.5
MiB in 29.7 s (8.1); the MongoDB 100k restore 36.2 and 33.2 MiB in 3.9 s;
the Cassandra 100k restore 19.7 and 20.1 MiB in 6.4 and 4.9 s. The vector
data files were byte-identical with the runs above (53,202,407 bytes at
100k).

What the vector restore's memory belongs to (SQLite unless stated):

| restore | default arenas | `MALLOC_ARENA_MAX=1` | load |
|---|---|---|---|
| 100k, 128 numbers per item, vector index | 373.3 / 376.5 MiB, 23.3 / 22.7 s | 192.3 MiB, 28.6 s | 3.0, 2.9 / 2.8 |
| 100k, the same items, no vector index | 407.5 MiB, 18.3 s | 184.0 MiB, 20.5 s | 3.4 / 2.6 |
| 20,000 items of 1,536 numbers, vector index | 668.6 MiB, 44.7 s | 301.0 MiB, 48.9 s; 315.9 MiB, 48.4 s | 2.3 / 1.6, 2.8 |
| two 100k restores at once (128 numbers, vector index) | 696.7 MiB, 24.9 s | 278.9 MiB, 57.5 s | 2.0 / 2.7 |
| 100k, 128 numbers, pinned to two cores: idle / beside four busy loops | 409.0 MiB, 22.5 s / 376.0 MiB, 60.6 s | | 2.2 / 2.7 |
| PostgreSQL 100k, 128 numbers, vector index | 112.5 MiB, 29.7 s | 48.8 MiB, 31.5 s | 8.1 / 6.2 |
| PostgreSQL 100k, the same items, no vector index | 60.0 MiB, 15.6 s | | 7.0 |

The backup of the 1,536-number table held 520.9 MiB over idle and took
47.0 s (load 3.3); its data files carry 26,987 bytes of line text per item. On
SQLite the items without the index held as much as those with it, so the
vector index is not what the memory belongs to. The string control of the
last section shows what is: the restore's write batch (up to
`RESTORE_BATCH_ITEMS`, 10,000 items, or `RESTORE_BATCH_BYTES`, 32 MiB of
item text, `crates/storage/src/restore_engine.rs`) and the decode chunks in
front of it (up to 1,000 items each), which grow with the item's size up to
the batch bound whatever the attribute types. Lists of numbers decode larger
than strings of the same text size, and the term grows with the numbers per
item. The vector index is about 20% of the 100k restore's duration (18.3
against 22.7 to 23.3 s). On PostgreSQL the index's share is given by the pairs below. The second
1,536-number one-arena run (315.9 MiB) was taken on the same build to
confirm the first.

Two more pairs on PostgreSQL, on release builds of 04c29155 with a third
harness written from the Method paragraph (a fresh deployment per cell,
`max_concurrent_jobs = 2`, the two `settings set` lines, the server
restarted before each phase, `VmHWM` read one second after the start and
sampled every 200 ms), each 100k vector restore followed at once by the
restore of the same items with no vector index: 99.9 MiB in 24.84 s (load
6.8) against 66.5 MiB in 16.26 s (6.0), and 98.5 MiB in 25.23 s (5.8)
against 71.4 MiB in 15.72 s (3.0). The vector index's share comes from
pairs whose two runs share a harness: these two, the later runs' pair in the
table above (112.5 MiB in 29.7 s against 60.0 MiB in 15.6 s) and the last
section's two (104.5 against 63.9 MiB in 26.99 against 18.05 s, and 99.8
against 69.8 MiB in 28.30 against 17.34 s). The five put the index at 33.4,
27.1, 52.5, 40.6 and 30.0 MiB and at 35, 38, 47, 33 and 39% of the restore's
duration: 27 to 53 MiB and 33 to 47%. This record's cell has no no-index run
from its own harness, and its pair with the later runs' no-index run (100.6
against 60.0 MiB), whose harness read the idle figure differently and kept
the server's default settings, is left out. On SQLite the same harness measured the 100k vector restore at
395.8 MiB in 21.86 s (load 4.1) and 358.2 MiB in 21.96 s (2.7), and the same items
with no index at 397.3 MiB in 18.01 s (5.5): the index 18% of the duration.

SQLite's WAL beside a backup, on the same build: the writer of the manual's
figure (`PutItem` overwrites of 1,000 200-byte items from four threads, no
secondary index) beside the 100k vector backup grew a 51 MiB `-wal` file to
58.7 MiB over 15,051 writes, the frames reusing the file until they pass its
end. The same writer on a table with two GSIs (hash keys `g0` and `g1`, each
changed by every overwrite, `ALL` projection, the default
`index_propagation_delay_ms`) grew it from 58.7 to 1,277.6 MiB over 13,953
writes in 23.4 s (91.6 KB per write) and, on fresh deployments, from 4.1 to
1,294.1 MiB over 13,996 writes in 25.6 s (96.6 KB, load 4.2) and from 4.1 to
1,261.8 MiB over 13,891 writes in 26.0 s (94.9 KB, load 3.7). With no backup
the same writer for 15 s left it at 4.1 MiB. Each file kept its size after
the server stopped. An asynchronous GSI write is queued in `gsi_pending`
inside the put's transaction and applied and removed by later transactions,
so one such overwrite appends about 22 to 24 pages of 4 KiB during a backup
(about 20 in a fourth run, below).

## Figures measured for the manual

The two database-cost figures of `docs/manuals/14-backup-and-restore.md`
("What a backup costs the database") for a table with no secondary index.
Release builds of 358ff8bd's Rust sources (those of 30553b38) on the host
above, a fresh deployment per run, a filesystem store,
`max_concurrent_jobs = 2`, the Method paragraph's two `settings set` lines (the reproductions
below used the defaults and measured the same in kind). The backup's source
is this record's 100k-item, 128-dimension vector table, loaded through the
wire, the server restarted after the load. The writer: four client threads
of `PutItem` overwrites of 1,000 items (a hash key and a 200-byte `S` pad)
in a second table with no secondary index, started with the `CreateBackup`
request and stopped when `DescribeBackup` reported `AVAILABLE`; the control
is the same writer for the same time with no backup.

- PostgreSQL 16.15. The relation measured is the writer table's
  `pg_total_relation_size` (with its indexes and TOAST), after a `VACUUM`
  before the run. A 25.2 s backup beside 12,156 overwrites grew it from 0.4 to
  4.4 MiB, about 345 bytes per write. A `VACUUM (VERBOSE)` of that table 10 s
  into the backup, 4,947 writes in, reported "4948 are dead but not yet
  removable"; `pg_stat_user_tables` counted 12,114 dead after the backup, and
  the first `VACUUM` after it left "0 are dead but not yet removable". The
  control, after a `VACUUM FULL`: 17,478 overwrites in 25.2 s, 0.4 to 0.5 MiB.
  Load 5.4 at the backup's start, 4.3 at its end.

- SQLite. The file measured is the database's `-wal`, sampled every 200 ms.
  The control first: 15,762 writes in 22.2 s left it at 4.0 MiB. Then a 22.8 s
  backup beside 15,472 writes grew it from 4.0 to 61.1 MiB, about 3.9 KB per
  write; it was 61.1 MiB 5 s after the backup, after 10.2 s more of the writer
  with no backup, and after the server stopped. Load 3.2 at the backup's end.

Reproduced on release builds of 04c29155 with the third harness above and
the server's default settings:

- PostgreSQL: a 28.1 s backup beside 12,623 overwrites grew the relation
  from 0.49 to 4.91 MiB (367 bytes per write); `VACUUM (VERBOSE)` 10 s in:
  "3952 are dead but not yet removable"; the `VACUUM` after the backup: 0. The
  same writer with no backup for the same time (17,042 writes), the relation
  not compacted in between, left it at 4.91 MiB: the space was reused. Load
  9.2 at the end.

- SQLite, the table with no index: 15,267 writes during a 22.5 s backup grew
  the `-wal` file from 4.1 to 63.0 MiB (4.0 KB per write); 15,165 writes with
  no backup left it at 4.1 MiB; 63.0 MiB 5 s after the backup and after a
  `SIGTERM` stop. Load 5.2.

- SQLite, the two-GSI writer of "Later runs": 14,212 writes during a 25.9 s
  backup grew it from 4.2 to 1,117.7 MiB (82.2 KB per write, about 20 frames),
  kept after the stop. Load 3.9.

## Runs on this commit's Rust sources

Release builds of this commit's Rust sources on the host above, with a
fourth harness written from the Method paragraph: a fresh deployment per
invocation, a filesystem store, `max_concurrent_jobs = 2`, the two
`settings set` lines, the shapes above loaded from four processes of eight
threads, the server stopped and started before each phase, `VmHWM` read one
second after the start and one second after the phase and sampled every
200 ms, every restore counted with a paged `Scan` (equal to the items in
every run) and every vector restore searched for item 7's vector (first in
every run). The host ran other work, and two cells often ran at once; the
1-minute load at each phase's start is in the last column. The string shape
replaces the 128-number list with a random 2,290-character `S` attribute and
has no vector index: 2,569 bytes of line text per item in the decompressed
data files, against 2,503 for the 128-number items.

| restore | default arenas | `MALLOC_ARENA_MAX=1` | load |
|---|---|---|---|
| SQLite 100k, 128 numbers, vector index | 367.2 MiB, 22.26 s; 344.6 MiB, 36.58 s | 185.4 MiB, 37.13 s | 13.1, 5.4 / 7.8 |
| SQLite 100k, the same items, no vector index | 399.2 MiB, 20.53 s; 424.5 MiB, 22.36 s | 186.2 MiB, 23.25 s | 7.0, 7.3 / 6.5 |
| SQLite 100k, a 2,290-character string per item, no index | 253.4 MiB, 10.78 s; 252.5 MiB, 9.20 s | 129.6 MiB, 11.66 s; 129.8 MiB, 9.44 s | 6.2, 2.8 / 6.2, 2.7 |
| SQLite 1M, 128 numbers, vector index | 500.8 MiB, 226.47 s | 208.0 MiB | 4.5 / 1.6 |
| PostgreSQL 100k, 128 numbers, vector index | 104.5 MiB, 26.99 s; 99.8 MiB, 28.30 s | 52.0 MiB, 35.40 s | 14.5, 6.0 / 8.5 |
| PostgreSQL 100k, the same items, no vector index | 63.9 MiB, 18.05 s; 69.8 MiB, 17.34 s | | 7.1, 6.1 |
| MongoDB 100k × 200 B | 35.5 MiB, 8.94 s; 35.4 MiB, 8.92 s | | 7.3, not read |

The 1M one-arena restore's duration is left out: it started while the
server was still dropping the previous 1M target.

The string items carry no number and held 253 MiB over idle (130 MiB under
one arena), 6.7 times the PR 9 record's 38 MiB for 100k items of 200 bytes.
So on SQLite a restore of items of more than a few hundred bytes holds its
write batch and decode chunks ("Later runs"), which grow with the item's
size up to the batch bound whatever the attribute types. At the same text
size the 128-number items held more, 344.6 to 424.5 MiB at 100k over the
measured runs with or without the index and 184.0 to 195.3 MiB under one
arena, since each number decodes to an attribute value with a string of its
own. Under one arena the 1M restore measured 203.9 to 208.0 MiB over three
runs, so that figure is flat from 100k to 1M within about 25 MiB over the
measured runs.

On MongoDB each of the two 100k restores logged the job's `DONE` "in 3.9 s;
index backfill pending", and the backfill worker's `ACTIVE` line came about
4.8 s later: the worker's cycle ("Restores"). The whole-table
`SearchVectors` over the 1M restored vectors took 15.78 s after the
default-arena restore (load 2.7) and 15.23 s after the one-arena one.

## The sizing sentence

A restore of 200-byte items held under 60 MiB over idle on MongoDB and under
40 MiB on Cassandra at 1M items. With a 128-dimension vector on every item,
PostgreSQL held about 110 MiB over idle to restore and about 105 MiB to back
up at 1M, growing about 10 and 20 MiB from 100k. SQLite held 552 MiB over
idle to restore and 138 MiB to back up at 1M under the default arenas,
higher than at 100k, and 204 and 58 MiB under `MALLOC_ARENA_MAX=1`, flat
from 100k to 1M within about 25 MiB over the measured runs. The runs above
show that on SQLite this memory is the restore's write batch and decode
chunks, not the vector index, and that it grows with the item's size up to
the 32 MiB batch bound whatever the attribute types: size a SQLite host that
restores tables of items of more than a few hundred bytes from the measured
points under one arena (130 MiB per running restore for 100k items of
2.5 KB strings, 184 to 208 MiB for items of 128 numbers, 301 to 316 MiB for
20,000 items of 1,536 numbers), times `max_concurrent_jobs`, more for larger
items, and treat the default-arena figures as unbounded by this record.
