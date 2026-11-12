# PR 8 memory and duration record (`bench-rss.sh`, 2026-10-31 and 2026-11-01)

This is the record the design cites (section 7, revision item 31 of
`15-backup-restore.md`). `kill-points.py`'s API half is committed as
`devtools/backup-kill-points.py`, with the kill-point driver
`devtools/backup-kill-points.sh`; `bench-rss.sh` and the raw samples
(`/proc/<pid>/status` every 200 ms per run, the servers' logs) are kept
beside the PR outside the repository, on the measuring host;
the figures below are what they produced. The runs of 2026-11-01, after the
PostgreSQL page's row bound changed, are in the section at the end and in the
sentences they correct; the runs of the same day after the row bound gained
its memory are in the last section.

What was measured: the release binary (`cargo build --release`, the default
PostgreSQL features; `--no-default-features --features sqlite`; `--features
mongodb-test-hooks`) running `serve --foreground` with a filesystem backup
store (SQLite: the defaulted root beside the database file), sampled from
`/proc/<pid>/status` every 200 ms (`VmRSS` and `VmHWM`). Each table is loaded
through the wire (`kill-points.py create-table`: BatchWriteItem from 16
threads, items of a 200-byte `S` pad under a composite key with two GSIs),
then the server is restarted so the process's high-water mark covers the
backup alone; the idle figure is read right before `CreateBackup`, the peak
is `VmHWM` once `DescribeBackup` reports `AVAILABLE`. Samples:
`rss/<dir>/{host.txt,results.txt,<run>.json,<run>.samples}` with `<dir>` the
backend for the record's runs and `sqlite-itemchannel1`,
`sqlite-itemchannel-swapped`, `sqlite-arena1`, `sqlite-worst24`,
`sqlite-rowbuffer4` for the comparison runs below; the series of 2026-11-01
under `sqlite-worst<N>-{default,arena1}/` and `postgres/`.

Host (`host.txt`): 16 cores, 30.3 GiB, Linux 5.10.270 (amzn2int); PostgreSQL
16.15 in a container on loopback (55432); the SQLite the binary bundles
(libsqlite3-sys 0.30.1, SQLite 3.46; the host's 3.7.17 is unused); MongoDB
7.0.43 single-node replica set in a container (27117). What was not held
constant, stated before any figure here is read across runs: the host ran
other work during every run (the PostgreSQL runs
beside two 1M-item loads and a wire suite; the SQLite and MongoDB runs beside
the Cassandra integration suite and the kill-point runs), so the durations
are upper bounds and comparable only within one back-to-back pair; the peaks
are the process's own and do not depend on the host's load. Everything is on
loopback: nothing here says what a network database or a remote S3 store does
to the duration; the upload window of an S3 store (`upload_part_size_bytes *
max_concurrent_uploads`) is not in these figures, which use the filesystem
store's one write buffer. The MongoDB binary was built before the pipeline's
last change (the sink's `BrokenPipe` answer after a stop, which touches no
allocation); the PostgreSQL and SQLite figures of the record are from binaries
of the PR's first build (`ITEM_CHANNEL = 4`); the PostgreSQL page changed
after these runs (the row bound from the largest row, the rows parsed as they
arrive), so the PostgreSQL worst-shape row below is the first build's and the
section of 2026-11-01 carries what changed.

## The bound the acceptance asserts: peak(1M) − peak(100k) < 112 MiB

The bound the script asserts is 64 + 32 + 16 MiB (the page budget, the line
cap, the channels). MongoDB's "1M" run is 500k: its 100k load took 361 s
and the 500k load 1,604 s through the wire on this host, so a 1M load would
have run for close to an hour; the 500k run is recorded in its place and the
difference is against it.

| backend | run | items | idle RSS | peak (VmHWM) | peak − idle | AVAILABLE after | backup size |
|---|---|---|---|---|---|---|---|
| PostgreSQL | 100k | 100,000 | 24.6 MiB | 35.9 MiB | 11.3 MiB | 1.85 s | 847,689 B |
| PostgreSQL | 1M | 1,000,000 | 24.8 MiB | 45.4 MiB | 20.6 MiB | 36.1 s | 6,452,803 B |
| PostgreSQL | 1M − 100k | | | **+9.5 MiB** | | | PASS (bound 112) |
| SQLite | 100k | 100,000 | 20.4 MiB | 65.4 MiB | 45.0 MiB | 1.44 s | 847,689 B |
| SQLite | 1M | 1,000,000 | 22.8 MiB | 73.3 MiB | 50.5 MiB | 12.45 s | 6,452,803 B |
| SQLite | 1M − 100k | | | **+7.9 MiB** | | | PASS (bound 112) |
| MongoDB | 100k | 100,000 | 26.8 MiB | 36.4 MiB | 9.6 MiB | 1.85 s | 962,185 B |
| MongoDB | 500k (in the 1M row's place) | 500,000 | 25.7 MiB | 41.2 MiB | 15.5 MiB | 8.81 s | 4,869,108 B |
| MongoDB | 500k − 100k | | | **+4.8 MiB** | | | PASS (bound 112) |

The page budget is never reached by 200-byte rows (a page is `n` rows, at
most `DEFAULT_FETCH_BATCH` = 1000, about 250 KB of text), and the data files
roll at 64 MiB of uncompressed text without the file being held (the encoder
streams 1 MiB chunks into the put), so the peak over idle is a few tens of
MiB on every backend and moves by under 10 MiB from 100k to 1M rows. SQLite's
45 MiB over idle at 100k is the backend's own: the pool's connections each
hold SQLite's page cache, which a backup's reads fill; it is bounded by the
pool and does not grow with the table (the 1M run adds 7.9 MiB to it); the
constant is `PRAGMA cache_size = -32000`
(`crates/storage-sqlite/src/sqlite_util.rs`), 31 MiB per connection, so the
pool's bound is 10 × 31 MiB at the default pool and 34 × 31 MiB at the raised
one, and the 45 MiB is about one and a half connections' caches filled, not
the pool's. The duration scales
with the items: 36 s for 1M on PostgreSQL under a loaded host (the server's
log line says 36.1 s of job time; 1.9 s for 100k), 12.5 s on SQLite, 8.8 s
for 500k on MongoDB.

Rows read past the cut on PostgreSQL (the `test-hooks` counter
`paging::rows_past_cut`, `crates/storage-postgres/tests/table_snapshot.rs::pages_are_cut_at_the_byte_budget`,
run with `--nocapture`): on the first build **602 rows over 77 pages of 8
rows** with `n` = 16 for every page (the row bound from the average row, whose
floor was 16; 8 per page where the budget cut each page in half), on both
layouts; after the paging change **8 rows over 77 pages**, all on the first page (the
row bound is then the budget divided by the largest row, 8, so every later
page reads exactly the rows it keeps), asserted exactly. The SQLite stream
pulls no row past its cut (its `rows_past_cut` is 0 and asserted so by
`snapshot_engine.rs`'s test); the driver's worker reads ahead of it, which
the worst-expansion runs below measure.

## The worst-expansion run

`put-worst`: 1,000 small rows (`a0000`.., a 50-byte pad) and then 12 items
of #394's worst-expansion shape (`zz000`..: a list of 2,262 `NS` sets of the
90 values `1.0E-130`..`9.9E-130`; 409,429 bytes as the service sizes an
item; 27.7 MB as a data-file line and as stored item text, every number
normalized to its 133-character decimal form; about 34 MB decoded, 203,580
`String`s of 133 bytes with their headers). The page the cut gives on such
rows is three rows (the rows whose predecessors' bytes are under 64 MiB: 0,
27.7, 55.4 MB), 83 MB of text, so 12 rows are four pages. MongoDB stores an
item as one BSON document and refuses the shape at 28.9 MB (`BSONObjectTooLarge`,
the 16 MiB cap; `rss-mongodb-worst.out`'s first attempt), so its run holds
12 items of 1,100 sets (13.5 MB lines, `--sets 1100`): there every row is a
page of its own (the server's 16 MiB reply bound) and the shape is half the
size of the other two backends'.

| backend | build | worst rows | idle RSS | peak (VmHWM) | peak − idle | AVAILABLE after | backup size |
|---|---|---|---|---|---|---|---|
| PostgreSQL | `ITEM_CHANNEL = 4` | 12 | 24.5 MiB | **904.5 MiB** | 880 MiB | 18.2 s | 3,793,555 B |
| SQLite | `ITEM_CHANNEL = 4`, the 15:34 build (before the sink's `BrokenPipe` answer) | 12 | 21.1 MiB | **782.7 MiB** | 762 MiB | 8.16 s | 3,793,555 B |
| SQLite | `ITEM_CHANNEL = 4`, the first build (with the `BrokenPipe` answer) | 12 | 20.7 MiB | 602.6 MiB | 582 MiB | 8.97 s | 3,793,555 B |
| SQLite | `ITEM_CHANNEL = 1` | 12 | 20.9 MiB | 555.4 MiB | 535 MiB | 8.13 s | 3,793,555 B |
| SQLite | `ITEM_CHANNEL = 1`, back to back with the row above it | 12 | 20.4 MiB | 552.2 MiB | 532 MiB | 7.95 s | 3,793,555 B |
| SQLite | `ITEM_CHANNEL = 4`, `MALLOC_ARENA_MAX=1` | 12 | 22.0 MiB | 529.5 MiB | 507 MiB | 8.76 s | 3,793,555 B |
| SQLite | `ITEM_CHANNEL = 4` | **24** | 20.3 MiB | **919.4 MiB** | 899 MiB | 15.7 s | 7,567,821 B |
| MongoDB | `ITEM_CHANNEL = 4`, 1,100 sets | 12 | 25.0 MiB | 498.7 MiB | 474 MiB | 4.7 s | 1,851,153 B |

The driver's run-ahead on SQLite, measured back to back on one 24-row table
(`rss/sqlite-rowbuffer4/results.txt`; the final tree sets `row_buffer_size`
to four on the pool, `crates/storage-sqlite/src/store.rs`):

| SQLite build | worst rows | idle RSS | peak (VmHWM) | peak − idle | AVAILABLE after |
|---|---|---|---|---|---|
| `row_buffer_size` 4 (final tree) | 24 | 20.2 MiB | 716.7 MiB | 696 MiB | 15.9 s |
| `row_buffer_size` 50 (sqlx's default) | 24 | 22.5 MiB | 866.2 MiB | 844 MiB | 15.7 s |
| `row_buffer_size` 4, the 1M-row table | 1,000,000 small | 22.3 MiB | 67.3 MiB | 45.0 MiB | 11.43 s |
| `row_buffer_size` 50, the 1M-row table | 1,000,000 small | 21.1 MiB | 67.6 MiB | 46.5 MiB | 11.42 s |

The samples show the shape of it: with 50 the RSS reaches 850 MiB within a
second of the start (the worker has read every one of the 24 rows while the
stream parses the first page's three), falls to 280 MiB when the first page's
cut drops the statement, and climbs again on the second; with four it climbs
with the pipeline (236, 292, 339, 424 MiB and on) to its peak. A 1M-row
backup takes the same 11.4 s with either, so the pool keeps four.

The terms, by the pipeline's parts, for a row of `r` = 27.7 MB of text whose
decoded `Item` is `d` ≈ 34 MB:

- the page's text: on the first build PostgreSQL fetched the page whole
  (`fetch_all`, 83 MB as the driver's rows) and copied each row's text out
  as it extracted it, about one page plus one row; after the paging change it pulls
  the rows from the connection one at a time and parses each as it arrives,
  so one row, as SQLite, which pulls the rows one at a time;
- the page's decoded rows (up to 3 × `d` = 102 MB) in the stream's buffer
  while the pipeline takes them one by one;
- one row's parse: the text, the deserializer's intermediate value and the
  item, `r + 2d` ≈ 96 MB, for the row being parsed;
- the pipeline's items: the feeder's, the item channel's (`ITEM_CHANNEL`)
  and the encoder's, (2 + `ITEM_CHANNEL`) × `d`: 204 MB at four, 102 MB at
  one;
- the encoder's line buffer (one line, `r`), its gzip window, the chunk
  sink's 1 MiB and the chunk channel's four 1 MiB chunks;
- on SQLite, the driver's run-ahead (below).

At the instant the stream fetches a new page while the pipeline is full the
terms sum to about 400 MB with a four-item channel and about 300 MB with a
one-item channel. The high-water marks are 1.4 to 2.2 times that, and the
difference is the allocator's: the row's 203,580 short strings are allocated
on the tokio worker that parses the row and freed on the blocking thread
that encodes it, and glibc's malloc keeps that memory in its arenas between
pages. Evidence: two builds of the same pipeline code (the table's two 12-row
rows, the 15:34 build and the first build) peaked at 762 and 582 MiB over
idle on one shape, two runs of one first-build binary at 12 rows peaked at
574 and 805 MiB (the terms do not vary; the allocator's state does), and
`MALLOC_ARENA_MAX=1` took the same run to 507 MiB. The 24-row run on SQLite
(899 MiB over idle against 582 to 762 for 12 rows) adds one term the 12-row
runs hid: sqlx's SQLite driver runs each statement on a worker thread that
buffers rows ahead of the reading task (`row_buffer_size`, 50 by default),
so while the stream parses its page's three rows the worker has already read
the rows after them, up to 50 or the table's end, whichever comes first (12
rows in the first runs, 24 in this one, 50 × the line cap, 1.6 GB, on a
large table of such rows); the page's cut drops the statement and frees them.
`rows_past_cut` is 0 because it counts the rows the stream pulls, not the
rows the driver read. The final tree sets the
pool's `row_buffer_size` to four (the table above): the run-ahead is then at
most five rows (the four the channel holds and the one the worker has stepped
and copied while its `send` waits; 160 MiB at the line cap) and the SQLite
peak is the pipeline's terms and the allocator's share.

The item channel. The proposal was a one-item channel to save the
three items the four-item channel holds. Measured back to back on SQLite on
one loaded 1M-row table, each build once in each order
(`rss/sqlite-itemchannel1/results.txt`, `rss/sqlite-itemchannel-swapped/results.txt`):

| order | `ITEM_CHANNEL = 4` | `ITEM_CHANNEL = 1` |
|---|---|---|
| one-item build first (18:22) | 12.45 s | 21.85 s |
| four-item build first (18:33) | 12.24 s | 21.41 s |

A one-item channel costs a 1M-row backup 75% of its duration (every item is
a hand-off between the feeder task and the encoder's blocking thread) and
saves about 50 MiB of the worst-shape peak (582 against 532 MiB over idle,
back to back), so `ITEM_CHANNEL` stays at four and `writer.rs` says why.

The terms do not grow with the number of small rows (the 1M run) and grow
with the row (every term but the budget and the chunks is a multiple of `r`
or `d`), which the line cap (32 MiB) and `max_item_size_bytes` bound; the
process's peak, with glibc's default arenas, grows with the number of large
rows a job reads, and does not under `MALLOC_ARENA_MAX=1` (the series of
2026-11-01 below). Section 7 of the design states these terms with the figures;
revision item 31 carries the correction (the earlier text, "the budget plus
one row of text, plus its decoded form", named the first term alone).

## The incompressible run

`put-random`: 256 items each holding a 256 KiB `B` attribute of
`os.urandom` bytes (64 MiB in all; 85 MB as base64 text).

| backend | idle RSS | peak (VmHWM) | peak − idle | AVAILABLE after | backup size |
|---|---|---|---|---|---|
| PostgreSQL | 24.6 MiB | 145.1 MiB | 120.5 MiB | 5.9 s | 68,016,482 B |
| SQLite | 22.7 MiB | 97.5 MiB | 74.8 MiB | 4.3 s | 68,016,702 B |
| MongoDB | 25.6 MiB | 115.8 MiB | 90.2 MiB | 4.9 s | 68,016,522 B |

The backup's size (68 MB) is the data's: gzip shrank nothing. The peak over
idle is one page of text (the 85 MB fits one page: 256 rows of 342 KB, which
PostgreSQL holds whole and SQLite a row at a time) plus the decoded items
(the 64 MiB of bytes) plus the channels; the encoder did not hold the file
(a 68 MB file through a 4 MiB chunk window), which is what this run proves.

## Durations for the record (the design's RTO baseline, PR 11)

CreateBackup to AVAILABLE, the `DescribeBackup` poll's figure, a composite
key with two GSIs, 200-byte items, a filesystem store on the same disk, the
host loaded as said above:

| backend | 100k items | 1M items | 500k items |
|---|---|---|---|
| PostgreSQL 16.15 (loopback, separate data and catalog databases) | 1.85 s | 36.1 s | |
| SQLite 3.46 (file) | 1.44 s | 12.45 s (12.24 and 12.45 s in the comparison runs, no GSIs) | |
| MongoDB 7.0 (loopback replica set) | 1.85 s | not run | 8.81 s |

The worst shape (12 rows of 27.7 MB, 332 MB of text): 18.2 s on PostgreSQL
on the first build and 16.5 s after the paging change (the section of 2026-11-01),
8.0 to 9.0 s on SQLite; the incompressible 64 MiB: 5.9, 4.3 and 4.9 s. The
loads themselves (the wire, 16 threads of BatchWriteItem): 100k in 217 s on
SQLite with two GSIs and 1M in 6,531 s; 1M in 463 s without GSIs; MongoDB
100k in 361 s and 500k in 1,604 s.

## 2026-11-01: the allocator series and the PostgreSQL page's cost

The worst shape (1,000 small rows then N rows of 27.7 MB) in two series of
six runs, each series on one SQLite release build, each run a fresh
deployment, `bench-rss.sh sqlite worst` with `WORST_COUNT` 12, 24, 48, first
with glibc's default arenas and then with `MALLOC_ARENA_MAX=1` in the
server's environment. Series 1: the build before the feeder's line
measurement was added (it measures no line at the default item limit, so the
pipeline's allocation is the same), a quiet host, samples under
`rss-series1/rss/`. Series 2: the final build, with the kill-point runs of the
same day running beside it on the host, samples under
`rss/sqlite-worst<N>-{default,arena1}/`; each binary's md5 is recorded beside
its samples:

| allocator | worst rows | series 1, quiet host: idle / peak / over idle, AVAILABLE after | series 2, final build, loaded host: idle / peak / over idle, AVAILABLE after |
|---|---|---|---|
| default arenas | 12 | 24.0 / 489.6 / **466 MiB**, 8.74 s | 22.7 / 729.3 / **707 MiB**, 9.15 s |
| default arenas | 24 | 23.7 / 611.4 / **588 MiB**, 18.07 s | 20.6 / 720.4 / **700 MiB**, 17.89 s |
| default arenas | 48 | 22.4 / 782.1 / **760 MiB**, 36.56 s | 23.2 / 1304.5 / **1,281 MiB**, 35.96 s |
| `MALLOC_ARENA_MAX=1` | 12 | 22.1 / 503.3 / **481 MiB**, 9.57 s | 24.0 / 476.2 / **452 MiB**, 9.99 s |
| `MALLOC_ARENA_MAX=1` | 24 | 23.5 / 475.6 / **452 MiB**, 19.55 s | 20.6 / 500.8 / **480 MiB**, 19.32 s |
| `MALLOC_ARENA_MAX=1` | 48 | 22.0 / 501.5 / **480 MiB**, 39.45 s | 22.2 / 477.6 / **455 MiB**, 39.67 s |

The terms of the section above at this row (`r` = 26.4 MiB, `d` ≈ 32.4 MiB:
the page's three decoded rows 97 MiB, one row's parse 91 MiB, the pipeline's
six items 195 MiB, the line buffer 26 MiB, the chunks 5 MiB, the five-row
run-ahead 132 MiB) sum to 545 MiB on SQLite. With the default arenas the peak
over idle is 0.86 to 1.39 of that sum in the first series and 1.28 to 2.35 in the
second, grows with the rows in both, and differs by 240 to 520 MiB between
the two series at the same count (runs of the first build on a loaded host
reached 1,096 MiB at 48 rows, and its 12-row runs of one binary spread over
574 to 805 MiB); under one arena it is 0.83 to 0.88 of the sum in both series and
flat from 12 to 48 rows. So the pipeline's terms do not grow with the table,
the growth and the spread under the default arenas are the allocator's
retention of the strings freed on the encoder's thread (how much it keeps
depends on the host's state at the time, which is why the host's load moves
it), and the bound the design states (section 7) is the terms at the line
cap times 1.2 per job under `MALLOC_ARENA_MAX=1`, which the admin guide and
the deployment guide's service unit now carry; with the default arenas the
design records the growth and names 2.35 of the terms at 48 rows as the
largest measured. The duration grows with the rows as it should (the 48-row
table is 1.3 GB of text) and is the same under either setting.

The PostgreSQL page (the probe `pg_adapt_probe.py`, kept with the scripts,
run twice with the first build's and the changed row-bound rule on a
1,000-small-then-60-worst table, `EXPLAIN (ANALYZE, BUFFERS)` of page 3):

| row bound rule | page 2 (`n` = 1000, reads 984 small + 16 worst, keeps 987) | page 3: `n` asked | rows read / past the cut | temp written (8 KiB blocks) | page 3 time | pages 4, 5 (`n`, time) |
|---|---|---|---|---|---|---|
| the first build: budget / average row, floor 16 | 4.6 s | 810 | 57 / 54 | 757,844 (5.9 GB) | 18.2 s (43.7 s on a loaded host) | 16, 4.4 s each (13 rows past the cut) |
| the change: budget / largest row, floor 4 | 14.0 s | 4 | 4 / 1 | 26,564 (208 MiB) | 1.13 s | 4, 1.2 s each (1 row past the cut) |

Page 2 is the over-read of this shape (it asks for the rows the small rows
justified and meets the large ones: up to `n` rows of up to the line cap);
its two timings differ by the host, not the rule, since both runs issue the
same query. On a table whose large rows are scattered among small ones that
page is not the only one: with the bound sized from the previous page alone
it recurs every other page, which the last section measures and the row
bound's memory corrects. The PostgreSQL release build with the changed page under
`bench-rss.sh postgres 100k 1m worst` with `GSIS=0` (one deployment, the
runs in that order, samples under `postgres/`): 100k, idle
23.7 MiB, peak 30.4 MiB, 1.87 s; 1M, idle 22.6 MiB, peak 34.4 MiB, 13.27 s
(peak(1M) − peak(100k) = +4.0 MiB, bound 112, PASS; the record's 36.1 s was
under two 1M loads and a wire suite); the worst shape at 12 rows, peak
790.2 MiB (`VmHWM`) after 16.5 s, against the first build's 904.5 MiB and 18.2 s,
with the page-text term gone and each large-row page reading one row past
its cut rather than thirteen (the idle before this run, 122.7 MiB, is the
restarted process's startup work on a table of 27.7 MB rows, not the 1M
load: on a fresh deployment holding only the worst table the restarted
process idled at 138 and 164 MiB in two later runs, climbing from 79 to
168 MiB within two seconds of the listener answering and before
`CreateBackup`, so the over-idle figure, 667 MiB, is not comparable with
the record's 880; the `VmHWM` is). The backup sizes of the `GSIS=0` tables (366,631 and 3,017,864
bytes) are smaller than the record's because the items carry no GSI
attributes; the backup reads the base table either way. The plan near the end of a table is the planner's: on
the 100k-row test table the index scan holds ten rows from the end, and on a
1,060-row table a page after the third of the last 60 at `n` = 810 is a
bitmap scan of the primary key index and a `Sort` of the 57 rows after the
seek (PostgreSQL 16; `the_last_page_sorts_the_rows_after_the_seek` pins
both).

## 2026-11-01, later: the row bound's memory, the server-side walk that was measured and not taken, the allocator on PostgreSQL

The shape the sections above do not cover is a table whose large rows are
scattered among small ones, which is what a table whose large items are a
minority looks like (key order does not sort items by size; `put-worst`
groups its `zz` keys after the `a` keys). With the row bound sized from the
previous page alone, a page that holds a 27 MB row sets the next bound to 4,
four small rows set the one after back to 1000, and that page meets large
rows again: the cycle repeats every other page for the whole table.

The SQL probe (`pg_memory.py`, kept with the scripts; the window-form page
query as `page_sql` writes it, a complete walk of a 1,000-row table with 30
rows of 27 MB text at every 33rd key, PostgreSQL 16.15, `work_mem` 4 MB,
one plain run per rule, the server's `temp_bytes` for that run):

| row bound rule | pages | rows the inner `LIMIT` read for 1,000 kept | `temp_bytes` | wall |
|---|---|---|---|---|
| the largest row of the previous page | 22 (`n` 16, 1000, 4, 1000, 4, …) | 5,555 | 13.38 GB | 71.5 s |
| the largest row of the last four pages | 50 (16, 1000, 4, 4, 4, 4, 1000, …) | 5,555 | 13.38 GB | 64.6 s |
| the largest row of the last pages holding at least 1,000 rows | 228 (16, 1000, then 4 to the end) | 1,901 | 3.89 GB | 22.4 s |

Four pages of memory read the same rows as one: the four pages after a
large row hold 16 small rows, so the bound returns to 1000 as before and
the 1000-row page recurs every fifth page instead of every other. A memory
in rows holds the bound at 4 for a row bound's worth of rows after the last
large one, so on this table it never returns to 1000, and the one over-read
left is the first page that meets the large rows (984 rows read for 83
kept). That is the rule the tree carries (`RowBoundMemory`,
`snapshot_engine.rs`): the bound that remains is the second page, and then
the page that follows a row bound's worth of small rows, each reading up to
`n` rows of up to the line cap, and such a page recurs at most once per row
bound's worth of small rows (on a table whose large rows are rarer than one
per thousand keys, at every large row). `the_row_bound_remembers_large_rows_among_small_ones`
pins the figure on a 300-row table with nine such rows: 201 rows read past
the cut, all on the second page, over 53 round trips (306 over 7 with one
page of memory; 103 round trips with a floor of 2).

The release binary (`pg-interleave.sh`, kept with the scripts; one
deployment with a filesystem store, two tables of 20,000 small rows loaded
through the wire with 30 worst-expansion items, at keys `a0032`, `a0065`,
… `a0989` in one and `zz000`.. in the other, the server restarted before
each backup, the data database's `temp_bytes` during it, both binaries on
the same tables back to back; host load 3.6 to 8.6):

| table | row bound rule (binary) | AVAILABLE after | data database `temp_bytes` | server `VmHWM` |
|---|---|---|---|---|
| interleaved | the previous page (`80f2d287`) | 72.0 s | 9.21 GB | 905 MiB |
| interleaved | the memory (`e020f82e`, the tree) | 54.6 s | 3.18 GB | 900 MiB |
| grouped | the previous page | 39.8 s | 2.85 GB | 1,143 MiB |
| grouped | the memory | 38.9 s | 2.85 GB | 1,066 MiB |

The interleaved table's remaining 16 s and 3.2 GB over the grouped one are
the second page, whose `LIMIT 1000` reads keys 17 to 1,016, which hold all
30 large rows (3 kept, 27 past the cut), for the 83 it keeps; the grouped
table is unchanged (its one 1000-row page meets small rows). A second pair on the same host
earlier that hour: 77.4 s and 9.21 GB for the previous-page rule on the
interleaved table, 38.9 s and 2.85 GB grouped. The `VmHWM` figures are
under glibc's default arenas and vary by about 200 MiB between runs; they
are reported, not rested on.

Measured and not taken: a page query that stops at the budget on the server,
a recursive walk of the primary key one index probe per row
(`WITH RECURSIVE … LATERAL … ORDER BY <key> LIMIT 1`, the running total
carried from step to step, the walk ending after the row that reaches the
budget or at `n` steps), so the server serializes exactly the rows the page
returns and reads none past the cut. On the same two tables and binaries it
took the interleaved table in 38.8 s with 2.51 GB of temp and the grouped
one in 38.0 s with 2.51 GB (the three rows of each large-row page spilled
three times: the work table, the CTE's materialization, the sort), and the
first-page and middle-page plans on the six key shapes were the two index
probes with no other scan of the relation. Its cost is one index probe per
row, which a table of small rows pays on every row: the 1M-row backup
(`bench-rss.sh postgres 100k 1m worst`, `GSIS=0`, the two binaries back to
back on one loaded table) took 23.07 s against 11.85 s for the window form
(+95%; the 100k run 1.64 s; the worst shape at 12 rows 13.4 s and 658 MiB
`VmHWM` against 17.2 s and 553 MiB). The rule for the choice was a 1M-row
duration under 21 s (within 60% of 13 s), so the window form with the
memory stays; the walk's query, tests and figures are kept with the scripts
(`option1/`). A walk in chunks of a few rows per step would bound the
over-read at the chunk less one and cost a probe per chunk; it was not
measured.

The 1M-row backup with the memory rule, the same back-to-back method: 13.69 s
against 12.06 s for the previous-page rule (`VmHWM` 36.7 against 36.3 MiB;
peak(1M) − peak(100k) = +4.1 MiB, bound 112, PASS); the rule changes no
page of a table with no large row (every page is the row bound either way),
and the tree's binary carries that day's other changes to the job's
validator and feeder as well, so the difference is the host's and theirs,
not the page's.

What the memory costs where the previous-page rule had no over-read: after
a page holding a row at the line cap the bound stays at the floor for a row
bound's worth of rows, 250 pages of four, whatever those rows hold, so a
table whose large rows are rarer than one per row bound pays 250 round
trips per large row where one page of memory pages it at the row bound (a
1000-row page holding one 27 MB row and 999 small ones is under the budget
and kept whole). Measured on a 20,000-row table with a 27 MB row at every
1,100th key (18 large rows, loaded through the wire with `kill-points.py
put-worst --small 20000 --count 18 --interleave 1100`; the PostgreSQL
release build against a one-line build with one page of memory, the data
table's `idx_scan` counting the page queries; `pg-rare.sh`, kept with the
scripts): 2,585 page queries against 38, 25.6 and 24.9 s in two runs
against 25.1 s, 1.06 against 1.09 GB of temp, the same 5,821,368-byte
backup; the SQL mirror (`pg_probe.py`) walks the same shape in 2,508 pages
against 38 with 0 rows past the cut under either rule. On loopback the
extra round trips cost about 0.3 ms each and disappear under the large rows'
own serialization (about 1.3 s per 27 MB row); on a 1M-row table with 900
such rows they are 125,000 round trips, about 40 s on loopback and about two
minutes on a 1 ms link, against the 20 minutes the large rows themselves
take. A bound that doubles after each page without a large row (4, 8, 16,
…) would reach the row bound after eight small pages instead of 250 with
the same rows read past the cut on the three shapes the mirror walked; it
was not taken, the cost above being bounded and the rule stated.
The worst shape at 12 rows under the default arenas: 787.8 MiB `VmHWM` in
17.7 s against 750.1 MiB in 17.5 s (the idle before the first of the two,
241.6 MiB, is the restarted process's startup work on the table of 27.7 MB
rows, the caveat of the previous section).

The allocator on PostgreSQL (`MALLOC_ARENA_MAX=1 bench-rss.sh postgres
worst`, `GSIS=0`, the tree's binary, a fresh deployment): the worst shape
at 12 rows peaked at **339.9 MiB `VmHWM`** in 18.7 s (idle 144.2 MiB, the
startup work above; another measurement of the same shape on a build of the
same day: 346.3 MiB, 208 MiB over an idle of 138.0 MiB, 22.4 s), against
750 to 788 MiB under the default arenas. The terms of the section above on
PostgreSQL at this row are 413 MiB (545 MiB less the five-row run-ahead,
which PostgreSQL has not), so the one-arena peak is 0.82 of the terms here,
0.84 in the other measurement, under the 1.2 the design states, which was
measured on SQLite alone until now.

What one arena costs the request path. On the SQLite release build with a
50k-item composite-key table, one boto3 client per thread, three
alternating repeats, 20 s per operation (`sqlite-ab.sh`,
`workload.py`, kept with the scripts; host load 2.4 to 5.2): GetItem at
8 threads 624, 621, 616 req/s against 605, 588, 598 under one arena
(−3.8%); Query of a 50-item partition at 8 threads 414, 407, 406 against
398, 391, 395 (−3.5%); Scan without Limit at 4 threads 47.7k, 46.7k, 46.0k
items/s against 44.2k, 42.7k, 43.9k (−6.9%); Scan with Limit 1000 over the
table 46.1k, 42.9k, 43.6k against 40.7k, 40.4k, 39.3k (−9.2%). On the
PostgreSQL release build (`pg-ab.sh`, the same workload and method, the
tree's binary, host load 2.6 to 3.8): GetItem 600, 631, 606 against 617,
623, 617 (+1.1%); Query 398, 413, 407 against 402, 401, 408 (−0.6%); Scan
46.5k, 46.2k, 47.3k against 46.1k, 45.8k, 45.0k (−2.1%); paged Scan 45.0k,
44.3k, 45.9k against 44.3k, 44.1k, 44.1k (−2.1%). glibc with one arena
serializes every allocation of the worker threads on one lock; SQLite's
request path runs in the process (the JSON parse and render per item and
the database's own work), where PostgreSQL's spends its time on the
database's round trip, which is why the setting shows on SQLite and is
within the spread between repeats on PostgreSQL but for the scans. A Python
client bounds these rates; the server's own share of the loss may be larger
under a faster client. The deployment guide's service unit carries the line
commented out, with the condition (a host that backs up tables of large
items) and this cost beside it.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc.
