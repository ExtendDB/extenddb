# Released SQLite catalogs

Each file is a text dump of a database that a released ExtendDB binary
created, taken with Python's `sqlite3` `iterdump` (the statements the
`sqlite3` shell's `.dump` prints). The tests execute a dump into a new file
and upgrade it.

| File | Made by |
|---|---|
| `dev-image-0.1.7.sql`, `dev-image-0.1.8.sql`, `dev-image-0.1.10.sql` to `dev-image-0.1.14.sql` | `extenddb/extenddb-dev:<version>` `serve --foreground` on an empty volume, then CreateTable `cu_orders` (pk S, sk S, GSI `by_gpk` on gpk S and gsk N, LSI `by_lsk`, stream NEW_AND_OLD_IMAGES), UpdateTimeToLive `expires` and 10 PutItem. The IAM user `dev` is the one the dev image creates |
| `dev-image-0.1.14-backup.sql` | The same on `extenddb/extenddb-dev:0.1.14`, then CreateBackup `cu_orders_fixture` of `cu_orders`, waited for until AVAILABLE: one `backups` row and 10 `backup_items` rows whose items have sort keys |
| `v0.1.3-to-v0.1.6.sql` | The v0.1.3 `SCHEMA_SQL`, identical from v0.1.3 to v0.1.6, executed on an empty file. Catalog only: no bootstrap rows and no tables |

There is no 0.1.9 state: no image was published for it, and its SQLite,
storage, server and app crates are identical to v0.1.10's. The npm builds of
0.1.10 to 0.1.14 use the same features as the images and dump identically, so
they are not repeated here.

The `encryption_key` row holds a fixed test key (32 zero bytes), not the key
the release generated. The access key is the public AWS example key, its
stored secret is encrypted under the key that was replaced, and the admin
password hash is of a throwaway password. No dump holds any other secret.

Each dump starts with `PRAGMA foreign_keys=OFF`, as the shell's does, because
tables are dumped in name order and child rows come before their parents.

## Regenerating a dev-image dump

`regenerate.sh` runs the published image on a local port with an empty work
directory as its volume, seeds it with `seed.py` (boto3), stops it, and
writes the dump with `dump.py`, which replaces the encryption key on a copy
of the database. It needs Docker, `curl` and Python 3 with boto3. From this
directory:

```
./regenerate.sh 0.1.14 21500 /tmp/fixture-0.1.14
./regenerate.sh 0.1.14 21501 /tmp/fixture-0.1.14-backup --backup
```

The first writes `dev-image-0.1.14.sql`, the second
`dev-image-0.1.14-backup.sql`. The work directory must be empty or absent and
is left in place for you to delete; the container (named
`extenddb-fixture-<version>`, or `$CONTAINER`) is removed. Ids, account
numbers and timestamps are new on every run, so the golden files under
`../golden/` that a regenerated dump feeds are regenerated with
`EXTENDDB_BLESS_EXPECTED=1 cargo test -p extenddb-storage-sqlite migrations`.

`v0.1.3-to-v0.1.6.sql` is not a server's output: execute the `SCHEMA_SQL` of
tag `v0.1.3` (`crates/storage-sqlite/src/schema.rs`) on an empty file and
dump it with `python3 dump.py <file> v0.1.3-to-v0.1.6.sql`.

The PostgreSQL fixtures (`crates/storage-postgres/tests/fixtures/released/`)
come from a separate script with another dataset and another key rule. Each
backend's released-catalogs test compares an upgraded state with a fresh
install of the same backend seeded the same way, so the two datasets do not
need to match.
