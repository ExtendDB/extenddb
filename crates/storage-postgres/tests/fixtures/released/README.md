# Released PostgreSQL catalogs

Each directory holds one released state as two plain dumps, `catalog.sql`
(the catalog database) and `data.sql` (the data database), made with
`pg_dump --inserts --no-owner --no-privileges`, plus the PostgreSQL and
release versions that made it and, for image states, the seed steps.
`tests/migrations_released.rs` loads each pair into two empty databases,
upgrades them with this binary, and compares the result with a fresh install
seeded the same way.

| State | Made by | Seeded |
|---|---|---|
| `0.1.5`, `0.1.6`, `0.1.8`, `0.1.10`, `0.1.11`, `0.1.12`, `0.1.13`, `0.1.14` | The published `extenddb/extenddb-postgres:<version>` image on `postgres:16.10-bookworm` | Yes |
| `pv-0.1.11`, `pv-0.1.14` | The same images on `pgvector/pgvector:pg16` | Yes, and a vector table |
| `v0.1.0`, `v0.1.2` | That tag's SQL files, applied as its `init` did; no image of these versions was published | No: catalog-only |

Tags without a fixture ship the same schema files as one that has one,
according to their blob hashes: v0.1.1 as v0.1.0, v0.1.3 and v0.1.4 as
v0.1.5, v0.1.7 and v0.1.9 as v0.1.8.

An image state is the release's `init` (as `docker/README.md` documents),
its `serve`, `extenddb manage` for account `123456789012`, user `tester`, a
policy and an access key, then `seed.py`: `seed_tbl` with a sort key, the GSI
`gsi1`, the LSI `lsi1`, a stream and TTL on `exp`, ten items, the backup
`seed_backup`, and `seed_simple` with three items; on pgvector also `vec_tbl`
with the vector index `vidx` and three vectors.

## Test values

`scrub.py` replaces, in every dump:

- the encryption key with bytes 0 to 31 (base64
  `AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=`);
- every password hash with one fixed bcrypt string;
- the data connection string with
  `postgresql://extenddb_app:fixture@fixture-host:5432/extenddb`, which the
  tests overwrite with their own database.

The access key's stored secret stays encrypted under the release's own key,
which is gone, so it cannot be decrypted; no test needs it. The dumps hold no
other secret.

## Regenerating

    TMPDIR=<dir> ./regenerate-all.sh

or one state with `./regenerate.sh <state> <n>`. Each run uses containers
`extenddb-fixture-pg-<n>`, `extenddb-fixture-pg-<n>-init` and
`extenddb-fixture-pg-<n>-srv` on the network `extenddb-fixture-pg-net-<n>`,
publishes the release server on 127.0.0.1 at port 21550 + n, and removes the
containers, their volumes, the network and the release image at the end
(an image that was present before the run stays). `FIXTURE_PREFIX` and
`FIXTURE_BASE_PORT` change the names and the port base. It needs Docker, `git` (for the tag states) and Python 3 with
`boto3` and `requests`.
