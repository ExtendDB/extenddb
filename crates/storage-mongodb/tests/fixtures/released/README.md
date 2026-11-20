# Released MongoDB catalogs

Each `<state>.json` is a deployment made by a released image, printed by
`dump.js` in canonical Extended JSON with every object's key order kept:
for both databases, every collection with its options, its indexes and its
documents. `load.js` loads one into an empty server with `mongosh`, and the
Rust tests load them with `tests/migrations_support/fixture.rs`.

| File | Made by |
|---|---|
| `0.1.12.json` | `make-fixture.sh 0.1.12 ...` with `extenddb/extenddb-mongodb:0.1.12`. It stands for v0.1.4 to v0.1.11 as well: their bootstrapper is the same file (`git rev-parse v0.1.4:crates/storage-mongodb/src/bootstrapper.rs` equals the v0.1.12 one), and no image older than 0.1.12 exists |
| `0.1.13.json` | `make-fixture.sh 0.1.13 ...` |
| `0.1.14.json` | `make-fixture.sh 0.1.14 ...`; the first release that seeds the `resource_tag_migration` row |
| `dotted-ttl.json` | `make-fixture.sh 0.1.12 ... dotted-ttl`: a second table, `cu_dotted`, with TTL on `exp.at`, and the index releases before v0.1.12 created for that attribute (`idx_ttl_exp.at` on `item_data.exp.at.N`, sparse), which v0.1.12 and later do not create |

Every state was made on 2026-10-29 against `mongo:7` (server 7.0.45) as a
one-node replica set: `init`, then the image's server, then through
`extenddb manage` the account 123456789012 with the IAM user `cu-tester`, the
inline policies `dynamodb-full` and `deny-prod` (a Deny whose condition
names `dynamodb:ResourceTag/env`) and an access key, then `seed.py`: the
table `cu_orders` with a GSI, an LSI, a stream, TTL on `expires` and ten
items. The deployments were throwaway; their encryption keys and password
hashes are kept so a loaded fixture can be served.

To make one again (Docker, `mongo:7`, Python with boto3; the ports are
published on 127.0.0.1 only):

```
crates/storage-mongodb/tests/fixtures/released/make-fixture.sh 0.1.14 fixture-mongo 27031 18491
```
