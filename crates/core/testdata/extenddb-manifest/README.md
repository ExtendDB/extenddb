# Golden `extenddb-manifest.json`, format version 1

`v1.json` is the frozen on-disk shape of the ExtendDB backup manifest at
format version 1, with every optional member present, and `v1.sha256` is its
sidecar in `sha256sum` format. `golden_tests.rs` parses `v1.json` and
re-serializes it byte for byte.

The nested members reuse the DynamoDB wire types from `crates/core/src/types`.
A change to one of those types (a rename, a `skip_serializing_if` toggle, a
new required member) changes this format without any other test noticing;
this fixture fails that change. Regenerate it only together with a
`format_version` bump:

```sh
cargo test -p extenddb-core write_golden_manifest -- --ignored
```

The values are synthetic; no backup was taken to produce them.
