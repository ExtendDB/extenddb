# Golden `extenddb-manifest.json`, format version 1

Three fixtures of the ExtendDB backup manifest at format version 1, each
with its sidecar in `sha256sum` format (`<name>.sha256`):

- `v1.json`: a key-ordered backup with every optional member present (a
  stream specification with a view type, a TTL description with its
  attribute name).
- `v1-minimal.json`: an empty table with every optional member absent
  (written as `null`) and every list empty.
- `v1-unordered.json`: an unordered backup, with the shapes the other two
  lack: data files with `"key_range":null`, and the absent state of every
  optional nested member `v1.json` sets: an `OnDemandThroughput` with both
  limits absent (each written as `null`, which is that wire type's
  serialization), an `SseDescription` without `SSEType` or
  `KMSMasterKeyArn`, a GSI without `ProvisionedThroughput` (the GSI of an
  on-demand table), a `StreamSpecification` without `StreamViewType` (a
  disabled stream) and a `time_to_live` without `AttributeName` (a disabled
  TTL, as `DescribeTimeToLive` reports it); and the nested members
  `v1.json` leaves absent: a vector index with `SearchSchema` and
  `Projection`, an LSI with an `INCLUDE` projection and `NonKeyAttributes`.

`golden_tests.rs` parses each fixture, checks its sidecar, re-serializes
it byte for byte, and compares it with the bytes the fixture writer's
constructors produce, so the writer and the files cannot drift apart.

The nested members reuse the DynamoDB wire types from `crates/core/src/types`,
so a change to one of those types changes this format. What catches which
change:

- The fixtures catch a change to the bytes written for their inputs: a
  renamed member, a `skip_serializing_if` toggle on a member, a new required
  member, and a renamed enum value that one of the fixtures uses. Every
  optional nested member is set in one fixture and absent in another.
- The test `nested_types_have_the_pinned_members` lists the members each
  type of the manifest reads and writes, so it also catches a new optional
  member that is omitted when absent, which leaves every fixture's bytes
  unchanged.
- The test `enum_values_are_pinned` lists the values each enum in the
  manifest reads and writes, so it catches a new variant and a renamed value
  that no fixture uses (`AES256`, `ALL`, `ENABLING` and others appear in no
  fixture). The derived enums are read through serde's variant list; the
  three with hand-written readers (`KeyType`, `ScalarAttributeType`,
  `DistanceFunction`) are pinned by a table of values filled from a `match`
  with no wildcard, so a new variant does not compile until it has a row.
  `AttributeValue`, the value type of every `key_range` endpoint and of
  every data file line, is pinned the same way through its ten type
  descriptors (`S N B SS NS BS BOOL NULL L M`), and the descriptors its
  reader accepts among every string of one to four upper-case letters are
  checked to be exactly those ten, so a descriptor the reader starts to
  accept (an alias) fails the test too.

Each test fails until its list is changed, which is a version bump.

Evolution rule (stated once more on `ExtenddbManifest` in `manifest.rs`):
any change to the bytes a writer produces for these fixtures' inputs is a
format version bump, a new member included. A member is added by bumping
`CURRENT_FORMAT_VERSION`, giving the member `#[serde(default)]` so the new
reader still reads v1, and writing `v{N}*.json` fixtures beside the v1
ones. A build reads every version from 1 to its own, so every version's
fixtures are permanent parse tests, never regenerated once that version is
released; a bump excludes older readers from newer backups whatever the
backup contains, which is the chosen trade. The writer writes the
fixtures of the current version, `v{CURRENT_FORMAT_VERSION}*.json`, and
refuses to overwrite a file that exists:

```sh
EXTENDDB_WRITE_GOLDEN=1 cargo test -p extenddb-core write_golden_manifest -- --ignored
```

Before version 1 is released its shape may still change in a PR; the
writer then writes into another directory (`EXTENDDB_WRITE_GOLDEN=<existing
directory>`) and the files are moved over the old ones, since the checked-in
files are `include_bytes!`'d by the tests and cannot be deleted first.

The values are synthetic; no backup was taken to produce them.
