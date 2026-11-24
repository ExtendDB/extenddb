# ADR-0031: How the PostgreSQL and MongoDB backends store U+0000

- Status: Accepted
- Date: 2026-11-24
- Deciders: @LeeroyHannigan

## Context

DynamoDB accepts the character U+0000 anywhere a string appears: in partition,
sort and index key values, in values, in map keys and in attribute names. It
sorts as the byte 0x00 (measured on 2026-09-18, issue #364). Two backends
cannot store it as written:

- PostgreSQL `TEXT` refuses the byte 0x00, and `jsonb` refuses the escape
  `\u0000`. Key values live in `TEXT COLLATE "C"` columns that form btree
  primary keys. Item documents are `jsonb`: `item_data`, the stream records,
  the item images of the queued index updates, and `backup_items`.
- BSON field names are C strings, so MongoDB cannot hold U+0000 in an
  attribute name or a map key. BSON string values are length-prefixed and
  hold it.

Before this decision, PostgreSQL answered 500 to every item that held U+0000,
and MongoDB answered 500 to an item whose attribute name or map key held it.

PostgreSQL refuses a btree entry over 2704 bytes. DynamoDB allows a partition
key value of 2048 bytes and a sort key value of 1024 bytes, so an encoding
that makes key text longer can push a key that an earlier release stored over
that limit.

Data that an earlier release stored must keep reading back as written.

## Options Considered

1. **Escape every string on PostgreSQL, keys included.** Store U+0000 as
   U+0001 U+0001 and U+0001 as U+0001 U+0002 in the key columns and in the
   documents. The mapping keeps byte order and prefixes, so key comparisons
   give the same rows. Each U+0000 and U+0001 adds one byte. A 2046-byte
   partition key with 682 U+0001, which DynamoDB and earlier releases store,
   becomes 2728 bytes and fails the btree limit: its PutItem answers 500, and
   a migration that rewrites the key columns stops on such a row.
2. **`bytea` key columns on PostgreSQL.** Key text stored as bytes holds 0x00
   at its raw length. Every key column, every bind site and every key
   comparison changes type, and a migration rewrites every key column of every
   table.
3. **Escape the documents only, and refuse U+0000 in keys on PostgreSQL.** The
   key columns keep the text earlier releases stored. A table or index key
   value that holds U+0000 gets a `ValidationException`, which issue #364
   accepts for a backend that cannot store the byte. The strings of the
   `jsonb` documents go through the escape of option 1; no btree entry covers
   them.
4. **MongoDB: escape every field name.** A name that an earlier release stored
   with U+0001 followed by U+0001 or U+0002 then reads back changed, and two
   such names, `c\x01` and `c\x01\x02`, read back as one. The next write of
   the item stores it without the other attribute.
5. **MongoDB: escape the field names of a document only when one holds
   U+0000, and mark that document.** No earlier release could store such a
   name, so every document an earlier release wrote is unmarked, and an
   unmarked document is read as it is stored.

## Decision

- PostgreSQL takes option 3: the strings of item documents go through the
  escape of `extenddb_storage::util::escape_control`, the key columns hold the
  key text as earlier releases stored it, and a request whose table key or
  index key value holds U+0000 gets a `ValidationException` that names the
  limitation.
- MongoDB takes option 5: a document whose field names were escaped carries
  the top-level field `names_escaped` set to `true`, and only such a document
  is decoded. Stream records follow the same rule.
- Catalog migration 13, `escape_control_chars`, re-encodes on PostgreSQL the
  documents earlier releases wrote that hold U+0001, in one transaction per
  database with its ledger row. It is breaking and not convergent, because
  the escape is not idempotent. It does not apply on SQLite, Cassandra and
  MongoDB.

## Consequences

- On PostgreSQL every key an earlier release stored keeps its bytes and its
  length. Migration 13 rewrites no key column, and `gsi_pending`'s
  `worker_partition`, a hash of the base key text, does not change.
- U+0000 in a key value is a documented difference on PostgreSQL
  ([Differences from DynamoDB](../differences-from-dynamodb.md)). An item
  written before an index existed whose key for that index holds U+0000 is
  left out of the index, by the backfill and by a restore, as an item whose
  key attribute has another type is.
- No index entry or constraint covers the strings of a document, so a rewrite
  of migration 13 fails only when the database does. The catalog body commits
  first, so such a failure leaves the data store behind the catalog store
  until `extenddb migrate --yes` runs the data body again
  ([upgrade manual](../manuals/07-upgrade-manual.md)).
- The native MongoDB update paths do not read the document, so they cannot
  tell a marked document from an unmarked one. An update whose attribute name,
  or a field name inside its value, holds U+0000 or U+0001 takes the session
  path.
- Going back to an earlier release on PostgreSQL means restoring both
  databases from the backups taken before the upgrade, as for every breaking
  migration.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
