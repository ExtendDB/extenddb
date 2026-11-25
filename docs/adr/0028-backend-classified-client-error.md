# ADR-0028: A backend-classified client error passes through the engine unchanged

- Status: Accepted
- Date: 2026-10-24
- Deciders: @LeeroyHannigan

## Context

Every storage trait method returns `Result<T, StorageError>`. The engine maps a
`StorageError` to the client-facing `DynamoDbError` in ten functions. Five end
in a catch-all that logs the error at error level and returns
`InternalServerError("Internal server error")`: the backup mapping
(`crates/engine/src/backup_requests.rs`), the `UpdateTable` mapping
(`update_table.rs`), the streams and TTL mappings (`streams.rs`, `ttl.rs`) and
`sanitize_storage_error` (`lib.rs`). The data path mapping (`create_table.rs`)
names every variant, and it logs and redacts the faults among them the same
way. The other four delegate to one of those six: the conditional check
mapping (`create_table.rs`) and the create-backup and restore mappings
(`backup.rs`) after a few variants of their own, and the shard lookup mapping
(`streams.rs`) after its check of the shard id length. The
server's authorization read delegates to the data path mapping. The redaction
is the rule the security model documents: driver text, SQL, paths and
connection details never reach a client (`docs/manuals/10-security-model.md`,
Error Message Security; RFC-0003 section 9.2).

The rule has a cost for a backend that knows more than the engine does. A
hosted database reports documented conditions with their own class and text:
a capacity limit, a maintenance window, a regional unavailability. The engine
has no variant for "here is the client error, already classified", so such a
backend can only return `Internal` (redacted to a generic 500 and logged as a
fault) or `Transient` (also redacted, and read by the in-tree queue workers as
the retry signal). The client sees a message that says nothing about what to
do, and the operator's log counts a documented condition among storage faults.

## Options Considered

1. **A `Service(DynamoDbError)` variant passed through unchanged.** The backend
   constructs the client error; every engine mapping site returns it without
   inspecting, rewriting or logging it. The backend takes on the redaction
   obligation for the message it supplies.
2. **A structured variant per condition** (`Throttled { kind, reasons }`,
   `Unavailable { retry_after }`, and so on), with the engine choosing the
   message. Keeps the message in the engine, at the cost of one variant and
   one engine mapping per condition a backend may want to report, and a
   message the backend cannot tailor.
3. **Leave the enum as it is.** A backend reports every failure as `Internal`
   or `Transient`; the client sees the fixed 500 text.

## Decision

Add `StorageError::Service(DynamoDbError)`, marked `#[error(transparent)]`,
and make every engine function that maps a `StorageError` to a client error
return it through one function, `create_table::service_err_to_dynamo`:
unchanged, and without the fault log line. That function applies the one rule
the engine keeps for the payload. A `ConditionalCheckFailedException` loses its
item, as the engine's own `ConditionFailed` does on the generic path, and only
the conditional check mapping returns the item, to a request that set
`ReturnValuesOnConditionCheckFailure` to `ALL_OLD`.

Three answers outside these functions depend on the error's class. In a
`BatchExecuteStatement` response, the entry of a statement that failed with a
throttle, an item collection size limit or an internal error carries the code
that the API Reference lists for that class (`BatchStatementError`).
`BatchGetItem` and `BatchWriteItem` hold back a throttle error for one entry
and return the entry as unprocessed for the client to send again; the storage
design (`docs/design/04-component-storage.md`, Error Types) states the rule
this puts on a backend's writes.
The server reads a table's key schema before the IAM decision, which needs it
for `dynamodb:LeadingKeys`, so that read treats a `ResourceNotFoundException`
as a missing table: a caller the IAM decision refuses gets the refusal, not the
backend's not-found.

## Rationale

- The engine cannot know the message for a condition only the backend sees;
  option 2 would re-derive each hosted database's documented text inside the
  engine, one variant at a time, and lag the databases.
- The status code and the body shape still come from the `DynamoDbError`
  variant through the one serializer, so a backend cannot produce a status or
  a shape outside the service's catalog through this variant.
- The redaction rule for `Internal` and every other variant is unchanged. A
  test checks it for `Internal` at the seven mappings the crate root can
  reach. The one new path is named in the security model, the backend author
  manual, RFC-0003 section 9.2 and the variant's doc comment, with the
  obligation stated the same way in each: the message crosses the wire
  verbatim and must be service-shaped text with no driver output, host names,
  connection strings or credentials.
- One set of rows, each a backend-classified error with a message the engine
  never writes, runs through every mapping and is compared with the body the
  client gets. At the seven mappings the crate root can reach, a subscriber
  also counts error-level log events: none for a `Service` error, one for an
  `Internal` error. The other three only delegate and write no log line of
  their own, and their modules run the rows through them.

## Consequences

- A backend can report a documented hosted-database condition with the
  service's error class and its own message, and the operator's fault log no
  longer counts it.
- Reviewers of a backend crate read the text it puts inside `Service` as
  client-visible wording, under the same rule as the engine's own messages.
- `StorageError` gains a variant; RFC-0002 now states that variants may be
  added in a minor release, so a backend crate matching on it exhaustively
  carries a catch-all arm. The engine's own data path match stays exhaustive
  so the compiler reports a mapping site that misses a new variant.
- No in-tree backend returns `Service` yet. The first one to do so adds the
  condition it reports and the measured service text to its fidelity notes.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
