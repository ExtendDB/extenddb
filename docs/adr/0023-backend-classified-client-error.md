# ADR-0023: A backend-classified client error passes through the engine unchanged

- Status: Accepted
- Date: 2026-10-24
- Deciders: @LeeroyHannigan

## Context

Every storage trait method returns `Result<T, StorageError>`. The engine maps a
`StorageError` to the client-facing `DynamoDbError` at nine functions
(`crates/engine/src/create_table.rs`, `backup.rs`, `update_table.rs`,
`streams.rs`, `ttl.rs`, `lib.rs`), each of which names the variants it knows
and ends in a catch-all that logs the error at error level and returns
`InternalServerError("Internal server error")`. The catch-all is the redaction
rule the security model documents: driver text, SQL, paths and connection
details never reach a client (`docs/manuals/10-security-model.md`, Error
Message Security; RFC-0003 section 9.2).

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
return it unchanged and without the fault log line. The engine applies one
rule of its own to the payload: for `ConditionalCheckFailedException`, the
request's `ReturnValuesOnConditionCheckFailure` decides whether the item is
returned, as it does for the engine's `ConditionFailed`.

## Rationale

- The engine cannot know the message for a condition only the backend sees;
  option 2 would re-derive each hosted database's documented text inside the
  engine, one variant at a time, and lag the databases.
- The status code and the body shape still come from the `DynamoDbError`
  variant through the one serializer, so a backend cannot produce a status or
  a shape outside the service's catalog through this variant.
- The redaction rule for `Internal` and every other variant is unchanged, and
  a test at each site keeps it so. The one new path is named in the security
  model, the backend author manual, RFC-0003 section 9.2 and the variant's
  doc comment, with the obligation stated the same way in each: the message
  crosses the wire verbatim and must be service-shaped text with no driver
  output, host names, connection strings or credentials.
- A subscriber-based test counts error-level log events at every mapping
  site: none for a `Service` error, one for an `Internal` error, so the
  "not logged as a fault" half of the contract cannot regress silently.

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
