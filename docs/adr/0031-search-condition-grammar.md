# ADR-0031: SearchVectors serves the extended search condition grammar from its own parser, behind a feature and a setting

- Status: Proposed
- Date: 2026-11-25
- Deciders: @demohan

## Context

DynamoDB accepts a `SearchConditionExpression` with `<>`, `<`, `<=`, `>`, `>=`,
`BETWEEN`, `IN`, `OR`, `NOT`, parentheses, `attribute_exists` and
`attribute_not_exists`, up to 50 conditions with one `=` on the index's HASH
attribute among them, on the accounts and regions measured (us-east-1,
eu-north-1, ap-southeast-2). Its API reference documents only `=`. ExtendDB
accepted only an AND of equalities.

Three parts of ExtendDB meet this grammar:

- **The parser.** Core already parses the condition grammar for
  ConditionExpression, FilterExpression, KeyConditionExpression and
  UpdateExpression (`tokenizer.rs`, `parser.rs`, `parser_common.rs`). It
  recurses, refuses past `max_expression_depth` (150 by default) with
  `expression nesting depth exceeded`, and words its syntax errors in its own
  terms. For a SearchConditionExpression the service answers a syntax error
  with `Syntax error; token: "<token>", near: "<text>"` and parses a condition
  nested as deep as the 4096-byte limit allows. Measured in eu-north-1 on
  2026-11-25, each request twice, the service answers ConditionExpression and
  FilterExpression the same way: `a == :s`, `a = 10` and `_Cat = :v` are
  syntax errors that name the token, 4089 unclosed `(` answer at `<EOF>`, and
  151 and 160 nested `NOT (` and `(a) = :v` are accepted. The existing parser
  answers each of those differently.
- **The storage seam.** `VectorSearch` carried the HASH equality and a list of
  equalities, which every backend applies in its own query. A range, `IN`,
  `OR`, `NOT` or a function cannot be written as an equality pair.
- **Authorization.** The service sets no `dynamodb:Attributes` value on a
  SearchVectors, whatever the projection and the condition name (measured
  2026-11-21: a Deny gated on `Null` `true` refused every search).

## Options Considered

1. **Extend the existing parser** to carry the token and near text and to
   parse without recursion, and parse the search condition with it. Every
   other expression would then answer its syntax errors and its depth as the
   service does, which changes their answers on the shapes above; keeping
   their earlier answers needs a mode at the tokenizer, at each error site, at
   the depth rule and at the reserved-word check.
2. **A separate parser for the search condition**, written to the service's
   answers for SearchConditionExpression, that lowers to the `Expr` the
   evaluator already runs.
3. **Lower every condition to backend SQL**, widening the equality pairs into
   a predicate type each backend translates.

## Decision

Option 2: the grammar has its own lexer, grammar and checks in
`crates/core/src/expression/search_filter`, behind the
`search-inequality-filters` Cargo feature and the `search_inequality_filters`
runtime setting (default `all`); a condition the equality pairs cannot express
reaches the backend as `VectorSearch::item_filter`, which the backend calls on
candidates to keep the first `top_k` that match (SQLite on each candidate,
PostgreSQL on the rows its SQL returns in score order, page by page); SearchVectors sets no `dynamodb:Attributes` value.

## Rationale

- Option 1 changes the answers of ConditionExpression and the other
  expressions, which is a change to those operations with its own measurements
  and its own review. Option 2 changes no answer outside SearchVectors.
- The evaluator that FilterExpression uses already decides a missing attribute
  and a type mismatch as the service does, so the lowered `Expr` reuses it. The
  BETWEEN bound check, the operand display, the map key checks, the usage
  checks and the size checks are shared with the other expressions.
- Option 3 needs a translation per backend for every operator and function,
  and two backends serve vector search. `item_filter` is one call per
  candidate; PostgreSQL reads a filtered search in bounded pages of its score
  ordering, and an equality-only condition keeps the SQL equality filter.
- The feature keeps the grammar removable: with it compiled out, SearchVectors
  serves the equality-only grammar. The setting lets an operator keep the
  earlier grammar for some or all accounts without a rebuild.

## Consequences

- Two parsers of the condition grammar exist in core. A change to the shared
  grammar is made in both, until the other expressions move to the second
  parser in a change of their own.
- `VectorSearch` has a public field, `item_filter`, that every caller sets and
  every backend that serves vector search honours.
- A deployment that gates SearchVectors on `dynamodb:Attributes` gets the
  service's answer for an absent key: a Deny gated on `Null` `true` refuses
  every search, and a bare negated operator over the key matches.
- Removing the feature is deleting the module, the dispatch arms and the
  setting.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
