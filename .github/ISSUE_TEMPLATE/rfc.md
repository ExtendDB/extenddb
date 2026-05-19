---
name: RFC (design proposal)
about: Propose a significant change to ExtendDB (new backend, API addition, breaking change)
title: "[RFC] "
labels: rfc
---

## Summary

One paragraph explaining the proposal.

## Motivation

Why should ExtendDB do this? What use cases does it support? What problems does it solve?

## Proposed design

Describe the design in enough detail that someone familiar with ExtendDB could implement it. Include:

- API surface (new operations, changed behavior, wire format)
- Storage implications (schema changes, new tables, migration path)
- Configuration (new `extenddb.toml` sections or flags)

## DynamoDB compatibility

How does this relate to the real DynamoDB API? Is this:
- [ ] Matching existing DynamoDB behavior
- [ ] Extending beyond DynamoDB (ExtendDB-specific)
- [ ] Deliberately diverging from DynamoDB (explain why)

## Alternatives considered

What other approaches did you consider? Why is this one better?

## Breaking changes

Does this break existing behavior? If yes:
- What breaks?
- What is the migration path?
- Can it be feature-gated during transition?

## Open questions

List anything unresolved that needs discussion before implementation.
