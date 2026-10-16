# ADR-0022: Role trust-policy evaluation follows IAM Deny-over-Allow order

- Status: Accepted
- Date: 2026-10-15
- Deciders: @navamol

## Context

`AssumeRole` (`crates/server/src/management/assume_role.rs`) decides whether a
caller may assume a role by evaluating the role's trust policy against the
caller ARN. The original `evaluate_trust_policy()` iterated the statements and
skipped any whose `Effect` was not `"Allow"`:

```rust
if effect != "Allow" {
    continue;
}
```

A matching `Allow` returned `true`; otherwise the function returned `false`
(implicit deny). The consequence is that `Deny` statements in a trust policy
were never evaluated; they behaved as no-ops. An administrator who added a
`Deny` for a specific principal alongside a broad `Allow` (for example
`Allow Principal "*"` plus `Deny Principal <arn>`) got no protection: the denied
principal was still granted assumption through the `Allow`.

This diverges from IAM semantics and, more importantly, from ExtendDB's own
identity-policy evaluator (`crates/auth/src/policy/evaluator.rs`), which already
implements the standard order: phase 1 is an explicit-deny scan across all
policies before any `Allow` is considered. Trust-policy evaluation and
identity-policy evaluation disagreeing on how `Deny` works is a correctness and
least-privilege hazard: the behavior a trust policy's author reasonably expects
(a `Deny` denies) is the behavior the identity evaluator already provides.

The endpoint is admin-only (Basic Auth) and the
admin supplies the caller ARN, which bounds the practical blast radius, but the
authorization logic itself was incorrect.

## Options Considered

1. **Leave Allow-only, document the limitation.** Keep the existing behavior and
   state in the docs that trust policies support only `Allow`. Cheapest, but it
   leaves a silently-ignored `Deny` as a trap: policy authors would still write
   `Deny` statements expecting them to work, and the two policy evaluators would
   remain inconsistent.

2. **Reject trust policies that contain `Deny` at role-creation time.** Fail
   fast so no one can author a `Deny` that does nothing. Removes the trap but
   also removes a legitimate, expected capability, and still leaves trust-policy
   evaluation less expressive than identity-policy evaluation.

3. **Evaluate `Deny` before `Allow` (IAM order).** Scan for a matching explicit
   `Deny` first and block assumption if one is found; only then look for a
   matching `Allow`. Matches IAM and the existing identity-policy evaluator.

## Decision

Trust-policy evaluation follows IAM order: a matching explicit `Deny` blocks
role assumption and is evaluated before any `Allow` (Option 3). With no matching
`Deny`, a matching `Allow` grants assumption; with neither, assumption is
implicitly denied.

## Rationale

- **Consistency with the identity-policy evaluator.** `policy/evaluator.rs`
  already does explicit-deny-first across identity policies, permissions
  boundaries, and session policies. Trust policies evaluating `Deny` differently
  was an inconsistency a reader would have to discover by accident.
- **Principle of least astonishment / least privilege.** A trust policy that
  says `Deny` must deny. Silently ignoring it fails safe only by luck (when
  there is no broad `Allow`) and fails open in exactly the case an author writes
  a `Deny` to guard against.
- **Matches the semantics callers already know.** ExtendDB speaks the DynamoDB /
  IAM wire contract; explicit-deny-wins is the IAM evaluation rule operators
  expect.
- **Not an auth-model change.** This corrects behavior to the documented and
  already-implemented-elsewhere model. It does not introduce a new policy
  language, condition key, or evaluation phase, so it is a bug fix rather than an
  RFC-level change to the auth model.

## Consequences

- **Easier:** trust-policy authors can rely on `Deny`, and the two policy
  evaluators now share one mental model (explicit deny wins).
- **Behavioral change to be aware of:** a trust policy that already contained a
  matching `Deny` alongside a broad `Allow` previously granted assumption and
  will now block it. This is the intended fix, but it is a behavior change for
  any deployment that (perhaps unknowingly) relied on `Deny` being ignored. There
  is no change for trust policies that contain only `Allow` statements.
- **Follow-up:** the decision logic was factored into a store-free
  `trust_decision` helper (plus `statement_matches` / `effect_of`) so the
  Deny-over-Allow ordering is unit-tested directly. A statement whose
  `Condition` block fails to parse is treated as non-matching in both the `Deny`
  and `Allow` phases, matching the pre-existing `Allow`-path handling.
- **Commitment:** trust-policy and identity-policy evaluation should stay aligned
  on evaluation order; a future change to one should consider the other.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
