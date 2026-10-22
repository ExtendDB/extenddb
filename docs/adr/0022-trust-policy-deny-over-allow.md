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
- **Upgrade impact for existing deployments:** two further changes affect
  deployments upgrading from a build that ignored `Deny`. First, passing session
  tags on an `AssumeRole` call now exercises `sts:TagSession`, so a role whose
  trust policy allows only `sts:AssumeRole` denies a tagged call that previously
  succeeded; allow `sts:TagSession` in the trust policy to keep passing tags.
  Second, `create_role` now validates the trust document the way IAM does, so
  automation that sent a document IAM rejects (a `Resource` field, `sts:*`, a
  bare `"*"` principal, a missing `Principal`, and so on) starts getting a 400
  at role creation. Existing stored roles are never re-validated, so no stored
  role is locked out at evaluation.
- **Condition-key resolution.** A condition key falls into one of three cases,
  in both trust-policy and identity-policy evaluation. (1) The context resolves
  the key to a value: evaluate normally. (2) The context resolves it as absent
  the same way the service would: IAM absent-key semantics apply, including that
  a negated operator on an absent single-valued key matches. (3) A documented
  global key the service populates for one of ExtendDB's callers but the context
  cannot resolve: the statement fails closed whatever the operator, so a `Deny`
  blocks and an `Allow` does not grant, rather than the key being treated as
  absent (which would let a negated operator spuriously match). ExtendDB resolves
  every global key the service populates for a signed IAM-user or assumed-role
  request, or an `AssumeRole` call, so case 3 is empty: the resolved set is
  `aws:SecureTransport` (listener TLS state), `aws:RequestedRegion` (server
  region), `aws:CurrentTime` and `aws:EpochTime` (request time),
  `aws:PrincipalType`, `aws:username` (user caller), `aws:userid`
  (`AIDA...` for a user, `<role-id>:<session-name>` for a role session),
  `aws:PrincipalArn`, `aws:PrincipalAccount`, `aws:ResourceAccount`,
  `aws:SourceIp` (connection peer address, evaluated with `IpAddress` and
  `NotIpAddress` as IPv4 and IPv6 CIDR membership), `aws:UserAgent` (the
  `User-Agent` header), `aws:ViaAWSService` and `aws:PrincipalIsAWSService`
  (both false for a direct caller), the tag keys (`aws:PrincipalTag/*`,
  `aws:RequestTag/*`, `aws:ResourceTag/*` on the identity path,
  each matched case-insensitively on the key),
  `sts:ExternalId` and `aws:TagKeys`, and, for a role-session caller only,
  `aws:TokenIssueTime` (the session issue time) and `aws:MultiFactorAuthPresent`
  (`false` for a session assumed without MFA), both absent for an IAM user.
  Every other `aws:` global key is absent,
  which is what the service does for a key it does not populate for this caller
  and for a key it does not recognize: the organization keys
  (`aws:PrincipalOrgID`, `aws:PrincipalOrgPaths`, and the resource and source
  organization keys), because an ExtendDB account is a standalone AWS account
  where IAM leaves them unset; the VPC-endpoint, federation, service-principal
  and cross-service keys, and `aws:MultiFactorAuthAge`, which the service does
  not populate for a direct IAM-user or assumed-role caller over a public
  endpoint; and any
  unknown or future `aws:` key. The evaluator keeps an explicit (currently
  empty) fail-closed list, so a future global key the service begins populating
  before ExtendDB resolves it can be made to fail closed by listing it, rather
  than silently granting through a negated operator. `aws:SourceIp` and
  `aws:UserAgent` come from the connection and the `User-Agent` header; behind a
  proxy the peer address is used and `X-Forwarded-For` is never trusted. The
  full classified key list is in the admin guide.
- **Follow-up:** the decision logic was factored into a store-free
  `trust_decision` helper (with `action_permitted`, `deny_statement_blocks`,
  `allow_statement_grants` and `effect_of`) so the
  Deny-over-Allow ordering is unit-tested directly. The decision is made per
  exercised STS action: `sts:AssumeRole`, plus `sts:TagSession` when the call
  passes session tags (ExtendDB does not implement source identity, so
  `sts:SetSourceIdentity` is never exercised). `Effect` is compared
  case-insensitively, `Action`/`NotAction` are matched, and account principals
  (`{"AWS": "<account>:root"}` and a bare account id) match any caller in the
  account. A statement whose `Condition` block fails to parse is handled
  asymmetrically: an unparseable `Allow` does not grant, and an unparseable
  `Deny` fails closed, so it blocks rather than being silently dropped. A
  malformed `Deny` therefore cannot grant assumption.
- **Write-time validation:** trust documents are validated when a role is
  created, so a document IAM would reject (a missing or invalid `Effect`,
  `NotPrincipal`, a missing `Principal`, a bare-string or wildcard-ARN
  principal, a missing `Action`, an action outside the STS trust set, a
  `Resource` field, or an unknown or malformed `Condition`) is not stored and
  then dropped at evaluation. Documents IAM accepts (a missing `Version`, a
  single-object `Statement`, numeric condition values, unknown condition keys)
  are accepted.
- **Commitment:** trust-policy and identity-policy evaluation should stay aligned
  on evaluation order; a future change to one should consider the other.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
