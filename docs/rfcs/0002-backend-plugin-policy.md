# RFC-0002: Backend Plugin Development and Maintenance Policy

- Status: Draft
- Author: @jcshepherd
- Created: 2026-06-22
- FCP ends: (set when entering Final Comment Period)
- Tracking issue: #NNN

## Summary

This RFC establishes policies for developing, contributing, and maintaining database backend plugins for ExtendDB. It recommends a mono-repo architecture where all officially supported backends live as separate crates within the main ExtendDB repository, defines conformance requirements for backend acceptance, and establishes processes for external contributions.

## Motivation

ExtendDB's core value proposition is providing an Amazon DynamoDB-compatible API over multiple database backends. The reference PostgreSQL backend demonstrates feasibility. Active development of Apache Cassandra and SQLite backends shows demand for backend diversity. As the project grows, we need policies that:

1. **Scale beyond the core team.** The ExtendDB organization cannot personally implement and maintain every backend indefinitely. We must enable external contributions while ensuring quality.

2. **Preserve DynamoDB compatibility.** ExtendDB's value is compatibility with Amazon DynamoDB. Any backend released by the ExtendDB organization must demonstrate conformance with DynamoDB's APIs and features.

3. **Support backend diversity as a feature.** Different backends serve different needs: PostgreSQL (reference implementation), Cassandra (horizontal scale), SQLite (testing/embedded), and future backends extend ExtendDB's reach in terms of the infrastructure it can operate on and the use-cases it can support.

4. **Maintain velocity while managing complexity.** We must balance fast iteration for a small team with the eventual need for modular external contributions.

Without formal policies, we risk inconsistent quality, fragmented implementations, and contributor confusion. This RFC provides the framework for sustainable growth.

## Detailed design

### Repository structure: Mono-repo with feature flags

All officially supported backends live in the main `extenddb` repository as separate crates under `crates/storage-{backend}/`. Examples:

- `crates/storage-postgres/` (reference implementation)
- `crates/storage-cassandra/`
- `crates/storage-sqlite/`

Backends are selected at build time via Cargo feature flags:

```toml
[features]
default = ["postgres"]
postgres = ["dep:storage-postgres"]
cassandra = ["dep:storage-cassandra"]
sqlite = ["dep:storage-sqlite"]
all-backends = ["postgres", "cassandra", "sqlite"]
```

Mono-repo maximizes development velocity while avoiding the operational overhead of managing N separate repositories with N CI pipelines, N release processes, N issue trackers and N documentation sites. Cross-cutting changes spanning frontend and backend require one PR, one review, one merge.

Additionally, the `Storage` trait is still evolving. It will take several iterations to stabilize the trait API, and it will continue to evolve as ExtendDB adds support for new DynamoDB features. The mono-repo approach makes it significantly easier to keep the storage traits and all implementing backends in sync during this evolution. Trait changes can be tested, reviewed, and landed atomically across all backends rather than coordinating updates across multiple repositories.

### Conformance requirements

Backends released under the ExtendDB organization must adhere to a trait-based conformance model:

1. **Mandatory traits.** All backends must implement the core `Storage` traits (data plane and control plane operations) and pass 100% of conformance tests for those traits. Core traits are the minimum required for a functional DynamoDB-compatible backend.

2. **Optional traits.** Backends may choose not to implement optional traits (example: Streams, Transactions) or may stub them out to return `Unimplemented` errors. However, if a backend *does* implement an optional trait, it must pass 100% of conformance tests for that trait. Partial implementations are not permitted.

3. **All-or-nothing per trait.** The trait is the finest-grained unit of conformance. Backends cannot claim partial support for a trait (example: "Streams partially works but records are incomplete" is not allowed). Either the trait is fully implemented and conformant, or it is not implemented.

4. **Maintain semantic correctness.** Where a backend implements an operation, it must match DynamoDB behavior including error responses, pagination, isolation guarantees, atomicity guarantees, and consistency models.

5. **Clear documentation.** Backends must document in their README which optional traits they implement and which they do not. Conformance test results (per-trait pass rates) must be published and tracked in CI.

This model reduces friction by allowing new backends to launch with a well-defined subset of functionality without requiring support for every ExtendDB feature from day one. As backends mature, they can add optional traits incrementally, but each addition must be complete and correct. This prevents fragmented, partially-working implementations that erode ExtendDB's compatibility guarantee.

#### Conformance expectations and communication

**Compatibility matrices:** Backends must publish a compatibility matrix documenting which DynamoDB operations and features are supported. This matrix should be maintained in the backend's README and updated with each release. Example format:

| Feature | Status | Notes |
|---------|--------|-------|
| Basic CRUD | Supported | PutItem, GetItem, DeleteItem, UpdateItem |
| Query/Scan | Supported | Filters and projections supported |
| GSI/LSI | Not supported | Returns `OperationNotSupported` |
| Transactions | Partial | TransactWriteItems only; read transactions unsupported |
| Streams | Supported | Full support with configurable retention |

**Error handling for unsupported features:** Backends MUST return explicit errors for unsupported operations rather than silently degrading behavior or returning incorrect results. Acceptable approaches:
- Return `OperationNotSupported` error for unimplemented operations
- Return `ValidationException` with clear message for unsupported parameters within implemented operations
- Document degraded behavior explicitly (e.g., "eventually consistent reads treated as strongly consistent")

Backends MUST NOT silently ignore unsupported features or return partial/incorrect data.

**Capability discovery and enforcement:** To help users understand what a backend supports and to prevent confusing error messages deep in operation execution, ExtendDB should provide a mechanism for backends to declare their capabilities and for the server to check support before dispatching requests.

In a trait-based model, Rust's type system provides some of this enforcement: if a backend doesn't implement `StreamEngine`, stream operations won't compile. However, this doesn't help with partial trait implementations (e.g., a backend that implements `DataEngine` but not all optional parameters within PutItem).

In an operation-based model, explicit capability declaration becomes more important. Backends would declare supported operations in a manifest (TOML, JSON, or Rust code), which the server reads at startup to populate a capability registry. When a request arrives, the server checks the registry before dispatching. If the operation isn't supported, the server returns `OperationNotSupported` immediately without calling the backend.

This approach provides benefits regardless of conformance model:
- Users can query capabilities programmatically (e.g., `DescribeBackendCapabilities` API)
- Conformance tests can adapt to backend capabilities without manual configuration
- Error messages are consistent and occur at request validation rather than deep in execution
- Backend developers get a single place to document support

Implementation could range from simple (static capability declaration in backend code) to sophisticated (dynamic capability querying, versioned capability schemas). The key principle: make it easy for users to discover what works before trying it.

**Beyond DynamoDB compatibility:** Backends may implement features beyond DynamoDB compatibility (e.g., backend-specific query optimizations, extended data types, native full-text search). These extensions:
- MUST NOT break DynamoDB compatibility for standard operations
- MUST NOT fundamentally change the DynamoDB API paradigm
- MUST be clearly documented as extensions
- SHOULD be exposed via backend-specific APIs or configuration, not by altering DynamoDB API semantics
- MUST NOT be required for core DynamoDB functionality to work

The "DynamoDB API paradigm" centers on predictable, consistent performance where developers can reason unambiguously about operation cost. Extensions that introduce unpredictable performance characteristics are prohibited even if they don't technically break API compatibility. Examples:

**Prohibited extensions:**
- SQL-style joins (unpredictable performance, violates DynamoDB's explicit access pattern design)
- Automatic GSI selection via query optimizer (hides performance characteristics from developer)
- Cross-table transactions (changes atomicity guarantees and performance model)

**Acceptable extensions:**
- Backend-specific full-text search via separate API endpoint (doesn't alter Query/Scan semantics)
- Extended data types exposed through backend-specific configuration (DynamoDB types still work)
- Read-your-writes consistency mode as opt-in configuration (doesn't change standard consistency guarantees)

When in doubt, extensions should be backend-specific APIs rather than modifications to DynamoDB operations.

#### Trait classification

ExtendDB's storage abstraction consists of 13 traits covering storage operations and management functions. The following classification defines which traits are mandatory for backend acceptance and which are optional:

**Mandatory storage traits:**
- `TableEngine` - table-level operations
- `DataEngine` - item-level data operations

**Optional storage traits:**
- `MetadataEngine` - metadata operations
- `StreamEngine` - DynamoDB Streams support
- `WorkerStore` - background worker state

**Mandatory management traits:**
- `ManagementStore` - core management operations
- `AdminStore` - administrative functions
- `Bootstrapper` - initialization and bootstrap

**Mandatory authentication/authorization traits:**
- `AuthorizationStore` - authorization data (policies, users, groups, roles)

ExtendDB requires SigV4 authentication on all DynamoDB API requests. Backends must persist auth primitives (users, groups, roles, policies, access keys) to support this requirement. While these traits don't map directly to DynamoDB APIs, they are infrastructure requirements for a conformant ExtendDB deployment.

**Optional management traits:**
- `SettingsStore` - settings persistence
- `MetricsStore` - metrics collection
- `RateLimitStore` - rate limiting state
- `BackupEngine` - backup and restore

Note: This classification may be refined as the trait design evolves. Some traits may be split, merged, or reclassified before this policy is finalized.

Conformance test results are published in the backend's README and tracked in CI.

#### Alternate proposal: Operation-based conformance

Trait boundaries and feature boundaries don't always align. If conformance is trait-based ("implement all or nothing of a trait"), backends must implement entire traits even when only a subset of operations is needed. If conformance is operation- or feature-based ("implement these specific operations"), backends stub out unneeded operations but must track conformance at finer granularity.

If we define conformance in terms of a stable set of DynamoDB features rather than traits, a classification more like the following emerges:

**Mandatory DynamoDB operations (control plane):**
- CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable
- TagResource, UntagResource, ListTagsOfResource

**Mandatory DynamoDB operations (data plane):**
- PutItem, GetItem, DeleteItem, UpdateItem
- Query, Scan (basic, no index selection)
- BatchGetItem, BatchWriteItem

**Mandatory infrastructure traits:**
- `ManagementStore` - core management operations
- `AdminStore` - administrative functions
- `AuthorizationStore` - authorization data (policies, users, groups, roles)
- `Bootstrapper` - initialization and bootstrap

ExtendDB requires SigV4 authentication on all DynamoDB API requests. Backends must implement these infrastructure traits to persist auth primitives, even though they don't map directly to DynamoDB APIs. Unlike optional DynamoDB operations, these infrastructure traits remain mandatory in both conformance models.

**Optional DynamoDB operation classes:**
- Secondary indexes (GSI, LSI)
- Transactions (TransactGetItems, TransactWriteItems)
- Streams (ListStreams, DescribeStream, GetRecords)
- TTL (UpdateTimeToLive, DescribeTimeToLive)
- Import/Export (ImportTable, ExportTableToPointInTime)
- Advanced Query/Scan features (filters, projections, index selection)

Under this model, backends would declare supported operations explicitly (via manifest or code), and the server would maintain a capability registry for runtime enforcement. See "Capability discovery and enforcement" above for implementation considerations that apply to both conformance models.

**Tradeoffs:**

| Aspect | Trait-based | Operation-based |
|--------|-------------|-----------------|
| Developer clarity | "Implement these traits" | "Implement these operations" |
| Granularity | Coarse (whole trait) | Fine (individual operation) |
| Partial features | Forces full trait implementation | Allows targeted implementation |
| Maintenance | Trait changes affect all backends | Operation registry must stay stable |
| Test complexity | Test entire trait or skip it | Test operation-by-operation |
| Runtime checks | None (compile-time trait bounds) | Capability registry + error handling |
| Stub implementations | Not required | Required for unimplemented operations |

**Current recommendation:** Start with trait-based conformance for simplicity. The ExtendDB team currently maintains all backends and can absorb the cost of implementing full traits. Revisit operation-based conformance if:
- External contributors request narrower conformance targets
- Trait/feature misalignment becomes a blocking issue
- Backends emerge with fundamentally different capability models (e.g., read-only, control-plane-only)

This alternate proposal is documented for future consideration, not immediate adoption.

### Contribution process

#### For ExtendDB organization members

Standard PR review process applies. Changes require:
- Code review from at least one maintainer
- Conformance tests passing in CI
- Documentation updates

#### For external contributors

External contributors may propose new backends or maintain existing ones. The process:

1. **Proposal.** Open a GitHub issue describing the proposed backend, target use cases, and maintenance commitment. Include links to the underlying database project and any existing compatibility layers.

2. **Review.** Maintainers evaluate whether the backend aligns with ExtendDB's goals and whether the contributor can sustain maintenance. Approval grants the contributor directory-level write access via GitHub CODEOWNERS.

3. **Implementation.** Contributor develops the backend in `crates/storage-{backend}/` following the `Storage` trait contract. The contributor owns their backend directory but ExtendDB maintainers retain override authority for repository-wide concerns.

4. **Acceptance criteria:**
   - Conformance tests pass for all required traits and for any implemented, optional traits.
   - Documentation includes setup guide, architecture notes, troubleshooting
   - Integration tests run successfully in CI (contributor may need to provide sandbox credentials for cloud-based backends via GitHub Secrets)
   - Maintainer review approved

5. **Ongoing maintenance.** Contributor commits to:
   - Responding to issues and PRs related to their backend
   - Keeping dependencies updated
   - Adapting to breaking changes in the `Storage` trait
   - Supporting new DynamoDB features as feasible

If the original contributor becomes unresponsive, ExtendDB maintainers may take over maintenance or deprecate the backend.

### Architectural discipline

To prevent accidental coupling between frontend and specific backend implementations, CI should enforce that:

1. The `extenddb` service binary builds successfully without any backend feature flags enabled (frontend depends only on `storage` trait crate, not concrete backends).

2. Backend crates depend only on the `storage` trait and common utilities, not on each other.

This will be validated via:
```bash
cargo build --no-default-features --bin extenddb
```

(Note: as of June 2026, `extenddb catalog-check` still has a direct PostgreSQL dependency.)

### Release model

ExtendDB will move to a release model that includes releases through <a href="https://crates.io/">Crates.io</a>. All crates follow <a href="https://semver.org/">semantic versioning</a>. Breaking changes to `extenddb-storage` trigger coordinated releases. Binaries will also be available as GitHub releases. The project will also prioritize releasing images through Docker Hub.

### Third-party backends outside ExtendDB organization

Anyone can implement ExtendDB backends independently. The ExtendDB organization:
- Does **not** endorse or guarantee compatibility of third-party backends
- Does **not** provide support for third-party backends
- **May** link to third-party backends in a community-maintained list in documentation

Third-party backends should use distinct package names prefixed with the name of the backend itself (e.g., `mongodb-extenddb-storage` when published by a third party, not by the ExtendDB organization).

## Drawbacks

1. **Dependency bloat.** Mono-repo means all backend dependencies exist in the workspace even if only one backend is used. Mitigation: Cargo feature flags and workspace optimization reduce impact. Users building from source select only needed backends.

2. **CI failure impact.** CI failure in any backend blocks all releases. Mitigation: Backend tests run in parallel jobs. Release process can gate on critical backends (PostgreSQL) while allowing others to lag.

3. **Coordination overhead for trait changes.** Breaking changes to `Storage` trait require updating all backends simultaneously. Mitigation: Small team currently maintains all backends; design changes are coordinated. As external contributions grow, we establish trait stability periods.

4. **Access control granularity.** External contributors receive directory-level write access to main repo rather than owning separate repos. Risk: contributor error impacts shared infrastructure (CI config, dependencies). Mitigation: CODEOWNERS enforces review requirements; maintainers retain override authority.

## Alternatives

### Option 2: Multiple repositories (one per backend)

Create separate repos: `extenddb` (frontend + postgres), `extenddb-cassandra-plugin`, `extenddb-sqlite-plugin`, etc. Backends publish as separate crates linking against `extenddb-storage` traits.

**Pros:**
- Clean separation; no dependency conflicts between backends
- Backend contributors don't need frontend write access
- Backend CI failures don't block frontend releases
- Natural separation of maintenance responsibilities

**Cons:**
- More repos to manage (N CI configs, N release processes, N issue trackers)
- Cross-repo coordination overhead (trait changes require synchronized PRs)
- Trait version management complexity (breaking changes in `extenddb-storage` break other repos)
- Higher infrastructure setup and maintenance overhead
- Slower iteration for small team

**Why rejected:** Operational overhead outweighs benefits at current scale. Managing multiple repos requires infrastructure (CICD, docs, releases) and coordination (cross-repo PRs, trait versioning) that a small team cannot sustain. Mono-repo maximizes velocity. If the project scales to dozens of backends or hundreds of contributors, we can migrate to multi-repo with minimal user disruption (artifacts remain on crates.io and Docker Hub).

### Option 3: Fork per backend

Fork entire ExtendDB repo for each backend. Each fork contains frontend + one backend.

**Pros:**
- Complete isolation

**Cons:**
- Frontend code duplicated N times
- Bug fixes must be ported to N repos
- Nearly impossible to keep in sync
- Contradicts ExtendDB's architecture (decoupled frontend/backend)

**Why rejected:** Unworkable for any team size. Violates DRY principle and ExtendDB's design.

## Unresolved questions

1. **Security review process.** Backend implementations handle database credentials, execute queries, and manage connections—all surfaces with security implications (SQL injection, connection security, credential leakage). Should we formalize security review as part of the PR process? If so, what does that look like? Do we need security-focused reviewers, automated scanning tools, or documented security patterns? This wasn't part of the original discussion but seems important for production deployments.

2. **Conformance test ownership.** Should conformance tests live in the main `extenddb` repo, or in a separate `extenddb-conformance` repo used by all backends? Leaning toward main repo for simplicity, but open to feedback.

3. **Credential management for cloud backends.** External contributors implementing cloud-based backends (e.g., AWS DynamoDB passthrough, Azure Cosmos DB) need sandbox credentials in CI. How do we handle credential provisioning? Options:
   - Contributor provides credentials via GitHub Secrets (preferred)
   - ExtendDB org funds test accounts (expensive, scales poorly)
   - Backend maintainers run their own CI externally and report results (trust issue)

4. **Trait stability guarantees.** As external backends grow, we need a policy for breaking changes to `Storage` trait. Options:
   - Major version bumps with migration guide
   - Deprecation periods (announce breaking change, grace period for backends to adapt)
   - Trait versioning (maintain old trait versions temporarily)

## Prior art

**Diesel (Rust SQL toolkit):** Mono-repo with multiple database backends (PostgreSQL, MySQL, SQLite). Backends are separate crates but all maintained in one repo. Uses feature flags for selection. Demonstrates that mono-repo scales for database abstraction layers.

**SQLx (Rust async SQL library):** Similar mono-repo structure with feature-flagged backends. Strong conformance via compile-time query checking.

**Apache Arrow DataFusion:** Query engine with pluggable data sources. Data sources live in-tree and out-of-tree. In-tree sources have strong conformance requirements; out-of-tree sources are community-maintained.

**PostgreSQL ecosystem:** Single Postgres core repo, extension ecosystem lives externally. Extensions link against stable APIs. Demonstrates successful decoupling but requires strong API stability guarantees (10+ year compatibility).

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
