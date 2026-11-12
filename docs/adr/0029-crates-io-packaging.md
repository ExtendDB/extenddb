# ADR-0029: The workspace crates go to crates.io together, at one version with exact pins

- Status: Accepted
- Date: 2026-11-10
- Deciders: @LeeroyHannigan

## Context

Today a user builds ExtendDB from a clone with `cargo build --release`, or
runs a container image or the npm launcher. Publishing to crates.io adds the
short form `cargo install extenddb --locked`, an entry in the registry's
search, and docs.rs pages for the library crates. These facts constrain how
the workspace can publish:

- crates.io refuses a dependency that is not on crates.io. The one exception
  is a dev-dependency without a version, which Cargo removes from the
  published manifest. Cargo drops the path of a path dependency when it
  packages a crate, so each path dependency also needs a version. The `extenddb` binary
  depends on the other 13 workspace crates, so publishing the binary means
  publishing all 14.
- On main at e681b8bf2, `cargo package --workspace` stopped at its first crate
  with an internal dependency: "all dependencies must have a version
  requirement specified when packaging. dependency `extenddb-cache` does not
  specify a version".
- `extenddb-engine` and `extenddb-storage-sqlite` are dev-dependencies of each
  other. With a version on every internal dependency, packaging stopped with
  "no matching package named `extenddb-engine` found". Each of the two crates
  must exist on crates.io before the other.
- ExtendDB patch releases change the Rust APIs between its crates. For a 0.x
  version, Cargo reads the requirement `0.1.14` as "0.1.14 or any later
  0.1.x".
- A build contains exactly one storage backend. `crates/bin/src/main.rs`
  rejects two backend features at compile time.
- Without `--locked`, `cargo install` resolves every dependency again. Measured
  on v0.1.14 from an empty directory: on Rust 1.88, the minimum supported
  version, resolution chose `uuid` 1.29.2, which needs Rust 1.89, and the build
  failed. With `resolver = "3"`, cargo resolved for Rust 1.88, chose `uuid`
  1.26.1, and the build passed. With either resolver, the fresh lock changed
  the version of 173 of the 425 third-party crates that it shares with the
  tested lock.
- No package contained LICENSE or NOTICE. Section 4 of the Apache License 2.0
  requires each redistributor to pass on a copy of the license and the NOTICE
  attributions. A crate from crates.io would have reached its user without
  either file.

## Options Considered

1. **Publish only the binary**: crates.io refuses it, because its
   dependencies are path dependencies.
2. **One binary crate per backend**, named like the container images:
   `extenddb-postgres`, `extenddb-mongodb`, `extenddb-cassandra` and
   `extenddb-dev`. Each installs with one short command, and the four binaries
   can be installed together. The workspace gets 4 more crates, 18 in all. The rate
   limit on new crates adds about 40 minutes to the first publish, and each
   crate needs its own owners and trusted publisher entry. Two database tools
   with millions of downloads each, diesel_cli and sqlx-cli, publish one
   binary crate each and select databases with Cargo features.
3. **One binary crate, `extenddb`, with a Cargo feature that selects the
   backend**, as the repository builds it today. The crate README gives one
   install command per backend.

For the version requirements between the workspace crates:

- Caret requirements (`0.1.14`): after the next release exists, a fresh
  install of an `extenddb` release resolves each library crate to the next
  release. The binary's own `main.rs` then builds against the next release's
  `extenddb-app`, and their APIs differ.
- Exact pins (`=0.1.14`): each release resolves only its own crates.

## Decision

All 14 workspace crates publish together at the workspace version, and each
internal dependency requires exactly that version. `extenddb` stays the one
binary crate, and a Cargo feature selects its backend.

## Rationale

- With exact pins, a fresh resolution of any release builds only the crates
  of that release.
- One binary crate follows the pattern of the two database tools above, and
  it adds no crate, owner or trusted publisher entry.
- Each crate declares its internal dev-dependencies by path, with no version.
  Cargo drops them from the published manifests, which removes the cycle between
  `extenddb-engine` and `extenddb-storage-sqlite`. Each crate's tests still
  build in the repository with the same dependencies.
- `resolver = "3"` makes a fresh resolution prefer versions that build on
  Rust 1.88. Install instructions still say `--locked`, because the fresh
  resolution is not the tested set.
- Each crate directory links LICENSE and NOTICE to the root files. Cargo packs
  the content of a linked file, and a link cannot drift from the root file as
  a copy can.
- The descriptions of the 13 library crates say that each is internal to the
  `extenddb` binary, with no API stability promise. The versioning draft in
  #897 lists the crate APIs under "Not covered", and RFC-0002 keeps the
  stability policy of the `Storage` trait as its unresolved question 4.

## Consequences

- Each pull request runs `cargo package --workspace --no-verify --locked` and
  `devtools/check-crate-packages`. The check fails on a pin that is not
  exact, or on an internal dev-dependency with a version. It also fails on a
  missing LICENSE, NOTICE or description, and on a package or manifest field
  over crates.io's limits. A crate whose version is not the workspace
  version fails too. A pin left at an older version also makes every cargo
  command that resolves dependencies fail.
- Compiling each package from its tarball takes minutes, so that build runs
  in the `verify` job of `.github/workflows/release-crates.yml`, before the
  upload, and not on each pull request.
- A release bump changes the 13 pins together with the workspace version.
- Every release publishes all 14 crates, including the crates that did not
  change.
- A user can have one backend build of `extenddb` installed at a time.
- A fresh resolution can choose a dependency release that breaks the build.
  cassandra-protocol 4.1.0 did this for the Cassandra backend, so it and
  cdrs-tokio carry upper bounds. `--locked` avoids the problem.
- The tests of the published library crates build only in the repository.
  Cargo drops the internal dev-dependencies, and some tests read fixtures
  from other crates.
- The license notices now list the workspace crates under the text of the
  repository's LICENSE file, with a link to the repository.
- `.github/workflows/release-crates.yml` publishes each release after
  `promote-image`, through trusted publishing, with `devtools/publish-crates`.
  Trusted publishing cannot create a crate. So a maintainer publishes the
  first version of each crate with an API token and
  `devtools/publish-crates --new-crates`. That mode publishes only the
  crates that crates.io does not have. Each crate then needs its owners and
  a trusted publisher entry.
- A release that adds one crate takes three runs. The workflow publishes the
  crates before the new crate, and it stops. A maintainer publishes the new
  crate with an API token, and a second dispatch publishes the rest.
  `packaging/README.md`, "Releasing to crates.io", describes the steps.
- A version on crates.io can never be replaced. A yank only stops new lock
  files from choosing it.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc.
