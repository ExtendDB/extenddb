# Versioning and support policy

ExtendDB follows [Semantic Versioning](https://semver.org/). A release number has the form MAJOR.MINOR.PATCH. The source tag `vMAJOR.MINOR.PATCH`, the container images and the npm packages of a release all carry that number.

## What a release can change

- A major release, such as 2.0, can change anything that this policy keeps compatible. Only a major release removes a deprecated item. Its release notes and the [upgrade manual](manuals/07-upgrade-manual.md) give the upgrade path. The 2.0 release upgrades the catalog of any 1.x release.
- A minor release can add operations, configuration keys, CLI flags and catalog migrations. It can deprecate an item. It can carry a breaking catalog migration, and its release notes then say so. A minor release can also raise the minimum Rust version that a build from source needs.
- A patch release fixes bugs other than the behavior changes under [The DynamoDB API](#what-stays-compatible-within-a-major-version), and it fixes security issues. It adds no configuration key, no breaking catalog migration and no new backup format version. A rollback to an earlier patch release of the same minor release needs only the earlier binary.

The [beta tier](https://github.com/ExtendDB/extenddb/blob/main/README.md#storage-backends) adds one exception to these rules. To remove a known limit of a beta backend, a minor release can change the behavior that the README table lists.

## What stays compatible within a major version

- The DynamoDB API: an operation, request member or response member that a release implements stays in the later releases. A change that makes ExtendDB answer a request as Amazon DynamoDB answers it is a bug fix. It ships in a minor release, or in a patch release when it closes a security issue. The upgrade manual lists each one under [Behavior Changes by Release](manuals/07-upgrade-manual.md#behavior-changes-by-release).
- Configuration: the keys of `extenddb.toml`, their `EXTENDDB__` environment variables and the runtime settings of `extenddb settings` keep their names and meanings. A key that a minor release adds has a default that keeps the earlier behavior. A server refuses a key that it does not know, so before a rollback, remove each key that a later release added.
- The CLI: the subcommands, flags and exit codes of `extenddb` stay. The [catalog status table](manuals/07-upgrade-manual.md#the-catalog-status) gives the exit codes of `serve`, `migrate` and `verify`. `healthcheck` exits 0 when `/health` answers 2xx, and 1 otherwise. The text of messages and log lines can change in any release.
- The management API that `extenddb manage` calls, and the options and return values of `start()` in the `@extenddb/dev` npm package.
- The HTTP status code of a `/health` response and its `status` field.
- The supported database versions: a minor release ends support for a database version only after the version's upstream end of life. The minor release before it announces the change.
- The catalog and the on-disk formats, under the rules in the next section.

## Catalog upgrades

Each change to the catalog is a numbered migration. A migration number is never used again, reordered or removed, and the body of a released migration does not change. `extenddb migrate --list` prints the migrations that a binary carries, with the kind of each one.

At startup, a production build of `extenddb serve --foreground` reads the catalog status and acts on it:

- Current: the catalog has every migration that the binary carries. The server serves it.
- Behind: migrations are pending. The server exits 3 and prints the `extenddb migrate --yes` command that applies them.
- AheadCompatible: the catalog has migrations that the binary does not know, and all of them are additive. The server serves it and logs a warning.
- AheadIncompatible: one of those unknown migrations is breaking. The server exits 4.

The [catalog status table](manuals/07-upgrade-manual.md#the-catalog-status) gives the other statuses and their exit codes. In daemon mode, `serve` exits 1 when it refuses a catalog, and writes the refusal to syslog.

A dev-mode build migrates a Behind catalog at startup, after it copies the database file.

A migration is additive when the previous release serves the catalog correctly with the migration applied, and breaking otherwise. A rollback across additive migrations runs the previous binary. A rollback across a breaking migration restores the backup taken before the upgrade. Every migration in 1.0 is breaking.

A backup in a backup store carries a format version. A release reads each format version that an earlier release wrote, and refuses a later version.

## Not covered

- The APIs of the Rust crates, the `Storage` trait among them: the crates are not published, and the trait still changes.
- Preview settings, such as `enable_multipart_keys`.
- The fields of a `/metrics` response.
- Backends in the experimental [tier](https://github.com/ExtendDB/extenddb/blob/main/README.md#storage-backends).

## Support

Fixes ship in patch releases. A fix for an earlier minor release gets a patch release of its own, and the `latest` image tag stays on the newest release.

- The newest minor release gets bug fixes and security fixes.
- The minor release before it gets security fixes for 3 months after the newest one ships.
- After 2.0 ships, the last 1.x minor release gets security fixes for 12 months.
- The 0.1.x releases get no fixes after 1.0 ships.

Support covers the stable and beta backends. An experimental backend gets fixes in the newest minor release only. To report a security issue, follow [SECURITY.md](https://github.com/ExtendDB/extenddb/blob/main/SECURITY.md).

## Deprecation

1. The release notes of the minor release that deprecates an item announce it and name what replaces it. The upgrade manual lists it under Behavior Changes by Release.
2. While the item remains, ExtendDB logs or prints a warning each time a request, a command or the configuration uses it. This step applies to items deprecated from 1.0 on.
3. A major release removes the item, no earlier than 6 months after the release that announced it.

The same steps apply when a release removes a stable or beta backend, or moves it to a lower tier. A security issue can need a faster change, and its release notes then give the reason.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
