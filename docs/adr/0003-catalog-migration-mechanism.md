# ADR-0003: Adopt sqlx::migrate for PostgreSQL catalog and data schema migrations

- Status: Proposed
- Date: 2026-06-17
- Deciders: ExtendDB CODEOWNERS

**TL;DR:** Our homegrown migration runner tracked files by name with no checksum,
so an in-place edit of an already-shipped migration produced a silent schema drift
that broke `CreateTable`. We replace it with sqlx's built-in `sqlx::migrate`, which
checksums every migration file and refuses to run when one changed after it was
applied. Existing catalogs must re-initialize, which is acceptable at v0.1.

## Context

ExtendDB stores its data in two kinds of PostgreSQL database: a single catalog
database that holds table definitions and account metadata, and a single shared
data database that holds the items themselves. Both carry schema that has
to evolve over time, and this ADR changes how migrations are handled for both.

The catalog schema was managed by an incomplete homegrown migration runner in
`crates/storage-postgres/src/migrations.rs`. It tracked applied migrations in a
`schema_history` table by filename only, with no checksum, so it could not detect
that a migration file had changed after it was applied. Schema files were applied
with `CREATE TABLE IF NOT EXISTS`. The data database never had a real runner,
only an ad-hoc existence check, which has the same blind spot.

This had a silent-failure mode. Commit `2f69c4c` added three columns
(`table_class`, `sse_specification`, `on_demand_throughput`) by editing the
already-shipped `001_schema.sql` in place instead of adding a new migration file.
On any catalog where `001_schema.sql` was already recorded, the runner skipped it,
the `CREATE TABLE IF NOT EXISTS` was a no-op on the existing table, and the new
binary's `INSERT INTO tables (...)` failed at request time with
`column "table_class" does not exist`.

Relevant facts:

- The project already depends on `sqlx = "0.8"`, which ships a migration
  framework (`sqlx::migrate!`) with embedded files, ordered application, and
  per-file checksum enforcement, recorded in a tracking table it manages called
  `_sqlx_migrations`. The homegrown runner reimplemented the weak half (filename
  tracking) and dropped the strong half (checksums).
- ExtendDB is at v0.1, with no production users, workloads, or CI depending on an
  existing catalog.

## Options Considered

1. **Harden the homegrown runner.** We would add checksum tracking, a
   guard test, and move the version write inside the runner. It stays fully under
   our control, but it keeps reimplementing what sqlx already does and leaves us
   maintaining a migration runner of our own.
2. **Adopt sqlx:migrate** We would switch to sqlx and seed its `_sqlx_migrations`
   table to match what each existing catalog already has, so no re-initialization
   is needed. This avoids a breaking change, but getting that seeding exactly
   right for every existing catalog is delicate and error-prone.
3. **Adopt sqlx:migrate** We switch to sqlx and accept that an existing catalog has to be
   torn down and re-initialized. There is no compatibility shim and no in-place
   upgrade.

## Decision

Adopt `sqlx::migrate` directly for both the catalog and data database (the re-init
option): existing catalogs re-initialize, and we do not build a compatibility shim.

## Rationale

- We already link `sqlx 0.8`. Turning on its `migrate` feature (not enabled
  today) lets us use a tested migration framework and delete custom code instead
  of hardening a parallel reimplementation.
- sqlx refuses to run a migration whose file has changed since it was first
  applied. The old runner had no such check, and that gap is what caused the
  incident.
- At v0.1 with no dependent catalogs, the cost of re-init is trivial, and it
  removes the only hard part of seed-and-keep (the byte-exact `_sqlx_migrations`
  seed and its CRLF/checksum footgun).

## Consequences

**What improves**

- A new schema change is a new numbered file. Editing a file that already shipped
  is now a hard error rather than a silent no-op, which is the failure that
  started this.
- sqlx checksums file bytes, so we pin `*.sql text eol=lf` in `.gitattributes` to
  stop a contributor's line-ending rewrite from tripping a false mismatch.
- Both databases move to sqlx. Each gets its own migrator and `_sqlx_migrations`
  table, pointed at `migrations/` and `data_migrations/`. `migrate`, not just
  `init`, will run the data migrator, so existing deployments pick up data-schema
  changes instead of silently skipping them, which retires the data database's
  ad-hoc existence check and a latent bug of the same class.

**The breaking change**

- Upgrading requires `destroy` + `init`. `destroy` drops the databases, so all
  stored items are wiped, not just schema. We document this in the upgrade manual
  and changelog. It is acceptable only at v0.1 with no dependent catalogs.

**Version gate and the version write**

- The catalog version gate stays: the server refuses to start when the binary's
  expected version and the stored `catalog_version` disagree. It is catalog-only;
  the data database relies on its `_sqlx_migrations` table and has no separate
  version. The gate is symmetric, so an older binary against a newer catalog also
  refuses to serve, which is intended.
- The old runner wrote `catalog_version` inside the migration transaction. sqlx
  knows nothing about our semver, so `init` and `migrate` will write it in a
  separate step after migrations run. This write is not atomic with the migration:
  - On `migrate` (upgrade), a crash between migrations and the version write leaves
    `catalog_version` stale; re-running `migrate` fixes it, since sqlx skips
    already-applied migrations.
  - On first-time `init`, the same crash leaves no `catalog_version` at all, and
    `migrate` cannot recover it (the config file is written only at the end of
    `init`). Recovery there is `destroy` + `init`.
- A unit test guards against version drift: it asserts the embedded migration
  count and `CATALOG_VERSION` match expected constants. This is a tripwire (adding
  a migration breaks the count and forces a version bump), not a strict invariant
  tying the version to the migration.

**Operational notes**

- Migrations run only during `init` and `migrate`, never while serving.
- If a migration dies mid-apply, sqlx marks it dirty and refuses to proceed until
  an operator resolves it.
- This is scoped to migration mechanics. It does not touch the separate gap in how
  ExtendDB checks columns at query time.
- CI guardrail (follow-up): apply every migration on a fresh database and assert an
  older binary refuses a newer catalog. Checksums are the runtime net; CI is the
  longer-term validation that a broken or edited migration is caught before it
  ships.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
