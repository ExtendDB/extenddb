# Catalog Symptoms

## Purpose

This file holds verbatim Cause and Fix entries for catalog-related extenddb symptoms: version mismatches, uninitialized catalogs, and name collisions. These symptoms typically appear during `extenddb init` on an existing deployment or after an incomplete destroy-and-reinit cycle.

---

### `Catalog version mismatch: expected X, found Y. Run 'extenddb migrate' to update.`

<a name="catalog-version-mismatch"></a>

**Error text:**
```
Catalog version mismatch: expected X, found Y. Run 'extenddb migrate' to update.
```

**Cause:** The catalog database was initialized with a different version of extenddb. The binary expects catalog version X but the database has version Y.

**Fix:**
Run `extenddb migrate` to apply schema migrations and update the catalog version. Back up the database first.

**Source:** `docs/troubleshooting.md`, section "`Catalog version mismatch: expected X, found Y. Run 'extenddb migrate' to update.`", last synced 2026-05-12.

---

### `Catalog not initialized. Run 'extenddb init' to set up the catalog.`

<a name="catalog-not-initialized"></a>

**Error text:**
```
Catalog not initialized. Run 'extenddb init' to set up the catalog.
```

**Cause:** The server connected to the database but the catalog tables don't exist. The database hasn't been initialized with `extenddb init`.

**Fix:**
Run `extenddb init` to create the catalog schema and data database. See `docs/getting-started.md`.

**Source:** `docs/troubleshooting.md`, section "`Catalog not initialized. Run 'extenddb init' to set up the catalog.`", last synced 2026-05-12.

---

### `Database '<name>' already exists. Run 'extenddb destroy --config <config>' first, then re-run 'extenddb init'.`

<a name="database-already-exists"></a>

**Error text:**
```
Database '<name>' already exists. Run 'extenddb destroy --config <config>' first, then re-run 'extenddb init'.
```

**Cause:** `extenddb init` detected that the catalog or data database already exists in PostgreSQL. To prevent accidental data loss, `extenddb init` refuses to proceed when either database is present.

**Fix:**
If you want to re-initialize from scratch, run `extenddb destroy --config extenddb.toml` first to drop both databases, then run `extenddb init` again. If you want to keep the existing data and just apply migrations, use `extenddb migrate` instead.

**Source:** `docs/troubleshooting.md`, section "`Database '<name>' already exists. Run 'extenddb destroy --config <config>' first, then re-run 'extenddb init'.`", last synced 2026-05-12.

---

## Note on destructive remediation

Remediation for "Database '<name>' already exists" may require `extenddb destroy --config extenddb.toml` (destructive: drops both the catalog and data databases). Per Requirement 14, the skill presents this command for the user to review and invoke, and does not run it.
