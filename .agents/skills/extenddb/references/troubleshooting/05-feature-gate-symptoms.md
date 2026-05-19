# Feature-Gate Symptoms

This file holds verbatim Cause and Fix entries for extenddb feature-gate symptoms: import and export are disabled by default and require configuration. These symptoms appear when a user runs `aws dynamodb import-table` or `aws dynamodb export-table-to-point-in-time` against a fresh deployment.

### Import is disabled. Configure [import] paths in extenddb.toml to enable.

<a name="import-disabled"></a>

**Error text:**
```
Import is disabled. Configure [import] paths in extenddb.toml to enable.
```

**Cause:** An `ImportTable` request was made, but no `[import]` paths are configured. Import is disabled by default for security.

**Fix:** Add an `[import]` section to `extenddb.toml`:
```toml
[import]
paths = ["/path/to/imports"]
```

**Source:** `docs/troubleshooting.md`, section "`Import is disabled. Configure [import] paths in extenddb.toml to enable.`", last synced 2026-05-12.

### Export is disabled. Configure [export] paths in extenddb.toml to enable.

<a name="export-disabled"></a>

**Error text:**
```
Export is disabled. Configure [export] paths in extenddb.toml to enable.
```

**Cause:** An `ExportTableToPointInTime` request was made, but no `[export]` paths are configured. Export is disabled by default for security.

**Fix:** Add an `[export]` section to `extenddb.toml`:
```toml
[export]
paths = ["/path/to/exports"]
```

**Source:** `docs/troubleshooting.md`, section "`Export is disabled. Configure [export] paths in extenddb.toml to enable.`", last synced 2026-05-12.
