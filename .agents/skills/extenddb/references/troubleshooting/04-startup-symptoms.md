# Startup Symptoms

This file holds verbatim Cause and Fix entries for extenddb server startup symptoms: port conflicts, TLS certificate load failures, config file permissions that are too open, and daemonization failures. These symptoms typically appear during or immediately after `extenddb serve`.

Entries are copied verbatim from `docs/troubleshooting.md` per Requirement 10.2. Present the Cause and Fix to the user as-is. Do not paraphrase.

### Failed to bind &lt;addr&gt;: Address already in use

<a name="address-already-in-use"></a>

**Error text:**
```
Failed to bind <addr>: Address already in use
```

**Cause:** Another process is already listening on the configured port (default 8000).

**Fix:** Check what's using the port and stop it, or use a different port:
```bash
ss -tlnp | grep :8000                    # find what's using the port
extenddb serve --port 8001 --config extenddb.toml  # use a different port
```

**Source:** `docs/troubleshooting.md`, section "`Failed to bind <addr>: Address already in use`", last synced 2026-05-12.

### Failed to load TLS certificates: &lt;error&gt;

<a name="failed-to-load-tls-certificates"></a>

**Error text:**
```
Failed to load TLS certificates: <error>
```

**Cause:** TLS is enabled (the default) but the server could not load the certificate or private key files. Possible causes:
- The certificate or key file does not exist at the configured path
- The file exists but the extenddb process does not have read permission
- The file is not valid PEM format (e.g., DER-encoded, corrupted, or contains extra data)
- The path in `extenddb.toml` is wrong (note: `~` is expanded to `$HOME`)

The error intentionally does not name the specific file to avoid leaking filesystem path information to logs that may be aggregated.

**Fix:**
1. Verify the files exist:
   ```bash
   ls -la ~/.extenddb/tls/cert.pem ~/.extenddb/tls/key.pem
   ```
2. If missing, run `extenddb init` to generate a self-signed certificate, or provide your own CA-signed certificate.
3. Check permissions — the extenddb process must be able to read both files:
   ```bash
   chmod 600 ~/.extenddb/tls/key.pem
   chmod 644 ~/.extenddb/tls/cert.pem
   ```
4. Verify PEM format — the cert file should start with `-----BEGIN CERTIFICATE-----` and the key file with `-----BEGIN PRIVATE KEY-----` (or `-----BEGIN EC PRIVATE KEY-----`).

**Source:** `docs/troubleshooting.md`, section "`Failed to load TLS certificates: <error>`", last synced 2026-05-12.

### Config file &lt;path&gt; has permissions &lt;mode&gt;, which is too open.

<a name="config-file-permissions"></a>

**Error text:**
```
Config file <path> has permissions <mode>, which is too open.
```

**Cause:** The config file has group or world-readable permissions. Since the config file may contain the encryption key for credential storage, it must be restricted to owner-only access.

**Fix:**
```bash
chmod 600 extenddb.toml
```

**Source:** `docs/troubleshooting.md`, section "`Config file <path> has permissions <mode>, which is too open.`", last synced 2026-05-12.

### Failed to daemonize: &lt;error&gt;

<a name="failed-to-daemonize"></a>

**Error text:**
```
Failed to daemonize: <error>
```

**Cause:** extenddb runs as a daemon by default. This error means the process could not fork.

**Fix:** Check that the process has permission to fork. If another extenddb instance is running, stop it first:
```bash
extenddb stop --config extenddb.toml     # preferred
extenddb status --config extenddb.toml   # shows PID (if stop is unavailable)
kill <pid>                        # manual fallback
```

To view logs in real time (useful for debugging):
```bash
journalctl -t extenddb -f
```

**Source:** `docs/troubleshooting.md`, section "`Failed to daemonize: <error>`", last synced 2026-05-12.

## Non-destructive operation reminder

The fixes above include commands that modify filesystem state (`chmod`), terminate processes (`kill`, `extenddb stop`), or change the server's listening port (`extenddb serve --port`). Present these commands to the user verbatim. Do not execute them on the user's behalf. Requirement 14 prohibits the skill from running state-changing commands.
