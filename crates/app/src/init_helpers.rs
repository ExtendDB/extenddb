// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helpers for `extenddb init`: TLS certificate generation and config file creation.

use rustls::pki_types::{CertificateDer, pem::PemObject};

/// Generate a self-signed TLS certificate and key if they don't already exist.
///
/// `extra_sans` are additional Subject Alternative Names, such as an in-cluster
/// service DNS name. They are appended to the default list of `localhost`,
/// `127.0.0.1`, and the bind address so the certificate is valid for the names
/// clients use.
///
/// An existing certificate is never regenerated, because rotating the key pair
/// under a live deployment would be a surprise. That means `--tls-san` cannot
/// take effect on a later run, so instead of exiting successfully having dropped
/// the requested names, we check that the existing certificate already covers
/// them and fail with an actionable error if it does not.
pub fn generate_tls_cert_if_needed(bind_addr: &str, extra_sans: &[String]) -> anyhow::Result<()> {
    let tls_dir = extenddb_config::expand_tilde("~/.extenddb/tls");
    let cert_path = format!("{tls_dir}/cert.pem");
    let key_path = format!("{tls_dir}/key.pem");

    // Requested SANs, trimmed, with blanks and duplicates removed.
    let requested = normalize_sans(extra_sans);

    // Validate every requested name before either path uses it, so a name we
    // could not verify later is rejected on the first run rather than only on
    // the next one.
    for san in &requested {
        coverage_probe(san)?;
    }

    if std::path::Path::new(&cert_path).exists() && std::path::Path::new(&key_path).exists() {
        ensure_cert_covers_sans(&cert_path, &key_path, &requested)?;
        println!("--- TLS certificate already exists, skipping generation.");
        return Ok(());
    }

    println!("--- Generating self-signed TLS certificate...");
    std::fs::create_dir_all(&tls_dir)
        .map_err(|e| anyhow::anyhow!("Failed to create TLS directory {tls_dir}: {e}"))?;

    // Build SAN list: always include localhost and 127.0.0.1, plus the
    // configured bind address if it differs.
    let mut sans: Vec<String> = vec!["localhost".to_owned(), "127.0.0.1".to_owned()];
    if bind_addr != "localhost" && bind_addr != "127.0.0.1" && bind_addr != "0.0.0.0" {
        sans.push(bind_addr.to_owned());
    }
    // Append the `--tls-san` values the defaults don't already cover.
    for san in &requested {
        if !sans.iter().any(|s| s.eq_ignore_ascii_case(san)) {
            sans.push(san.clone());
        }
    }

    let sans_display = sans.join(", ");
    let mut params = rcgen::CertificateParams::new(sans)
        .map_err(|e| anyhow::anyhow!("Failed to create certificate params: {e}"))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "extenddb self-signed");
    params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "extenddb");
    params.not_after = rcgen::date_time_ymd(2036, 1, 1);

    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| anyhow::anyhow!("Failed to generate key pair: {e}"))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| anyhow::anyhow!("Failed to generate self-signed certificate: {e}"))?;

    std::fs::write(&cert_path, cert.pem())
        .map_err(|e| anyhow::anyhow!("Failed to write certificate: {e}"))?;
    std::fs::write(&key_path, key_pair.serialize_pem())
        .map_err(|e| anyhow::anyhow!("Failed to write private key: {e}"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| anyhow::anyhow!("Failed to set key file permissions: {e}"))?;
    }

    println!("    Certificate: {cert_path}");
    println!("    Private key: {key_path}");
    println!("    SANs: {sans_display}");

    Ok(())
}

/// Trim `--tls-san` values, drop blanks, and remove duplicates.
///
/// DNS names are case-insensitive, so `Foo.example.com` and `foo.example.com`
/// are the same name and only the first spelling given is kept.
fn normalize_sans(sans: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for san in sans {
        let san = san.trim();
        if !san.is_empty() && !out.iter().any(|s: &String| s.eq_ignore_ascii_case(san)) {
            out.push(san.to_owned());
        }
    }
    out
}

/// Label substituted for the `*` when testing whether a certificate covers a
/// wildcard SAN. Any single label works; this one is chosen to be implausible
/// as a real hostname.
const WILDCARD_PROBE_LABEL: &str = "extenddb-tls-san-probe";

/// The server name used to test whether a certificate covers `san`.
///
/// A wildcard such as `*.svc.cluster.local` is a legitimate certificate entry
/// but not a legitimate server name, so it cannot be verified directly. Verify a
/// synthetic single-label substitution instead: a certificate is valid for
/// `<label>.svc.cluster.local` only if it carries the wildcard (or that exact
/// name), which is what we want to know. Wildcards match exactly one leftmost
/// label, so one substitution is enough.
///
/// Doubles as validation: callers run every requested `--tls-san` through this
/// before generating a certificate, so a name that could never be verified is
/// rejected up front instead of on the next run.
fn coverage_probe(san: &str) -> anyhow::Result<rustls::pki_types::ServerName<'static>> {
    let probe = match san.strip_prefix("*.") {
        Some(rest) if rest.is_empty() || rest.contains('*') => {
            anyhow::bail!(
                "Invalid --tls-san value '{san}': a wildcard must be a single leading \
                 label followed by a domain, as in '*.svc.cluster.local'"
            )
        }
        Some(rest) => format!("{WILDCARD_PROBE_LABEL}.{rest}"),
        None if san.contains('*') => anyhow::bail!(
            "Invalid --tls-san value '{san}': a wildcard is only valid as the leftmost \
             label, as in '*.svc.cluster.local'"
        ),
        None => san.to_owned(),
    };
    rustls::pki_types::ServerName::try_from(probe).map_err(|e| {
        anyhow::anyhow!("Invalid --tls-san value '{san}' (not a DNS name or IP address): {e}")
    })
}

/// Fail unless the certificate at `cert_path` is already valid for every
/// requested SAN.
///
/// Since `init` never regenerates an existing certificate, this check is all
/// that stands between a mistyped or newly added `--tls-san` and clients failing
/// hostname verification at runtime against a name `init` appeared to accept.
fn ensure_cert_covers_sans(
    cert_path: &str,
    key_path: &str,
    requested: &[String],
) -> anyhow::Result<()> {
    if requested.is_empty() {
        return Ok(());
    }

    let pem = std::fs::read(cert_path)
        .map_err(|e| anyhow::anyhow!("Failed to read existing certificate {cert_path}: {e}"))?;
    let der = CertificateDer::pem_slice_iter(&pem)
        .next()
        .ok_or_else(|| anyhow::anyhow!("No certificate found in {cert_path}"))?
        .map_err(|e| anyhow::anyhow!("Failed to parse certificate {cert_path}: {e}"))?;
    let cert = rustls::server::ParsedCertificate::try_from(&der)
        .map_err(|e| anyhow::anyhow!("Failed to parse certificate {cert_path}: {e}"))?;

    let mut missing = Vec::new();
    for san in requested {
        let name = coverage_probe(san)?;
        if rustls::client::verify_server_name(&cert, &name).is_err() {
            missing.push(san.clone());
        }
    }

    if missing.is_empty() {
        println!(
            "    Existing certificate already covers --tls-san: {}",
            requested.join(", ")
        );
        return Ok(());
    }

    anyhow::bail!(
        "The existing TLS certificate {cert_path} is not valid for the requested \
         --tls-san value(s): {}. init does not regenerate an existing certificate, \
         so these names would be silently dropped and clients using them would fail \
         TLS hostname verification. Either delete {cert_path} and {key_path} and \
         re-run init to generate a certificate covering them, or mount a certificate \
         that already covers them.",
        missing.join(", "),
    )
}

/// Commented bulk `[limits]` keys written into the generated config, with the
/// defaults. They are extenddb's own; DynamoDB exposes no equivalent. Kept in
/// sync with `extenddb.sample.toml` and covered by a test in this module.
const BULK_LIMITS_TEMPLATE: &str = "\
# Bulk operations (StartBulkPutItems and the workers that drive it).
# These keys are extenddb's own; DynamoDB exposes no equivalent.
# max_concurrent_bulk_operations = 50        # per account
# bulk_operation_retention_days = 30         # floored at 1; a purge pass runs at startup and daily
# bulk_operation_worker_concurrency = 4      # operations running at once per process; floored at 1
# bulk_operation_worker_per_account = 2      # of those, at most this many per account; floored at 1
# bulk_operation_lease_seconds = 60          # floored at 1
# bulk_put_batch_size = 25                   # rows per checkpoint; floored at 1, capped at 10000
# bulk_put_write_batch_rows = 25             # rows per storage transaction; capped at 100 and by bulk_put_batch_size
# max_bulk_input_bytes = 10737418240         # enumerated (compressed) total per operation; Start rejects above it
# max_bulk_input_files = 50000               # enumerated object count per operation; above it the operation fails
# max_bulk_input_object_bytes = 5368709120   # one enumerated input object; above it the operation fails
# max_bulk_client_token_bytes = 1024         # longest accepted ClientToken on StartBulkPutItems, in bytes
# max_bulk_line_bytes = 4194304              # one input line; longer abandons the file
# max_bulk_item_count = 100000000            # rows read per operation; more fails the operation
# max_bulk_report_records = 1000000          # records kept per operation; more are dropped and logged
# max_bulk_report_bytes = 1073741824         # serialized bytes of records kept per operation; more are dropped and logged
# max_bulk_failed_starts_per_hour = 100      # terminal-born starts per account per hour; above it Start answers ThrottlingException
# bulk_put_write_units_per_second = 80000    # ceiling on bulk writes per process, in 1 KB write units per second; 0 disables";

/// Commented `[bulk]` store block written into the generated config, the
/// same block `extenddb.sample.toml` carries. The feature is disabled until
/// an operator uncomments one of its two examples. Covered by a test in this
/// module.
const BULK_STORE_TEMPLATE: &str = "\
# Bulk operations ([bulk]) are disabled by default (secure default). The bulk
# put operations read their input files and write their error reports through
# one store, independent of [import] and [export]. store selects it:
# \"filesystem\" or \"s3\".
#
# The filesystem store reads and writes beneath one root. The layout is
# {path}/{account id}/{S3Bucket}/...: the server creates {path} and the
# per-account directories; the caller creates the bucket directories beneath
# its own subtree and stages files there.
#
# Whoever can write beneath {path}/{account id} can feed data into any table
# that account starts a load on, so an account's subtree is as trusted as the
# mechanism that populates it.
#
# [bulk]
# store = \"filesystem\"
#
# [bulk.filesystem]
# path = \"/var/lib/extenddb/buckets\"
#
# The S3 store reads and writes the buckets the requests name, in AWS S3 or in
# an S3-compatible service at endpoint_url, with the credentials of the
# standard AWS credential chain (environment variables, the shared config and
# credentials files, web identity, container and instance credentials). This
# file holds no credentials. region defaults to [server] region; most
# S3-compatible services need force_path_style = true. An http:// endpoint_url
# must be loopback (127.x.y.z, ::1 or localhost) unless
# allow_insecure_endpoint = true, which accepts it and logs a warning at
# startup. Requests and responses to such an endpoint cross the network
# unencrypted.
#
# Every caller reaches the buckets the server's credentials reach, as far as
# its ExtendDB policies allow s3:ListBucket, s3:GetObject and s3:PutObject on
# them; dev mode evaluates no policy.
#
# [bulk]
# store = \"s3\"
#
# [bulk.s3]
# region = \"us-east-1\"
# endpoint_url = \"https://objects.example.internal:9000\"
# allow_insecure_endpoint = false
# force_path_style = true";

/// Generate a comprehensive config file with all settings.
///
/// Computed values (connection string, bind address) are uncommented.
/// All other settings are commented out with their defaults.
pub(crate) fn generate_config(
    config_path: &str,
    backend: &str,
    bootstrapper: &dyn extenddb_storage::bootstrapper::Bootstrapper,
    bind_addr: &str,
    docs_dir: Option<&str>,
) -> anyhow::Result<()> {
    println!("--- Generating {config_path}...");
    let timestamp = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_owned());
    let tls_cert = extenddb_config::expand_tilde("~/.extenddb/tls/cert.pem");
    let tls_key = extenddb_config::expand_tilde("~/.extenddb/tls/key.pem");
    let run_dir = extenddb_config::expand_tilde("~/.extenddb/run");

    // Compute docs_dir line before the template so it lands in the top-level
    // TOML section (before any [section] header).
    let docs_line = match docs_dir {
        Some(d) => format!(
            "# Path to rendered documentation (HTML + PDF from docs/build-docs.py).\ndocs_dir = \"{d}\"\n"
        ),
        None => {
            "# docs_dir = \"/path/to/docs/rendered\"  # Path to rendered documentation\n".to_owned()
        }
    };

    // Generate storage section with backend-specific config
    let backend_config = bootstrapper.generate_backend_config_section();
    let storage_section = format!(
        r#"[storage]
backend = "{backend}"

{backend_config}"#
    );

    let toml = format!(
        r#"# Generated by extenddb init on {timestamp}
#
# SECURITY: This file may contain the encryption key for credential storage.
# Set permissions to 0600 (owner read/write only): chmod 600 {config_path}
#
# Environment variable overrides use the EXTENDDB__ prefix with __ as separator:
#   EXTENDDB__SERVER__PORT=9000
#   EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING="postgresql://..."

{docs_line}
[server]
bind_addr = "{bind_addr}"
# port = 18443                   # HTTPS port
# region = "us-east-1"           # AWS region for ARN generation
# run_dir = "{run_dir}"          # Directory for PID file
# throttling_enabled = false     # Enable provisioned throughput throttling

[server.tls]
# TLS is mandatory. The server refuses to start with enabled = false.
# enabled = true
cert_path = "{tls_cert}"
key_path = "{tls_key}"

{storage_section}

[auth]
# provider = "builtin"           # SigV4 with local credential store (mandatory)

[auth.cache]
# In-memory caches eliminate per-request catalog queries for IAM data.
# Defaults: ttl 60s, soft_ttl 30s, negative_ttl 5s, max 10000 entries/cache.
# Set enabled = false to disable all caches (for incident response).
# enabled = true
# ttl_seconds = 60
# soft_ttl_seconds = 30
# negative_ttl_seconds = 5
# max_entries = 10000

[logging]
# level = "info"                 # trace, debug, info, warn, error
# format = "pretty"              # "pretty" (human) or "json" (structured)

[limits]
# All defaults match real DynamoDB limits. Override only for testing.
# max_item_size_bytes = 409600
# max_partition_key_size_bytes = 2048
# max_sort_key_size_bytes = 1024
# max_tables_per_account = 2500
# max_gsis_per_table = 20
# max_lsis_per_table = 5
# list_tables_max_per_page = 100
# max_table_name_length = 255
# min_table_name_length = 3
# max_attribute_name_bytes = 65535
# max_expression_tokens = 4096
# max_expression_depth = 150
# per_table_max_rcu = 40000
# per_table_max_wcu = 40000
# per_account_max_rcu = 80000
# per_account_max_wcu = 80000
# allow_multipart_table_keys = false
# max_policy_document_bytes = 6144
# max_import_file_bytes = 10737418240
# max_import_item_count = 10000000
# max_export_item_count = 10000000

{BULK_LIMITS_TEMPLATE}

# [backup]
# Backup and restore jobs. stale_job_after_secs must be at least three
# heartbeat periods, the larger of heartbeat_interval_secs and the 15 s the
# jobs of this release heartbeat at (45 at the least); pool_size is raised to
# at least 2 * max_concurrent_jobs + 10 on PostgreSQL; max_concurrent_jobs
# is at most 1024.
# data_file_target_bytes = 67108864    # Uncompressed bytes per data file (minimum 32 MiB)
# heartbeat_interval_secs = 15         # Job heartbeat timer
# stale_job_after_secs = 300           # Heartbeat age after which a job's owner is dead
# failed_row_retention_secs = 604800   # FAILED tombstones kept for the operator (7 days)
# reconcile_interval_secs = 60         # Reconciler cycle
# max_concurrent_jobs = 4              # Backup and restore jobs running at once on this instance
# spool_path = "/var/lib/extenddb/spool" # Restore download area for an S3 store

# [import]
# paths = []                     # Allowed directories for import operations

# [export]
# paths = []                     # Allowed directories for export operations

# max_import_bytes = 10737418240 # Maximum import file size (10 GB)

{BULK_STORE_TEMPLATE}
"#,
    );

    write_owner_only(config_path, toml.as_bytes())?;
    println!("    Created {config_path}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BULK_LIMITS_TEMPLATE, BULK_STORE_TEMPLATE};

    /// Strip one comment marker (`# ` or a bare `#`) from a template line.
    fn uncomment(line: &str) -> &str {
        line.strip_prefix("# ")
            .or_else(|| line.strip_prefix('#'))
            .unwrap_or(line)
    }

    /// Each `[bulk]` block the generated config carries must uncomment to a
    /// section the config parser accepts, so the examples cannot drift from
    /// what the server reads. A block runs from a `# [bulk]` line to the
    /// next prose sentence (a line that uncomments to text starting with a
    /// capital letter); the prose between the blocks is dropped.
    #[test]
    fn bulk_store_template_uncomments_to_a_valid_section() {
        #[derive(serde::Deserialize)]
        struct Section {
            bulk: extenddb_config::BulkConfig,
        }
        let mut closed: Vec<String> = Vec::new();
        let mut current: Vec<&str> = Vec::new();
        for line in BULK_STORE_TEMPLATE.lines() {
            let text = uncomment(line);
            if line == "# [bulk]" {
                if !current.is_empty() {
                    closed.push(current.join("\n"));
                }
                current = vec![text];
            } else if text.starts_with(|c: char| c.is_ascii_uppercase()) {
                if !current.is_empty() {
                    closed.push(current.join("\n"));
                    current = Vec::new();
                }
            } else if !current.is_empty() {
                current.push(text);
            }
        }
        if !current.is_empty() {
            closed.push(current.join("\n"));
        }
        assert_eq!(closed.len(), 2, "one filesystem and one s3 example");

        let filesystem: Section = toml::from_str(&closed[0]).expect("filesystem block parses");
        assert_eq!(
            filesystem.bulk.store,
            extenddb_config::BulkStoreKind::Filesystem,
            "the first example is the filesystem store"
        );
        assert!(
            filesystem
                .bulk
                .filesystem
                .as_ref()
                .is_some_and(|fs| !fs.path.is_empty()),
            "the example names a filesystem path"
        );

        let s3: Section = toml::from_str(&closed[1]).expect("s3 block parses");
        assert_eq!(
            s3.bulk.store,
            extenddb_config::BulkStoreKind::S3,
            "the second example is the S3 store"
        );
        let section = s3.bulk.s3.expect("the example carries [bulk.s3]");
        assert_eq!(section.region.as_deref(), Some("us-east-1"));
        assert!(section.endpoint_url.is_some());
        assert!(!section.allow_insecure_endpoint);
        assert!(section.force_path_style);
        assert_eq!(section.validate(), Ok(Vec::new()), "the example validates");
    }

    /// The generated `[limits]` block must carry every bulk limit key. Taking
    /// the lines from the first key onward drops the prose, and parsing the
    /// uncommented keys confirms each is a valid TOML integer.
    #[test]
    fn bulk_limits_template_lists_every_bulk_key() {
        let body: String = BULK_LIMITS_TEMPLATE
            .lines()
            .skip_while(|l| !l.starts_with("# max_concurrent_bulk_operations"))
            .map(uncomment)
            .collect::<Vec<_>>()
            .join("\n");
        let value: toml::Value = toml::from_str(&body).expect("bulk limit keys parse as TOML");
        let table = value.as_table().expect("a table of keys");
        let expected = [
            "max_concurrent_bulk_operations",
            "bulk_operation_retention_days",
            "bulk_operation_worker_concurrency",
            "bulk_operation_worker_per_account",
            "bulk_operation_lease_seconds",
            "bulk_put_batch_size",
            "bulk_put_write_batch_rows",
            "max_bulk_input_bytes",
            "max_bulk_input_files",
            "max_bulk_input_object_bytes",
            "max_bulk_client_token_bytes",
            "max_bulk_line_bytes",
            "max_bulk_item_count",
            "max_bulk_report_records",
            "max_bulk_report_bytes",
            "max_bulk_failed_starts_per_hour",
            "bulk_put_write_units_per_second",
        ];
        for key in expected {
            assert!(
                table.get(key).is_some_and(toml::Value::is_integer),
                "{key} is present with an integer default"
            );
        }
        assert_eq!(
            table.len(),
            expected.len(),
            "no bulk limit key is listed that the test does not name"
        );
    }
}

/// Write `contents` to `path` so that no other user can read it at any point:
/// a new file is created 0600, and an existing one is narrowed to 0600 before
/// it is truncated and rewritten. The config can carry the PostgreSQL
/// connection string's password and the Cassandra application password.
fn write_owner_only(path: &str, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        if std::path::Path::new(path).exists() {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(contents)?;
        // `mode` applies only when the file is created, and the umask can only
        // narrow it; this pins the result for both paths.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
    }
    #[cfg(not(unix))]
    {
        std::fs::File::create(path)?.write_all(contents)
    }
}

#[cfg(all(test, unix))]
mod write_owner_only_tests {
    use std::os::unix::fs::PermissionsExt;

    fn mode(path: &std::path::Path) -> u32 {
        std::fs::metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777
    }

    /// The config is 0600 from creation, and an existing world-readable one
    /// is narrowed before the new contents (with a password) are written.
    #[test]
    fn the_config_is_never_readable_by_others() {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-write-owner-only-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir(&dir).expect("scratch dir");
        let path = dir.join("e.toml");
        let p = path.to_str().expect("utf-8 path");

        super::write_owner_only(p, b"first").expect("create");
        assert_eq!(mode(&path), 0o600);
        assert_eq!(std::fs::read(&path).expect("read"), b"first");

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        super::write_owner_only(p, b"password = \"s\"").expect("overwrite");
        assert_eq!(mode(&path), 0o600);
        assert_eq!(std::fs::read(&path).expect("read"), b"password = \"s\"");

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
