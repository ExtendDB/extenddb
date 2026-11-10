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
    generate_tls_cert_in(
        &extenddb_config::expand_tilde("~/.extenddb/tls"),
        bind_addr,
        extra_sans,
    )
}

/// [`generate_tls_cert_if_needed`] with the TLS directory as a parameter.
fn generate_tls_cert_in(
    tls_dir: &str,
    bind_addr: &str,
    extra_sans: &[String],
) -> anyhow::Result<()> {
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
    std::fs::create_dir_all(tls_dir)
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
    // A key.pem left by an earlier run may have been readable by others, and a
    // descriptor opened then would read a new key written into the same file.
    // Remove it so the new key goes into a new file, as init does for the config.
    match std::fs::remove_file(&key_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(anyhow::anyhow!("Failed to remove the old private key: {e}")),
    }
    write_owner_only(&key_path, key_pair.serialize_pem().as_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to write private key: {e}"))?;

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

# [backup]
# Backup and restore jobs. stale_job_after_secs must be at least three
# heartbeat periods, the larger of heartbeat_interval_secs and the 15 s the
# in-catalog jobs heartbeat at (45 at the least); pool_size is raised to
# at least 2 * max_concurrent_jobs + 10 on PostgreSQL; max_concurrent_jobs
# is at most 1024.
# data_file_target_bytes = 67108864    # Uncompressed bytes per data file (minimum 32 MiB)
# heartbeat_interval_secs = 15         # Job heartbeat timer
# stale_job_after_secs = 300           # Heartbeat age after which a job's owner is dead
# failed_row_retention_secs = 604800   # FAILED tombstones kept for the operator (7 days)
# reconcile_interval_secs = 60         # Reconciler cycle
# max_concurrent_jobs = 4              # Backup and restore jobs running at once on this instance
# spool_path = "/var/lib/extenddb/spool" # Restore download area for an S3 store; required
#                                        # with an S3 store except on file-backed SQLite, where
#                                        # it defaults to backup-spool/ beside the database file

# [backup.store]
# Where CreateBackup writes its backups on PostgreSQL, SQLite and MongoDB
# (CreateBackup answers CREATING and a job of the server uploads; Cassandra
# writes in-catalog backups with or without a store), and where restores and
# deletes of store-backed backups read. Absent on file-backed SQLite: a filesystem
# store at backups/ beside the database file. Absent on PostgreSQL, MongoDB,
# Cassandra and an in-memory SQLite database: no store, and CreateBackup
# writes in-catalog backups with a deprecation warning. kind = "disabled"
# opts out on any backend. Instances that share a catalog must share one
# store root (an S3 bucket and prefix, or a shared mount).
# kind = "filesystem"                  # "filesystem" | "s3" | "disabled"
# [backup.store.filesystem]
# path = "/var/lib/extenddb/backups"   # Must exist, owned by the extenddb user, mode 0700
# [backup.store.s3]
# bucket = "my-extenddb-backups"
# prefix = "extenddb/"                 # Optional key prefix
# region = "us-east-1"                 # Optional; the AWS SDK's chain otherwise
# endpoint = "http://127.0.0.1:9000"   # Optional, for an S3-compatible server
# force_path_style = true              # Required with an http:// endpoint
# upload_part_size_bytes = 8388608     # 5 MiB to 5 GiB
# max_concurrent_uploads = 4

# [import]
# paths = []                     # Allowed directories for import operations

# [export]
# paths = []                     # Allowed directories for export operations

# max_import_bytes = 10737418240 # Maximum import file size (10 GB)
"#,
    );

    write_owner_only(config_path, toml.as_bytes())?;
    println!("    Created {config_path}");
    Ok(())
}

/// Write `contents` to a new file at `path` with mode 0600 throughout. The
/// write fails if `path` exists, so callers remove an existing file first, as
/// init does for the config and the TLS key. The config can carry the
/// `PostgreSQL` connection string's password and the Cassandra application
/// password, and the TLS key file holds the server's private key.
fn write_owner_only(path: &str, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    open_owner_only(path)?.write_all(contents)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // The umask can narrow `mode` further; this pins the result at 0600.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Create `path` for writing with mode 0600, so other users have no access
/// before any byte is written. The open fails if `path` exists, also when it
/// is a symlink: a file that existed may be open in another user's process,
/// and a symlink would put the contents where it points.
fn open_owner_only(path: &str) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
    }
    #[cfg(not(unix))]
    {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
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

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-write-owner-only-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir(&dir).expect("scratch dir");
        dir
    }

    /// A new file is 0600 as soon as it exists, before any byte of the TLS key
    /// or the config is written to it.
    #[test]
    fn a_new_file_is_owner_only_before_any_bytes_are_written() {
        let dir = scratch_dir("open");
        let control = dir.join("control");
        std::fs::File::create(&control).expect("control file");
        let group_and_other_bits = mode(&control) & 0o077;
        if group_and_other_bits == 0 {
            // The umask removes every group and other bit (umask 077 does), so
            // a plain create is 0600 too and this test cannot tell them apart.
            eprintln!("skipped: under this umask a plain create has no group or other bits");
            std::fs::remove_dir_all(&dir).expect("cleanup");
            return;
        }
        let path = dir.join("key.pem");

        let file = super::open_owner_only(path.to_str().expect("utf-8 path")).expect("create");
        assert_eq!(file.metadata().expect("metadata").len(), 0);
        assert_eq!(mode(&path), 0o600);

        drop(file);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// A key.pem left without its cert.pem and readable by others is replaced
    /// by a new 0600 file, so a descriptor opened on the old file still reads
    /// only the old contents.
    #[test]
    fn a_left_over_key_is_replaced_by_a_new_owner_only_file() {
        use std::io::Read;
        use std::os::unix::fs::MetadataExt;

        let dir = scratch_dir("tls");
        let key = dir.join("key.pem");
        std::fs::write(&key, b"old").expect("old key");
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        let mut old = std::fs::File::open(&key).expect("open old key");
        let old_ino = old.metadata().expect("metadata").ino();

        super::generate_tls_cert_in(dir.to_str().expect("utf-8 path"), "127.0.0.1", &[])
            .expect("generate");

        assert_ne!(std::fs::metadata(&key).expect("metadata").ino(), old_ino);
        assert_eq!(mode(&key), 0o600);
        let mut seen = String::new();
        old.read_to_string(&mut seen).expect("read old descriptor");
        assert_eq!(seen, "old");

        drop(old);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// The config is 0600 from creation. A write to a path that exists fails
    /// and leaves that file as it was, so the new contents (with a password)
    /// never go into a file that was readable by others.
    #[test]
    fn the_config_is_never_readable_by_others() {
        let dir = scratch_dir("config");
        let path = dir.join("e.toml");
        let p = path.to_str().expect("utf-8 path");

        super::write_owner_only(p, b"first").expect("create");
        assert_eq!(mode(&path), 0o600);
        assert_eq!(std::fs::read(&path).expect("read"), b"first");

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        let err = super::write_owner_only(p, b"password = \"s\"").expect_err("existing file");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(mode(&path), 0o644);
        assert_eq!(std::fs::read(&path).expect("read"), b"first");

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// A symlink at the path is not followed: the write fails, and the file
    /// the symlink points to keeps its contents and its mode.
    #[test]
    fn a_symlink_at_the_path_is_not_followed() {
        let dir = scratch_dir("symlink");
        let target = dir.join("target");
        std::fs::write(&target, b"other").expect("target");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        let link = dir.join("key.pem");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");

        let err = super::write_owner_only(link.to_str().expect("utf-8 path"), b"secret")
            .expect_err("symlink");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&target).expect("read"), b"other");
        assert_eq!(mode(&target), 0o644);

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
