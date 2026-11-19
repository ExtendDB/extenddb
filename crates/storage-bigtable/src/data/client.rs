// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! BigTable connection wrapper, deterministic timestamp helpers, RE2 byte
//! escaping, and gRPC error classification.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bigtable_rs::bigtable::{BigTable, BigTableConnection, Error as BigtableError};
use extenddb_storage::error::StorageError;
use gcp_auth::TokenProvider;

use crate::config::BigtableStorageConfig;

/// Microseconds per millisecond (`TimestampGranularity::MILLIS` in Cloud Bigtable).
pub const MICROS_PER_MILLI: i64 = 1_000;

static LAST_MONOTONIC_MILLIS: AtomicI64 = AtomicI64::new(0);

/// Strictly monotonically increasing, millisecond-aligned timestamp in
/// microseconds guaranteed to be strictly greater than `exclusive_floor_micros`.
///
/// Used for `__extenddb_txn_log__` state transitions (`PENDING` < `ABORTED` /
/// `COMMITTED` < `CLEANED`) so that even if another coordinator node had a
/// slightly ahead NTP clock when writing `started_at`, subsequent state
/// transitions on any node always carry a strictly newer millisecond timestamp,
/// while still staying anchored at wall-clock `now` so cells written after a
/// `DeleteFromRow` tombstone are never masked in production Cloud Bigtable.
#[must_use]
pub fn monotonic_timestamp_after_micros(exclusive_floor_micros: i64) -> i64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX / MICROS_PER_MILLI))
        .unwrap_or(1)
        .max(1);
    let min_ms = if exclusive_floor_micros <= 0 {
        1
    } else {
        (exclusive_floor_micros / MICROS_PER_MILLI).saturating_add(1)
    };
    let floor_ms = now_ms.max(min_ms);

    let mut prev = LAST_MONOTONIC_MILLIS.load(Ordering::Relaxed);
    loop {
        let next = if floor_ms > prev {
            floor_ms
        } else {
            prev.saturating_add(1)
        };
        match LAST_MONOTONIC_MILLIS.compare_exchange_weak(
            prev,
            next,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => return next.saturating_mul(MICROS_PER_MILLI),
            Err(actual) => prev = actual,
        }
    }
}

/// Strictly monotonically increasing, millisecond-aligned timestamp in
/// microseconds for non-OCC rows (`__extenddb_catalog__`, `__extenddb_txn_log__`
/// coordinator state transitions, and `__extenddb_ttl_index__`) so back-to-back
/// writes within the same wall-clock millisecond never collide at identical
/// timestamps under `MaxVersions(1)`.
#[must_use]
pub fn monotonic_timestamp_micros() -> i64 {
    monotonic_timestamp_after_micros(0)
}

/// Escape arbitrary bytes into an anchored RE2 regular expression (`^...$`)
/// matching the exact byte sequence safely across printable ASCII, RE2
/// metacharacters, and arbitrary binary / UTF-8 bytes.
#[must_use]
pub fn exact_re2_bytes(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() * 2 + 2);
    out.push(b'^');
    for &b in raw {
        match b {
            b'\\' | b'.' | b'+' | b'*' | b'?' | b'(' | b')' | b'|' | b'[' | b']' | b'{' | b'}'
            | b'^' | b'$' => {
                out.push(b'\\');
                out.push(b);
            }
            0x20..=0x7E => out.push(b),
            _ => {
                out.extend_from_slice(format!("\\x{b:02x}").as_bytes());
            }
        }
    }
    out.push(b'$');
    out
}

/// Classify a gRPC status code into the appropriate `StorageError` variant so
/// retryable transport/service hiccups map to `StorageError::Transient` (HTTP
/// 503 `ServiceUnavailable` at the engine boundary) rather than opaque 500s.
#[must_use]
pub fn classify_grpc_status(context: &str, status: &tonic::Status) -> StorageError {
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Aborted
        | tonic::Code::ResourceExhausted => StorageError::Transient(format!("{context}: {status}")),
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange => {
            StorageError::Validation(format!("{context}: {status}"))
        }
        tonic::Code::Unimplemented => StorageError::Unsupported(format!("{context}: {status}")),
        _ => StorageError::Internal(format!("{context}: {status}")),
    }
}

/// Map a `bigtable_rs` data-plane error into `StorageError`.
#[must_use]
pub fn map_bigtable_err(context: &str, err: BigtableError) -> StorageError {
    match err {
        BigtableError::RpcError(status) => classify_grpc_status(context, &status),
        BigtableError::TimeoutError(secs) => {
            StorageError::Transient(format!("{context}: request timed out after {secs}s"))
        }
        BigtableError::TransportError(e) => StorageError::Connection(format!("{context}: {e}")),
        BigtableError::IoError(e) => StorageError::Transient(format!("{context}: {e}")),
        other => StorageError::Internal(format!("{context}: {other}")),
    }
}

/// Returns `true` when Bigtable rejected `ReadRowsRequest { reversed: true }`
/// with gRPC `UNIMPLEMENTED` (`cbtemulator`) or `FAILED_PRECONDITION` (when
/// reverse-scan feature routing is unavailable), signaling that `QueryScan`
/// should fall back to forward scan + in-memory reversal.
#[must_use]
pub fn is_reverse_scan_unsupported_err(err: &BigtableError) -> bool {
    matches!(
        err,
        BigtableError::RpcError(status)
            if matches!(
                status.code(),
                tonic::Code::Unimplemented | tonic::Code::FailedPrecondition
            )
    )
}

/// Map a stringified catalog/admin error to `StorageError`, preserving
/// transient retryability when the underlying gRPC error was transient.
#[must_use]
pub fn map_catalog_err(err: String) -> StorageError {
    let lower = err.to_ascii_lowercase();
    if lower.contains("unavailable")
        || lower.contains("deadlineexceeded")
        || lower.contains("deadline exceeded")
        || lower.contains("resourceexhausted")
        || lower.contains("resource exhausted")
        || lower.contains("code: aborted")
        || lower.contains("aborted:")
        || lower.contains("timed out")
    {
        StorageError::Transient(err)
    } else if lower.contains("transport error") || lower.contains("connection refused") {
        StorageError::Connection(err)
    } else {
        StorageError::Internal(err)
    }
}

/// Bundles a data-plane connection to BigTable. The admin-plane handle is
/// built lazily by `data::admin` when needed (it uses a different transport
/// crate and we don't want to pay for its setup when only data ops happen).
pub struct BigtableClient {
    pub project_id: String,
    pub instance_id: String,
    pub emulator_host: Option<String>,
    pub credentials_path: Option<String>,
    pub app_profile_id: Option<String>,
    connection: BigTableConnection,
}

impl BigtableClient {
    pub async fn connect(config: &BigtableStorageConfig) -> Result<Self, String> {
        let channel_size = config.pool_size.max(1) as usize;
        let timeout = Some(Duration::from_secs(30));

        let effective_emulator = config.emulator_host.clone().or_else(|| {
            std::env::var("BIGTABLE_EMULATOR_HOST")
                .ok()
                .filter(|s| !s.trim().is_empty())
        });

        let connection = if let Some(ref host) = effective_emulator {
            BigTableConnection::new_with_emulator(
                host,
                &config.project_id,
                &config.instance_id,
                /* is_read_only */ false,
                channel_size,
                timeout,
            )
            .map_err(|e| format!("bigtable emulator connect ({host}): {e}"))?
        } else {
            // Real BigTable uses rustls (via tonic). Install aws-lc-rs
            // explicitly once; subsequent calls are no-ops.
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

            let token_provider: Arc<dyn TokenProvider> =
                if let Some(cred_path) = &config.credentials_path {
                    let sa = gcp_auth::CustomServiceAccount::from_file(cred_path)
                        .map_err(|e| format!("gcp_auth CustomServiceAccount load: {e}"))?;
                    Arc::new(sa)
                } else {
                    gcp_auth::provider()
                        .await
                        .map_err(|e| format!("gcp_auth provider: {e}"))?
                };

            BigTableConnection::new_with_token_provider(
                &config.project_id,
                &config.instance_id,
                /* is_read_only */ false,
                channel_size,
                timeout,
                token_provider,
            )
            .map_err(|e| format!("bigtable connect: {e}"))?
        };

        Ok(Self {
            project_id: config.project_id.clone(),
            instance_id: config.instance_id.clone(),
            emulator_host: effective_emulator,
            credentials_path: config.credentials_path.clone(),
            app_profile_id: config.app_profile_id.clone(),
            connection,
        })
    }

    /// Returns a per-request BigTable data client. Cheap to clone.
    pub fn data(&self) -> BigTable {
        self.connection.client()
    }

    /// Convenience: build a fully-qualified BigTable table name from a short
    /// name. Format: `projects/<project>/instances/<instance>/tables/<table>`.
    pub fn full_table_name(&self, short: &str) -> String {
        format!(
            "projects/{}/instances/{}/tables/{}",
            self.project_id, self.instance_id, short
        )
    }

    /// Instance resource name (admin API needs this for CreateTable parent).
    pub fn instance_name(&self) -> String {
        format!(
            "projects/{}/instances/{}",
            self.project_id, self.instance_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_timestamps_strictly_increase_within_same_millisecond() {
        let mut prev = monotonic_timestamp_micros();
        assert_eq!(prev % MICROS_PER_MILLI, 0);
        for _ in 0..256 {
            let next = monotonic_timestamp_micros();
            assert!(next > prev, "expected {next} > {prev}");
            assert_eq!(next % MICROS_PER_MILLI, 0);
            prev = next;
        }

        let future_floor = prev + 5_000;
        let after_floor = monotonic_timestamp_after_micros(future_floor);
        assert!(
            after_floor > future_floor,
            "expected {after_floor} > {future_floor}"
        );
        assert_eq!(after_floor % MICROS_PER_MILLI, 0);
    }

    #[test]
    fn exact_re2_bytes_escapes_metacharacters_and_binary() {
        let escaped = exact_re2_bytes(b"node-1.test+[a]\x00\xff");
        assert_eq!(escaped, b"^node-1\\.test\\+\\[a\\]\\x00\\xff$");
    }

    #[test]
    fn grpc_transient_codes_map_to_transient() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Aborted,
            tonic::Code::ResourceExhausted,
        ] {
            let status = tonic::Status::new(code, "temporary hiccup");
            let err = map_bigtable_err("ReadRows", BigtableError::RpcError(status));
            assert!(
                matches!(err, StorageError::Transient(_)),
                "expected Transient for {code:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn grpc_non_transient_codes_map_appropriately() {
        let inv = map_bigtable_err(
            "MutateRow",
            BigtableError::RpcError(tonic::Status::invalid_argument("bad timestamp")),
        );
        assert!(matches!(inv, StorageError::Validation(_)));

        let unimp = BigtableError::RpcError(tonic::Status::unimplemented("reverse scan"));
        assert!(is_reverse_scan_unsupported_err(&unimp));
        assert!(matches!(
            map_bigtable_err("ReadRows", unimp),
            StorageError::Unsupported(_)
        ));

        let precond =
            BigtableError::RpcError(tonic::Status::failed_precondition("reverse scan disabled"));
        assert!(is_reverse_scan_unsupported_err(&precond));

        let internal = map_bigtable_err(
            "MutateRow",
            BigtableError::RpcError(tonic::Status::internal("server fault")),
        );
        assert!(matches!(internal, StorageError::Internal(_)));

        let timeout = map_bigtable_err("ReadRows", BigtableError::TimeoutError(30));
        assert!(matches!(timeout, StorageError::Transient(_)));
    }
}
