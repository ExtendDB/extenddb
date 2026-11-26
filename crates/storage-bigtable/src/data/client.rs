// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cloud Bigtable data-plane gRPC client pool, stateful multi-frame `ReadRows`
//! chunk assembler, HTTP/2 `GoAway` reconnection, deterministic timestamp
//! helpers, RE2 byte escaping, and gRPC error classification.

use std::error::Error as StdError;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use extenddb_storage::error::StorageError;
use gcp_auth::TokenProvider;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient as TonicBigtableClient;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::CellChunk;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    CheckAndMutateRowRequest, CheckAndMutateRowResponse, MutateRowRequest, MutateRowResponse,
    MutateRowsRequest, ReadRowsRequest, ReadRowsResponse,
};
use tonic::Streaming;
use tonic::metadata::MetadataValue;
use tonic::transport::channel::Change;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::config::BigtableStorageConfig;

/// Microseconds per millisecond (`TimestampGranularity::MILLIS` in Cloud Bigtable).
pub const MICROS_PER_MILLI: i64 = 1_000;

/// Maximum gRPC payload size (64 MiB) for both incoming and outgoing Bigtable
/// messages, well above tonic's 4 MiB default so multi-row coordinator rows
/// and wide items never fail message decoding.
pub const MAX_GRPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

const BIGTABLE_DATA_ENDPOINT: &str = "https://bigtable.googleapis.com:443";
const BIGTABLE_DATA_HOST: &str = "bigtable.googleapis.com";
const BIGTABLE_DATA_SCOPE: &str = "https://www.googleapis.com/auth/bigtable.data";
/// URL-safe base64 encoding of `google.bigtable.v2.FeatureFlags { reverse_scans: true }`
/// (`[0x08, 0x01]`), required by Cloud Bigtable when `ReadRowsRequest.reversed == true`.
const BIGTABLE_FEATURES_REVERSE_SCANS: &str = "CAE=";
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RPC_ATTEMPTS: usize = 6;

/// Decoded Bigtable cell returned by [`DataHandle::read_rows`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowCell {
    pub family_name: String,
    pub qualifier: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp_micros: i64,
    pub labels: Vec<String>,
}

/// Typed data-plane error returned by [`DataHandle`] operations before mapping
/// into [`StorageError`].
#[derive(Debug, thiserror::Error)]
pub enum BigtableError {
    #[error("RPC error: {0}")]
    RpcError(#[from] tonic::Status),

    #[error("Transport error: {0}")]
    TransportError(String),

    #[error("Timeout error after {0} seconds")]
    TimeoutError(u64),

    #[error("Chunk decode error: {0}")]
    ChunkError(String),

    #[error("Authentication error: {0}")]
    AuthError(String),
}

/// Millisecond-aligned wall-clock timestamp in microseconds (`>= 1_000`)
/// guaranteed to be strictly greater than `exclusive_floor_micros`.
///
/// Per-row state transitions pass the cell timestamp observed on that row so
/// updates strictly advance the row's cell version without a process-wide
/// counter that could push timestamps ahead of wall-clock time under sustained
/// multi-row throughput.
#[must_use]
pub fn monotonic_timestamp_after_micros(exclusive_floor_micros: i64) -> i64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |d| {
            i64::try_from(d.as_millis()).unwrap_or(i64::MAX / MICROS_PER_MILLI)
        })
        .max(1);
    let min_ms = if exclusive_floor_micros <= 0 {
        1
    } else {
        (exclusive_floor_micros / MICROS_PER_MILLI).saturating_add(1)
    };
    now_ms.max(min_ms).saturating_mul(MICROS_PER_MILLI)
}

/// Millisecond-aligned wall-clock timestamp in microseconds (`>= 1_000`) for
/// newly inserted cells (`__extenddb_catalog__` `put_if_absent`,
/// `__extenddb_txn_log__` open, and `__extenddb_ttl_index__`).
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

/// Returns `true` when a `tonic::Status` carries an HTTP/2 `GoAway`
/// (`ENHANCE_YOUR_CALM`, `TOO_MANY_PINGS`) or low-level connection reset in its
/// message or error source chain.
#[must_use]
pub fn is_status_goaway_or_transport(status: &tonic::Status) -> bool {
    let mut text = status.message().to_ascii_lowercase();
    let mut src = status.source();
    while let Some(s) = src {
        text.push(' ');
        text.push_str(&s.to_string().to_ascii_lowercase());
        src = s.source();
    }
    text.contains("goaway")
        || text.contains("enhance_your_calm")
        || text.contains("too_many_pings")
        || text.contains("h2 protocol error")
        || text.contains("connection error")
        || text.contains("transport error")
        || text.contains("broken pipe")
        || text.contains("reset by peer")
        || text.contains("connection reset")
        || text.contains("error reading a body from connection")
}

/// Classify a gRPC status code into the appropriate `StorageError` variant.
///
/// Bigtable requests are constructed exclusively by this storage backend, so
/// `InvalidArgument`, `OutOfRange`, and `Unimplemented` indicate an internal
/// backend fault rather than a client validation error and map to
/// `StorageError::Internal`. HTTP/2 `GoAway` / transport resets wrapped in
/// `Status` map to `StorageError::Transient`.
#[must_use]
pub fn classify_grpc_status(context: &str, status: &tonic::Status) -> StorageError {
    if is_status_goaway_or_transport(status) {
        return StorageError::Transient(format!("{context}: {status}"));
    }
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Aborted
        | tonic::Code::ResourceExhausted => StorageError::Transient(format!("{context}: {status}")),
        tonic::Code::NotFound => StorageError::TableNotFound(format!("{context}: {status}")),
        _ => StorageError::Internal(format!("{context}: {status}")),
    }
}

/// Map a data-plane [`BigtableError`] into [`StorageError`].
#[must_use]
pub fn map_bigtable_err(context: &str, err: BigtableError) -> StorageError {
    match err {
        BigtableError::RpcError(status) => classify_grpc_status(context, &status),
        BigtableError::TimeoutError(secs) => {
            StorageError::Transient(format!("{context}: request timed out after {secs}s"))
        }
        BigtableError::TransportError(e) => StorageError::Connection(format!("{context}: {e}")),
        BigtableError::ChunkError(e) | BigtableError::AuthError(e) => {
            StorageError::Internal(format!("{context}: {e}"))
        }
    }
}

/// Returns `true` when Bigtable rejected `ReadRowsRequest { reversed: true }`
/// with gRPC `UNIMPLEMENTED` (`cbtemulator`) or `FAILED_PRECONDITION`.
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

/// Returns `true` when Bigtable reported `NOT_FOUND` (e.g. a dropped table).
#[must_use]
pub fn is_table_not_found_err(err: &BigtableError) -> bool {
    matches!(
        err,
        BigtableError::RpcError(status) if status.code() == tonic::Code::NotFound
    )
}

/// Returns `true` when a `BigtableError` came from a broken or reset HTTP/2
/// connection (such as `GoAway` with `ENHANCE_YOUR_CALM`, `h2 protocol error`,
/// or a closed transport channel) so the pool should replace its channels.
#[must_use]
pub fn is_goaway_or_transport_err(err: &BigtableError) -> bool {
    match err {
        BigtableError::TransportError(_) => true,
        BigtableError::RpcError(status) => {
            status.code() == tonic::Code::Unavailable || is_status_goaway_or_transport(status)
        }
        _ => false,
    }
}

/// Returns `true` when a read or idempotent write can be safely retried.
#[must_use]
pub fn is_retryable_err(err: &BigtableError) -> bool {
    if is_reverse_scan_unsupported_err(err) {
        return false;
    }
    if is_goaway_or_transport_err(err) {
        return true;
    }
    match err {
        BigtableError::TimeoutError(_) => true,
        BigtableError::RpcError(status) => matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::ResourceExhausted
                | tonic::Code::Aborted
                | tonic::Code::DeadlineExceeded
        ),
        _ => false,
    }
}

/// Returns `true` when a non-idempotent `CheckAndMutateRow` failed strictly
/// before Bigtable applied any mutation (`ResourceExhausted` or `Aborted`).
#[must_use]
fn is_pre_execution_rejection(err: &BigtableError) -> bool {
    matches!(
        err,
        BigtableError::RpcError(status)
            if matches!(
                status.code(),
                tonic::Code::ResourceExhausted | tonic::Code::Aborted
            )
    )
}

/// A single reassembled Bigtable row `(row_key, cells)`.
pub type AssembledRow = (Vec<u8>, Vec<RowCell>);

/// Stateful `ReadRowsResponse` `CellChunk` assembler supporting multi-megabyte
/// cells and multi-cell rows split across consecutive `ReadRowsResponse` stream
/// messages, `ResetRow` server retries, and standalone trailing `CommitRow`
/// marker chunks.
#[derive(Default)]
struct ReadRowsAssembler {
    committed_rows: Vec<AssembledRow>,
    cur_row_key: Option<Vec<u8>>,
    cur_row_cells: Vec<RowCell>,
    cur_family: Option<String>,
    cur_qualifier: Option<Vec<u8>>,
    cur_timestamp: i64,
    cur_labels: Vec<String>,
    cur_value: Vec<u8>,
    in_split_cell: bool,
}

impl ReadRowsAssembler {
    fn reset_current_row(&mut self) {
        self.cur_row_key = None;
        self.cur_row_cells.clear();
        self.cur_family = None;
        self.cur_qualifier = None;
        self.cur_timestamp = 0;
        self.cur_labels.clear();
        self.cur_value.clear();
        self.in_split_cell = false;
    }

    fn feed_chunks(&mut self, chunks: Vec<CellChunk>) -> Result<(), BigtableError> {
        for mut chunk in chunks {
            if let Some(RowStatus::ResetRow(_)) = chunk.row_status {
                if !chunk.value.is_empty() {
                    return Err(BigtableError::ChunkError(
                        "ResetRow chunk carried unexpected cell value bytes".to_owned(),
                    ));
                }
                self.reset_current_row();
                continue;
            }

            if !chunk.row_key.is_empty() {
                match &self.cur_row_key {
                    None => self.cur_row_key = Some(std::mem::take(&mut chunk.row_key)),
                    Some(existing) if existing == &chunk.row_key => {}
                    Some(_) => {
                        return Err(BigtableError::ChunkError(
                            "row_key changed before CommitRow".to_owned(),
                        ));
                    }
                }
            } else if self.cur_row_key.is_none() {
                return Err(BigtableError::ChunkError(
                    "chunk missing row_key at start of row".to_owned(),
                ));
            }

            // Invariant: all Bigtable writes in `extenddb-storage-bigtable` stamp
            // explicit millisecond-aligned timestamps `>= 1_000` (`MICROS_PER_MILLI`;
            // server-assigned `-1` and epoch `0` are never written), so a trailing
            // `CommitRow(true)` frame with `timestamp_micros == 0` and omitted
            // family/qualifier/labels/value after `>= 1` decoded cell is unambiguously
            // a standalone commit marker rather than a second empty cell.
            let is_pure_commit_marker = !self.in_split_cell
                && !self.cur_row_cells.is_empty()
                && chunk.family_name.is_none()
                && chunk.qualifier.is_none()
                && chunk.timestamp_micros == 0
                && chunk.labels.is_empty()
                && chunk.value.is_empty()
                && chunk.value_size == 0
                && matches!(chunk.row_status, Some(RowStatus::CommitRow(true)));

            if !is_pure_commit_marker {
                self.append_cell_chunk(&mut chunk)?;
            }

            if let Some(RowStatus::CommitRow(true)) = chunk.row_status {
                if self.in_split_cell {
                    return Err(BigtableError::ChunkError(
                        "CommitRow received before split cell finished".to_owned(),
                    ));
                }
                let rk = self.cur_row_key.take().ok_or_else(|| {
                    BigtableError::ChunkError("CommitRow missing row_key".to_owned())
                })?;
                let cells = std::mem::take(&mut self.cur_row_cells);
                self.committed_rows.push((rk, cells));
                self.cur_family = None;
                self.cur_qualifier = None;
            }
        }
        Ok(())
    }

    fn append_cell_chunk(&mut self, chunk: &mut CellChunk) -> Result<(), BigtableError> {
        if !self.in_split_cell {
            if chunk.family_name.is_some()
                && chunk.family_name != self.cur_family
                && chunk.qualifier.is_none()
            {
                return Err(BigtableError::ChunkError(
                    "new column family missing qualifier".to_owned(),
                ));
            }
            if let Some(fam) = chunk.family_name.take() {
                self.cur_family = Some(fam);
            }
            if let Some(qual) = chunk.qualifier.take() {
                self.cur_qualifier = Some(qual);
            }
            if self.cur_qualifier.is_none() {
                return Err(BigtableError::ChunkError(
                    "cell chunk missing column qualifier".to_owned(),
                ));
            }
            self.cur_timestamp = chunk.timestamp_micros;
            self.cur_labels = std::mem::take(&mut chunk.labels);
            let cap = if chunk.value_size > 0 {
                usize::try_from(chunk.value_size)
                    .unwrap_or(MAX_GRPC_MESSAGE_BYTES)
                    .min(MAX_GRPC_MESSAGE_BYTES)
            } else {
                chunk.value.len()
            };
            self.cur_value = Vec::with_capacity(cap);
        }

        self.cur_value.append(&mut chunk.value);
        if chunk.value_size > 0 {
            self.in_split_cell = true;
        } else {
            self.in_split_cell = false;
            let qual = self
                .cur_qualifier
                .clone()
                .ok_or_else(|| BigtableError::ChunkError("cell missing qualifier".to_owned()))?;
            let fam = self.cur_family.clone().unwrap_or_default();
            self.cur_row_cells.push(RowCell {
                family_name: fam,
                qualifier: qual,
                value: std::mem::take(&mut self.cur_value),
                timestamp_micros: self.cur_timestamp,
                labels: std::mem::take(&mut self.cur_labels),
            });
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<AssembledRow>, BigtableError> {
        if self.cur_row_key.is_some() || self.in_split_cell {
            return Err(BigtableError::ChunkError(
                "ReadRows stream ended with uncommitted row".to_owned(),
            ));
        }
        Ok(self.committed_rows)
    }
}

async fn decode_read_rows_stream(
    deadline: Instant,
    timeout_secs: u64,
    mut stream: Streaming<ReadRowsResponse>,
) -> Result<Vec<AssembledRow>, BigtableError> {
    let mut assembler = ReadRowsAssembler::default();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(BigtableError::TimeoutError(timeout_secs));
        }
        let next = tokio::time::timeout(remaining, stream.message())
            .await
            .map_err(|_| BigtableError::TimeoutError(timeout_secs))?;
        let Some(resp) = next? else {
            break;
        };
        assembler.feed_chunks(resp.chunks)?;
    }
    assembler.finish()
}

struct PooledGrpc {
    client: TonicBigtableClient<Channel>,
    /// Keep `tx` alive for the lifetime of the pool so `balance_channel`
    /// never closes its endpoint discovery stream prematurely.
    _balance_tx: tokio::sync::mpsc::Sender<Change<usize, Endpoint>>,
}

fn build_pooled_grpc(
    emulator_host: Option<&str>,
    channel_size: usize,
) -> Result<Arc<PooledGrpc>, String> {
    let count = channel_size.max(1);
    let (channel, tx) = Channel::balance_channel(count * 2 + 4);

    if let Some(host) = emulator_host {
        let uri = if host.starts_with("http://") || host.starts_with("https://") {
            host.to_owned()
        } else {
            format!("http://{host}")
        };
        for i in 0..count {
            // Do NOT enable `keep_alive_while_idle` against `cbtemulator`:
            // Go's default `grpc-go` server enforces a 5-minute minimum ping
            // interval and closes connections with `GoAway` (`ENHANCE_YOUR_CALM`)
            // when idle channels ping every 60 seconds.
            let endpoint = Endpoint::from_shared(uri.clone())
                .map_err(|e| format!("invalid Bigtable emulator endpoint '{host}': {e}"))?
                .timeout(RPC_TIMEOUT)
                .connect_timeout(Duration::from_secs(10))
                .tcp_nodelay(true)
                .http2_adaptive_window(true);
            tx.try_send(Change::Insert(i, endpoint))
                .map_err(|e| format!("register Bigtable emulator channel {i}: {e}"))?;
        }
    } else {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls = ClientTlsConfig::new()
            .with_native_roots()
            .domain_name(BIGTABLE_DATA_HOST);
        for i in 0..count {
            let endpoint = Endpoint::from_static(BIGTABLE_DATA_ENDPOINT)
                .tls_config(tls.clone())
                .map_err(|e| format!("configure Bigtable TLS endpoint: {e}"))?
                .http2_keep_alive_interval(Duration::from_mins(5))
                .keep_alive_timeout(Duration::from_secs(20))
                .http2_adaptive_window(true)
                .timeout(RPC_TIMEOUT)
                .connect_timeout(Duration::from_secs(10))
                .tcp_nodelay(true);
            tx.try_send(Change::Insert(i, endpoint))
                .map_err(|e| format!("register Bigtable TLS channel {i}: {e}"))?;
        }
    }

    let client = TonicBigtableClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);

    Ok(Arc::new(PooledGrpc {
        client,
        _balance_tx: tx,
    }))
}

/// Bundles a resilient data-plane connection pool to Cloud Bigtable (or
/// `cbtemulator`). Automatically replaces broken HTTP/2 channels on `GoAway`
/// or transport errors and injects fresh `OAuth2` bearer tokens plus
/// `x-goog-request-params` routing headers on GCP requests.
pub struct BigtableClient {
    config: BigtableStorageConfig,
    pub project_id: String,
    pub instance_id: String,
    pub emulator_host: Option<String>,
    pub credentials_path: Option<String>,
    pub app_profile_id: Option<String>,
    channel_size: usize,
    token_provider: Option<Arc<dyn TokenProvider>>,
    pool: RwLock<Arc<PooledGrpc>>,
}

impl BigtableClient {
    #[must_use]
    pub fn config(&self) -> &BigtableStorageConfig {
        &self.config
    }

    /// Shared `OAuth2` token provider (absent when connected to `cbtemulator`).
    #[must_use]
    pub fn token_provider(&self) -> Option<&Arc<dyn TokenProvider>> {
        self.token_provider.as_ref()
    }

    /// Connect a resilient Bigtable data-plane pool using `config`.
    ///
    /// # Errors
    ///
    /// Returns an error description if TLS endpoint setup or GCP credential
    /// initialization fails.
    pub async fn connect(config: &BigtableStorageConfig) -> Result<Self, String> {
        let channel_size = config.pool_size.max(1) as usize;
        let effective_emulator = config.resolve_emulator_host();

        let token_provider: Option<Arc<dyn TokenProvider>> = if effective_emulator.is_some() {
            None
        } else {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let provider: Arc<dyn TokenProvider> = if let Some(cred_path) = &config.credentials_path
            {
                let sa = gcp_auth::CustomServiceAccount::from_file(cred_path)
                    .map_err(|e| format!("gcp_auth CustomServiceAccount load: {e}"))?;
                Arc::new(sa)
            } else {
                gcp_auth::provider()
                    .await
                    .map_err(|e| format!("gcp_auth provider: {e}"))?
            };
            Some(provider)
        };

        let pool = build_pooled_grpc(effective_emulator.as_deref(), channel_size)?;

        Ok(Self {
            config: config.clone(),
            project_id: config.project_id.clone(),
            instance_id: config.instance_id.clone(),
            emulator_host: effective_emulator,
            credentials_path: config.credentials_path.clone(),
            app_profile_id: config.app_profile_id.clone(),
            channel_size,
            token_provider,
            pool: RwLock::new(pool),
        })
    }

    /// Log the resolved Bigtable endpoint at server startup (`#33`), emitting a
    /// warning when `emulator_host` / `BIGTABLE_EMULATOR_HOST` routes traffic
    /// over plaintext HTTP/2 without GCP credentials.
    pub fn log_startup_endpoint(&self, role: &str) {
        if let Some(ref host) = self.emulator_host {
            tracing::warn!(
                role,
                emulator_host = %host,
                project_id = %self.project_id,
                instance_id = %self.instance_id,
                "Bigtable backend connecting to emulator endpoint over plaintext HTTP/2 without credentials"
            );
        } else {
            tracing::info!(
                role,
                endpoint = BIGTABLE_DATA_ENDPOINT,
                project_id = %self.project_id,
                instance_id = %self.instance_id,
                app_profile_id = self.app_profile_id.as_deref().unwrap_or("default"),
                "Bigtable backend connected to Google Cloud Bigtable over TLS"
            );
        }
    }

    fn current_pool(&self) -> Arc<PooledGrpc> {
        self.pool
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn reconnect_if_same(&self, failed_pool: &Arc<PooledGrpc>) {
        let mut guard = self.pool.write().unwrap_or_else(PoisonError::into_inner);
        if !Arc::ptr_eq(&*guard, failed_pool) {
            return;
        }
        if let Ok(fresh) = build_pooled_grpc(self.emulator_host.as_deref(), self.channel_size) {
            *guard = fresh;
        }
    }

    async fn make_request<R>(
        &self,
        payload: R,
        table_name: &str,
        app_profile_id: &str,
    ) -> Result<tonic::Request<R>, BigtableError> {
        let mut req = tonic::Request::new(payload);
        if let Some(provider) = &self.token_provider {
            let token = provider
                .token(&[BIGTABLE_DATA_SCOPE])
                .await
                .map_err(|e| BigtableError::AuthError(e.to_string()))?;
            let header = format!("Bearer {}", token.as_str());
            let val = MetadataValue::try_from(header.as_str())
                .map_err(|e| BigtableError::AuthError(format!("invalid bearer token: {e}")))?;
            req.metadata_mut().insert("authorization", val);
        }
        let routing = format!(
            "table_name={}&app_profile_id={}",
            urlencoding::encode(table_name),
            urlencoding::encode(app_profile_id)
        );
        if let Ok(routing_val) = MetadataValue::try_from(routing) {
            req.metadata_mut()
                .insert("x-goog-request-params", routing_val);
        }
        req.metadata_mut().insert(
            "bigtable-features",
            MetadataValue::from_static(BIGTABLE_FEATURES_REVERSE_SCANS),
        );
        Ok(req)
    }

    /// Returns a lightweight per-request data-plane handle.
    #[must_use]
    pub fn data(&self) -> DataHandle<'_> {
        DataHandle { parent: self }
    }

    /// Build a fully-qualified Bigtable table name:
    /// `projects/<project>/instances/<instance>/tables/<table>`.
    #[must_use]
    pub fn full_table_name(&self, short: &str) -> String {
        format!(
            "projects/{}/instances/{}/tables/{}",
            self.project_id, self.instance_id, short
        )
    }

    /// Instance resource name (`projects/<project>/instances/<instance>`).
    #[must_use]
    pub fn instance_name(&self) -> String {
        format!(
            "projects/{}/instances/{}",
            self.project_id, self.instance_id
        )
    }
}

/// Per-request Bigtable data-plane handle with automatic `GoAway` channel
/// replacement and bounded transient backoff on safe operations.
#[derive(Clone, Copy)]
pub struct DataHandle<'a> {
    parent: &'a BigtableClient,
}

async fn sleep_retry_backoff(attempt: usize) {
    let base_ms = 5u64.saturating_mul(1u64 << attempt.min(5));
    let jitter_ms = rand::random_range(0..=base_ms);
    tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
}

impl DataHandle<'_> {
    /// Execute `ReadRows`, automatically reconnecting and retrying on `GoAway`,
    /// transport drops, or transient gRPC errors.
    ///
    /// # Errors
    ///
    /// Returns [`BigtableError`] if all retry attempts fail, stream frames are
    /// malformed, or reading exceeds `RPC_TIMEOUT`.
    pub async fn read_rows(
        &mut self,
        request: ReadRowsRequest,
    ) -> Result<Vec<(Vec<u8>, Vec<RowCell>)>, BigtableError> {
        for attempt in 0..MAX_RPC_ATTEMPTS {
            let deadline = Instant::now() + RPC_TIMEOUT;
            let pool = self.parent.current_pool();
            let mut client = pool.client.clone();
            let tonic_req = self
                .parent
                .make_request(
                    request.clone(),
                    &request.table_name,
                    &request.app_profile_id,
                )
                .await?;
            let outcome = match client.read_rows(tonic_req).await {
                Ok(resp) => {
                    decode_read_rows_stream(deadline, RPC_TIMEOUT.as_secs(), resp.into_inner())
                        .await
                }
                Err(status) => Err(BigtableError::RpcError(status)),
            };
            match outcome {
                Ok(rows) => return Ok(rows),
                Err(err) => {
                    if is_goaway_or_transport_err(&err) {
                        self.parent.reconnect_if_same(&pool);
                    }
                    if attempt + 1 < MAX_RPC_ATTEMPTS && is_retryable_err(&err) {
                        sleep_retry_backoff(attempt).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        unreachable!("MAX_RPC_ATTEMPTS is non-zero")
    }

    /// Execute `MutateRow` (all callers use explicit timestamps or row deletes,
    /// making retries idempotent across channel reconnects).
    ///
    /// # Errors
    ///
    /// Returns [`BigtableError`] if the RPC fails after bounded retries.
    pub async fn mutate_row(
        &mut self,
        request: MutateRowRequest,
    ) -> Result<tonic::Response<MutateRowResponse>, BigtableError> {
        for attempt in 0..MAX_RPC_ATTEMPTS {
            let pool = self.parent.current_pool();
            let mut client = pool.client.clone();
            let tonic_req = self
                .parent
                .make_request(
                    request.clone(),
                    &request.table_name,
                    &request.app_profile_id,
                )
                .await?;
            match client.mutate_row(tonic_req).await {
                Ok(resp) => return Ok(resp),
                Err(status) => {
                    let err = BigtableError::RpcError(status);
                    if is_goaway_or_transport_err(&err) {
                        self.parent.reconnect_if_same(&pool);
                    }
                    if attempt + 1 < MAX_RPC_ATTEMPTS && is_retryable_err(&err) {
                        sleep_retry_backoff(attempt).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        unreachable!("MAX_RPC_ATTEMPTS is non-zero")
    }

    /// Execute `MutateRows`, draining the streaming response within `RPC_TIMEOUT`
    /// and verifying every per-entry status code before returning.
    ///
    /// # Errors
    ///
    /// Returns [`BigtableError`] if any batch entry fails, the stream stalls
    /// past `RPC_TIMEOUT`, or retries are exhausted.
    pub async fn mutate_rows(&mut self, request: MutateRowsRequest) -> Result<(), BigtableError> {
        for attempt in 0..MAX_RPC_ATTEMPTS {
            let deadline = Instant::now() + RPC_TIMEOUT;
            let pool = self.parent.current_pool();
            let mut client = pool.client.clone();
            let tonic_req = self
                .parent
                .make_request(
                    request.clone(),
                    &request.table_name,
                    &request.app_profile_id,
                )
                .await?;
            let stream_res = client.mutate_rows(tonic_req).await;
            let outcome: Result<(), BigtableError> = match stream_res {
                Err(status) => Err(BigtableError::RpcError(status)),
                Ok(resp) => {
                    let mut stream = resp.into_inner();
                    let mut entry_err: Option<BigtableError> = None;
                    loop {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            entry_err = Some(BigtableError::TimeoutError(RPC_TIMEOUT.as_secs()));
                            break;
                        }
                        let Ok(next_msg) = tokio::time::timeout(remaining, stream.message()).await
                        else {
                            entry_err = Some(BigtableError::TimeoutError(RPC_TIMEOUT.as_secs()));
                            break;
                        };
                        match next_msg {
                            Ok(Some(batch)) => {
                                for entry in batch.entries {
                                    if let Some(st) = entry.status
                                        && st.code != 0
                                    {
                                        entry_err =
                                            Some(BigtableError::RpcError(tonic::Status::new(
                                                tonic::Code::from_i32(st.code),
                                                st.message,
                                            )));
                                        break;
                                    }
                                }
                                if entry_err.is_some() {
                                    break;
                                }
                            }
                            Ok(None) => break,
                            Err(status) => {
                                entry_err = Some(BigtableError::RpcError(status));
                                break;
                            }
                        }
                    }
                    match entry_err {
                        Some(e) => Err(e),
                        None => Ok(()),
                    }
                }
            };
            match outcome {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if is_goaway_or_transport_err(&err) {
                        self.parent.reconnect_if_same(&pool);
                    }
                    if attempt + 1 < MAX_RPC_ATTEMPTS && is_retryable_err(&err) {
                        sleep_retry_backoff(attempt).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        unreachable!("MAX_RPC_ATTEMPTS is non-zero")
    }

    /// Execute `CheckAndMutateRow`. Reconnects on `GoAway` / transport drops
    /// and retries pre-execution rejections (`ResourceExhausted`, `Aborted`)
    /// where Bigtable has not applied any mutation.
    ///
    /// # Errors
    ///
    /// Returns [`BigtableError`] if the RPC fails after pre-execution retries.
    pub async fn check_and_mutate_row(
        &mut self,
        request: CheckAndMutateRowRequest,
    ) -> Result<CheckAndMutateRowResponse, BigtableError> {
        self.check_and_mutate_row_inner(request, false)
            .await
            .map(|(resp, _)| resp)
    }

    /// Execute a `CheckAndMutateRow` whose caller either uses an idempotent
    /// mutation (such as lease renewal or version-only tombstone reclamation) or
    /// explicitly disambiguates an applied first attempt on retry (such as
    /// coordinator `PENDING -> COMMITTED`/`ABORTED` state transitions and
    /// `place_intent_once`), retrying `Unavailable` and `GoAway` transport
    /// errors as well.
    ///
    /// # Errors
    ///
    /// Returns [`BigtableError`] if the RPC fails after bounded retries.
    pub async fn check_and_mutate_row_idempotent(
        &mut self,
        request: CheckAndMutateRowRequest,
    ) -> Result<CheckAndMutateRowResponse, BigtableError> {
        self.check_and_mutate_row_inner(request, true)
            .await
            .map(|(resp, _)| resp)
    }

    /// Same as [`Self::check_and_mutate_row_idempotent`], also returning the
    /// 0-based retry `attempt` index that succeeded so callers such as
    /// `TxnCoordinator::open_with_token` can disambiguate a retried response
    /// after Attempt 0 already applied on Bigtable.
    ///
    /// # Errors
    ///
    /// Returns [`BigtableError`] if the RPC fails after bounded retries.
    pub async fn check_and_mutate_row_idempotent_with_attempt(
        &mut self,
        request: CheckAndMutateRowRequest,
    ) -> Result<(CheckAndMutateRowResponse, usize), BigtableError> {
        self.check_and_mutate_row_inner(request, true).await
    }

    async fn check_and_mutate_row_inner(
        &mut self,
        request: CheckAndMutateRowRequest,
        idempotent: bool,
    ) -> Result<(CheckAndMutateRowResponse, usize), BigtableError> {
        for attempt in 0..MAX_RPC_ATTEMPTS {
            let pool = self.parent.current_pool();
            let mut client = pool.client.clone();
            let tonic_req = self
                .parent
                .make_request(
                    request.clone(),
                    &request.table_name,
                    &request.app_profile_id,
                )
                .await?;
            match client.check_and_mutate_row(tonic_req).await {
                Ok(resp) => return Ok((resp.into_inner(), attempt)),
                Err(status) => {
                    let err = BigtableError::RpcError(status);
                    if is_goaway_or_transport_err(&err) {
                        self.parent.reconnect_if_same(&pool);
                    }
                    let can_retry = if idempotent {
                        is_retryable_err(&err)
                    } else {
                        is_pre_execution_rejection(&err)
                    };
                    if attempt + 1 < MAX_RPC_ATTEMPTS && can_retry {
                        sleep_retry_backoff(attempt).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        unreachable!("MAX_RPC_ATTEMPTS is non-zero")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_stay_on_wall_clock_without_global_drift_and_advance_past_floor() {
        let before_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(1))
            .unwrap_or(1);
        let mut last = 0i64;
        for _ in 0..5_000 {
            let ts = monotonic_timestamp_micros();
            assert!(ts >= MICROS_PER_MILLI, "expected {ts} >= 1_000");
            assert_eq!(ts % MICROS_PER_MILLI, 0);
            assert!(ts >= last, "wall-clock timestamp regressed: {ts} < {last}");
            last = ts;
        }
        let after_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(1))
            .unwrap_or(1);
        assert!(
            last >= before_ms * MICROS_PER_MILLI,
            "expected {last} >= {}",
            before_ms * MICROS_PER_MILLI
        );
        assert!(
            last <= (after_ms + 1_000) * MICROS_PER_MILLI,
            "5,000 calls drifted ahead of wall-clock time: {last} vs {} ms",
            after_ms
        );

        let same_ms_step = monotonic_timestamp_after_micros(last);
        assert!(same_ms_step > last, "expected {same_ms_step} > {last}");
        assert_eq!(same_ms_step % MICROS_PER_MILLI, 0);

        let future_floor = same_ms_step + 5_000;
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
    fn grpc_transient_codes_and_goaway_map_to_transient() {
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

        let goaway = BigtableError::RpcError(tonic::Status::internal(
            "h2 protocol error: GoAway(ENHANCE_YOUR_CALM)",
        ));
        assert!(is_goaway_or_transport_err(&goaway));
        assert!(is_retryable_err(&goaway));
        assert!(matches!(
            map_bigtable_err("ReadRows", goaway),
            StorageError::Transient(_)
        ));
    }

    #[test]
    fn grpc_backend_fault_codes_map_to_internal() {
        for status in [
            tonic::Status::invalid_argument("bad timestamp"),
            tonic::Status::out_of_range("bad range"),
            tonic::Status::unimplemented("unsupported filter"),
            tonic::Status::internal("server fault"),
        ] {
            let err = map_bigtable_err("MutateRow", BigtableError::RpcError(status));
            assert!(
                matches!(err, StorageError::Internal(_)),
                "expected Internal, got {err:?}"
            );
        }

        let unimp = BigtableError::RpcError(tonic::Status::unimplemented("reverse scan"));
        assert!(is_reverse_scan_unsupported_err(&unimp));
        assert!(!is_retryable_err(&unimp));

        let not_found = BigtableError::RpcError(tonic::Status::not_found("table deleted"));
        assert!(is_table_not_found_err(&not_found));
        assert!(matches!(
            map_bigtable_err("MutateRow", not_found),
            StorageError::TableNotFound(_)
        ));

        let timeout = map_bigtable_err("ReadRows", BigtableError::TimeoutError(30));
        assert!(matches!(timeout, StorageError::Transient(_)));
    }

    #[test]
    fn read_rows_assembler_handles_multi_frame_split_cells_reset_and_trailing_commit() {
        let mut asm = ReadRowsAssembler::default();

        // Frame 1: start of a 3 MiB cell (split across 3 frames) — NOT committed yet.
        let part1 = vec![0xABu8; 1_048_576];
        asm.feed_chunks(vec![CellChunk {
            row_key: b"row-large".to_vec(),
            family_name: Some("t".to_owned()),
            qualifier: Some(b"mutations".to_vec()),
            timestamp_micros: 5_000,
            labels: vec![],
            value: part1.clone(),
            value_size: 3 * 1_048_576,
            row_status: None,
        }])
        .expect("frame 1");

        // Frame 2: middle 1 MiB continuation chunk (`value_size` still > 0).
        let part2 = vec![0xCDu8; 1_048_576];
        asm.feed_chunks(vec![CellChunk {
            row_key: vec![],
            family_name: None,
            qualifier: None,
            timestamp_micros: 0,
            labels: vec![],
            value: part2.clone(),
            value_size: 3 * 1_048_576,
            row_status: None,
        }])
        .expect("frame 2");

        // Frame 3: final 1 MiB chunk (`value_size == 0`) + standalone trailing CommitRow(true)
        // marker chunk + a second row that undergoes ResetRow before succeeding.
        let part3 = vec![0xEFu8; 1_048_576];
        asm.feed_chunks(vec![
            CellChunk {
                row_key: vec![],
                family_name: None,
                qualifier: None,
                timestamp_micros: 0,
                labels: vec![],
                value: part3.clone(),
                value_size: 0,
                row_status: None,
            },
            CellChunk {
                row_key: vec![],
                family_name: None,
                qualifier: None,
                timestamp_micros: 0,
                labels: vec![],
                value: vec![],
                value_size: 0,
                row_status: Some(RowStatus::CommitRow(true)),
            },
            CellChunk {
                row_key: b"row-reset".to_vec(),
                family_name: Some("d".to_owned()),
                qualifier: Some(b"stale".to_vec()),
                timestamp_micros: 1_000,
                labels: vec![],
                value: b"discard-me".to_vec(),
                value_size: 0,
                row_status: None,
            },
            CellChunk {
                row_key: vec![],
                family_name: None,
                qualifier: None,
                timestamp_micros: 0,
                labels: vec![],
                value: vec![],
                value_size: 0,
                row_status: Some(RowStatus::ResetRow(true)),
            },
            CellChunk {
                row_key: b"row-2".to_vec(),
                family_name: Some("d".to_owned()),
                qualifier: Some(Vec::new()),
                timestamp_micros: 2_000,
                labels: vec![],
                value: Vec::new(),
                value_size: 0,
                row_status: Some(RowStatus::CommitRow(true)),
            },
        ])
        .expect("frame 3");

        let rows = asm.finish().expect("finish cleanly");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"row-large");
        assert_eq!(
            rows[0].1.len(),
            1,
            "trailing CommitRow must not duplicate cell"
        );
        assert_eq!(rows[0].1[0].family_name, "t");
        assert_eq!(rows[0].1[0].qualifier, b"mutations");
        assert_eq!(rows[0].1[0].timestamp_micros, 5_000);
        assert_eq!(rows[0].1[0].value.len(), 3 * 1_048_576);
        assert_eq!(&rows[0].1[0].value[..1_048_576], part1.as_slice());
        assert_eq!(
            &rows[0].1[0].value[1_048_576..2 * 1_048_576],
            part2.as_slice()
        );
        assert_eq!(&rows[0].1[0].value[2 * 1_048_576..], part3.as_slice());

        assert_eq!(rows[1].0, b"row-2");
        assert_eq!(rows[1].1.len(), 1);
        assert!(rows[1].1[0].qualifier.is_empty());
        assert!(rows[1].1[0].value.is_empty());
    }

    #[tokio::test]
    async fn make_request_attaches_routing_and_reverse_scan_feature_headers() {
        use base64::Engine as _;
        use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::FeatureFlags;
        use prost::Message as _;

        let flags_proto = FeatureFlags {
            reverse_scans: true,
            ..FeatureFlags::default()
        }
        .encode_to_vec();
        assert_eq!(flags_proto, vec![0x08, 0x01]);
        assert_eq!(
            base64::engine::general_purpose::URL_SAFE.encode(&flags_proto),
            BIGTABLE_FEATURES_REVERSE_SCANS
        );

        let cfg = BigtableStorageConfig {
            project_id: "example.com:proj-a".to_owned(),
            instance_id: "inst-b".to_owned(),
            app_profile_id: Some("single-tx".to_owned()),
            emulator_host: Some("127.0.0.1:8086".to_owned()),
            ..BigtableStorageConfig::default()
        };
        let client = BigtableClient::connect(&cfg).await.expect("connect");
        let full_table = client.full_table_name("t0123456789abcdef");
        let req = client
            .make_request((), &full_table, "single-tx")
            .await
            .expect("make_request");
        let hdr = req
            .metadata()
            .get("x-goog-request-params")
            .expect("x-goog-request-params header present")
            .to_str()
            .expect("ascii header");
        assert_eq!(
            hdr,
            "table_name=projects%2Fexample.com%3Aproj-a%2Finstances%2Finst-b%2Ftables%2Ft0123456789abcdef&app_profile_id=single-tx"
        );
        let feat_hdr = req
            .metadata()
            .get("bigtable-features")
            .expect("bigtable-features header present")
            .to_str()
            .expect("ascii header");
        assert_eq!(feat_hdr, BIGTABLE_FEATURES_REVERSE_SCANS);
    }
}
