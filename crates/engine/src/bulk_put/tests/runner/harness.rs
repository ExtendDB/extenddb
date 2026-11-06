// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

mod postgres;
mod tables;

pub(crate) use postgres::{Backend, on_both_backends, postgres_configured, postgres_permit};

/// A fresh directory acting as a configured import or export root.
pub(crate) struct Root(pub(crate) PathBuf);

impl Root {
    pub(crate) fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("eb-bulk-run-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir.canonicalize().unwrap())
    }

    pub(crate) fn root(&self) -> Arc<PathBuf> {
        Arc::new(self.0.clone())
    }

    /// Create `<root>/<account>/<bucket>` the way a caller stages a
    /// bucket directory before naming it in a request.
    pub(crate) fn bucket_dir(&self, account: &str, bucket: &str) {
        std::fs::create_dir_all(self.0.join(account).join(bucket)).unwrap();
    }

    /// Create `<root>/<account>/<bucket>/<rel>` with content.
    pub(crate) fn file(&self, account: &str, bucket: &str, rel: &str, contents: &[u8]) -> PathBuf {
        let path = self.0.join(account).join(bucket).join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, contents).unwrap();
        path
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A store root with the two bucket directories the tests name staged for
/// the main account, matching what a caller does before its first request:
/// Start refuses a missing input or report bucket with a terminal FAILED
/// operation, and the sink opens the report bucket without creating it.
pub(crate) fn store_with_buckets() -> Root {
    let store = Root::new();
    store.bucket_dir(ACCOUNT, IN_BUCKET);
    store.bucket_dir(ACCOUNT, OUT_BUCKET);
    store
}

/// Storage engine, the store root, and a real table: everything one
/// runner test needs.
pub(crate) struct Harness {
    pub(crate) storage: Arc<dyn StorageEngine>,
    pub(crate) store: Root,
    pub(crate) table_id: String,
    pub(crate) table_arn: String,
    /// Catalog handle for the handler tests: account creation and the
    /// control-plane delay setting that keeps a table in `CREATING`.
    pub(crate) catalog: Arc<dyn extenddb_storage::CatalogStore>,
    /// The scratch database of a PostgreSQL harness, dropped by
    /// `cleanup`; `None` on `SQLite`.
    pub(crate) pg: Option<postgres::PgScratch>,
    /// The directory of a `SQLite` harness's database file, removed when
    /// the harness drops; `None` on PostgreSQL.
    pub(crate) _db_dir: Option<Root>,
}

/// Which table shape a harness is built with.
enum TableShape {
    Plain,
    WithGsi,
    WithGsiHashKey,
    WithGsiKeysOnlyHashKey,
    WithStream,
    WithVector,
}

impl Harness {
    pub(crate) fn write_input(&self, rel: &str, contents: &[u8]) {
        self.store.file(ACCOUNT, IN_BUCKET, rel, contents);
    }

    pub(crate) fn fs_source(&self) -> FsSource {
        FsSource {
            root: self.store.root(),
        }
    }

    pub(crate) fn fs_sink(&self) -> FsSink {
        FsSink {
            root: self.store.root(),
        }
    }

    /// Enumerate the input bucket the way the start handler will: through
    /// `BulkSource::list`, so the stored file list carries real sizes.
    pub(crate) async fn enumerate(&self) -> Vec<InputFile> {
        self.fs_source()
            .list(&BucketRef::new(ACCOUNT, IN_BUCKET, None), "", u64::MAX)
            .await
            .expect("enumerate input files")
            .into_iter()
            .map(|f: SourceFile| InputFile {
                path: f.path,
                size: i64::try_from(f.size).unwrap(),
                rows: None,
                etag: None,
                abandon: None,
            })
            .collect()
    }

    /// Create the operation record the real way and return its id.
    pub(crate) async fn start(&self, files: Vec<InputFile>, input: Value, output: Value) -> String {
        self.start_with_units(files, input, output, None).await
    }

    /// `start` with the request's `MaxWriteRequestUnits` recorded, so a
    /// test can drive the operation's own write ceiling.
    pub(crate) async fn start_with_units(
        &self,
        files: Vec<InputFile>,
        input: Value,
        output: Value,
        max_write_request_units: Option<i64>,
    ) -> String {
        self.start_with(files, input, output, max_write_request_units, None)
            .await
    }

    /// `start` with the run context the Start handler would record.
    pub(crate) async fn start_with_run_context(
        &self,
        files: Vec<InputFile>,
        input: Value,
        output: Value,
        run_context: Option<Value>,
    ) -> String {
        self.start_with(files, input, output, None, run_context)
            .await
    }

    async fn start_with(
        &self,
        files: Vec<InputFile>,
        input: Value,
        output: Value,
        max_write_request_units: Option<i64>,
        run_context: Option<Value>,
    ) -> String {
        let operation_id = arn::generate_operation_id();
        let total: i64 = files.iter().map(|f| f.size).sum();
        let outcome = self
            .storage
            .start_bulk_operation(
                ACCOUNT,
                NewBulkOperation {
                    operation_id: operation_id.clone(),
                    table_id: self.table_id.clone(),
                    table_name: TABLE.to_owned(),
                    table_arn: self.table_arn.clone(),
                    client_token: None,
                    client_token_hash: None,
                    request_fingerprint: "runner-test".to_owned(),
                    input_config: input,
                    output_config: output,
                    input_files: files,
                    max_write_request_units,
                    performance_tier: None,
                    start_time_ms: now_ms(),
                    total_input_bytes: Some(total),
                    initial_failure: None,
                    run_context,
                },
                50,
            )
            .await
            .expect("start bulk operation");
        assert!(
            matches!(outcome, StartOutcome::Created(_)),
            "the operation must be created"
        );
        operation_id
    }

    pub(crate) async fn claim(&self, owner: &str, lease: Duration) -> BulkOperationRecord {
        let mut claimed = self
            .storage
            .claim_runnable(owner, lease, 10, &[], &[])
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "exactly one operation must be claimable");
        claimed.remove(0)
    }

    pub(crate) fn deps_with(
        &self,
        owner: &str,
        config: RunnerConfig,
        source: Arc<dyn BulkSource>,
        sink: Arc<dyn BulkSink>,
    ) -> RunnerDeps {
        RunnerDeps {
            storage: Arc::clone(&self.storage),
            source,
            sink,
            limits: Arc::new(LimitsConfig::default()),
            region: Arc::from(REGION),
            owner: owner.to_owned(),
            config,
            metrics: Arc::new(MetricsCollector::new()),
            pacer: Arc::new(WritePacer::unbounded()),
            store_kind: STORE_FILESYSTEM,
            authorizer: None,
            shutdown: CancellationToken::new(),
        }
    }

    pub(crate) fn deps(&self, owner: &str, config: RunnerConfig) -> RunnerDeps {
        self.deps_with(
            owner,
            config,
            Arc::new(self.fs_source()),
            Arc::new(self.fs_sink()),
        )
    }

    /// Deps whose storage engine is substituted; source and sink stay real.
    pub(crate) fn deps_with_storage(
        &self,
        owner: &str,
        config: RunnerConfig,
        storage: Arc<dyn StorageEngine>,
    ) -> RunnerDeps {
        RunnerDeps {
            storage,
            source: Arc::new(self.fs_source()),
            sink: Arc::new(self.fs_sink()),
            limits: Arc::new(LimitsConfig::default()),
            region: Arc::from(REGION),
            owner: owner.to_owned(),
            config,
            metrics: Arc::new(MetricsCollector::new()),
            pacer: Arc::new(WritePacer::unbounded()),
            store_kind: STORE_FILESYSTEM,
            authorizer: None,
            shutdown: CancellationToken::new(),
        }
    }

    /// `deps` with every checkpoint asking the store for a 100 ms lease, so
    /// the operation is claimable soon after its run stops, while the
    /// runner's own lease, from `deps.config`, forces no renewal and no stop
    /// inside the test however slowly the host runs it.
    pub(crate) fn with_short_store_lease(&self, mut deps: RunnerDeps) -> RunnerDeps {
        let mut faulting = FaultingStorage::new(&deps.storage);
        faulting.checkpoint_lease = Some(Duration::from_millis(100));
        deps.storage = Arc::new(faulting);
        deps
    }

    /// Start, claim, and run one operation to a terminal state, returning
    /// the final record.
    pub(crate) async fn run_to_end(
        &self,
        files: Vec<InputFile>,
        input: Value,
        output: Value,
        config: RunnerConfig,
    ) -> BulkOperationRecord {
        let op = self.start(files, input, output).await;
        let record = self.claim("runner-1", Duration::from_secs(60)).await;
        runner::run(&self.deps("runner-1", config), record)
            .await
            .expect("runner run");
        self.record(&op).await
    }

    pub(crate) async fn record(&self, operation_id: &str) -> BulkOperationRecord {
        self.storage
            .get_bulk_operation(ACCOUNT, operation_id)
            .await
            .expect("get bulk operation")
            .expect("operation record exists")
    }

    pub(crate) async fn key_info(&self) -> TableKeyInfo {
        self.storage
            .table_key_info(ACCOUNT, TABLE)
            .await
            .expect("table key info")
    }

    pub(crate) async fn get(&self, pk: &str) -> Option<Item> {
        let info = self.key_info().await;
        self.storage
            .get_item(&info, &key(pk))
            .await
            .expect("get item")
    }

    pub(crate) async fn put_direct(&self, item: Item) {
        let info = self.key_info().await;
        self.storage
            .put_item(
                &info,
                item,
                false,
                None,
                &extenddb_core::expression::ExpressionMaps::default(),
                None,
                None,
            )
            .await
            .expect("direct put");
    }

    pub(crate) async fn count_items(&self) -> usize {
        let info = self.key_info().await;
        let mut count = 0;
        let mut start: Option<Item> = None;
        loop {
            let (items, next) = self
                .storage
                .scan(&info, None, start.as_ref(), None, None, None)
                .await
                .expect("scan");
            count += items.len();
            match next {
                Some(k) => start = Some(k),
                None => return count,
            }
        }
    }

    pub(crate) fn report_path(&self, operation_id: &str, kind: ReportKind) -> PathBuf {
        let dir = match kind {
            ReportKind::Error => "errors",
            ReportKind::ConditionCheckFailure => "condition-check-failures",
        };
        self.store
            .0
            .join(ACCOUNT)
            .join(OUT_BUCKET)
            .join(OUT_PREFIX)
            .join("AWSDynamoDB")
            .join("BulkOperation")
            .join(format!("{ACCOUNT}:{operation_id}-0"))
            .join("output")
            .join(dir)
            .join("output-00000.json.gz")
    }

    /// The sorted names of the rendered report objects of one kind
    /// (`output-00000.json.gz` onward); empty when none was rendered.
    pub(crate) fn report_objects(&self, operation_id: &str, kind: ReportKind) -> Vec<String> {
        let dir = self.report_path(operation_id, kind);
        let Some(dir) = dir.parent() else {
            return Vec::new();
        };
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| {
                name.strip_prefix("output-")
                    .and_then(|rest| rest.strip_suffix(".json.gz"))
                    .is_some_and(|digits| {
                        digits.len() == 5 && digits.bytes().all(|b| b.is_ascii_digit())
                    })
            })
            .collect();
        names.sort();
        names
    }

    /// Read one rendered report, every object in name order concatenated;
    /// `None` when it was not rendered.
    pub(crate) fn read_report(&self, operation_id: &str, kind: ReportKind) -> Option<Vec<Value>> {
        let dir = self.report_path(operation_id, kind).parent()?.to_path_buf();
        let names = self.report_objects(operation_id, kind);
        if names.is_empty() {
            return None;
        }
        let mut records = Vec::new();
        for name in names {
            let bytes = std::fs::read(dir.join(name)).ok()?;
            records.extend(gunzip_lines(&bytes));
        }
        Some(records)
    }

    /// The manifest's path: the operation directory, no shard suffix.
    pub(crate) fn manifest_path(&self, operation_id: &str) -> PathBuf {
        self.store
            .0
            .join(ACCOUNT)
            .join(OUT_BUCKET)
            .join(OUT_PREFIX)
            .join("AWSDynamoDB")
            .join("BulkOperation")
            .join(format!("{ACCOUNT}:{operation_id}"))
            .join("manifest-files.json")
    }

    /// Read the manifest's lines; `None` when none was written.
    pub(crate) fn read_manifest(&self, operation_id: &str) -> Option<Vec<Value>> {
        let text = std::fs::read_to_string(self.manifest_path(operation_id)).ok()?;
        Some(
            text.lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).expect("manifest line is JSON"))
                .collect(),
        )
    }

    /// The lowercase hex MD5 of one staged input file, what the manifest
    /// reports as its etag.
    pub(crate) fn input_md5(&self, rel: &str) -> String {
        use md5::Digest;
        let bytes = std::fs::read(self.store.0.join(ACCOUNT).join(IN_BUCKET).join(rel)).unwrap();
        format!("{:x}", md5::Md5::digest(bytes))
    }
}

pub(crate) fn config() -> RunnerConfig {
    RunnerConfig {
        batch_size: 25,
        write_batch_rows: 25,
        max_line_bytes: 4 << 20,
        max_item_count: 1_000_000,
        max_report_records: 10_000,
        max_report_bytes: 64 << 20,
        lease: Duration::from_secs(60),
    }
}
