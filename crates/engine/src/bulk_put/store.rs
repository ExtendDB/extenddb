// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bulk store handle: the store the server configuration designates,
//! with the source every operation reads its input through and the sink it
//! writes its reports through, and the builders that turn an operation's
//! configurations into the arguments the source and sink take.

use std::path::PathBuf;
use std::sync::Arc;

use extenddb_core::types::{OutputS3Configuration, S3SseAlgorithm};
use extenddb_storage::BulkOperationRecord;

use crate::bulk_put::run_context::{RecordedRunContext, store_kind};
use crate::bulk_put::s3::{S3Sink, S3Source, connect};
use crate::bulk_put::source::{
    BucketRef, BulkSink, BulkSource, FsSink, FsSource, OutputSse, StoreFault,
};

/// Which store a [`BulkStore`] is.
enum StoreKind {
    /// The filesystem store: one directory tree, `{root}/{account id}/
    /// {S3Bucket}/...`, under the canonical root the server resolved at
    /// startup.
    Filesystem { root: Arc<PathBuf> },
    /// The S3 store: the buckets the requests name, in AWS S3 or an
    /// S3-compatible service.
    S3 {
        /// Whether `endpoint_url` names an S3-compatible service, which
        /// resolves `..` key segments (see `source::stored_form`).
        endpoint: bool,
        /// The access key ID of the credentials the store's startup check
        /// resolved.
        access_key_id: Option<String>,
    },
}

/// The name the S3 store's startup lines give it.
pub const S3_STORE_LABEL: &str = "S3 bulk store";

/// The settings of the S3 store, from `[bulk.s3]`.
pub use extenddb_s3::S3StoreSettings;

/// The bulk store: inputs are read through `source` and reports written
/// through `sink`. The server builds one at startup and shares it with the
/// request handlers, the account management API and the worker.
pub struct BulkStore {
    kind: StoreKind,
    pub(crate) source: Arc<dyn BulkSource>,
    pub(crate) sink: Arc<dyn BulkSink>,
}

impl BulkStore {
    /// The filesystem store under `root`, the canonical path
    /// [`crate::bulk_put::store_root::resolve_store_root`] returned.
    #[must_use]
    pub fn filesystem(root: Arc<PathBuf>) -> Self {
        Self {
            source: Arc::new(FsSource {
                root: Arc::clone(&root),
            }),
            sink: Arc::new(FsSink {
                root: Arc::clone(&root),
            }),
            kind: StoreKind::Filesystem { root },
        }
    }

    /// The S3 store with `settings`, its clients built from the standard
    /// AWS credential chain (see [`crate::bulk_put::s3::connect`]).
    pub async fn s3(settings: &S3StoreSettings) -> Self {
        let clients = Arc::new(connect(settings).await);
        Self {
            kind: StoreKind::S3 {
                endpoint: settings.endpoint_url.is_some(),
                access_key_id: clients.access_key_id().map(str::to_owned),
            },
            source: Arc::new(S3Source {
                clients: Arc::clone(&clients),
            }),
            sink: Arc::new(S3Sink { clients }),
        }
    }

    /// The access key ID of the credentials the S3 store's startup check
    /// resolved, for the server to compare with the keys it accepts from
    /// its clients; `None` for the filesystem store and for credentials
    /// that did not resolve. Never logged.
    #[must_use]
    pub fn s3_access_key_id(&self) -> Option<&str> {
        match &self.kind {
            StoreKind::S3 { access_key_id, .. } => access_key_id.as_deref(),
            StoreKind::Filesystem { .. } => None,
        }
    }

    /// The kind of this store as the `[bulk] store` setting spells it,
    /// `filesystem` or `s3`: what Start records on each operation it
    /// starts.
    #[must_use]
    pub fn kind_name(&self) -> &'static str {
        store_kind(self)
    }

    /// The filesystem store's canonical root, beneath which each account
    /// has its directory; `None` for a store with no directory tree.
    #[must_use]
    pub fn filesystem_root(&self) -> Option<&Arc<PathBuf>> {
        match &self.kind {
            StoreKind::Filesystem { root } => Some(root),
            StoreKind::S3 { .. } => None,
        }
    }

    /// Whether this store resolves a `..` key segment against the segment
    /// before it: the S3 store against a configured endpoint. AWS S3 keeps
    /// such a key as given, and the filesystem store refuses it at Start.
    #[must_use]
    pub fn resolves_parent(&self) -> bool {
        matches!(self.kind, StoreKind::S3 { endpoint: true, .. })
    }

    /// Test hook: a filesystem store under `root` whose source and sink are
    /// replaced, for handler tests that need a store answer the filesystem
    /// cannot give.
    #[cfg(test)]
    pub(crate) fn with_parts(
        root: Arc<PathBuf>,
        source: Arc<dyn BulkSource>,
        sink: Arc<dyn BulkSink>,
    ) -> Self {
        Self {
            kind: StoreKind::Filesystem { root },
            source,
            sink,
        }
    }

    /// Test hook: an S3 store over `source` and `sink`, for handler tests of
    /// the rules that depend on the kind of store, with no client built.
    #[cfg(test)]
    pub(crate) fn s3_with_parts(source: Arc<dyn BulkSource>, sink: Arc<dyn BulkSink>) -> Self {
        Self {
            kind: StoreKind::S3 {
                endpoint: false,
                access_key_id: None,
            },
            source,
            sink,
        }
    }

    /// Test hook: [`Self::s3_with_parts`] for an S3 store against a
    /// configured endpoint.
    #[cfg(test)]
    pub(crate) fn s3_endpoint_with_parts(
        source: Arc<dyn BulkSource>,
        sink: Arc<dyn BulkSink>,
    ) -> Self {
        Self {
            kind: StoreKind::S3 {
                endpoint: true,
                access_key_id: None,
            },
            source,
            sink,
        }
    }
}

/// The kind of store the operation `record` was started on, as Start
/// recorded it and spelled as [`BulkStore::kind_name`] spells it. An
/// operation with no recorded run context was started on the filesystem
/// store, the only one there was.
///
/// # Errors
///
/// The detail of a recorded run context that does not parse.
pub fn recorded_store_kind(record: &BulkOperationRecord) -> Result<String, String> {
    RecordedRunContext::of(record).map(|context| context.store)
}

impl<'a> BucketRef<'a> {
    /// The bucket `bucket` in the caller's account `account`, with the
    /// configuration's `S3BucketOwner` as `owner`. An owner equal to the
    /// caller's account sets no expectation, the normalization the request
    /// fingerprint applies, so an absent owner and the caller's own account
    /// behave alike.
    pub(crate) fn new(account: &'a str, bucket: &'a str, owner: Option<&'a str>) -> Self {
        Self {
            account,
            bucket,
            expected_owner: owner.filter(|owner| *owner != account),
        }
    }
}

impl OutputSse {
    /// The encryption the output configuration asks for. A key id is kept
    /// only with `KMS`, the algorithm that uses one.
    pub(crate) fn of(output: &OutputS3Configuration) -> Self {
        match output.s3_sse_algorithm {
            None => Self::None,
            Some(S3SseAlgorithm::Aes256) => Self::Aes256,
            Some(S3SseAlgorithm::Kms) => Self::Kms {
                key_id: output.s3_sse_kms_key_id.clone(),
            },
        }
    }
}

impl StoreFault {
    /// A bucket the caller may not use (measured on the service: the
    /// hosted service fails the operation with this code). The filesystem
    /// store answers it for an `S3BucketOwner` other than the caller's
    /// account.
    pub(crate) fn access_denied() -> Self {
        Self {
            code: "S3AccessDenied",
            message: "Access Denied".to_owned(),
            after_first_byte: false,
        }
    }

    /// A bucket that does not exist (measured on the service). The
    /// filesystem store answers it for a bucket directory that is missing
    /// or is not a directory.
    pub(crate) fn no_such_bucket() -> Self {
        Self {
            code: "S3NoSuchBucket",
            message: "The specified bucket does not exist".to_owned(),
            after_first_byte: false,
        }
    }
}
