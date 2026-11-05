// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The failure a run ends with: the `FailureCode` and `FailureMessage`
//! pairs the runner records, and the mapping from a source fault to one.

use super::*;

/// The failure a run ends with, when one is set: the `FailureCode` and
/// `FailureMessage` of the terminal record.
#[derive(Debug, Clone)]
pub(super) struct Failure {
    pub(super) code: Cow<'static, str>,
    pub(super) message: String,
}

/// The code of the failure a store answers when it was unavailable past its
/// retries. It is not an outcome: the run stops with the operation in
/// progress and a later claim resumes it from its checkpoint (`finalize`),
/// so the code is never written to a record.
const CODE_STORE_UNAVAILABLE: &str = "StoreUnavailable";

/// The failure code of the object-count quota class. The Describe handler
/// recognises the class by this code to omit the two byte members the
/// hosted service leaves out of that description.
pub(crate) const OBJECTS_COUNT_LIMIT_CODE: &str = "S3ObjectsCountLimitExceeded";

impl Failure {
    /// The table with the bound `table_id` is gone or unusable.
    pub(super) fn table_not_found() -> Self {
        Self {
            code: Cow::Borrowed(CODE_RESOURCE_NOT_FOUND),
            message: MESSAGE_RESOURCE_NOT_FOUND.to_owned(),
        }
    }

    /// The input or output store failed with a transient fault that
    /// outlasted its retries (`StoreUnavailable`): the operation resumes
    /// from its checkpoint, as after a transient database fault.
    pub(super) fn store_unavailable() -> Self {
        Self {
            code: Cow::Borrowed(CODE_STORE_UNAVAILABLE),
            message: INTERNAL_MESSAGE.to_owned(),
        }
    }

    /// Whether this is [`Self::store_unavailable`], directly or as the
    /// location fault of [`Self::manifest_unavailable`].
    pub(super) fn is_store_unavailable(&self) -> bool {
        self.code == CODE_STORE_UNAVAILABLE
    }

    /// Anything whose detail belongs in the log and not on the wire.
    pub(super) fn internal() -> Self {
        Self {
            code: Cow::Borrowed(CODE_INTERNAL),
            message: INTERNAL_MESSAGE.to_owned(),
        }
    }

    /// An input file was not a compressed object under `GZIP` (measured
    /// ): the operation runs to its end, then fails with this.
    pub(super) fn items_failed_validation() -> Self {
        Self {
            code: Cow::Borrowed(CODE_VALIDATION),
            message: "Some of the items failed validation checks and were not ingested".to_owned(),
        }
    }

    /// The processed row count passed the per-operation cap.
    pub(super) fn row_limit(limit: u64) -> Self {
        Self {
            code: Cow::Borrowed(CODE_VALIDATION),
            message: format!("Operation exceeded max_bulk_item_count ({limit} rows)"),
        }
    }

    /// The manifest could not be written because the output location is
    /// unusable: the bucket is missing or the configured owner is not the
    /// caller (measured on the service). The code is the
    /// location fault's own; the message carries the measured prefix.
    pub(super) fn manifest_unavailable(location: Self) -> Self {
        Self {
            code: location.code,
            message: format!("Manifest is not available. {}", location.message),
        }
    }

    /// The output bucket directory is missing at the first report record.
    pub(super) fn output_bucket_missing() -> Self {
        Self {
            code: Cow::Borrowed("S3NoSuchBucket"),
            message: "The specified bucket does not exist".to_owned(),
        }
    }

    /// An input object the operation had enumerated is gone when the
    /// runner opens or reads it (measured on the service: an object
    /// deleted mid-run ends the operation FAILED with this code). The
    /// service appends an S3 request-id tail in parentheses, which a
    /// local store cannot reproduce; the sentence is emitted alone, as
    /// `output_bucket_missing` does for the bucket.
    pub(super) fn no_such_key() -> Self {
        Self {
            code: Cow::Borrowed("S3NoSuchKey"),
            message: "The specified key does not exist.".to_owned(),
        }
    }

    /// More enumerated input objects under the prefix than the configured
    /// count cap allows (measured on the service: the Start is
    /// accepted and the operation ends FAILED with this code, the prefix
    /// interpolated as the request gave it and the cap as a bare number).
    pub(super) fn objects_count_limit_exceeded(prefix: &str, limit: u64) -> Self {
        Self {
            code: Cow::Borrowed(OBJECTS_COUNT_LIMIT_CODE),
            message: format!(
                "You have reached the maximum number of objects allowed in a single bulk \
                 operation for the specified prefix {prefix}, which is {limit}. You can reach \
                 out to AWS Support to request a quota increase."
            ),
        }
    }

    /// One enumerated input object above the configured input byte cap
    /// (measured on the service). The measured text renders the cap
    /// as `5.0 GB` for 5,368,709,120 bytes, so the figure is the cap
    /// divided by 2^30 to one decimal place, labelled GB.
    pub(super) fn input_object_too_large(limit_bytes: u64) -> Self {
        Self {
            code: Cow::Borrowed("InputObjectTooLarge"),
            message: format!(
                "This bulk operation contains one or more input objects larger than the \
                 allowed per-object size of {}. Split those objects into smaller parts and \
                 retry.",
                format_gibibytes(limit_bytes)
            ),
        }
    }

    /// A location under the store the operation may not use: the output
    /// `S3BucketOwner` is not the caller's account, or a path in the
    /// caller's own tree is refused (a symlink in the input or report path,
    /// a component that is not a directory, a file that is not a regular
    /// file, a path that escapes the account directory). The code is the
    /// one the hosted service answers for an input or report location it
    /// may not read or write; the detail stays in the log.
    pub(super) fn access_denied() -> Self {
        Self {
            code: Cow::Borrowed("S3AccessDenied"),
            message: "Access Denied".to_owned(),
        }
    }

    /// A fault the store rendered, with its own code and message.
    pub(super) fn from_store(fault: &StoreFault) -> Self {
        Self {
            code: Cow::Borrowed(fault.code),
            message: fault.message.clone(),
        }
    }
}

/// The cap figure as the measured message renders it: the byte count
/// divided by 2^30 to one decimal place, labelled GB (measured on the
/// service: an object of 5,369,000,000 bytes was refused against a cap the
/// message spells `5.0 GB`, which is 5,368,709,120 bytes).
fn format_gibibytes(bytes: u64) -> String {
    // The cap is a configuration value, far below the precision limit of
    // f64, so the division is exact to the one decimal the text carries.
    #[allow(clippy::cast_precision_loss)]
    let gib = bytes as f64 / 1_073_741_824.0;
    format!("{gib:.1} GB")
}

/// The failure for an input file that could not be opened or read. A
/// refusal of the caller's own tree (a symlink, a component that is not a
/// directory, a file that is not a regular file, a jail escape) is the
/// caller's to fix and is reported as access denied; a file that is gone
/// or unreadable answers the measured S3 code for that class; a store
/// unavailable past its retries is [`Failure::store_unavailable`]; any other
/// I/O error is the server's and is reported as internal. The detail is
/// logged by the caller of this function.
pub(super) fn source_failure(error: &SourceError) -> Failure {
    match error {
        SourceError::Io(io) if StoreUnavailable::of_io(io) => Failure::store_unavailable(),
        SourceError::Jail(_) | SourceError::NotRegularFile => Failure::access_denied(),
        SourceError::Io(io) => match io.kind() {
            std::io::ErrorKind::NotFound => Failure::no_such_key(),
            std::io::ErrorKind::PermissionDenied => Failure::access_denied(),
            _ => Failure::internal(),
        },
        SourceError::Store(fault) => Failure::from_store(fault),
        SourceError::LineTooLong
        | SourceError::InvalidUtf8
        | SourceError::Corrupt
        | SourceError::NotCompressed
        | SourceError::DecodeFailed
        | SourceError::Overwritten
        | SourceError::TooManyFiles => Failure::internal(),
    }
}

/// Whether a source failure means the input object itself is gone or
/// unreadable, which the manifest reports with no processed range (see
/// `RunState::unavailable_file`). A store fault carries the answer in
/// `after_first_byte`.
pub(super) fn is_unavailable_object(error: &SourceError) -> bool {
    match error {
        SourceError::Jail(_) | SourceError::NotRegularFile => true,
        SourceError::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
        ),
        SourceError::Store(fault) => fault.after_first_byte,
        _ => false,
    }
}
