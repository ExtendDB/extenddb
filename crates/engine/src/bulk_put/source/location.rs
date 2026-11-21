// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What a store is told about a location and what it answers: the bucket
//! one side of an operation names, the encryption a write asks for, and a
//! fault the store renders for the client. The builders are in
//! `bulk_put::store`.

/// The bucket one side of an operation names, as a store sees it. Built
/// with [`BucketRef::new`], which applies the owner normalization.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BucketRef<'a> {
    /// The caller's account id: the filesystem store's namespace, and the
    /// account an `S3BucketOwner` is compared with.
    pub account: &'a str,
    pub bucket: &'a str,
    /// The configuration's `S3BucketOwner` when it names an account other
    /// than the caller's; `None` when it is absent or equal to the caller's
    /// account.
    pub expected_owner: Option<&'a str>,
}

/// The server-side encryption a report or manifest write asks for, from
/// the output configuration's `S3SseAlgorithm` and `S3SseKmsKeyId`. The
/// filesystem store records it and writes plain files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutputSse {
    None,
    Aes256,
    Kms { key_id: Option<String> },
}

/// A fault the store has already rendered for the client: the operation
/// ends with `code` as its `FailureCode` and `message` as its
/// `FailureMessage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoreFault {
    pub code: &'static str,
    pub message: String,
    /// Whether the fault came after the first byte of an input object was
    /// read. A location check sets it false.
    pub after_first_byte: bool,
}

impl StoreFault {
    /// The fault an I/O error carries, when a store's reader raised it with
    /// `std::io::Error::other(fault)`: a read that fails mid-object reaches
    /// the decode chain as an I/O error, and the chain hands the fault on
    /// as `SourceError::Store` with its code and message.
    pub(crate) fn of_io(error: &std::io::Error) -> Option<Self> {
        error.get_ref()?.downcast_ref::<Self>().cloned()
    }
}

impl std::fmt::Display for StoreFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for StoreFault {}

/// A store failure the store classifies as transient (a 5xx answer,
/// throttling, a timeout, a connection failure) that outlasted its retries.
/// It travels inside `std::io::Error::other`, as a [`StoreFault`] does
/// mid-object, so it reaches the runner through every decode chain as
/// `SourceError::Io` or `SinkError::Io`. The runner then leaves the
/// operation in progress for the next claim, as after a transient database
/// fault; every other caller answers it as the server's failure.
#[derive(Debug, Clone)]
pub(crate) struct StoreUnavailable(pub(crate) String);

impl StoreUnavailable {
    /// The I/O error that carries the failure, `detail` its text.
    pub(crate) fn io(detail: String) -> std::io::Error {
        std::io::Error::other(Self(detail))
    }

    /// Whether `error` carries a [`StoreUnavailable`].
    pub(crate) fn of_io(error: &std::io::Error) -> bool {
        error.get_ref().is_some_and(|inner| inner.is::<Self>())
    }
}

impl std::fmt::Display for StoreUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StoreUnavailable {}
