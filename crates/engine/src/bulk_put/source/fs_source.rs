// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The filesystem input source: the bucket check, enumeration and
//! opening of input files beneath the caller's account subtree.

use super::*;

impl FsSource {
    /// Whether `{root}/{account}/{bucket}` exists and is a directory,
    /// decided with the same primitives the enumeration uses: the bucket
    /// name is checked as one component and opened with `openat(O_DIRECTORY
    /// | O_NOFOLLOW)` from the verified account handle, so a symlink is
    /// never followed, and a symlink or a non-directory at the name answers
    /// `false` exactly as the enumeration would find no files beneath it.
    /// `check_bucket` decides the input bucket's existence with this.
    pub(crate) fn bucket_exists(
        &self,
        account: &str,
        bucket: &str,
    ) -> BoxFuture<'_, Result<bool, SourceError>> {
        let root = Arc::clone(&self.root);
        let account = account.to_owned();
        let bucket = bucket.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || bucket_exists_blocking(&[root], &account, &bucket))
                .await
                .map_err(|e| SourceError::Io(std::io::Error::other(e)))?
        })
    }
}

/// The enumerated size the record stores for an input file, the read
/// ceiling; a negative stored size reads as zero.
fn enumerated_size(file: &InputFile) -> u64 {
    u64::try_from(file.size).unwrap_or(0)
}

impl BulkSource for FsSource {
    fn check_bucket(
        &self,
        at: &BucketRef<'_>,
        _prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>> {
        let root = Arc::clone(&self.root);
        let account = at.account.to_owned();
        let bucket = at.bucket.to_owned();
        let owner_mismatch = at.expected_owner.is_some();
        Box::pin(async move {
            // The caller's account directory under the store root, created
            // here when it is missing so a caller can always find
            // `{root}/{account}`. A failure is the server's fault, not the
            // request's: the details go to the log and the client sees the
            // sanitized error.
            let dir_account = account.clone();
            tokio::task::spawn_blocking(move || ensure_account_dir(&root, &dir_account))
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, "bulk store account directory task failed");
                    SourceError::Io(std::io::Error::other(e))
                })?
                .map_err(|error| {
                    tracing::error!(
                        account_id = %account,
                        %error,
                        "cannot create the bulk store account directory"
                    );
                    SourceError::Io(error)
                })?;
            // An input S3BucketOwner that is not the caller's account is
            // S3AccessDenied (measured: the hosted service flips the
            // operation to FAILED with that code); on the filesystem store
            // the caller's account is the only owner a bucket directory can
            // have. Ownership is decided before existence (ExtendDB's
            // choice), so a caller naming another owner cannot learn
            // whether the bucket exists. An input bucket directory that
            // does not exist, or is not a directory, is S3NoSuchBucket.
            if owner_mismatch {
                return Err(SourceError::Store(StoreFault::access_denied()));
            }
            if self.bucket_exists(&account, &bucket).await? {
                Ok(())
            } else {
                Err(SourceError::Store(StoreFault::no_such_bucket()))
            }
        })
    }

    fn list(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        // The jail primitives (`account_scoped_roots`, `validate_path`)
        // are shared with `ImportTable` and `ExportTableToPointInTime`,
        // whose configuration remains a list, so the one bulk store root
        // travels as a one-element list.
        let root = Arc::clone(&self.root);
        let account = at.account.to_owned();
        let bucket = at.bucket.to_owned();
        let prefix = prefix.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                list_blocking(&[root], &account, &bucket, &prefix, max_files)
            })
            .await
            .map_err(|e| SourceError::Io(std::io::Error::other(e)))?
        })
    }

    fn open(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        let root = Arc::clone(&self.root);
        let account = at.account.to_owned();
        let bucket = at.bucket.to_owned();
        let path = file.path.clone();
        let size = enumerated_size(file);
        Box::pin(async move {
            let reader = tokio::task::spawn_blocking(move || {
                open_blocking(&[root], &account, &bucket, &path, compression, size)
            })
            .await
            .map_err(|e| SourceError::Io(std::io::Error::other(e)))??;
            Ok(Box::new(reader) as Box<dyn LineReader>)
        })
    }

    fn etag(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>> {
        let root = Arc::clone(&self.root);
        let account = at.account.to_owned();
        let bucket = at.bucket.to_owned();
        let path = file.path.clone();
        let size = enumerated_size(file);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                etag_blocking(&[root], &account, &bucket, &path, size)
            })
            .await
            .map_err(|e| SourceError::Io(std::io::Error::other(e)))?
        })
    }

    fn line_count(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>> {
        let root = Arc::clone(&self.root);
        let account = at.account.to_owned();
        let bucket = at.bucket.to_owned();
        let path = file.path.clone();
        let size = enumerated_size(file);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut reader =
                    open_blocking(&[root], &account, &bucket, &path, compression, size)?;
                let Some(mut inner) = reader.inner.take() else {
                    return Ok(0);
                };
                let mut count: u64 = 0;
                let mut buf = Vec::new();
                loop {
                    buf.clear();
                    match inner.read_until(b'\n', &mut buf) {
                        Ok(0) => break,
                        Ok(_) => count += 1,
                        Err(e) => return Err(SourceError::Io(e)),
                    }
                }
                Ok(count)
            })
            .await
            .map_err(|e| SourceError::Io(std::io::Error::other(e)))?
        })
    }
}
