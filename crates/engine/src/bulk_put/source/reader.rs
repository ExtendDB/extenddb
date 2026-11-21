// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Input file reading on the filesystem store: opening a jailed file for
//! the decode chain in `chain.rs`, and hashing a jailed file for the
//! manifest.

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use crate::import_export_io::account_scoped_roots;

use super::chain::{ChainLineReader, line_reader};
use super::jail::{check_component, open_one, split_input_components};
use super::*;

pub(super) fn open_blocking(
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
    rel_path: &str,
    compression: InputCodec,
    size: u64,
) -> Result<ChainLineReader, SourceError> {
    check_component(bucket).map_err(SourceError::Jail)?;
    let components = split_input_components(rel_path).map_err(SourceError::Jail)?;
    let scoped =
        account_scoped_roots(roots, account).map_err(|e| SourceError::Jail(e.to_string()))?;
    for scoped_root in &scoped {
        let Some(file) = open_one(scoped_root, roots, account, bucket, &components)? else {
            continue;
        };
        // The audit event the import and export operations emit, with the
        // path the walk resolved. Paths stay in the log and never reach a
        // client.
        tracing::info!(
            account_id = %account,
            operation = "StartBulkPutItems",
            resolved_path = %scoped_root.join(bucket).join(rel_path).display(),
            "import/export file access"
        );
        return line_reader(Box::new(file), size, compression);
    }
    Err(SourceError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "input file not found under any import root",
    )))
}

/// Hash one input file for the manifest `etag`: the same jailed resolution
/// as `open_blocking`, then the MD5 of the first `size` bytes (the
/// enumerated size, so bytes appended since enumeration are not hashed,
/// matching what the reader consumed).
pub(super) fn etag_blocking(
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
    rel_path: &str,
    size: u64,
) -> Result<String, SourceError> {
    use md5::Digest;
    check_component(bucket).map_err(SourceError::Jail)?;
    let components = split_input_components(rel_path).map_err(SourceError::Jail)?;
    let scoped =
        account_scoped_roots(roots, account).map_err(|e| SourceError::Jail(e.to_string()))?;
    for scoped_root in &scoped {
        let Some(file) = open_one(scoped_root, roots, account, bucket, &components)? else {
            continue;
        };
        let mut hasher = md5::Md5::new();
        std::io::copy(&mut file.take(size), &mut hasher).map_err(SourceError::Io)?;
        return Ok(format!("{:x}", hasher.finalize()));
    }
    Err(SourceError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "input file not found under any import root",
    )))
}
