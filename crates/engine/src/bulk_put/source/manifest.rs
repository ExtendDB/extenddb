// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The operation manifest file: the list of input files an operation
//! ingested, written to the operation directory.

use std::path::PathBuf;
use std::sync::Arc;

use crate::bulk_put::report;
use crate::import_export_io::account_scoped_roots;

use super::jail::{check_component, check_prefix};
use super::key_form::walked_segments;
use super::sink::ReportDirectory;
use super::*;

/// The manifest's file name in the operation directory.
const MANIFEST_NAME: &str = "manifest-files.json";

/// Write the manifest: the same lexical checks and walk as a report, to the
/// operation directory itself (the report directories hang beneath the
/// `-0` shard directory beside it), then the temp file, the lines, sync,
/// and the rename over the final name.
pub(super) fn write_manifest_blocking(
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
    prefix: &str,
    operation_id: &str,
    owner: &str,
    lines: &[Value],
) -> Result<String, SinkError> {
    use std::io::Write;
    check_component(bucket).map_err(SinkError::Jail)?;
    check_prefix(prefix).map_err(SinkError::Jail)?;
    check_component(operation_id).map_err(SinkError::Jail)?;
    check_component(owner).map_err(SinkError::Jail)?;
    if roots.is_empty() {
        return Err(SinkError::Jail("no bulk store is configured".to_owned()));
    }
    let scoped =
        account_scoped_roots(roots, account).map_err(|e| SinkError::Jail(e.to_string()))?;
    let existing = scoped.iter().find(|dir| {
        std::fs::symlink_metadata(dir)
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
    });
    let base = if let Some(dir) = existing {
        dir.clone()
    } else {
        let Some(first) = scoped.first() else {
            return Err(SinkError::Jail("no bulk store is configured".to_owned()));
        };
        std::fs::create_dir_all(first).map_err(SinkError::Io)?;
        first.clone()
    };
    let mut components: Vec<String> = vec![bucket.to_owned()];
    components.extend(walked_segments(prefix).map(str::to_owned));
    components.push("AWSDynamoDB".to_owned());
    components.push("BulkOperation".to_owned());
    components.push(format!("{account}:{operation_id}"));
    let dir_handle = ReportDirectory::establish(&base, roots, account, &components)?;
    dir_handle.remove_stale_temps(&format!("{MANIFEST_NAME}.tmp-"))?;
    let tmp_name = format!("{MANIFEST_NAME}.tmp-{owner}");
    let mut file = dir_handle.create_new(&tmp_name)?;
    let written = (|| -> std::io::Result<()> {
        for line in lines {
            report::write_manifest_line(&mut file, line)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()
    })();
    if let Err(error) = written {
        drop(file);
        let _ = dir_handle.remove_leftover(&tmp_name);
        return Err(SinkError::Io(error));
    }
    drop(file);
    dir_handle.rename(&tmp_name, MANIFEST_NAME)?;
    tracing::info!(
        account_id = %account,
        operation = "StartBulkPutItems",
        resolved_path = %{
            let mut path = base.clone();
            for component in &components {
                path.push(component);
            }
            path.push(MANIFEST_NAME);
            path.display().to_string()
        },
        "import/export file access"
    );
    Ok(report::manifest_key(prefix, account, operation_id))
}
