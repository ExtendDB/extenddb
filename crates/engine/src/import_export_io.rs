// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! File I/O helpers for import/export operations.
//!
//! Extracted from `import_export.rs` to keep both files under the 500-line limit.

use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, InputFormat, Item};

/// Read items from a file in the specified format.
pub(crate) fn read_items(
    path: &Path,
    format: InputFormat,
    options: Option<&extenddb_core::types::InputFormatOptions>,
    max_items: u64,
) -> Result<Vec<Item>, DynamoDbError> {
    let file = std::fs::File::open(path)
        .map_err(|_| DynamoDbError::ValidationException("Cannot open source file".to_owned()))?;
    let reader = std::io::BufReader::new(file);

    match format {
        InputFormat::DynamoDbJson => read_dynamodb_json(reader, max_items),
        InputFormat::Ion => read_dynamodb_json(reader, max_items),
        InputFormat::Csv => read_csv(reader, options, max_items),
    }
}

/// Read DynamoDB JSON format: one JSON object per line with `{"Item": {...}}` wrapper.
fn read_dynamodb_json(reader: impl BufRead, max_items: u64) -> Result<Vec<Item>, DynamoDbError> {
    let mut items = Vec::new();
    for (line_num, line) in reader.lines().enumerate() {
        let line = line.map_err(|_| {
            DynamoDbError::ValidationException(format!(
                "I/O error reading import file at line {}",
                line_num + 1
            ))
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed: Value = serde_json::from_str(trimmed).map_err(|_| {
            DynamoDbError::ValidationException(format!("Invalid JSON at line {}", line_num + 1))
        })?;

        let item_value = if let Some(inner) = parsed.get("Item") {
            inner.clone()
        } else {
            parsed
        };

        let item: Item = serde_json::from_value(item_value).map_err(|_| {
            DynamoDbError::ValidationException(format!(
                "Invalid DynamoDB item at line {}",
                line_num + 1
            ))
        })?;
        items.push(item);
        if u64::try_from(items.len()).unwrap_or(u64::MAX) > max_items {
            return Err(DynamoDbError::ValidationException(format!(
                "Import item count exceeds maximum ({max_items})"
            )));
        }
    }
    Ok(items)
}

/// Read CSV format.
fn read_csv(
    reader: impl BufRead,
    options: Option<&extenddb_core::types::InputFormatOptions>,
    max_items: u64,
) -> Result<Vec<Item>, DynamoDbError> {
    let delimiter = options
        .and_then(|o| o.csv.as_ref())
        .map(|c| c.delimiter.as_str())
        .unwrap_or(",");
    let explicit_headers = options
        .and_then(|o| o.csv.as_ref())
        .and_then(|c| c.header_list.as_ref());

    let delim_byte = if delimiter.len() == 1 {
        delimiter.as_bytes()[0]
    } else {
        return Err(DynamoDbError::ValidationException(
            "CSV delimiter must be a single character".to_owned(),
        ));
    };

    let lines: Vec<String> = reader
        .lines()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| DynamoDbError::ValidationException("I/O error reading CSV file".to_owned()))?;

    if lines.is_empty() {
        return Ok(Vec::new());
    }

    let (headers, data_start) = if let Some(h) = explicit_headers {
        (h.clone(), 0)
    } else {
        let first_line = &lines[0];
        let headers: Vec<String> = split_csv_line(first_line, delim_byte);
        (headers, 1)
    };

    let mut items = Vec::new();
    for line in &lines[data_start..] {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let values = split_csv_line(trimmed, delim_byte);
        let mut item = Item::new();
        for (i, header) in headers.iter().enumerate() {
            if let Some(val) = values.get(i)
                && !val.is_empty()
            {
                item.insert(header.clone(), AttributeValue::S(val.clone()));
            }
        }
        if !item.is_empty() {
            items.push(item);
            if u64::try_from(items.len()).unwrap_or(u64::MAX) > max_items {
                return Err(DynamoDbError::ValidationException(format!(
                    "Import item count exceeds maximum ({max_items})"
                )));
            }
        }
    }
    Ok(items)
}

/// Split a CSV line by delimiter with RFC 4180 quoting support (CB-24).
fn split_csv_line(line: &str, delim: u8) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(c);
            }
        } else if c == '"' && current.is_empty() {
            in_quotes = true;
        } else if c == delim as char {
            fields.push(current.trim().to_owned());
            current = String::new();
        } else {
            current.push(c);
        }
    }
    fields.push(current.trim().to_owned());
    fields
}

/// Validate and canonicalize a filesystem path for import/export.
///
/// Rejects symlinks and paths with `..` components to prevent path traversal.
/// When `jail_roots` is non-empty, the canonical path must resolve under at
/// least one of the allowed roots.
pub(crate) fn validate_path(
    raw: &str,
    jail_roots: &[Arc<PathBuf>],
) -> Result<PathBuf, DynamoDbError> {
    let path = Path::new(raw);

    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(DynamoDbError::ValidationException(
                "Path must not contain '..' components".to_owned(),
            ));
        }
    }

    let canonical = path.canonicalize().map_err(|_| {
        DynamoDbError::ValidationException("Path does not exist or is not accessible".to_owned())
    })?;

    let meta = std::fs::symlink_metadata(path)
        .map_err(|_| DynamoDbError::ValidationException("Cannot read path metadata".to_owned()))?;
    if meta.file_type().is_symlink() {
        return Err(DynamoDbError::ValidationException(
            "Symbolic links are not allowed in import/export paths".to_owned(),
        ));
    }

    if !jail_roots.is_empty()
        && !jail_roots
            .iter()
            .any(|root| canonical.starts_with(root.as_path()))
    {
        return Err(DynamoDbError::ValidationException(
            "Path must resolve under one of the configured allowed paths".to_owned(),
        ));
    }

    Ok(canonical)
}

/// Validate an export output path. The file may not exist yet, so we validate
/// the parent directory instead.
/// When `jail_roots` is non-empty, the resolved path must be under at least
/// one of the allowed roots.
pub(crate) fn validate_path_parent(
    raw: &str,
    jail_roots: &[Arc<PathBuf>],
) -> Result<PathBuf, DynamoDbError> {
    let path = Path::new(raw);

    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(DynamoDbError::ValidationException(
                "Path must not contain '..' components".to_owned(),
            ));
        }
    }

    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            if !jail_roots.is_empty() {
                // Relative path with no parent — resolve against CWD and check jail.
                let cwd = std::env::current_dir().map_err(|_| {
                    DynamoDbError::ValidationException(
                        "Cannot determine current directory".to_owned(),
                    )
                })?;
                let canonical_cwd = cwd.canonicalize().map_err(|_| {
                    DynamoDbError::ValidationException(
                        "Cannot resolve current directory".to_owned(),
                    )
                })?;
                let resolved = canonical_cwd.join(path);
                if !jail_roots
                    .iter()
                    .any(|root| resolved.starts_with(root.as_path()))
                {
                    return Err(DynamoDbError::ValidationException(
                        "Path must resolve under one of the configured allowed paths".to_owned(),
                    ));
                }
            }
            return Ok(path.to_path_buf());
        }
        let parent_meta = std::fs::symlink_metadata(parent).map_err(|_| {
            DynamoDbError::ValidationException(
                "Parent directory does not exist or is not accessible".to_owned(),
            )
        })?;
        if parent_meta.file_type().is_symlink() {
            return Err(DynamoDbError::ValidationException(
                "Symbolic links are not allowed in import/export paths".to_owned(),
            ));
        }
        // Jail check: canonicalize parent and verify it's under at least one root.
        if !jail_roots.is_empty() {
            let canonical_parent = parent.canonicalize().map_err(|_| {
                DynamoDbError::ValidationException(
                    "Parent directory does not exist or is not accessible".to_owned(),
                )
            })?;
            if !jail_roots
                .iter()
                .any(|root| canonical_parent.starts_with(root.as_path()))
            {
                return Err(DynamoDbError::ValidationException(
                    "Path must resolve under one of the configured allowed paths".to_owned(),
                ));
            }
        }
    }

    Ok(path.to_path_buf())
}
