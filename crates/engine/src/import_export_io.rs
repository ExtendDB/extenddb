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

/// Read `DynamoDB` JSON format: one JSON object per line with `{"Item": {...}}` wrapper.
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
        .map_or(",", |c| c.delimiter.as_str());
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

/// Derive the per-account subtree of every configured root.
///
/// Import/export files are namespaced by account: a caller in account `A` may
/// only read and write beneath `<root>/A` for each configured root. Containment
/// in the bare root is not sufficient, because a single root shared by several
/// tenants would otherwise let any tenant name another tenant's file.
///
/// The account id is used as a single path component. It is supplied by the
/// authenticated identity rather than the request (see `OperationContext`), and
/// account ids are validated on ingress, but the separator check below is kept
/// as a defence in depth so a malformed id can never widen the subtree.
pub(crate) fn account_scoped_roots(
    jail_roots: &[Arc<PathBuf>],
    account_id: &str,
) -> Result<Vec<PathBuf>, DynamoDbError> {
    if account_id.is_empty()
        || account_id.contains('/')
        || account_id.contains('\\')
        || account_id == "."
        || account_id == ".."
    {
        return Err(DynamoDbError::ValidationException(
            "Cannot resolve an account-scoped import/export path".to_owned(),
        ));
    }
    Ok(jail_roots
        .iter()
        .map(|root| root.join(account_id))
        .collect())
}

/// Create the calling account's subtree beneath each configured root.
///
/// Export writes to a file that does not exist yet, so the account subtree has
/// to exist before the path can be canonicalized and jailed. Creating it is
/// idempotent and cheap (one call per configured root), and it keeps the first
/// export for a new account from failing on a missing parent directory.
pub(crate) fn ensure_account_dirs(
    jail_roots: &[Arc<PathBuf>],
    account_id: &str,
) -> Result<(), DynamoDbError> {
    for scoped in account_scoped_roots(jail_roots, account_id)? {
        std::fs::create_dir_all(&scoped).map_err(|_| {
            DynamoDbError::ValidationException(
                "Cannot create the account export directory".to_owned(),
            )
        })?;
    }
    Ok(())
}

/// Whether `candidate` lies within the caller's subtree of some configured root.
///
/// The single containment predicate for import/export. `Path::starts_with` is
/// component-wise rather than a string prefix, so `<root>/1111` does not match a
/// caller scoped to `<root>/111`.
fn is_under_scoped_root(
    candidate: &Path,
    jail_roots: &[Arc<PathBuf>],
    account_id: &str,
) -> Result<bool, DynamoDbError> {
    let scoped = account_scoped_roots(jail_roots, account_id)?;
    Ok(scoped.iter().any(|root| candidate.starts_with(root)))
}

/// Make `path` absolute without touching the filesystem, for a containment check
/// that must not reveal whether the path exists.
fn absolutize(path: &Path) -> Result<PathBuf, DynamoDbError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let cwd = std::env::current_dir()
        .map_err(|_| {
            DynamoDbError::ValidationException("Cannot determine current directory".to_owned())
        })?
        .canonicalize()
        .map_err(|_| {
            DynamoDbError::ValidationException("Cannot resolve current directory".to_owned())
        })?;
    Ok(cwd.join(path))
}

/// Reject a path that does not lie within the caller's subtree of some root.
///
/// This runs *before* any existence check, so a path belonging to another
/// account answers identically whether or not the file is there. Deciding the
/// jail after canonicalization would turn the error into an oracle for other
/// tenants' filenames. `..` is rejected by the caller, so a lexical prefix test
/// is sound here; the post-canonicalization check still catches symlink escapes.
fn reject_outside_account_subtree(
    path: &Path,
    jail_roots: &[Arc<PathBuf>],
    account_id: &str,
) -> Result<(), DynamoDbError> {
    if jail_roots.is_empty() {
        return Ok(());
    }
    if is_under_scoped_root(&absolutize(path)?, jail_roots, account_id)? {
        return Ok(());
    }
    Err(DynamoDbError::ValidationException(
        "Path must resolve under one of the configured allowed paths".to_owned(),
    ))
}

/// Validate and canonicalize a filesystem path for import/export.
///
/// Rejects symlinks and paths with `..` components to prevent path traversal.
/// When `jail_roots` is non-empty, the path must resolve under the calling
/// account's subtree of at least one allowed root.
pub(crate) fn validate_path(
    raw: &str,
    jail_roots: &[Arc<PathBuf>],
    account_id: &str,
) -> Result<PathBuf, DynamoDbError> {
    let path = Path::new(raw);

    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(DynamoDbError::ValidationException(
                "Path must not contain '..' components".to_owned(),
            ));
        }
    }

    // Decided before any filesystem access, so a path in another account's
    // subtree cannot be distinguished from one that does not exist.
    reject_outside_account_subtree(path, jail_roots, account_id)?;

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

    // Re-checked after canonicalization: the lexical test above cannot see a
    // symlinked ancestor that redirects out of the subtree.
    if !jail_roots.is_empty() && !is_under_scoped_root(&canonical, jail_roots, account_id)? {
        return Err(DynamoDbError::ValidationException(
            "Path must resolve under one of the configured allowed paths".to_owned(),
        ));
    }

    Ok(canonical)
}

/// Validate an export output path. The file may not exist yet, so we validate
/// the parent directory instead.
/// When `jail_roots` is non-empty, the resolved path must be under the calling
/// account's subtree of at least one allowed root.
pub(crate) fn validate_path_parent(
    raw: &str,
    jail_roots: &[Arc<PathBuf>],
    account_id: &str,
) -> Result<PathBuf, DynamoDbError> {
    let path = Path::new(raw);

    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(DynamoDbError::ValidationException(
                "Path must not contain '..' components".to_owned(),
            ));
        }
    }

    // Decided before any filesystem access, so a path in another account's
    // subtree cannot be distinguished from one that does not exist.
    reject_outside_account_subtree(path, jail_roots, account_id)?;

    // The final component is checked before the parent: a symlink pre-planted at
    // the target filename inside an allowed directory would otherwise be followed
    // on write, escaping the jail. `create_new` on the write itself also refuses
    // an existing symlink, but checking here gives the caller a precise error.
    if let Ok(meta) = std::fs::symlink_metadata(path)
        && meta.file_type().is_symlink()
    {
        return Err(DynamoDbError::ValidationException(
            "Symbolic links are not allowed in import/export paths".to_owned(),
        ));
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
                if !is_under_scoped_root(&resolved, jail_roots, account_id)? {
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
        // Jail check: canonicalize parent and verify it's under the calling
        // account's subtree of at least one root.
        if !jail_roots.is_empty() {
            let canonical_parent = parent.canonicalize().map_err(|_| {
                DynamoDbError::ValidationException(
                    "Parent directory does not exist or is not accessible".to_owned(),
                )
            })?;
            if !is_under_scoped_root(&canonical_parent, jail_roots, account_id)? {
                return Err(DynamoDbError::ValidationException(
                    "Path must resolve under one of the configured allowed paths".to_owned(),
                ));
            }
        }
    }

    Ok(path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "111122223333";
    const OTHER: &str = "999999999999";
    /// An account whose subtree is never created on disk.
    const FOREIGN_UNSEEN: &str = "888811112222";

    /// A fresh directory acting as a configured import/export root.
    struct Root(PathBuf);

    impl Root {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("eb-ie-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir.canonicalize().unwrap())
        }

        fn roots(&self) -> Vec<Arc<PathBuf>> {
            vec![Arc::new(self.0.clone())]
        }

        /// Create `<root>/<account>/<name>` with content and return its path.
        fn file_for(&self, account: &str, name: &str) -> PathBuf {
            let dir = self.0.join(account);
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join(name);
            std::fs::write(&path, b"{}\n").unwrap();
            path
        }

        /// Create `<root>/<name>` directly under the bare root.
        fn file_at_root(&self, name: &str) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, b"{}\n").unwrap();
            path
        }
    }

    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A file outside every configured root, removed even if an assert panics.
    struct Stray(PathBuf);

    impl Stray {
        fn new(contents: &[u8]) -> Self {
            let path = std::env::temp_dir().join(format!("eb-stray-{}.json", uuid::Uuid::new_v4()));
            std::fs::write(&path, contents).unwrap();
            Self(path)
        }

        fn as_str(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for Stray {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn message(err: &DynamoDbError) -> String {
        match err {
            DynamoDbError::ValidationException(m) => m.clone(),
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn import_from_own_account_subtree_is_accepted() {
        let root = Root::new();
        let path = root.file_for(OWNER, "mine.json");
        let got = validate_path(path.to_str().unwrap(), &root.roots(), OWNER).unwrap();
        assert_eq!(got, path.canonicalize().unwrap());
    }

    /// The reported read primitive. The victim's file genuinely exists, so a
    /// rejection here can only come from the account scoping and not from the
    /// path being absent.
    #[test]
    fn import_from_another_accounts_subtree_is_rejected() {
        let root = Root::new();
        let victim = root.file_for(OTHER, "secrets.json");
        assert!(
            victim.exists(),
            "victim file must exist for this to discriminate"
        );

        let err = validate_path(victim.to_str().unwrap(), &root.roots(), OWNER)
            .expect_err("reading another account's export must be refused");
        assert_eq!(
            message(&err),
            "Path must resolve under one of the configured allowed paths"
        );
    }

    /// Containment in the bare root is deliberately not sufficient: that is the
    /// condition that made a shared root a cross-tenant channel.
    #[test]
    fn import_from_the_bare_root_is_rejected() {
        let root = Root::new();
        let stray = root.file_at_root("loose.json");
        let err = validate_path(stray.to_str().unwrap(), &root.roots(), OWNER)
            .expect_err("a path directly under the root is not account-scoped");
        assert_eq!(
            message(&err),
            "Path must resolve under one of the configured allowed paths"
        );
    }

    #[test]
    fn export_into_own_account_subtree_is_accepted() {
        let root = Root::new();
        ensure_account_dirs(&root.roots(), OWNER).unwrap();
        let target = root.0.join(OWNER).join("out.json");
        validate_path_parent(target.to_str().unwrap(), &root.roots(), OWNER).unwrap();
    }

    /// The reported destruction primitive: an export naming a path inside
    /// another account's subtree must be refused before any file is opened.
    #[test]
    fn export_into_another_accounts_subtree_is_rejected() {
        let root = Root::new();
        let victim = root.file_for(OTHER, "backup.json");
        let err = validate_path_parent(victim.to_str().unwrap(), &root.roots(), OWNER)
            .expect_err("writing into another account's subtree must be refused");
        assert_eq!(
            message(&err),
            "Path must resolve under one of the configured allowed paths"
        );
        // The victim's file is untouched: refusal happens before the write.
        assert_eq!(std::fs::read(&victim).unwrap(), b"{}\n");
    }

    /// "Not yours" and "not under any root at all" answer identically, so the
    /// scoping does not itself become a probe for other tenants' directories.
    #[test]
    fn foreign_subtree_and_outside_jail_are_indistinguishable() {
        let root = Root::new();
        let foreign = root.file_for(OTHER, "a.json");
        let outside = Stray::new(b"{}\n");

        let a = validate_path(foreign.to_str().unwrap(), &root.roots(), OWNER).unwrap_err();
        let b = validate_path(outside.as_str(), &root.roots(), OWNER).unwrap_err();
        assert_eq!(message(&a), message(&b));
    }

    /// A symlink planted at the export filename inside the caller's own subtree
    /// is refused rather than followed.
    #[cfg(unix)]
    #[test]
    fn export_onto_a_symlink_is_rejected() {
        let root = Root::new();
        ensure_account_dirs(&root.roots(), OWNER).unwrap();
        let outside = Stray::new(b"original\n");
        let link = root.0.join(OWNER).join("link.json");
        std::os::unix::fs::symlink(&outside.0, &link).unwrap();

        let err = validate_path_parent(link.to_str().unwrap(), &root.roots(), OWNER)
            .expect_err("a symlink at the target filename must be refused");
        assert_eq!(
            message(&err),
            "Symbolic links are not allowed in import/export paths"
        );
        assert_eq!(std::fs::read(&outside.0).unwrap(), b"original\n");
    }

    /// Defence in depth: a malformed account id must never widen the subtree.
    #[test]
    fn account_id_with_a_separator_cannot_widen_the_subtree() {
        let root = Root::new();
        for bad in ["", "../999999999999", "a/b", ".", ".."] {
            let err = account_scoped_roots(&root.roots(), bad)
                .expect_err("account id {bad} must not resolve to a path");
            assert_eq!(
                message(&err),
                "Cannot resolve an account-scoped import/export path"
            );
        }
    }

    /// The jail verdict must not depend on whether the foreign file exists, or
    /// the error becomes an oracle for other tenants' filenames.
    #[test]
    fn foreign_path_answers_the_same_whether_or_not_it_exists() {
        let root = Root::new();
        let present = root.file_for(OTHER, "present.json");
        let absent = root.0.join(OTHER).join("absent.json");
        assert!(present.exists());
        assert!(!absent.exists());

        let a = validate_path(present.to_str().unwrap(), &root.roots(), OWNER).unwrap_err();
        let b = validate_path(absent.to_str().unwrap(), &root.roots(), OWNER).unwrap_err();
        assert_eq!(
            message(&a),
            message(&b),
            "import path leaks foreign existence"
        );
    }

    /// Same requirement on the export side, where the leak would be of another
    /// tenant's directory rather than its files.
    #[test]
    fn foreign_export_target_answers_the_same_whether_or_not_it_exists() {
        let root = Root::new();
        let present = root.file_for(OTHER, "present.json");
        let absent = root.0.join(FOREIGN_UNSEEN).join("absent.json");

        let a = validate_path_parent(present.to_str().unwrap(), &root.roots(), OWNER).unwrap_err();
        let b = validate_path_parent(absent.to_str().unwrap(), &root.roots(), OWNER).unwrap_err();
        assert_eq!(
            message(&a),
            message(&b),
            "export path leaks foreign existence"
        );
    }

    /// A relative path is resolved against the working directory before the
    /// containment test, so it cannot sidestep the account subtree.
    #[test]
    fn relative_path_is_resolved_before_the_containment_test() {
        let root = Root::new();
        let err = validate_path("relative-name.json", &root.roots(), OWNER)
            .expect_err("a bare relative name is not inside the account subtree");
        assert_eq!(
            message(&err),
            "Path must resolve under one of the configured allowed paths"
        );

        // And a relative path that does resolve into the subtree is accepted,
        // so the branch is proven in both directions rather than just refusing.
        let cwd = std::env::current_dir().unwrap().canonicalize().unwrap();
        let nested = cwd.join(format!("eb-rel-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(nested.join(OWNER)).unwrap();
        let file = nested.join(OWNER).join("in.json");
        std::fs::write(&file, b"{}\n").unwrap();
        let roots = vec![Arc::new(nested.clone())];
        let relative = file
            .strip_prefix(&cwd)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(!relative.starts_with('/'), "must be a relative path");
        validate_path(&relative, &roots, OWNER).unwrap();
        let _ = std::fs::remove_dir_all(&nested);
    }

    #[test]
    fn account_scoped_roots_appends_one_component_per_root() {
        let a = Arc::new(PathBuf::from("/srv/imports"));
        let b = Arc::new(PathBuf::from("/mnt/data"));
        let got = account_scoped_roots(&[a, b], OWNER).unwrap();
        assert_eq!(
            got,
            vec![
                PathBuf::from("/srv/imports/111122223333"),
                PathBuf::from("/mnt/data/111122223333"),
            ]
        );
    }
}
