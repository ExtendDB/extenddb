// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Input file enumeration: the bounded, symlink-safe directory walk
//! that lists regular files beneath a bucket in lexical order.

#[cfg(not(unix))]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::import_export_io::account_scoped_roots;

use super::jail::{BucketDirState, bucket_dir_state, check_component};
use super::*;

pub(super) fn list_blocking(
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
    prefix: &str,
    max_files: u64,
) -> Result<Vec<SourceFile>, SourceError> {
    check_component(bucket).map_err(SourceError::Jail)?;
    let scoped =
        account_scoped_roots(roots, account).map_err(|e| SourceError::Jail(e.to_string()))?;
    for scoped_root in &scoped {
        // The same handle walk the open and render paths use: the verified
        // account handle, then the bucket component. The handle the state
        // carries is the one the walk descends from, so nothing is
        // resolved by path between the check and the descent.
        let bucket_dir = match bucket_dir_state(scoped_root, roots, account, bucket)? {
            BucketDirState::Missing => continue,
            BucketDirState::Symlink => {
                return Err(SourceError::Jail(
                    "bucket directory is a symlink".to_owned(),
                ));
            }
            // A non-directory at the bucket name has no files beneath it.
            BucketDirState::NotADirectory => return Ok(Vec::new()),
            BucketDirState::Directory(dir) => dir,
        };
        let mut files = Vec::new();
        let mut bounds = WalkBounds {
            max_files,
            visit_ceiling: max_files.saturating_mul(4),
            visited: 0,
        };
        match walk_regular_files(&bucket_dir, "", prefix, 0, &mut bounds, &mut files) {
            Ok(()) => {}
            // The walk collected one match past `max_files` and stopped:
            // the caller sees `max_files + 1` entries and knows the count
            // quota is passed, without the walk reading the rest of the
            // tree. The visit ceiling, which stops the walk with fewer
            // matches than that, stays an error.
            Err(SourceError::TooManyFiles)
                if u64::try_from(files.len()).unwrap_or(u64::MAX) > max_files => {}
            Err(error) => return Err(error),
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        return Ok(files);
    }
    Ok(Vec::new())
}

/// Deepest directory nesting the enumeration walk descends, counted from
/// the bucket directory. The recursion runs on a blocking thread whose
/// stack is finite and each level holds one open directory handle, so the
/// depth is capped explicitly rather than left to `PATH_MAX` or the file
/// descriptor limit. Bucket-relative paths deeper than this are not
/// enumerable; the walk refuses rather than skipping them silently.
pub(super) const MAX_WALK_DEPTH: usize = 256;

/// Bounds on one enumeration walk, derived from the caller's file limit.
/// `visited` counts every directory entry that can hold a key under the
/// prefix, matching or not, so the ceiling holds however the names beneath
/// the prefix are arranged; entries beside the prefix are not counted.
pub(super) struct WalkBounds {
    /// Collecting more than this many matches ends the walk.
    pub(super) max_files: u64,
    /// Visiting more than this many directory entries that can hold a key
    /// under the prefix ends the walk.
    pub(super) visit_ceiling: u64,
    /// Such entries seen so far, across the whole walk.
    pub(super) visited: u64,
}

/// Whether the entry at bucket-relative path `rel` can hold a key under
/// `prefix` (byte-wise, no decoding, as with the file rule): its own path
/// starts with the prefix, or the prefix runs on below it, so the entry is a
/// directory on the way to the matches. No path beneath any other entry
/// starts with the prefix.
fn may_hold_a_key(rel: &str, prefix: &str) -> bool {
    let (rel, prefix) = (rel.as_bytes(), prefix.as_bytes());
    rel.starts_with(prefix)
        || (prefix.len() > rel.len() && prefix.starts_with(rel) && prefix[rel.len()] == b'/')
}

/// Collect regular files beneath the directory handle `dir`, recording
/// paths relative to the bucket directory (`rel_dir` is the walked-so-far
/// relative path, empty at the bucket itself). Entries are read from the
/// handle with `rustix::fs::Dir::read_from` and each subdirectory is
/// opened from its parent's handle with `openat(O_DIRECTORY | O_NOFOLLOW)`,
/// so there is no interval between examining a component and descending
/// into it in which a swapped symlink could be followed. A symlinked file
/// is not included and a symlinked directory is not descended into, so
/// nothing outside the account subtree can be reached however the links
/// are arranged.
///
/// The walk is bounded while it runs, not only checked afterwards. An entry
/// is examined only when it can hold a key under the prefix: every path
/// below a directory begins with its own relative path plus `/`, so an
/// entry is kept only when its path starts with `prefix` or `prefix` starts
/// with its path plus `/` (byte-wise, no decoding, as with the file rule).
/// Every other entry is passed over by name, before its `stat`, without
/// counting toward the ceiling, and no directory among them is opened. The
/// walk ends with `TooManyFiles` the moment the collected matches exceed
/// `bounds.max_files` (the match that passed the bound is kept, so `out`
/// holds `max_files + 1` entries) or the entries kept exceed
/// `bounds.visit_ceiling`, so an oversized tree under the prefix costs at
/// most the ceiling in entries examined and the limit plus one in entries
/// collected.
#[cfg(unix)]
pub(super) fn walk_regular_files(
    dir: &std::fs::File,
    rel_dir: &str,
    prefix: &str,
    depth: usize,
    bounds: &mut WalkBounds,
    out: &mut Vec<SourceFile>,
) -> Result<(), SourceError> {
    use rustix::fs::{AtFlags, FileType, Mode, OFlags, openat, statat};
    if depth > MAX_WALK_DEPTH {
        return Err(SourceError::Jail(format!(
            "input directory nesting exceeds {MAX_WALK_DEPTH} levels"
        )));
    }
    // Entry names are streamed from the handle and kept when they can hold
    // a key under the prefix. The stream is dropped before any descent so
    // the recursion holds one handle per level, as the path walk did. Each
    // kept entry is counted against the visit ceiling when the second pass
    // examines it, after the matches before it, so a directory of matches
    // ends the walk at the limit plus one, as a listing does, before it
    // reaches the ceiling. The first pass keeps at most one name past the
    // ceiling's remaining budget: the second pass ends the walk by then.
    let budget = bounds
        .visit_ceiling
        .saturating_sub(bounds.visited)
        .saturating_add(1);
    let mut names = Vec::new();
    {
        let entries = rustix::fs::Dir::read_from(dir).map_err(|e| SourceError::Io(e.into()))?;
        for entry in entries {
            let entry = entry.map_err(|e| SourceError::Io(e.into()))?;
            // The handle stream yields the directory itself and its
            // parent, which a path-based listing never shows; neither is
            // an entry.
            let name = entry.file_name();
            if name == c"." || name == c".." {
                continue;
            }
            // Input paths are stored as UTF-8 strings on the operation
            // record; a name that cannot round-trip is not enumerated.
            let Ok(name) = name.to_str() else {
                continue;
            };
            let rel = if rel_dir.is_empty() {
                name.to_owned()
            } else {
                format!("{rel_dir}/{name}")
            };
            // An entry that can hold no key under the prefix is passed
            // over before its stat and is not counted, however many of
            // them the directory holds.
            if !may_hold_a_key(&rel, prefix) {
                continue;
            }
            names.push((name.to_owned(), rel));
            if u64::try_from(names.len()).unwrap_or(u64::MAX) >= budget {
                break;
            }
        }
    }
    for (name, rel) in names {
        bounds.visited = bounds.visited.saturating_add(1);
        if bounds.visited > bounds.visit_ceiling {
            return Err(SourceError::TooManyFiles);
        }
        let stat = statat(dir, name.as_str(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|e| SourceError::Io(e.into()))?;
        let file_type = FileType::from_raw_mode(stat.st_mode);
        if file_type == FileType::Symlink {
            continue;
        }
        if file_type == FileType::Directory {
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let child = match openat(dir, name.as_str(), flags, Mode::empty()) {
                Ok(fd) => std::fs::File::from(fd),
                // The entry changed between the stat and the open: a
                // symlink swapped in fails with LOOP and is skipped
                // exactly as a symlink seen at the stat would be, and
                // an entry removed or replaced is not enumerated.
                Err(e)
                    if e == rustix::io::Errno::NOENT
                        || e == rustix::io::Errno::LOOP
                        || e == rustix::io::Errno::NOTDIR =>
                {
                    continue;
                }
                Err(e) => return Err(SourceError::Io(e.into())),
            };
            walk_regular_files(&child, &rel, prefix, depth + 1, bounds, out)?;
            continue;
        }
        if file_type != FileType::RegularFile {
            // FIFOs, sockets, and devices are not input files.
            continue;
        }
        if rel.as_bytes().starts_with(prefix.as_bytes()) {
            out.push(SourceFile {
                path: rel,
                size: u64::try_from(stat.st_size).unwrap_or(0),
                etag: None,
            });
            if u64::try_from(out.len()).unwrap_or(u64::MAX) > bounds.max_files {
                return Err(SourceError::TooManyFiles);
            }
        }
    }
    Ok(())
}

/// The non-unix fallback walk resolves by path with `symlink_metadata`,
/// without the handle-relative hardening; the request handler refuses the
/// API on these targets. Bounds and prefix rules match the unix walk.
#[cfg(not(unix))]
pub(super) fn walk_regular_files(
    dir: &Path,
    rel_dir: &str,
    prefix: &str,
    depth: usize,
    bounds: &mut WalkBounds,
    out: &mut Vec<SourceFile>,
) -> Result<(), SourceError> {
    if depth > MAX_WALK_DEPTH {
        return Err(SourceError::Jail(format!(
            "input directory nesting exceeds {MAX_WALK_DEPTH} levels"
        )));
    }
    let entries = std::fs::read_dir(dir).map_err(SourceError::Io)?;
    for entry in entries {
        let entry = entry.map_err(SourceError::Io)?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            // Input paths are stored as UTF-8 strings on the operation
            // record; a name that cannot round-trip is not enumerated.
            continue;
        };
        let rel = if rel_dir.is_empty() {
            name.to_owned()
        } else {
            format!("{rel_dir}/{name}")
        };
        if !may_hold_a_key(&rel, prefix) {
            continue;
        }
        bounds.visited = bounds.visited.saturating_add(1);
        if bounds.visited > bounds.visit_ceiling {
            return Err(SourceError::TooManyFiles);
        }
        let meta = std::fs::symlink_metadata(&path).map_err(SourceError::Io)?;
        let file_type = meta.file_type();
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            walk_regular_files(&path, &rel, prefix, depth + 1, bounds, out)?;
            continue;
        }
        if !file_type.is_file() {
            // FIFOs, sockets, and devices are not input files.
            continue;
        }
        if rel.as_bytes().starts_with(prefix.as_bytes()) {
            out.push(SourceFile {
                path: rel,
                size: meta.len(),
                etag: None,
            });
            if u64::try_from(out.len()).unwrap_or(u64::MAX) > bounds.max_files {
                return Err(SourceError::TooManyFiles);
            }
        }
    }
    Ok(())
}
