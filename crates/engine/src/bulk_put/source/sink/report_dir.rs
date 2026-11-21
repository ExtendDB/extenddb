// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The report directory handle: the result of the write-side component
//! walk, and the handle-relative create, unlink, and rename primitives
//! every report and manifest write runs through.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::bulk_put::source::SinkError;
#[cfg(unix)]
use crate::bulk_put::source::jail::{
    AccountDirError, open_or_create_report_component, open_report_component,
    open_verified_account_dir,
};

/// The directory handle a report write runs against, produced by the
/// component walk from the verified account handle (rendering steps 2 and
/// 3): one `openat(O_DIRECTORY | O_NOFOLLOW)` per component, `mkdirat` on
/// the parent's handle when the component does not exist, and the same
/// open again after creating it. A component that is a symlink fails its
/// own `openat`, wherever it points, so no path is resolved again after
/// the account root and a component swapped for a symlink at any point
/// fails instead of redirecting the write. Every later create, unlink, and
/// rename is performed relative to the held handle.
#[cfg(unix)]
pub(crate) struct ReportDirectory {
    handle: std::fs::File,
}

#[cfg(unix)]
impl ReportDirectory {
    pub(crate) fn establish(
        base: &Path,
        roots: &[Arc<PathBuf>],
        account: &str,
        components: &[String],
    ) -> Result<Self, SinkError> {
        let account_dir = match open_verified_account_dir(base, roots, account) {
            Ok(Some(handle)) => handle,
            // The caller chose or created the base just before this call,
            // so a missing directory means it was removed underneath the
            // operation.
            Ok(None) => {
                return Err(SinkError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "account export directory does not exist",
                )));
            }
            Err(AccountDirError::Jail(message)) => return Err(SinkError::Jail(message)),
            Err(AccountDirError::Io(e)) => return Err(SinkError::Io(e)),
        };
        let mut cursor = account_dir;
        for (index, component) in components.iter().enumerate() {
            // The first component is the report bucket directory: the
            // caller stages it, the render only opens it. Start already
            // failed the operation when it was missing, so reaching a
            // missing bucket here means it was removed while the operation
            // ran, and the render fails rather than recreating a directory
            // the caller owns. Everything beneath the bucket is the
            // report tree the render creates as needed.
            cursor = if index == 0 {
                open_report_component(&cursor, component)?
            } else {
                open_or_create_report_component(&cursor, component)?
            };
        }
        Ok(Self { handle: cursor })
    }

    pub(crate) fn remove_leftover(&self, name: &str) -> Result<(), SinkError> {
        match rustix::fs::unlinkat(&self.handle, name, rustix::fs::AtFlags::empty()) {
            Ok(()) => Ok(()),
            Err(e) if e == rustix::io::Errno::NOENT => Ok(()),
            Err(e) => Err(SinkError::Io(e.into())),
        }
    }

    /// Unlink every entry whose name starts with `prefix`, through the
    /// held handle.
    pub(crate) fn remove_stale_temps(&self, prefix: &str) -> Result<(), SinkError> {
        self.remove_matching(&|name| name.starts_with(prefix))
    }

    /// Unlink every entry whose name `matches`, through the held handle.
    /// Names are collected before any unlink so the sweep never mutates
    /// the directory stream it is reading; an entry another sweep removed
    /// first is fine.
    pub(crate) fn remove_matching(&self, matches: &dyn Fn(&str) -> bool) -> Result<(), SinkError> {
        let dir = rustix::fs::Dir::read_from(&self.handle).map_err(|e| SinkError::Io(e.into()))?;
        let mut stale = Vec::new();
        for entry in dir {
            let entry = entry.map_err(|e| SinkError::Io(e.into()))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if matches(&name) {
                stale.push(name);
            }
        }
        for name in stale {
            self.remove_leftover(&name)?;
        }
        Ok(())
    }

    /// Create `name` in the report directory, failing when anything is
    /// already there. Report objects and the manifest hold item values, so
    /// the file is readable and writable by the server's own user only.
    pub(crate) fn create_new(&self, name: &str) -> Result<std::fs::File, SinkError> {
        use rustix::fs::{Mode, OFlags};
        let fd = rustix::fs::openat(
            &self.handle,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )
        .map_err(|e| SinkError::Io(e.into()))?;
        Ok(std::fs::File::from(fd))
    }

    pub(crate) fn rename(&self, from: &str, to: &str) -> Result<(), SinkError> {
        rustix::fs::renameat(&self.handle, from, &self.handle, to)
            .map_err(|e| SinkError::Io(e.into()))
    }
}

/// The bulk report layout is POSIX-only; on other platforms the writer
/// falls back to path-based operations without the handle-relative
/// hardening, and the request handler refuses the API.
#[cfg(not(unix))]
pub(crate) struct ReportDirectory {
    path: PathBuf,
}

#[cfg(not(unix))]
impl ReportDirectory {
    pub(crate) fn establish(
        base: &Path,
        _roots: &[Arc<PathBuf>],
        _account: &str,
        components: &[String],
    ) -> Result<Self, SinkError> {
        let mut path = base.to_path_buf();
        let Some((bucket, rest)) = components.split_first() else {
            return Err(SinkError::Jail("report path has no components".to_owned()));
        };
        // The bucket directory is the caller's; the render opens it and
        // creates only the report tree beneath it, mirroring the unix walk.
        path.push(bucket);
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(SinkError::Symlink(
                    "report bucket is a symlink or not a directory".to_owned(),
                ));
            }
            Err(e) => return Err(SinkError::Io(e)),
        }
        for component in rest {
            path.push(component);
        }
        std::fs::create_dir_all(&path).map_err(SinkError::Io)?;
        Ok(Self { path })
    }

    pub(crate) fn remove_leftover(&self, name: &str) -> Result<(), SinkError> {
        match std::fs::remove_file(self.path.join(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(SinkError::Io(e)),
        }
    }

    /// Remove every entry whose name starts with `prefix`.
    pub(crate) fn remove_stale_temps(&self, prefix: &str) -> Result<(), SinkError> {
        self.remove_matching(&|name| name.starts_with(prefix))
    }

    /// Remove every entry whose name `matches`. Names are collected before
    /// any removal; an entry another sweep removed first is fine.
    pub(crate) fn remove_matching(&self, matches: &dyn Fn(&str) -> bool) -> Result<(), SinkError> {
        let mut stale = Vec::new();
        for entry in std::fs::read_dir(&self.path).map_err(SinkError::Io)? {
            let entry = entry.map_err(SinkError::Io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if matches(&name) {
                stale.push(name);
            }
        }
        for name in stale {
            self.remove_leftover(&name)?;
        }
        Ok(())
    }

    pub(crate) fn create_new(&self, name: &str) -> Result<std::fs::File, SinkError> {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path.join(name))
            .map_err(SinkError::Io)
    }

    pub(crate) fn rename(&self, from: &str, to: &str) -> Result<(), SinkError> {
        std::fs::rename(self.path.join(from), self.path.join(to)).map_err(SinkError::Io)
    }
}
