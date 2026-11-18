// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The key-encryption key (KEK) file: the file rule that `serve` and every
//! `extenddb encryption-key` command apply before they read the key, and the
//! file's format.
//!
//! The file rule, in order:
//!
//! 1. The path resolves to its canonical form, so a Kubernetes secret volume's
//!    symlink is judged by its target.
//! 2. Every parent directory of the canonical path passes the backup store's
//!    ancestor check: owned by root or the effective user, and not writable by
//!    group or others unless sticky.
//! 3. The canonical path opens with `O_NOFOLLOW`, and the checks run on the
//!    descriptor's `fstat`, so the file cannot be swapped between the check
//!    and the read. It is a regular file that root or the effective user
//!    owns, and others have no permission bits.
//! 4. On Linux the POSIX ACL of the descriptor is read before the group bits
//!    are judged, because with a mask entry the group bits of `st_mode` are
//!    the mask (acl(5)). Each named user entry names the effective user, each
//!    named group entry names the effective group, and the entry for others
//!    grants nothing. Group access (the mode's group bits without a mask, the
//!    owning group's entry under one, a named group entry) is accepted only
//!    on a file that root owns, and only for the effective group: the rule
//!    cannot see the group's other members. Other platforms check the owner
//!    and the mode only ([`ACL_NOT_READ`]).
//!
//! The file's format is the key ring's ([`parse_kek_file`]): the 32-byte key
//! in standard base64 with padding, 44 characters, and at most one trailing
//! newline. Every buffer that holds the file's bytes or the key is wiped when
//! it drops, and no message carries a byte of the file. The key is decoded
//! into one heap allocation ([`Kek`]), so a move of it leaves no copy.

// The ACL parser runs on Linux only, and the descriptor checks on Unix only;
// the tests run every part on every platform.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::path::Path;

use extenddb_storage::key_ring::{Kek, parse_kek_file};
use extenddb_storage::secret_cipher::KEY_LEN;
use zeroize::Zeroizing;

/// The startup log line on a platform where the file rule reads no ACL, or
/// `None` on Linux, where it does.
pub const ACL_NOT_READ: Option<&str> = if cfg!(target_os = "linux") {
    None
} else {
    Some(
        "The KEK file check reads the file's owner and mode only on this platform; an access \
         control list on the file is not checked.",
    )
};

/// A KEK file at a configured path, read through the file rule each time the
/// commands ask for it ([`read_key_file`]).
#[derive(Debug, Clone)]
pub struct KeyFile(pub String);

impl extenddb_storage::key_commands::KekFile for KeyFile {
    fn path(&self) -> &str {
        &self.0
    }

    fn read(&self) -> Result<Zeroizing<[u8; KEY_LEN]>, String> {
        read_key_file(&self.0).map(|kek| Zeroizing::new(**kek))
    }
}

use extenddb_auth::secmem;
pub use extenddb_auth::secmem::Protection;

/// The keys that the startup loader can hold at once (RFC-0000 section 7): the
/// two DEKs of a ring with an unfinished replacement, which it unwraps before
/// it reads the other fields, and the catalog key. The DEK the server keeps is
/// one of them.
const STARTUP_KEY_SLOTS: usize = 3;

/// The locked key store's slots for `serve`: two signing keys per entry of
/// the credential cache (`[auth.cache] max_entries`), the keys of startup
/// ([`STARTUP_KEY_SLOTS`]), and one temporary buffer per runtime worker
/// thread.
#[must_use]
pub fn serve_slots(max_entries: u64, workers: usize) -> usize {
    usize::try_from(max_entries)
        .unwrap_or(usize::MAX)
        .saturating_mul(2)
        .saturating_add(STARTUP_KEY_SLOTS)
        .saturating_add(workers)
}

/// The locked key store's slots for an `encryption-key` command, which
/// caches no credential: one 4 KiB page of 32-byte slots, which the store
/// locks whole for any smaller count, and more than a run holds at once (the
/// catalog key, the two DEKs of a ring, a new DEK, and their copies inside a
/// step).
pub const COMMAND_SLOTS: usize = 128;

/// Set the process's locked key store up with `slots` slots, as the last
/// rows of RFC-0000 section 7 say: when locking fails, refuse while a KEK is
/// in use (`key_file` set, or a command's `--key-file`) and
/// `require_locked_memory` is true; otherwise log a warning and keep the
/// keys in memory that is not locked. Returns the protection in force, the
/// word of the startup log line.
///
/// # Errors
///
/// The refusal, with the memory the store needs and the fixes.
pub fn init_key_store(
    kek_in_use: bool,
    require_locked_memory: bool,
    slots: usize,
) -> Result<Protection, String> {
    match secmem::init(slots) {
        Ok(protection) => Ok(protection),
        Err(error) => {
            let warning = lock_failure(kek_in_use, require_locked_memory, &error)?;
            tracing::warn!("{warning}");
            Ok(secmem::init_unlocked(slots))
        }
    }
}

/// The warning for a store that could not lock, or the refusal when a KEK is
/// in use and locked memory is required.
fn lock_failure(
    kek_in_use: bool,
    require_locked_memory: bool,
    error: &dyn std::fmt::Display,
) -> Result<String, String> {
    if kek_in_use && require_locked_memory {
        return Err(format!(
            "{error}. With a KEK file the keys must be in locked memory; \
             [auth.encryption] require_locked_memory = false starts with them in ordinary \
             memory instead, which swap and core dumps can hold"
        ));
    }
    Ok(format!(
        "{error}. The keys are in ordinary memory, which swap and core dumps can hold"
    ))
}

/// The key a factory loads for this configuration (RFC-0000 section 7): the
/// KEK read through the file rule when `[auth.encryption] key_file` is set,
/// else the catalog key. `serve` and the backup commands pass it in
/// `ServerComponentsOptions::key_source`.
///
/// # Errors
///
/// The file rule's refusal.
pub fn key_source(
    encryption: &extenddb_config::EncryptionConfig,
) -> Result<extenddb_storage::server_components::KeySource, String> {
    use extenddb_storage::server_components::KeySource;
    let Some(path) = encryption.key_file_path() else {
        return Ok(KeySource::CatalogKey);
    };
    if let Some(note) = ACL_NOT_READ {
        tracing::info!("{note}");
    }
    read_key_file(&path).map(KeySource::Kek)
}

/// Read the KEK file at `path` through the file rule.
///
/// # Errors
///
/// The refusal, naming the path and the check that failed; on a platform
/// without POSIX ownership and modes, a refusal of every path.
pub fn read_key_file(path: &str) -> Result<Kek, String> {
    let canonical = std::fs::canonicalize(path)
        .map_err(|e| format!("KEK file {path} cannot be resolved: {e}"))?;
    let name = if canonical == Path::new(path) {
        path.to_owned()
    } else {
        format!("{path} (resolved to {})", canonical.display())
    };
    // Check 2: the backup store's ancestor check, which the backup root and
    // the restore spool use too.
    extenddb_backup_store::check_private_ancestors(&canonical, "KEK file")
        .map_err(|e| format!("KEK file {name}: {e}"))?;
    read_checked(&canonical).map_err(|reason| format!("KEK file {name} {reason}"))
}

/// Whether the files at `a` and `b` are on one file system (the same
/// `st_dev`), so that one snapshot of it holds both (RFC-0000 section 11).
/// `false` when either cannot be read, and where there is no `st_dev`.
#[must_use]
pub fn same_file_system(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        matches!(
            (std::fs::metadata(a), std::fs::metadata(b)),
            (Ok(a), Ok(b)) if a.dev() == b.dev()
        )
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        false
    }
}

/// Checks 3 and 4 and the format, on the file at the canonical path. The
/// reason starts with a verb, after the file's name.
#[cfg(unix)]
fn read_checked(canonical: &Path) -> Result<Kek, String> {
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};

    // `O_NONBLOCK` keeps the open of a FIFO from waiting for a writer and
    // `O_NOCTTY` keeps a terminal from becoming the daemon's; check 3 then
    // refuses both as not a regular file.
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_NOCTTY)
        .open(canonical)
        .map_err(|e| {
            if e.raw_os_error() == Some(libc::ELOOP) {
                "was replaced by a symlink after its path was resolved".to_owned()
            } else {
                format!("cannot be opened: {e}")
            }
        })?;
    let meta = file
        .metadata()
        .map_err(|e| format!("cannot be read: {e}"))?;
    let facts = FileFacts {
        regular: meta.file_type().is_file(),
        uid: meta.uid(),
        gid: meta.gid(),
        mode: meta.mode() & 0o7777,
    };
    let acl = read_acl(&file)?;
    judge(&facts, acl.as_deref(), effective_ids())?;

    // A KEK file holds at most 45 bytes; this many more lets the parser say
    // how long a wrong file is, up to its size.
    let mut contents = Zeroizing::new([0u8; 4096]);
    let filled =
        read_bounded(&mut file, &mut contents[..]).map_err(|e| format!("cannot be read: {e}"))?;
    parse_kek_file(&contents[..filled]).map_err(|reason| format!("is not a KEK file: {reason}"))
}

/// Read `reader` to its end, or until `buffer` is full, and return the
/// count. The buffer has a fixed size, so no reallocation leaves a copy of a
/// secret behind; the KEK file and the connection string that two commands
/// read from standard input go through it.
///
/// # Errors
///
/// The reader's error, other than an interrupted read, which is retried.
pub fn read_bounded<R: std::io::Read + ?Sized>(
    reader: &mut R,
    buffer: &mut [u8],
) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

#[cfg(not(unix))]
fn read_checked(_canonical: &Path) -> Result<Kek, String> {
    Err(
        "cannot be checked on this platform: the KEK file rule needs POSIX file ownership \
         and modes"
            .to_owned(),
    )
}

/// What checks 3 and 4 read of the descriptor's `fstat`.
#[derive(Debug, Clone, Copy)]
struct FileFacts {
    regular: bool,
    uid: u32,
    gid: u32,
    /// The permission bits, with setuid, setgid and sticky.
    mode: u32,
}

/// The effective user and group of this process.
#[derive(Debug, Clone, Copy)]
struct Ids {
    uid: u32,
    gid: u32,
}

#[cfg(unix)]
fn effective_ids() -> Ids {
    Ids {
        uid: extenddb_backup_store::effective_uid(),
        // SAFETY: getegid has no preconditions and cannot fail.
        gid: unsafe { libc::getegid() },
    }
}

/// One entry of a POSIX ACL, as the kernel stores it in the
/// `system.posix_acl_access` extended attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AclEntry {
    tag: u16,
    perm: u16,
    id: u32,
}

/// The ACL entry tags (`linux/posix_acl.h`).
const ACL_USER_OBJ: u16 = 0x01;
const ACL_USER: u16 = 0x02;
const ACL_GROUP_OBJ: u16 = 0x04;
const ACL_GROUP: u16 = 0x08;
const ACL_MASK: u16 = 0x10;
const ACL_OTHER: u16 = 0x20;

/// The version word of the attribute (`POSIX_ACL_XATTR_VERSION`).
const ACL_XATTR_VERSION: u32 = 2;

/// The entries of a `system.posix_acl_access` value: a 4-byte little-endian
/// version, then 8 bytes per entry (tag, permission, id). `None` for any
/// other layout or an unknown tag.
fn parse_acl(bytes: &[u8]) -> Option<Vec<AclEntry>> {
    let (version, entries) = bytes.split_first_chunk::<4>()?;
    if u32::from_le_bytes(*version) != ACL_XATTR_VERSION || entries.len() % 8 != 0 {
        return None;
    }
    entries
        .chunks_exact(8)
        .map(|e| {
            let entry = AclEntry {
                tag: u16::from_le_bytes([e[0], e[1]]),
                perm: u16::from_le_bytes([e[2], e[3]]),
                id: u32::from_le_bytes([e[4], e[5], e[6], e[7]]),
            };
            [
                ACL_USER_OBJ,
                ACL_USER,
                ACL_GROUP_OBJ,
                ACL_GROUP,
                ACL_MASK,
                ACL_OTHER,
            ]
            .contains(&entry.tag)
            .then_some(entry)
        })
        .collect()
}

/// The access ACL of `file`, or `None` when it has none (the file system
/// keeps no ACL, or the file's ACL is its mode).
#[cfg(target_os = "linux")]
fn read_acl(file: &std::fs::File) -> Result<Option<Vec<AclEntry>>, String> {
    use std::os::fd::AsRawFd as _;

    const NAME: &std::ffi::CStr = c"system.posix_acl_access";
    let fd = file.as_raw_fd();
    let failed = |e: std::io::Error| match e.raw_os_error() {
        Some(libc::ENODATA | libc::EOPNOTSUPP) => Ok(None),
        _ => Err(format!(
            "has an access control list that cannot be read: {e}"
        )),
    };
    // SAFETY: a zero size asks for the value's length and writes nothing.
    let len = unsafe { libc::fgetxattr(fd, NAME.as_ptr(), std::ptr::null_mut(), 0) };
    let Ok(len) = usize::try_from(len) else {
        return failed(std::io::Error::last_os_error());
    };
    let mut value = vec![0u8; len];
    // SAFETY: `value` has room for `len` bytes, and the call writes at most
    // that many.
    let read = unsafe { libc::fgetxattr(fd, NAME.as_ptr(), value.as_mut_ptr().cast(), len) };
    let Ok(read) = usize::try_from(read) else {
        return failed(std::io::Error::last_os_error());
    };
    value.truncate(read);
    parse_acl(&value).map(Some).ok_or_else(|| {
        "has an access control list in a format this server does not read".to_owned()
    })
}

#[cfg(all(unix, not(target_os = "linux")))]
fn read_acl(_file: &std::fs::File) -> Result<Option<Vec<AclEntry>>, String> {
    Ok(None)
}

/// Checks 3 and 4 on what the descriptor gave, with `acl` read before the
/// group bits are judged. The reason starts with a verb, after the file's
/// name.
fn judge(facts: &FileFacts, acl: Option<&[AclEntry]>, ids: Ids) -> Result<(), String> {
    let mode = facts.mode;
    if !facts.regular {
        return Err("is not a regular file".to_owned());
    }
    if facts.uid != 0 && facts.uid != ids.uid {
        return Err(format!(
            "is owned by uid {}; root or the server's user (uid {}) must own it",
            facts.uid, ids.uid
        ));
    }
    if mode & 0o007 != 0 {
        return Err(format!(
            "has mode {mode:04o}, which gives other users access; remove it with `chmod o-rwx`"
        ));
    }
    let entries = acl.unwrap_or_default();
    // Group access is accepted only on a file that root owns (the Kubernetes
    // `fsGroup` route), and then only for the server's group. The rule cannot
    // see who else is in that group.
    let root_owned = facts.uid == 0;
    for entry in entries {
        match entry.tag {
            ACL_USER if entry.id != ids.uid => {
                return Err(format!(
                    "has an access control list entry for uid {}, which is not the server's \
                     user (uid {}); remove it with `setfacl -x u:{}`",
                    entry.id, ids.uid, entry.id
                ));
            }
            ACL_GROUP if entry.id != ids.gid => {
                return Err(format!(
                    "has an access control list entry for gid {}, which is not the server's \
                     group (gid {}); remove it with `setfacl -x g:{}`",
                    entry.id, ids.gid, entry.id
                ));
            }
            ACL_GROUP if !root_owned && entry.perm != 0 => {
                return Err(format!(
                    "has an access control list entry for the server's group (gid {}), and \
                     group access is accepted only on a file that root owns; remove it with \
                     `setfacl -x g:{}`",
                    entry.id, entry.id
                ));
            }
            ACL_OTHER if entry.perm != 0 => {
                return Err(
                    "has an access control list entry that gives other users access; remove \
                     it with `setfacl -m o::---`"
                        .to_owned(),
                );
            }
            _ => {}
        }
    }
    if root_owned && facts.gid == ids.gid {
        return Ok(());
    }
    // Why the owning group may have no access, and the second fix.
    let (why, or_chgrp) = if root_owned {
        (
            format!("that group is not the server's group (gid {})", ids.gid),
            format!(
                ", or give the file to the server's group with `chgrp {}`",
                ids.gid
            ),
        )
    } else {
        (
            "group access is accepted only on a file that root owns".to_owned(),
            String::new(),
        )
    };
    // With a mask entry the mode's group bits are the mask, and the owning
    // group's own permission is its entry (acl(5)).
    if entries.iter().any(|e| e.tag == ACL_MASK) {
        if entries
            .iter()
            .any(|e| e.tag == ACL_GROUP_OBJ && e.perm != 0)
        {
            return Err(format!(
                "gives its group (gid {}) access through its access control list, and {why}; \
                 remove it with `setfacl -m g::---`{or_chgrp}",
                facts.gid
            ));
        }
    } else if mode & 0o070 != 0 {
        return Err(format!(
            "has mode {mode:04o}, which gives its group (gid {}) access, and {why}; remove it \
             with `chmod g-rwx`{or_chgrp}",
            facts.gid
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "key_file_tests.rs"]
mod tests;
