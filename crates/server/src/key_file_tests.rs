// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the KEK file rule and format.
//!
//! The rule's judgement is one table over `fstat` facts and ACL entries, so
//! that root-owned files and other users' ids are covered without root. The
//! file tests then run the descriptor path on real files.

use super::*;

/// The server's ids in the judgement table: the image's uid and gid.
const SERVER: Ids = Ids {
    uid: 10001,
    gid: 10001,
};

/// An ACL entry with no id, for the owner, owning group, mask and others.
fn base(tag: u16, perm: u16) -> AclEntry {
    AclEntry {
        tag,
        perm,
        id: u32::MAX,
    }
}

fn named(tag: u16, perm: u16, id: u32) -> AclEntry {
    AclEntry { tag, perm, id }
}

fn facts(uid: u32, gid: u32, mode: u32) -> FileFacts {
    FileFacts {
        regular: true,
        uid,
        gid,
        mode,
    }
}

/// Checks 3 and 4: each case is the file's owner, group, mode and ACL, the
/// server's ids, and `None` for a file the rule accepts or a part of the
/// refusal.
#[test]
fn the_file_rule_judges_owner_mode_and_acl() {
    const R: u16 = 4;
    const RW: u16 = 6;
    // systemd's tmpfs credential: root:root at 0400 with an entry for the
    // service user, which the kernel reports as 0440 (the mask).
    let systemd = |user: u32, group_obj: u16| {
        vec![
            base(ACL_USER_OBJ, R),
            named(ACL_USER, R, user),
            base(ACL_GROUP_OBJ, group_obj),
            base(ACL_MASK, R),
            base(ACL_OTHER, 0),
        ]
    };
    let not_regular = FileFacts {
        regular: false,
        ..facts(SERVER.uid, SERVER.gid, 0o600)
    };
    let in_group_0 = Ids { gid: 0, ..SERVER };
    /// What the case is, the file, its ACL, the server's ids, the refusal.
    type Case = (
        &'static str,
        FileFacts,
        Option<Vec<AclEntry>>,
        Ids,
        Option<&'static str>,
    );
    let cases: Vec<Case> = vec![
        (
            "own file 0600",
            facts(10001, 10001, 0o600),
            None,
            SERVER,
            None,
        ),
        (
            "own file 0400",
            facts(10001, 10001, 0o400),
            None,
            SERVER,
            None,
        ),
        (
            "own file 0640, the server's group",
            facts(10001, 10001, 0o640),
            None,
            SERVER,
            Some(
                "has mode 0640, which gives its group (gid 10001) access, and group access is \
                 accepted only on a file that root owns",
            ),
        ),
        (
            "own file, the server's group readable through the ACL",
            facts(10001, 10001, 0o640),
            Some(vec![
                base(ACL_USER_OBJ, RW),
                base(ACL_GROUP_OBJ, R),
                base(ACL_MASK, R),
                base(ACL_OTHER, 0),
            ]),
            SERVER,
            Some(
                "gives its group (gid 10001) access through its access control list, and group \
                 access is accepted only on a file that root owns",
            ),
        ),
        (
            "systemd tmpfs: root owner, named-user entry for the server",
            facts(0, 0, 0o440),
            Some(systemd(10001, 0)),
            SERVER,
            None,
        ),
        (
            "root owner, named-user entry for another user",
            facts(0, 0, 0o440),
            Some(systemd(10002, 0)),
            SERVER,
            Some("entry for uid 10002, which is not the server's user (uid 10001)"),
        ),
        (
            "root owner, the owning group readable through the ACL",
            facts(0, 0, 0o440),
            Some(systemd(10001, R)),
            SERVER,
            Some("gives its group (gid 0) access through its access control list"),
        ),
        (
            "the owning group readable through the ACL, the server's group",
            facts(0, 10001, 0o440),
            Some(systemd(10001, R)),
            SERVER,
            None,
        ),
        (
            "Kubernetes fsGroup: root owner, group 10001, 0440",
            facts(0, 10001, 0o440),
            None,
            SERVER,
            None,
        ),
        (
            "Kubernetes fsGroup, server with effective group 0",
            facts(0, 10001, 0o440),
            None,
            in_group_0,
            Some(
                "gives its group (gid 10001) access, and that group is not the server's group (gid 0)",
            ),
        ),
        (
            "group-readable, root owner, the group is not the server's",
            facts(0, 20, 0o640),
            None,
            SERVER,
            Some("has mode 0640, which gives its group (gid 20) access"),
        ),
        (
            "readable by others",
            facts(10001, 10001, 0o604),
            None,
            SERVER,
            Some("has mode 0604, which gives other users access"),
        ),
        (
            "owned by another user",
            facts(10002, 10001, 0o600),
            None,
            SERVER,
            Some("is owned by uid 10002; root or the server's user (uid 10001) must own it"),
        ),
        (
            "not a regular file",
            not_regular,
            None,
            SERVER,
            Some("is not a regular file"),
        ),
        (
            "named-group entry for another group",
            facts(10001, 10001, 0o640),
            Some(vec![
                base(ACL_USER_OBJ, RW),
                base(ACL_GROUP_OBJ, 0),
                named(ACL_GROUP, R, 30),
                base(ACL_MASK, R),
                base(ACL_OTHER, 0),
            ]),
            SERVER,
            Some("entry for gid 30, which is not the server's group (gid 10001)"),
        ),
        (
            "named-group entry for the server's group, another owning group",
            facts(10001, 20, 0o640),
            Some(vec![
                base(ACL_USER_OBJ, RW),
                base(ACL_GROUP_OBJ, 0),
                named(ACL_GROUP, R, 10001),
                base(ACL_MASK, R),
                base(ACL_OTHER, 0),
            ]),
            SERVER,
            Some(
                "entry for the server's group (gid 10001), and group access is accepted only on \
                 a file that root owns",
            ),
        ),
        (
            "root owner, named-group entry for the server's group, another owning group",
            facts(0, 20, 0o440),
            Some(vec![
                base(ACL_USER_OBJ, R),
                base(ACL_GROUP_OBJ, 0),
                named(ACL_GROUP, R, 10001),
                base(ACL_MASK, R),
                base(ACL_OTHER, 0),
            ]),
            SERVER,
            None,
        ),
        (
            "an ACL entry for others that grants access",
            facts(10001, 10001, 0o600),
            Some(vec![
                base(ACL_USER_OBJ, RW),
                base(ACL_GROUP_OBJ, 0),
                base(ACL_OTHER, R),
            ]),
            SERVER,
            Some("entry that gives other users access"),
        ),
    ];
    for (what, facts, acl, ids, refusal) in cases {
        let got = judge(&facts, acl.as_deref(), ids);
        match refusal {
            None => assert_eq!(got, Ok(()), "{what}"),
            Some(part) => {
                let reason = got.expect_err(what);
                assert!(
                    reason.contains(part),
                    "{what}: the refusal does not contain {part:?}"
                );
            }
        }
    }
}

/// A store that could not lock refuses startup only while a KEK is in use
/// and locked memory is required; every other case warns. Both carry the
/// store's own error, which names the memory needed and the fixes.
#[test]
fn a_lock_failure_refuses_only_with_a_kek_and_the_requirement() {
    let error = "the locked key store needs 643072 bytes";
    for (kek_in_use, required, refuses) in [
        (true, true, true),
        (true, false, false),
        (false, true, false),
        (false, false, false),
    ] {
        let outcome = lock_failure(kek_in_use, required, &error);
        let text = match &outcome {
            Ok(warning) => warning,
            Err(refusal) => refusal,
        };
        assert_eq!(outcome.is_err(), refuses, "{kek_in_use} {required}: {text}");
        assert!(text.starts_with(error), "{text}");
        assert_eq!(
            text.contains("require_locked_memory = false"),
            refuses,
            "{text}"
        );
    }
    assert_eq!(serve_slots(10_000, 16), 20_019);
    assert_eq!(serve_slots(u64::MAX, 16), usize::MAX);
}

/// The attribute's layout: a version word of 2, then whole 8-byte entries of
/// known tags.
#[test]
fn the_acl_attribute_parses_only_in_the_kernel_layout() {
    let entry = |tag: u16, perm: u16, id: u32| {
        let mut bytes = tag.to_le_bytes().to_vec();
        bytes.extend(perm.to_le_bytes());
        bytes.extend(id.to_le_bytes());
        bytes
    };
    let value = |version: u32, entries: &[Vec<u8>]| {
        let mut bytes = version.to_le_bytes().to_vec();
        for e in entries {
            bytes.extend(e);
        }
        bytes
    };
    let owner = entry(ACL_USER_OBJ, 6, u32::MAX);
    let user = entry(ACL_USER, 4, 10001);
    let one = std::slice::from_ref(&owner);
    /// What the case is, the attribute's bytes, and its entries.
    type Case = (&'static str, Vec<u8>, Option<Vec<AclEntry>>);
    let cases: Vec<Case> = vec![
        (
            "two entries",
            value(2, &[owner.clone(), user]),
            Some(vec![base(ACL_USER_OBJ, 6), named(ACL_USER, 4, 10001)]),
        ),
        ("version 1", value(1, one), None),
        ("a cut entry", value(2, one)[..11].to_vec(), None),
        ("an unknown tag", value(2, &[entry(0x40, 4, 0)]), None),
        ("no version word", vec![2, 0], None),
    ];
    for (what, bytes, expected) in cases {
        assert_eq!(parse_acl(&bytes), expected, "{what}");
    }
}

#[cfg(unix)]
mod files {
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};

    use super::*;

    const KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

    /// A private directory for one test's files.
    fn private_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod dir");
        dir
    }

    /// A KEK file holding `KEY` and a newline, at `mode`.
    fn key_file(dir: &Path, name: &str, mode: u32) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("{KEY}\n")).expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
        path
    }

    fn key_bytes() -> Vec<u8> {
        (0u8..32).collect()
    }

    /// Checks 3 and 4 and the format, through the descriptor: each case is a
    /// file this user makes, and `None` for a file the rule reads or a part
    /// of the refusal.
    #[test]
    fn the_descriptor_checks_run_on_real_files() {
        let dir = private_dir();
        let fifo = dir.path().join("fifo");
        let fifo_c = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).expect("path");
        // SAFETY: `fifo_c` is a NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0, "mkfifo");
        let target = key_file(dir.path(), "target", 0o600);
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        let subdir = dir.path().join("dir");
        std::fs::create_dir(&subdir).expect("mkdir");
        // The key with one character outside the base64 alphabet, which the
        // refusal must not repeat.
        let bad = dir.path().join("bad");
        std::fs::write(&bad, format!("{}*{}\n", &KEY[..5], &KEY[6..])).expect("write");
        std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        let cases: Vec<(&str, PathBuf, Option<&str>)> = vec![
            ("0600", key_file(dir.path(), "a", 0o600), None),
            ("0400", key_file(dir.path(), "b", 0o400), None),
            (
                "0640, own group",
                key_file(dir.path(), "c", 0o640),
                Some("group access is accepted only on a file that root owns"),
            ),
            (
                "0604",
                key_file(dir.path(), "d", 0o604),
                Some("has mode 0604, which gives other users access"),
            ),
            (
                "a FIFO, which must not block the open",
                fifo,
                Some("is not a regular file"),
            ),
            ("a directory", subdir, Some("is not a regular file")),
            (
                "a symlink at the resolved path",
                link,
                Some("was replaced by a symlink after its path was resolved"),
            ),
            (
                "a character outside the alphabet",
                bad,
                Some("is not a KEK file: decode encryption key: invalid symbol at offset 5"),
            ),
        ];
        for (what, path, refusal) in cases {
            match (read_checked(&path), refusal) {
                (Ok(key), None) => assert_eq!(key.as_slice(), key_bytes(), "{what}"),
                (Err(reason), Some(part)) => {
                    assert!(reason.contains(part), "{what}: {reason}");
                    assert!(!reason.contains('*'), "{what}: {reason}");
                }
                (got, _) => panic!("{what}: {:?}", got.map(|_| "a key")),
            }
        }
    }

    /// An ACL the kernel stored reads back as its entries, and the rule
    /// accepts one whose named entries are the server's own ids and whose
    /// group entries grant nothing, on a file the server's user owns.
    #[cfg(target_os = "linux")]
    #[test]
    fn an_acl_the_kernel_stored_is_read_from_the_descriptor() {
        let dir = private_dir();
        let path = key_file(dir.path(), "kek", 0o600);
        let ids = effective_ids();
        let entries = [
            base(ACL_USER_OBJ, 6),
            named(ACL_USER, 4, ids.uid),
            base(ACL_GROUP_OBJ, 0),
            named(ACL_GROUP, 0, ids.gid),
            base(ACL_MASK, 4),
            base(ACL_OTHER, 0),
        ];
        let mut value = ACL_XATTR_VERSION.to_le_bytes().to_vec();
        for e in &entries {
            value.extend(e.tag.to_le_bytes());
            value.extend(e.perm.to_le_bytes());
            value.extend(e.id.to_le_bytes());
        }
        let file = std::fs::File::open(&path).expect("open");
        {
            use std::os::fd::AsRawFd as _;
            // SAFETY: `value` is valid for its length, and the name is
            // NUL-terminated.
            let set = unsafe {
                libc::fsetxattr(
                    file.as_raw_fd(),
                    c"system.posix_acl_access".as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            };
            if set != 0 {
                let e = std::io::Error::last_os_error();
                eprintln!("skipped: this file system keeps no ACL here: {e}");
                return;
            }
        }
        assert_eq!(read_acl(&file).expect("read"), Some(entries.to_vec()));
        // The mask is now the mode's group bits.
        let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o640);
        assert_eq!(read_checked(&path).expect("read").as_slice(), key_bytes());
    }

    /// The whole rule from the configured path: a missing file names the
    /// path, a parent writable by others is refused as an ancestor, and a
    /// symlink is judged by its target.
    #[test]
    fn the_rule_resolves_the_path_and_checks_its_ancestors() {
        let dir = private_dir();
        let missing = dir.path().join("missing");
        let missing = missing.to_str().expect("utf-8");
        let reason = read_key_file(missing).expect_err("missing");
        assert!(
            reason.starts_with(&format!("KEK file {missing} cannot be resolved: ")),
            "{reason}"
        );

        let open = dir.path().join("open");
        std::fs::create_dir(&open).expect("mkdir");
        let in_open = key_file(&open, "kek", 0o600);
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o777)).expect("chmod");
        let reason = read_key_file(in_open.to_str().expect("utf-8")).expect_err("open parent");
        assert!(reason.contains("an ancestor of the KEK file"), "{reason}");
        assert!(reason.contains("writable by group or others"), "{reason}");

        let target = key_file(dir.path(), "target", 0o600);
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        match read_key_file(link.to_str().expect("utf-8")) {
            Ok(key) => assert_eq!(key.as_slice(), key_bytes()),
            // A host whose directories above the temp directory belong to a
            // user outside this user namespace refuses every path there.
            Err(reason) if reason.contains("(an ancestor of the KEK file) is owned by uid") => {
                eprintln!("skipped the symlink case: {reason}");
            }
            Err(reason) => panic!("{reason}"),
        }
    }
}
