// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite's reading of a `file:` URI, ported from `sqlite3ParseUri` in the
//! bundled SQLite (3.46.0), for code that must name the file SQLite opens.
//!
//! After `file:`, a `//` starts an authority, which SQLite accepts only empty
//! or `localhost`. The path runs to the first `?` and the query to the first
//! `#`. The query splits into options at `&`, and each option into a name and
//! a value at its first `=`. `%HH` escapes are decoded in the path, the names
//! and the values, and a decoded character is never a separator. `%00` ends
//! the path, name or value it appears in. An option with an empty name is
//! skipped.

use std::path::PathBuf;

/// A `file:` URI as SQLite reads it.
pub(crate) struct FileUri {
    /// The path, decoded.
    path: Vec<u8>,
    /// The query options in order, decoded.
    options: Vec<(Vec<u8>, Vec<u8>)>,
}

/// SQLite's `SQLITE_OPEN_*` access flags.
const READONLY: u32 = 0x1;
const READWRITE: u32 = 0x2;
const CREATE: u32 = 0x4;
const MEMORY: u32 = 0x80;
const ACCESS: u32 = READONLY | READWRITE | CREATE | MEMORY;

impl FileUri {
    /// Read `rest`, the text after `file:`. An authority other than empty or
    /// `localhost`, which SQLite refuses, is returned as the error.
    pub(crate) fn parse(rest: &str) -> Result<Self, String> {
        let uri = rest.as_bytes();
        // A NUL ends the text, as it ends the C string SQLite reads.
        let at = |i: usize| uri.get(i).copied().unwrap_or(0);
        let mut i = 0;
        if uri.starts_with(b"//") {
            i = 2;
            while at(i) != 0 && at(i) != b'/' {
                i += 1;
            }
            let authority = &rest[2..i];
            if !(authority.is_empty() || authority == "localhost") {
                return Err(authority.to_owned());
            }
        }
        // The path, then each name and value, each followed by a NUL, as
        // SQLite lays them out in one buffer. State 0 is the path, 1 a name,
        // 2 a value.
        let mut out: Vec<u8> = Vec::new();
        let mut state = 0;
        while at(i) != 0 && at(i) != b'#' {
            let mut c = at(i);
            i += 1;
            if c == b'%' && at(i).is_ascii_hexdigit() && at(i + 1).is_ascii_hexdigit() {
                let octet = (hex(at(i)) << 4) | hex(at(i + 1));
                i += 2;
                if octet == 0 {
                    while at(i) != 0
                        && at(i) != b'#'
                        && (state != 0 || at(i) != b'?')
                        && (state != 1 || (at(i) != b'=' && at(i) != b'&'))
                        && (state != 2 || at(i) != b'&')
                    {
                        i += 1;
                    }
                    continue;
                }
                c = octet;
            } else if state == 1 && (c == b'&' || c == b'=') {
                if out.last() == Some(&0) {
                    // An empty name: skip the option.
                    while at(i) != 0 && at(i) != b'#' && at(i - 1) != b'&' {
                        i += 1;
                    }
                    continue;
                }
                if c == b'&' {
                    out.push(0);
                } else {
                    state = 2;
                }
                c = 0;
            } else if (state == 0 && c == b'?') || (state == 2 && c == b'&') {
                c = 0;
                state = 1;
            }
            out.push(c);
        }
        if state == 1 {
            out.push(0);
        }
        out.extend_from_slice(&[0, 0, 0, 0]);
        Ok(Self::from_buffer(&out))
    }

    /// Read the buffer the way SQLite does: the path, then name and value
    /// pairs until an empty name.
    fn from_buffer(out: &[u8]) -> Self {
        let mut parts = out.split(|&b| b == 0);
        let path = parts.next().unwrap_or_default().to_vec();
        let mut options = Vec::new();
        while let Some(name) = parts.next().filter(|name| !name.is_empty()) {
            let value = parts.next().unwrap_or_default();
            options.push((name.to_vec(), value.to_vec()));
        }
        Self { path, options }
    }

    /// The path SQLite opens.
    pub(crate) fn path(&self) -> PathBuf {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            PathBuf::from(std::ffi::OsString::from_vec(self.path.clone()))
        }
        #[cfg(not(unix))]
        {
            PathBuf::from(String::from_utf8_lossy(&self.path).into_owned())
        }
    }

    /// Whether SQLite creates the file on disk when it is missing, opened with
    /// the read-write-create access sqlx passes for `mode=rwc`, after these
    /// options. Every `cache` and `mode` value must be one SQLite knows; a
    /// `mode` may not ask for more access than the flags before it allow; the
    /// last `mode` decides; and the last `vfs` must be in the unix family.
    pub(crate) fn creates_file(&self) -> bool {
        let mut flags = READWRITE | CREATE;
        let mut vfs: Option<&[u8]> = None;
        for (name, value) in &self.options {
            match name.as_slice() {
                b"vfs" => vfs = Some(value),
                b"cache" if !matches!(value.as_slice(), b"shared" | b"private") => return false,
                b"mode" => {
                    let mode = match value.as_slice() {
                        b"ro" => READONLY,
                        b"rw" => READWRITE,
                        b"rwc" => READWRITE | CREATE,
                        b"memory" => MEMORY,
                        _ => return false,
                    };
                    if (mode & !MEMORY) > (ACCESS & flags) {
                        return false;
                    }
                    flags = (flags & !ACCESS) | mode;
                }
                _ => {}
            }
        }
        flags & CREATE != 0 && flags & MEMORY == 0 && vfs.is_none_or(|vfs| vfs.starts_with(b"unix"))
    }
}

fn hex(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        _ => c - b'A' + 10,
    }
}

#[cfg(test)]
mod tests {
    use super::FileUri;
    use std::path::PathBuf;

    fn parse(rest: &str) -> FileUri {
        FileUri::parse(rest).expect(rest)
    }

    fn options(rest: &str) -> Vec<(String, String)> {
        let text = |b: &[u8]| String::from_utf8(b.to_vec()).expect("UTF-8");
        parse(rest)
            .options
            .iter()
            .map(|(name, value)| (text(name), text(value)))
            .collect()
    }

    #[test]
    fn the_path_is_decoded_and_ends_at_the_query_or_the_fragment() {
        assert_eq!(
            parse("/d/a%20b.sqlite").path(),
            PathBuf::from("/d/a b.sqlite")
        );
        assert_eq!(parse("/d/a%3fb?x=1").path(), PathBuf::from("/d/a?b"));
        assert_eq!(
            parse("/d/x.sqlite#frag?mode=ro").path(),
            PathBuf::from("/d/x.sqlite")
        );
        assert_eq!(parse("/d/a%00b?mode=ro").path(), PathBuf::from("/d/a"));
        assert_eq!(
            parse("/d/100%.sqlite").path(),
            PathBuf::from("/d/100%.sqlite")
        );
        assert_eq!(parse("///d/x").path(), PathBuf::from("/d/x"));
        assert_eq!(parse("//localhost/d/x").path(), PathBuf::from("/d/x"));
        assert_eq!(parse("rel.sqlite").path(), PathBuf::from("rel.sqlite"));
        assert_eq!(
            FileUri::parse("//otherhost/d/x").err().as_deref(),
            Some("otherhost")
        );
    }

    #[test]
    fn options_are_split_on_raw_separators_and_decoded() {
        assert_eq!(
            options("/d?mode=ro&cache=shared&psow"),
            [("mode", "ro"), ("cache", "shared"), ("psow", "")].map(|(n, v)| (n.into(), v.into()))
        );
        // A decoded `&` or `=` is part of the name or value.
        assert_eq!(options("/d?mo%64e=r%26o"), [("mode".into(), "r&o".into())]);
        // An empty name is skipped, with its value.
        assert_eq!(options("/d?=x&&mode=rw"), [("mode".into(), "rw".into())]);
        // `%00` ends the name or value it is in.
        assert_eq!(
            options("/d?mode=rw%00c&vfs=unix"),
            [("mode", "rw"), ("vfs", "unix")].map(|(n, v)| (n.into(), v.into()))
        );
    }

    #[test]
    fn creates_file_follows_the_last_mode_and_vfs() {
        for (rest, creates) in [
            ("/d", true),
            ("/d?cache=shared", true),
            ("/d?mode=rwc", true),
            ("/d?psow=0", true),
            ("/d?mode=ro", false),
            ("/d?mode=rw", false),
            ("/d?mode=memory", false),
            ("/d?mode=memory&mode=rwc", true),
            // Asking for more access than an earlier mode allows is refused.
            ("/d?mode=ro&mode=rwc", false),
            ("/d?mode=rwx", false),
            ("/d?cache=none", false),
            ("/d?vfs=memdb", false),
            ("/d?vfs=memdb&vfs=unix-excl", true),
        ] {
            assert_eq!(parse(rest).creates_file(), creates, "{rest}");
        }
    }
}
