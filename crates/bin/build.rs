// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Build script for the extenddb binary.
//!
//! Bakes the git commit hash and build timestamp into the binary at compile
//! time so `extenddb version` can report them without runtime git access.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    println!("cargo:rerun-if-env-changed=EXTENDDB_GIT_HASH");
    println!("cargo:rerun-if-env-changed=EXTENDDB_BUILD_TIME");

    // Git commit hash. Container builds exclude .git from the context, so the
    // release workflow injects EXTENDDB_GIT_HASH (the full SHA) explicitly. A
    // build of the published crate has no git repository either, so the
    // commit `cargo package` recorded is read next. Local builds retain the
    // convenient git fallback (short form).
    let git_hash = env::var("EXTENDDB_GIT_HASH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(packaged_commit)
        .unwrap_or_else(|| {
            Command::new("git")
                .args(["rev-parse", "--short", "HEAD"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map_or_else(
                    || "unknown".to_owned(),
                    |output| String::from_utf8_lossy(&output.stdout).trim().to_owned(),
                )
        });
    println!("cargo:rustc-env=EXTENDDB_GIT_HASH={git_hash}");

    // Build timestamp (UTC, ISO 8601). Uses std::time to avoid shelling out
    // to `date`, which is not portable to Windows or minimal containers.
    let build_time = env::var("EXTENDDB_BUILD_TIME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
                |_| "unknown".to_owned(),
                |d| {
                    let secs = d.as_secs();
                    // Manual UTC formatting — avoids adding a build dependency.
                    let days = secs / 86400;
                    let time_of_day = secs % 86400;
                    let hours = time_of_day / 3600;
                    let minutes = (time_of_day % 3600) / 60;
                    let seconds = time_of_day % 60;

                    // Days since 1970-01-01 → (year, month, day) via civil-from-days.
                    // Algorithm from Howard Hinnant's date library (public domain).
                    // These casts are safe: days since epoch and year-of-era both fit
                    // in i64 for any realistic build timestamp (compile-time only).
                    #[allow(clippy::cast_possible_wrap)]
                    let z = (days as i64) + 719_468;
                    let era = z.div_euclid(146_097);
                    #[allow(clippy::cast_possible_truncation)]
                    let doe = z.rem_euclid(146_097) as u64;
                    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
                    #[allow(clippy::cast_possible_wrap)]
                    let y = (yoe as i64) + era * 400;
                    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
                    let mp = (5 * doy + 2) / 153;
                    let d = doy - (153 * mp + 2) / 5 + 1;
                    let m = if mp < 10 { mp + 3 } else { mp - 9 };
                    let y = if m <= 2 { y + 1 } else { y };

                    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
                },
            )
        });
    println!("cargo:rustc-env=EXTENDDB_BUILD_TIME={build_time}");

    // No file-specific rerun directives: Cargo still re-runs build.rs when any
    // file in the package changes, while the env directives above make metadata
    // overrides participate in Cargo's normal rebuild tracking.
}

/// The commit of a build from the published crate.
///
/// `cargo package` writes `.cargo_vcs_info.json` beside the manifest, with
/// `"sha1": "<40 hex digits>"` for the packaged commit and `"dirty": true`
/// when the tree had uncommitted changes. The file exists only in a packaged
/// crate, so a build in the repository returns `None` here and falls back to
/// git. A file without a well-formed SHA also returns `None`.
fn packaged_commit() -> Option<String> {
    let dir = env::var_os("CARGO_MANIFEST_DIR")?;
    let text = fs::read_to_string(Path::new(&dir).join(".cargo_vcs_info.json")).ok()?;
    let after_key = &text[text.find("\"sha1\"")? + "\"sha1\"".len()..];
    let value = &after_key[after_key.find('"')? + 1..];
    let sha = value.get(..40)?;
    if !sha.bytes().all(|b| b.is_ascii_hexdigit()) || !value[40..].starts_with('"') {
        return None;
    }
    let dirty = text.find("\"dirty\"").is_some_and(|at| {
        let rest = text[at + "\"dirty\"".len()..].trim_start();
        rest.strip_prefix(':')
            .is_some_and(|value| value.trim_start().starts_with("true"))
    });
    Some(if dirty {
        format!("{sha}-dirty")
    } else {
        sha.to_owned()
    })
}
