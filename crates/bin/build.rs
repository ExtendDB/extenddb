// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Build script for the extenddb binary.
//!
//! Bakes the git commit hash and build timestamp into the binary at compile
//! time so `extenddb version` can report them without runtime git access.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // Git commit hash (short).
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_owned(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_owned(),
        );
    println!("cargo:rustc-env=EXTENDDB_GIT_HASH={git_hash}");

    // Build timestamp (UTC, ISO 8601). Uses std::time to avoid shelling out
    // to `date`, which is not portable to Windows or minimal containers.
    let build_time = SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
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
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            #[allow(clippy::cast_possible_wrap)]
            let y = (yoe as i64) + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if m <= 2 { y + 1 } else { y };

            format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
        },
    );
    println!("cargo:rustc-env=EXTENDDB_BUILD_TIME={build_time}");

    // No explicit rerun-if-changed directives — without them, Cargo re-runs
    // build.rs when any file in the package changes. This is fast (one git
    // subprocess + one SystemTime call) and ensures the git hash is always fresh.
}
