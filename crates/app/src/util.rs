// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared utility functions for the extenddb binary crate.

/// Check whether a process is alive using `kill(pid, 0)` (POSIX signal 0).
/// Works on both Linux and macOS (no `/proc` dependency).
#[cfg(unix)]
pub fn is_process_alive(pid: i32) -> bool {
    // SAFETY: kill with signal 0 performs error checking without sending a
    // signal. Returns 0 if the process exists and we have permission.
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Non-unix fallback: there is no dependency-free liveness probe, so assume
/// the process is alive. This is the conservative direction for every caller:
/// guards that refuse to run alongside a live server keep refusing, and
/// `status` at worst reports a stale PID. PID files only exist in daemon
/// mode, which is unix-only, so this path is effectively unreachable.
#[cfg(not(unix))]
pub fn is_process_alive(_pid: i32) -> bool {
    true
}

/// The configuration `migrate` and `verify` read: the file `--config`
/// names, else `extenddb.toml` when it exists, else, in a dev-mode build,
/// the built-in defaults with the `EXTENDDB__` environment variables, as
/// `serve` reads them when it starts without a file. Returns the file read,
/// if any.
pub(crate) fn command_config(
    given: Option<&str>,
) -> anyhow::Result<(extenddb_config::AppConfig, Option<String>)> {
    let path = given.unwrap_or(crate::cmd_migrate::DEFAULT_CONFIG);
    if std::path::Path::new(path).exists() {
        return Ok((extenddb_config::load(path)?, Some(path.to_owned())));
    }
    if given.is_none() && cfg!(feature = "dev-mode") {
        return Ok((extenddb_config::load_builtin_defaults()?, None));
    }
    anyhow::bail!(
        "Config file '{path}' not found. Run 'extenddb init' to set up a deployment, \
         or use --config <path> to specify a different location."
    )
}

/// Print each step a run applied, as `<indent><verb> <noun> <id> <name>`
/// (without `<noun> ` when `noun` is empty), naming the store only when the
/// run touched more than one, and each step's notes below it. A step of a
/// migration that does not apply on this backend recorded its ledger row and
/// ran nothing, and is printed as `<indent>Recorded <noun> <id> <name> as not
/// applicable`.
pub(crate) fn print_applied(
    report: &extenddb_storage::migrations::RunReport,
    indent: &str,
    verb: &str,
    noun: &str,
) {
    for line in applied_lines(report, verb, noun) {
        println!("{indent}{line}");
    }
}

/// The lines [`print_applied`] prints, without the indent.
fn applied_lines(
    report: &extenddb_storage::migrations::RunReport,
    verb: &str,
    noun: &str,
) -> Vec<String> {
    use extenddb_storage::migrations::Outcome;

    let several = report
        .applied
        .windows(2)
        .any(|w| w[0].store_name != w[1].store_name);
    let mut lines = Vec::new();
    for step in &report.applied {
        let mut what = if noun.is_empty() {
            format!("{} {}", step.id, step.name)
        } else {
            format!("{noun} {} {}", step.id, step.name)
        };
        if several {
            what = format!("{what} in {}", step.store_name);
        }
        lines.push(match step.outcome {
            Outcome::Applied => format!("{verb} {what}"),
            Outcome::NotApplicable => format!("Recorded {what} as not applicable"),
        });
        lines.extend(step.notes.iter().map(|note| format!("  {note}")));
    }
    lines
}

/// Append a secret to the backend argument vector when the corresponding CLI
/// flag was not supplied. The value may come from an environment variable, so
/// it is added only to this in-process copy and never to the process command
/// line visible through tools such as `ps`.
pub(crate) fn append_secret_arg(
    args: &mut Vec<String>,
    flag: &str,
    value: Option<std::ffi::OsString>,
    env_name: &str,
) -> anyhow::Result<()> {
    let equals_prefix = format!("{flag}=");
    if args
        .iter()
        .any(|arg| arg == flag || arg.starts_with(&equals_prefix))
    {
        return Ok(());
    }

    let Some(value) = value else {
        return Ok(());
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{env_name} must contain valid UTF-8"))?;
    args.push(flag.to_owned());
    args.push(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use extenddb_storage::migrations::{AppliedStep, Outcome, RunReport};

    use super::{append_secret_arg, applied_lines};

    fn step(id: u32, name: &'static str, store: &str, outcome: Outcome) -> AppliedStep {
        AppliedStep {
            id,
            name,
            store_name: store.to_owned(),
            outcome,
            notes: Vec::new(),
        }
    }

    /// A step of a migration that does not apply on the backend is printed
    /// as recorded, with the verb's noun, never as applied or run again.
    #[test]
    fn a_step_that_does_not_apply_is_printed_as_recorded() {
        let report = RunReport {
            applied: vec![
                step(1, "baseline", "ks_catalog", Outcome::Applied),
                step(1, "baseline", "ks_a1", Outcome::Applied),
                step(
                    3,
                    "sort_key_definitions",
                    "ks_catalog",
                    Outcome::NotApplicable,
                ),
            ],
            ..RunReport::default()
        };
        assert_eq!(
            applied_lines(&report, "Ran again", ""),
            [
                "Ran again 1 baseline in ks_catalog",
                "Ran again 1 baseline in ks_a1",
                "Recorded 3 sort_key_definitions in ks_catalog as not applicable",
            ]
        );
        let one_store = RunReport {
            applied: vec![
                step(1, "baseline", "db", Outcome::Applied),
                step(3, "sort_key_definitions", "db", Outcome::NotApplicable),
            ],
            ..RunReport::default()
        };
        assert_eq!(
            applied_lines(&one_store, "Applied", "migration"),
            [
                "Applied migration 1 baseline",
                "Recorded migration 3 sort_key_definitions as not applicable",
            ]
        );
    }

    #[test]
    fn appends_environment_secret_when_flag_is_absent() {
        let mut args = vec!["extenddb".to_owned(), "migrate".to_owned()];
        append_secret_arg(
            &mut args,
            "--pg-pass",
            Some(OsString::from("secret")),
            "EXTENDDB_PG_PASSWORD",
        )
        .unwrap();
        assert_eq!(args, ["extenddb", "migrate", "--pg-pass", "secret"]);
    }

    #[test]
    fn explicit_cli_secret_takes_precedence() {
        let mut args = vec![
            "extenddb".to_owned(),
            "migrate".to_owned(),
            "--pg-pass=cli-secret".to_owned(),
        ];
        append_secret_arg(
            &mut args,
            "--pg-pass",
            Some(OsString::from("environment-secret")),
            "EXTENDDB_PG_PASSWORD",
        )
        .unwrap();
        assert_eq!(args.len(), 3);
    }
}
