// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared utility functions for the extenddb binary crate.

/// Check whether a process is alive using `kill(pid, 0)` (POSIX signal 0).
/// Works on both Linux and macOS (no `/proc` dependency).
pub fn is_process_alive(pid: i32) -> bool {
    // SAFETY: kill with signal 0 performs error checking without sending a
    // signal. Returns 0 if the process exists and we have permission.
    unsafe { libc::kill(pid, 0) == 0 }
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
    use super::append_secret_arg;
    use std::ffi::OsString;

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
