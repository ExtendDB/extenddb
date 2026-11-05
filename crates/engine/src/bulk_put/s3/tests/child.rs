// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A child process of this test binary with a fixed environment, for the
//! tests that load the SDK's configuration from the process environment
//! and the AWS config files. The host's variables, profiles and instance
//! metadata stay out of the child: its environment holds a scratch `HOME`,
//! a config file of the test's own, a credentials file that does not
//! exist, instance metadata off, and the variables the test names.

use std::future::Future;
use std::process::{Command, Stdio};

/// The variable that tells a child process of this test binary to run the
/// test's body.
const CHILD: &str = "EXTENDDB_TEST_S3_STORE_CHILD";

/// Whether this process is a child [`run`] started.
pub(super) fn is_child() -> bool {
    std::env::var_os(CHILD).is_some()
}

/// Drive `body` to its end on a current-thread runtime, so the events it
/// logs reach a subscriber installed on this thread.
pub(super) fn block_on<F: Future>(body: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(body)
}

/// Run the test `test` of the module `module` (its `module_path!()`) in a
/// child process with `variables` and the AWS config file `config`, and
/// assert that it ran and passed. `case` names the run in a failure.
pub(super) fn run(module: &str, test: &str, case: &str, variables: &[(&str, &str)], config: &str) {
    let module = module.split_once("::").map_or("", |(_, rest)| rest);
    let dir = std::env::temp_dir().join(format!("eb-s3-child-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("config"), config).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            format!("{module}::{test}").as_str(),
            "--exact",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env_clear()
        .env(CHILD, case)
        .env("HOME", &dir)
        .env("AWS_CONFIG_FILE", dir.join("config"))
        .env("AWS_SHARED_CREDENTIALS_FILE", dir.join("no-credentials"))
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .envs(variables.iter().copied())
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{case}:\n{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed"), "{case}:\n{stdout}\n{stderr}");
}
