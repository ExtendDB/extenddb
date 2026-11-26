// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Temporary diagnostic. It sets up the locked key store under several
//! memlock limits. Each limit runs in its own child process. It prints the
//! protection, the refusal, or the signal that ended the child.

use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use extenddb_auth::secmem;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const SLOT_BYTES: u64 = 32;

fn set_limit(bytes: u64) {
    let limit = libc::rlimit {
        rlim_cur: bytes as libc::rlim_t,
        rlim_max: bytes as libc::rlim_t,
    };
    // SAFETY: a valid `rlimit`; lowering a limit needs no privilege.
    let result = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &raw const limit) };
    assert_eq!(result, 0, "setrlimit");
}

fn memlock() -> String {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid out-pointer.
    unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &raw mut limit) };
    if limit.rlim_cur == libc::RLIM_INFINITY {
        "unlimited".to_owned()
    } else {
        format!("{} KiB", limit.rlim_cur / 1024)
    }
}

/// The child side: `store <limit|default> <bytes> [hold]`.
fn store(limit: &str, bytes: u64, hold: bool) {
    if limit != "default" {
        set_limit(limit.parse().expect("limit"));
    }
    let slots = usize::try_from(bytes / SLOT_BYTES).expect("slots");
    match secmem::init(slots) {
        Ok(protection) => println!("Ok({protection})"),
        Err(error) => println!("Err({error})"),
    }
    if hold {
        println!("held");
        std::thread::sleep(Duration::from_secs(300));
    }
}

fn spawn(args: &[String]) -> Child {
    Command::new(std::env::current_exe().expect("exe"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn")
}

fn describe(output: &Output) -> String {
    let status = match (output.status.code(), output.status.signal()) {
        (Some(code), _) => format!("exit {code}"),
        (None, Some(7)) => "killed by SIGBUS".to_owned(),
        (None, Some(signal)) => format!("killed by signal {signal}"),
        (None, None) => "unknown".to_owned(),
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let first = stdout.lines().next().unwrap_or("-");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let err = stderr.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
    format!("{status} | {first} | {err}")
}

fn args(limit: &str, bytes: u64, hold: bool) -> Vec<String> {
    let mut v = vec!["store".to_owned(), limit.to_owned(), bytes.to_string()];
    if hold {
        v.push("hold".to_owned());
    }
    v
}

fn run(name: &str, limit: &str, bytes: u64) {
    let output = spawn(&args(limit, bytes, false)).wait_with_output().expect("wait");
    println!("{name} | {}", describe(&output));
}

/// Another process of the same user holds `held` bytes of store while this
/// one sets up `bytes` under the default limit.
fn run_beside(name: &str, held: u64, bytes: u64) {
    let mut holder = spawn(&args("default", held, true));
    let mut lines = BufReader::new(holder.stdout.take().expect("stdout")).lines();
    let first = lines.next().and_then(Result::ok).unwrap_or_default();
    let ready = lines.next().and_then(Result::ok).unwrap_or_default();
    if ready != "held" {
        let _ = holder.kill();
        let output = holder.wait_with_output().expect("wait");
        println!("{name} | holder did not hold: {first} | {}", describe(&output));
        return;
    }
    let output = spawn(&args("default", bytes, false)).wait_with_output().expect("wait");
    println!("{name} | holder {first} | {}", describe(&output));
    let _ = holder.kill();
    let _ = holder.wait();
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.first().map(String::as_str) == Some("store") {
        let hold = argv.get(3).map(String::as_str) == Some("hold");
        store(&argv[1], argv[2].parse().expect("bytes"), hold);
        return;
    }
    println!("default memlock limit: {}", memlock());
    run("A default limit, 4 KiB store", "default", 4 * KIB);
    run("B 64 KiB limit, 1 MiB store", &(64 * KIB).to_string(), MIB);
    run("C 64 KiB limit, 32 KiB store", &(64 * KIB).to_string(), 32 * KIB);
    run("D 1 MiB limit, 1 MiB store", &MIB.to_string(), MIB);
    run("E default limit, 1 MiB store", "default", MIB);
    run_beside("F other process holds 6 MiB, then 4 MiB", 6 * MIB, 4 * MIB);
    run_beside("G other process holds 6 MiB, then 1 MiB", 6 * MIB, MIB);
}
