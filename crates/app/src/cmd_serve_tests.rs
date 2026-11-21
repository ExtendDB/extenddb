// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::{
    ServeArgs, dev_mode_banner_lines, dev_mode_s3_line, is_loopback_host, render_startup_warnings,
};
use clap::Parser;
use extenddb_server::dev_credentials::{
    DevCredential, ENV_DISABLE_EXAMPLE, EXAMPLE_ACCESS_KEY_ID, EXAMPLE_SECRET_ACCESS_KEY,
};
use extenddb_storage::config::StartupWarning;

fn example() -> DevCredential {
    DevCredential {
        access_key_id: EXAMPLE_ACCESS_KEY_ID.to_owned(),
        secret_access_key: EXAMPLE_SECRET_ACCESS_KEY.to_owned(),
        is_builtin: true,
    }
}

fn team() -> DevCredential {
    DevCredential {
        access_key_id: "AKIADEVTEAMKEY000001".to_owned(),
        secret_access_key: "team-secret".to_owned(),
        is_builtin: false,
    }
}

#[test]
fn loopback_hosts_are_recognised() {
    for host in ["127.0.0.1", "127.1.2.3", "::1", "localhost"] {
        assert!(is_loopback_host(host), "{host}");
    }
    for host in ["0.0.0.0", "::", "192.168.1.10", "example.com", ""] {
        assert!(!is_loopback_host(host), "{host}");
    }
}

#[test]
fn loopback_dev_banner_has_no_exposure_warning() {
    let text =
        dev_mode_banner_lines(":memory:", "127.0.0.1", 18080, &[example()], None, None).join("\n");
    assert!(text.contains("plain HTTP on loopback"), "{text}");
    assert!(!text.contains("WARNING"), "{text}");
}

#[test]
fn non_loopback_dev_banner_warns_with_the_publish_command() {
    let text =
        dev_mode_banner_lines(":memory:", "0.0.0.0", 18080, &[example()], None, None).join("\n");
    // The banner used to say "on loopback" whatever the bind was.
    assert!(!text.contains("plain HTTP on loopback"), "{text}");
    assert!(text.contains("plain HTTP on 0.0.0.0"), "{text}");
    assert!(
        text.contains("WARNING: dev mode is listening on 0.0.0.0:18080"),
        "{text}"
    );
    assert!(text.contains("credential above is public"), "{text}");
    assert!(text.contains("-p 127.0.0.1:18080:18080"), "{text}");
    assert!(text.contains(&format!("{ENV_DISABLE_EXAMPLE}=1")), "{text}");
}

#[test]
fn non_loopback_dev_banner_without_the_example_pair_drops_the_public_wording() {
    let text = dev_mode_banner_lines(":memory:", "0.0.0.0", 9000, &[team()], None, None).join("\n");
    assert!(
        text.contains("WARNING: dev mode is listening on 0.0.0.0:9000"),
        "{text}"
    );
    assert!(text.contains("-p 127.0.0.1:9000:9000"), "{text}");
    assert!(!text.contains("is public"), "{text}");
    assert!(!text.contains(EXAMPLE_ACCESS_KEY_ID), "{text}");
    // The switch is already set; do not tell the operator to set it.
    assert!(
        !text.contains(&format!("{ENV_DISABLE_EXAMPLE}=1")),
        "{text}"
    );
    assert!(!text.contains("team-secret"), "{text}");
}

#[test]
fn dev_banner_keeps_the_ignored_aws_key_note() {
    let text = dev_mode_banner_lines(
        ":memory:",
        "127.0.0.1",
        18080,
        &[example()],
        None,
        Some("ASIAFOREIGNSESSION01"),
    )
    .join("\n");
    assert!(
        text.contains("'ASIAFOREIGNSESSION01' will be rejected"),
        "{text}"
    );
    assert!(!text.contains("is not accepted because"), "{text}");
}

#[test]
fn dev_banner_names_the_switch_when_it_removed_the_ignored_example_key() {
    let text = dev_mode_banner_lines(
        ":memory:",
        "127.0.0.1",
        18080,
        &[team()],
        None,
        Some(EXAMPLE_ACCESS_KEY_ID),
    )
    .join("\n");
    assert!(
        text.contains(&format!("'{EXAMPLE_ACCESS_KEY_ID}' will be rejected")),
        "{text}"
    );
    assert!(
        text.contains(&format!("because {ENV_DISABLE_EXAMPLE} is set")),
        "{text}"
    );
}

/// Test wrapper so clap has a top-level `Parser` to drive `ServeArgs`.
#[derive(Parser)]
struct TestCli {
    #[command(flatten)]
    args: ServeArgs,
}

fn parse(argv: &[&str]) -> ServeArgs {
    TestCli::try_parse_from(argv)
        .expect("ServeArgs should parse from valid argv")
        .args
}

#[test]
fn defaults_run_in_daemon_mode() {
    // No --foreground flag preserves the historical daemon behavior so
    // existing users and scripts are unaffected by the new flag.
    let args = parse(&["extenddb-serve"]);
    assert!(!args.foreground);
    assert_eq!(args.config, "extenddb.toml");
    assert!(args.port.is_none());
}

#[test]
fn foreground_flag_is_recognized() {
    let args = parse(&["extenddb-serve", "--foreground"]);
    assert!(args.foreground);
}

#[test]
fn no_daemon_alias_is_recognized() {
    // The issue proposed either `--foreground` or `--no-daemon`; make
    // sure the alias keeps working so users have a choice.
    let args = parse(&["extenddb-serve", "--no-daemon"]);
    assert!(args.foreground);
}

#[test]
fn foreground_combines_with_other_flags() {
    let args = parse(&[
        "extenddb-serve",
        "--config",
        "/etc/extenddb/extenddb.toml",
        "--port",
        "9000",
        "--foreground",
    ]);
    assert!(args.foreground);
    assert_eq!(args.config, "/etc/extenddb/extenddb.toml");
    assert_eq!(args.port, Some(9000));
}

#[test]
fn unknown_flag_is_rejected() {
    // Guard against accidental future renames silently dropping the flag.
    let result = TestCli::try_parse_from(["extenddb-serve", "--daemon-off"]);
    assert!(result.is_err());
}

#[test]
fn startup_warning_banner_is_prominent_and_actionable() {
    let rendered = render_startup_warnings(&[StartupWarning {
        title: "transaction_read_concern = \"majority\" (default: \"snapshot\").".to_owned(),
        details: vec![
            "Transactions run below DynamoDB isolation.".to_owned(),
            "Writes remain atomic and durable.".to_owned(),
            "Remove the setting to restore full DynamoDB transaction semantics.".to_owned(),
        ],
    }]);

    assert!(rendered.starts_with("\n  WARNING: transaction_read_concern"));
    assert!(rendered.contains("below DynamoDB isolation"));
    assert!(rendered.contains("Writes remain atomic and durable"));
    assert!(rendered.contains("restore full DynamoDB transaction semantics"));
}

fn bulk(section: serde_json::Value) -> extenddb_config::BulkConfig {
    serde_json::from_value(section).expect("a [bulk] section")
}

#[test]
fn the_dev_mode_banner_checks_the_s3_section_before_naming_it() {
    for store in ["s3", "filesystem"] {
        let section = bulk(serde_json::json!({
            "store": store,
            "filesystem": {"path": "/srv/bulk"},
            "s3": {"endpoint_url": "http://AKIDEXAMPLE:SECRETEXAMPLE@127.0.0.1:17070"},
        }));
        let error = dev_mode_s3_line(Some(&section), "us-east-1")
            .expect_err("user information is refused before the banner")
            .to_string();
        assert_eq!(
            error, "[bulk.s3] endpoint_url must not carry user information",
            "{store}"
        );
    }
}

#[test]
fn the_dev_mode_banner_names_the_endpoint_by_scheme_host_and_port() {
    let section = bulk(serde_json::json!({
        "store": "s3",
        "s3": {"endpoint_url": "http://127.0.0.1:17070/base/path", "force_path_style": true},
    }));
    assert_eq!(
        dev_mode_s3_line(Some(&section), "us-east-1").unwrap(),
        Some(
            "  Bulk store: s3 (region us-east-1, endpoint http://127.0.0.1:17070, path-style) \
             with the server's AWS credentials"
                .to_owned()
        )
    );
    let filesystem = bulk(serde_json::json!({"filesystem": {"path": "/srv/bulk"}}));
    assert_eq!(
        dev_mode_s3_line(Some(&filesystem), "us-east-1").unwrap(),
        None
    );
    assert_eq!(dev_mode_s3_line(None, "us-east-1").unwrap(), None);
}
