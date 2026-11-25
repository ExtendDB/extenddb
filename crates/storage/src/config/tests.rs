// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the startup warnings of `extenddb_storage::config`.

use super::server_region_warning;

#[test]
fn a_region_the_backup_arn_parser_accepts_draws_no_warning() {
    for region in ["us-east-1", "local", "eu-west-2", "a", "0-9"] {
        assert!(server_region_warning(region).is_none(), "{region}");
    }
}

#[test]
fn a_region_the_backup_arn_parser_refuses_is_warned_about_with_the_consequence() {
    for region in [
        "LOCAL",
        "us_east_1",
        "dev.local",
        "",
        "us east",
        "Us-East-1",
        "ü",
    ] {
        let warning = server_region_warning(region).unwrap_or_else(|| panic!("{region:?}"));
        assert!(warning.title.contains("server.region"), "{region:?}");
        assert!(warning.title.contains(&format!("{region:?}")), "{region:?}");
        let text = warning.log_message();
        assert!(
            text.contains("Invalid Request: BackupArn is not valid"),
            "{region:?}: {text}"
        );
        assert!(text.contains("DescribeBackup"), "{region:?}: {text}");
        assert!(
            text.contains("ListBackups refuses a cursor that is such an ARN"),
            "{region:?}: {text}"
        );
        assert!(!text.contains("first page"), "{region:?}: {text}");
        assert!(
            text.contains("CreateBackup answers ValidationException naming server.region"),
            "{region:?}: {text}"
        );
        // The warning agrees with the parser it speaks for.
        let arn =
            crate::util::backup_arn(region, "123456789012", "Music", "01489602797149-73d8d5bc");
        assert!(crate::util::parse_backup_arn(&arn).is_none(), "{region:?}");
    }
    let arn = crate::util::backup_arn(
        "us-east-1",
        "123456789012",
        "Music",
        "01489602797149-73d8d5bc",
    );
    assert!(crate::util::parse_backup_arn(&arn).is_some());
}
