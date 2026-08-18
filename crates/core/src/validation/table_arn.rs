// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Resolve a table ARN supplied in place of a bare `TableName`.

use crate::error::DynamoDbError;

/// Resolve a `TableName` that may be supplied as a table ARN to its bare name.
///
/// A non-ARN value is returned unchanged. A table ARN
/// (`arn:<partition>:<vendor>:<region>:<account>:table/<name>`) resolves to
/// `<name>`; the account and region are ignored, so the name resolves within
/// the caller's account. This matches Amazon DynamoDB and DynamoDB Local, which
/// both accept a table ARN wherever a table name is expected. Index, non-table,
/// or malformed ARNs are rejected as a validation error.
///
/// # Errors
///
/// Returns `ValidationException` when `name` begins with `arn:` but is not a
/// well-formed `table/<name>` ARN.
pub fn resolve_table_arn(name: &str) -> Result<&str, DynamoDbError> {
    if !name.starts_with("arn:") {
        return Ok(name);
    }
    // arn:<partition>:<vendor>:<region>:<account>:<resource>. The 6th field
    // keeps everything after the 5th colon, including any slashes.
    let resource = match name.splitn(6, ':').nth(5) {
        Some(resource) if !resource.is_empty() => resource,
        _ => return Err(invalid_arn_format(name)),
    };
    let table = match resource.split_once('/') {
        Some(("table", table)) => table,
        Some(_) => return Err(constraint_error(name, "Invalid resource type")),
        None => return Err(invalid_arn_format(name)),
    };
    // A bare table name only: an index ARN (`table/T/index/i`) leaves a slash
    // here and fails the character check below.
    if table.len() < 3 {
        return Err(constraint_error(
            name,
            "Table name of ARN must have length greater than or equal to 3",
        ));
    }
    if table.len() > 255
        || !table
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    {
        return Err(invalid_arn_format(name));
    }
    Ok(table)
}

fn constraint_error(arn: &str, constraint: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value '{arn}' at 'tableName' failed to satisfy constraint: \
         {constraint}"
    ))
}

fn invalid_arn_format(arn: &str) -> DynamoDbError {
    constraint_error(
        arn,
        "Valid ARN format is 'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', \
         where Resource name must have length less than or equal to 255, Resource name must have length \
         greater than or equal to 3, Resource name must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_msg(name: &str) -> String {
        match resolve_table_arn(name) {
            Err(DynamoDbError::ValidationException(m)) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn non_arn_passthrough() {
        assert_eq!(resolve_table_arn("my-table").unwrap(), "my-table");
        // A plain name with a colon is not an ARN and is left for the normal
        // table-name validator to reject.
        assert_eq!(resolve_table_arn("foo:bar").unwrap(), "foo:bar");
    }

    #[test]
    fn table_arn_resolves_to_bare_name() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/my-table";
        assert_eq!(resolve_table_arn(arn).unwrap(), "my-table");
    }

    #[test]
    fn account_and_region_are_ignored() {
        // Any account/region resolves to the same bare name.
        let a = "arn:aws:dynamodb:us-east-1:000000000000:table/T.able_1";
        let b = "arn:aws:dynamodb:eu-west-1:999999999999:table/T.able_1";
        assert_eq!(resolve_table_arn(a).unwrap(), "T.able_1");
        assert_eq!(resolve_table_arn(b).unwrap(), "T.able_1");
    }

    #[test]
    fn index_arn_rejected() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/my-table/index/gsi1";
        assert!(err_msg(arn).contains("Valid ARN format is"));
        assert!(err_msg(arn).contains("[a-zA-Z0-9_.-]+"));
    }

    #[test]
    fn non_table_resource_type_rejected() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:stream/my-table";
        assert!(err_msg(arn).contains("Invalid resource type"));
    }

    #[test]
    fn empty_table_name_rejected() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/";
        assert!(
            err_msg(arn).contains("Table name of ARN must have length greater than or equal to 3")
        );
    }

    #[test]
    fn malformed_arn_missing_resource_rejected() {
        assert!(err_msg("arn:aws:dynamodb").contains("Valid ARN format is"));
        assert!(
            err_msg("arn:aws:dynamodb:us-east-1:123456789012:").contains("Valid ARN format is")
        );
    }
}
