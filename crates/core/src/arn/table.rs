// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The wire answers for a `TableName` spelled as a table ARN, shared by the
//! server's authorization layer and the bulk put handler so the two cannot
//! answer the same spelling differently. The server layer runs first and
//! answers a request over HTTP; the handler answers the same spelling when
//! it is reached without that layer (dev mode and the engine tests).

use super::TableArnError;
use crate::error::DynamoDbError;

/// The longest table name the table-name rule admits.
const MAX_NAME_CHARS: usize = 255;
/// The shortest table name the table-name rule admits.
const MIN_NAME_CHARS: usize = 3;

/// The wire error for a table ARN that [`super::own_table_arn_name`] refused.
///
/// A well-formed table ARN of another account answers `Access is denied`,
/// and one of the caller's account in another region answers the region
/// message (both measured). A table ARN in the caller's partition, service,
/// region and account whose resource name is a sub-resource path (an index
/// ARN, for example) answers the service's resource-name format message
/// (measured beside an enum fault, which it is answered without). Every
/// other refused spelling keeps the project's `Invalid table ARN` text.
#[must_use]
pub fn table_arn_refusal(
    value: &str,
    region: &str,
    account: &str,
    error: TableArnError,
) -> DynamoDbError {
    match error {
        TableArnError::ForeignAccount => {
            DynamoDbError::AccessDeniedException("Access is denied".to_owned())
        }
        TableArnError::ForeignRegion => DynamoDbError::ValidationException(format!(
            "1 validation error detected: Invalid AWS region in '{value}'"
        )),
        TableArnError::Invalid => {
            let own_table_prefix = format!("arn:aws:dynamodb:{region}:{account}:table/");
            if let Some(name) = value.strip_prefix(&own_table_prefix)
                && name.contains('/')
            {
                DynamoDbError::ValidationException(format!(
                    "1 validation error detected: Value '{value}' at 'tableName' failed to \
                     satisfy constraint: Valid ARN format is \
                     'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', \
                     where Resource name must have length less than or equal to 255, Resource \
                     name must have length greater than or equal to 3, Resource name must \
                     satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
                ))
            } else {
                DynamoDbError::ValidationException(format!("Invalid table ARN: {value}"))
            }
        }
    }
}

/// Hold the name a table ARN of the caller's own account resolved to, to
/// the table-name rule. The service answers one clause naming the whole ARN,
/// the length bounds before the pattern (measured with `ab`, `..`, `a*`, a
/// 256-character name, `deni*`, `den ied`, and a 252-character name ending
/// in ` *`), where a bare name answers every failing rule in one envelope.
///
/// # Errors
///
/// Returns `ValidationException` with the measured clause.
pub fn check_table_arn_name(value: &str, name: &str) -> Result<(), DynamoDbError> {
    let chars = name.chars().count();
    let rule = if chars < MIN_NAME_CHARS {
        format!("must have length greater than or equal to {MIN_NAME_CHARS}")
    } else if chars > MAX_NAME_CHARS {
        format!("must have length less than or equal to {MAX_NAME_CHARS}")
    } else if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    {
        "must satisfy regular expression pattern: [a-zA-Z0-9_.-]+".to_owned()
    } else {
        return Ok(());
    };
    Err(DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value '{value}' at 'tableName' failed to satisfy \
         constraint: Table name of ARN {rule}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGION: &str = "us-east-1";
    const ACCOUNT: &str = "123456789012";

    fn message(error: &DynamoDbError) -> &str {
        match error {
            DynamoDbError::ValidationException(message)
            | DynamoDbError::AccessDeniedException(message) => message,
            other => panic!("unexpected error {other:?}"),
        }
    }

    fn arn(name: &str) -> String {
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{name}")
    }

    #[test]
    fn a_name_under_three_characters_answers_the_minimum() {
        for name in ["ab", "..", "a*"] {
            let value = arn(name);
            let error = check_table_arn_name(&value, name).unwrap_err();
            assert_eq!(
                message(&error),
                format!(
                    "1 validation error detected: Value '{value}' at 'tableName' failed to \
                     satisfy constraint: Table name of ARN must have length greater than or \
                     equal to 3"
                ),
                "{name}"
            );
        }
    }

    #[test]
    fn a_name_over_255_characters_answers_the_maximum() {
        let name = "a".repeat(256);
        let value = arn(&name);
        let error = check_table_arn_name(&value, &name).unwrap_err();
        assert!(
            message(&error)
                .ends_with("Table name of ARN must have length less than or equal to 255"),
            "{}",
            message(&error)
        );
    }

    #[test]
    fn a_name_outside_the_pattern_answers_the_pattern() {
        let long = format!("{} *", "b".repeat(250));
        for name in ["deni*", "den ied", long.as_str()] {
            let value = arn(name);
            let error = check_table_arn_name(&value, name).unwrap_err();
            assert_eq!(
                message(&error),
                format!(
                    "1 validation error detected: Value '{value}' at 'tableName' failed to \
                     satisfy constraint: Table name of ARN must satisfy regular expression \
                     pattern: [a-zA-Z0-9_.-]+"
                ),
                "{name}"
            );
        }
    }

    #[test]
    fn a_name_inside_the_rule_passes() {
        for name in ["abc", "a.b-c_d", &"z".repeat(255)] {
            check_table_arn_name(&arn(name), name).unwrap();
        }
    }

    #[test]
    fn an_index_arn_answers_the_resource_name_format() {
        let value = arn("x/index/gsi");
        let error = table_arn_refusal(&value, REGION, ACCOUNT, TableArnError::Invalid);
        assert!(
            message(&error)
                .contains("at 'tableName' failed to satisfy constraint: Valid ARN format is"),
            "{}",
            message(&error)
        );
    }

    #[test]
    fn other_refusals_keep_their_texts() {
        let foreign = "arn:aws:dynamodb:us-east-1:111111111111:table/xyz";
        assert_eq!(
            message(&table_arn_refusal(
                foreign,
                REGION,
                ACCOUNT,
                TableArnError::ForeignAccount
            )),
            "Access is denied"
        );
        let shape = "arn:aws:s3:::orders";
        assert_eq!(
            message(&table_arn_refusal(
                shape,
                REGION,
                ACCOUNT,
                TableArnError::Invalid
            )),
            format!("Invalid table ARN: {shape}")
        );
    }
}
