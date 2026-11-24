// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy resource derivation for the bulk operations.

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::RequestParams;
use extenddb_core::error::DynamoDbError;
use serde_json::Value;

use crate::AppState;
use crate::authorization;
use crate::authz_resource::AuthzTarget;

/// Build a `DynamoDB` table ARN for authorization.
///
/// If no table name is available, uses `*` as the resource.
pub(super) fn build_resource_arn(
    region: &str,
    account_id: &str,
    table_name: Option<&str>,
) -> String {
    match table_name {
        Some(name) => format!("arn:aws:dynamodb:{region}:{account_id}:table/{name}"),
        None => format!("arn:aws:dynamodb:{region}:{account_id}:table/*"),
    }
}

/// The account's bulk-operation wildcard resource.
pub(super) fn bulk_operation_wildcard(region: &str, account_id: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account_id}:bulk-operation/*")
}

/// The control-plane action `StartBulkPutItems` requires, in the
/// operation-name form `check_authorization` prefixes with `dynamodb:`.
/// Account scoped; evaluated against the bulk-operation wildcard.
pub(crate) const START_BULK_OPERATION_ACTION: &str = "StartBulkOperation";

/// The data-plane action `StartBulkPutItems` requires, in the same form.
/// Table scoped; evaluated against the table ARN.
pub(crate) const BULK_PUT_ITEMS_ACTION: &str = "BulkPutItems";

/// The policy resource for a bulk operation request; `None` for every
/// other operation, which the caller authorizes on the generic table path.
///
/// `StartBulkPutItems` derives the table ARN, which the caller evaluates
/// for the data-plane action after the account-scoped control-plane
/// action has been allowed. A `TableName` spelled as
/// the caller's own table ARN resolves to its bare name through
/// `extenddb_core::arn::own_table_arn_name`, the same parse the handler
/// applies, or the ARN spelling would be evaluated embedded inside another
/// ARN and an explicit Deny written against the table would not bind. A
/// well-formed table ARN naming another account is refused with
/// `AccessDeniedException`, `Access is denied` (measured), one of the
/// caller's account naming another region with `ValidationException`,
/// `1 validation error detected: Invalid AWS region in '{arn}'`
/// (measured), and any other ARN spelling with the handler's `Invalid
/// table ARN` message, all before any policy is fetched or evaluated.
///
/// `DescribeBulkOperation` and `CancelBulkOperation` evaluate the body's
/// `BulkOperationArn` once it passes every check, in the order
/// `extenddb_core::arn::BulkOperationArnError` documents: the shape
/// (layer 1), then the account and the region, then the partition, the
/// service and the operation id (layer 2). A layer 1 or layer 2 failure
/// returns that `ValidationException`. An ARN of that shape naming
/// another account returns the denial the authorization layer would
/// produce (measured), even when its id is malformed, and one naming
/// another region returns the handler's not-found error, each with no
/// resource derived, no policy fetched, and no storage read, so neither
/// answer depends on whether the foreign operation exists.
///
/// `ListBulkOperations` evaluates the account's bulk-operation wildcard.
///
/// A body whose `TableName` or `BulkOperationArn` is absent or not a
/// string evaluates the operation's wildcard resource, as the generic
/// path does for a missing `TableName`; the handler rejects such a body
/// after authorization.
///
/// # Errors
///
/// Returns the two parse-layer `ValidationException`s, the
/// foreign-account `AccessDeniedException`, and the foreign-region
/// `ResourceNotFoundException` described above; the caller returns them
/// without evaluating any policy.
pub(super) fn bulk_operation_authz_resource(
    operation: &str,
    input: &Value,
    region: &str,
    account_id: &str,
    identity: &AuthIdentity,
) -> Result<Option<String>, DynamoDbError> {
    match operation {
        "StartBulkPutItems" => {
            let table_name = match input.get("TableName").and_then(Value::as_str) {
                Some(value) if value.starts_with("arn:") => {
                    // The account and region checks of validation step 2 run
                    // here, ahead of policy evaluation: a table ARN of
                    // another account or region is answered with the
                    // handler's own message and never reaches a policy. The
                    // tableName rules (an index ARN's format message, the
                    // name rule on an own ARN) run here too, so a
                    // validation fault is answered before any policy, with
                    // the texts the handler shares through
                    // `extenddb_core::arn`.
                    let name = extenddb_core::arn::own_table_arn_name(value, region, account_id)
                        .map_err(|error| {
                            extenddb_core::arn::table_arn_refusal(value, region, account_id, error)
                        })?;
                    extenddb_core::arn::check_table_arn_name(value, &name)?;
                    Some(name)
                }
                other => other.map(ToOwned::to_owned),
            };
            Ok(Some(build_resource_arn(
                region,
                account_id,
                table_name.as_deref(),
            )))
        }
        "DescribeBulkOperation" | "CancelBulkOperation" => {
            let Some(value) = input.get("BulkOperationArn").and_then(Value::as_str) else {
                return Ok(Some(bulk_operation_wildcard(region, account_id)));
            };
            // The account comparison runs ahead of the operation id check
            // (measured): a foreign-account ARN is denied whatever shape
            // its id has, so a caller cannot probe another account's id
            // shapes through the validation message.
            match extenddb_core::arn::own_bulk_operation_arn(value, region, account_id) {
                Ok(_) => Ok(Some(value.to_owned())),
                Err(extenddb_core::arn::BulkOperationArnError::ForeignAccount) => Err(denial(
                    &principal_arn(identity),
                    &format!("dynamodb:{operation}"),
                    value,
                )),
                Err(extenddb_core::arn::BulkOperationArnError::ForeignRegion) => {
                    Err(extenddb_core::arn::bulk_operation_does_not_exist(value))
                }
                Err(extenddb_core::arn::BulkOperationArnError::Shape) => {
                    Err(extenddb_core::arn::bulk_operation_arn_shape_error())
                }
                Err(extenddb_core::arn::BulkOperationArnError::InvalidId) => {
                    Err(extenddb_core::arn::bulk_operation_arn_id_error())
                }
            }
        }
        "ListBulkOperations" => Ok(Some(bulk_operation_wildcard(region, account_id))),
        _ => Ok(None),
    }
}

/// The ARN of the authenticated principal, as denial messages name it.
pub(crate) fn principal_arn(identity: &AuthIdentity) -> String {
    match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => format!("arn:aws:iam::{account_id}:user/{user_name}"),
        AuthIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
        } => format!("arn:aws:iam::{account_id}:assumed-role/{role_name}/{session_name}"),
    }
}

/// The denial a bulk operation ARN naming another account answers before any
/// policy is evaluated: the service's text for that case carries no
/// explanation suffix, where a denial decided by policy evaluation does (see
/// `authorization::deny_message`).
pub(crate) fn denial(principal_arn: &str, action: &str, resource_arn: &str) -> DynamoDbError {
    DynamoDbError::AccessDeniedException(format!(
        "User: {principal_arn} is not authorized to perform: {action} on resource: {resource_arn}"
    ))
}

/// Authorize a bulk operation against its operation-specific resources.
/// Returns `Ok(true)` when `operation` is one of the four bulk operations
/// and every check allowed it, and `Ok(false)` for any other operation,
/// which the caller then authorizes on its generic path.
pub(super) async fn authorize_bulk(
    state: &AppState,
    identity: &AuthIdentity,
    operation: &str,
    input: &Value,
    account_id: &str,
    table_name: Option<&str>,
    base_params: &(dyn Fn() -> RequestParams + Sync),
) -> Result<bool, DynamoDbError> {
    // Bulk operations evaluate operation-specific resources: the table ARN
    // for StartBulkPutItems (with an own-ARN TableName spelling resolved to
    // its bare name so a table-scoped Deny binds on both spellings), the
    // body's BulkOperationArn for Describe and Cancel, and the account's
    // bulk-operation wildcard for List. Describe and Cancel errors from the
    // derivation return before any policy evaluation: a layer 1 or layer 2
    // invalid ARN answers its ValidationException, a well-formed
    // foreign-account ARN answers the denial (measured on the service), and a
    // well-formed foreign-region ARN answers the handler's not-found error, so
    // a foreign ARN is never evaluated against any policy and neither answer
    // depends on whether the foreign operation exists. Each check carries the
    // same request-level condition keys as every other operation, through
    // `base_params`.
    let bulk_resource =
        bulk_operation_authz_resource(operation, input, &state.region, account_id, identity)?;

    // StartBulkPutItems is gated by two IAM actions on the hosted service: the
    // account-scoped control-plane action `dynamodb:StartBulkOperation`,
    // evaluated against the account's bulk-operation wildcard because the
    // operation ARN does not exist until the row is inserted, and the
    // table-scoped data-plane action `dynamodb:BulkPutItems`, evaluated against
    // the table ARN the derivation resolved. Both must allow. The control-plane
    // check runs first so a caller with neither grant is told about the
    // account-scoped action. A grant spelled `dynamodb:StartBulkPutItems` is
    // not an action the service defines and does not authorize a start.
    if operation == "StartBulkPutItems" {
        let table_arn = bulk_resource
            .unwrap_or_else(|| build_resource_arn(&state.region, account_id, table_name));
        let checks = [
            (
                START_BULK_OPERATION_ACTION,
                bulk_operation_wildcard(&state.region, account_id),
            ),
            (BULK_PUT_ITEMS_ACTION, table_arn),
        ];
        for (action, resource_arn) in checks {
            authorization::check_authorization(
                state.authz_cache.as_ref(),
                identity,
                &AuthzTarget::new(action, resource_arn),
                false,
                base_params(),
            )
            .await?;
        }
        return Ok(true);
    }

    // DescribeBulkOperation, CancelBulkOperation and ListBulkOperations evaluate
    // the resource the derivation produced (the operation ARN, or the account's
    // bulk-operation wildcard) under the operation's own action. The derivation
    // returns `None` for every non-bulk operation, which answers `Ok(false)` so
    // the caller authorizes it on its generic path.
    if let Some(resource_arn) = bulk_resource {
        authorization::check_authorization(
            state.authz_cache.as_ref(),
            identity,
            &AuthzTarget::new(operation, resource_arn),
            false,
            base_params(),
        )
        .await?;
        return Ok(true);
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const REGION: &str = "us-east-1";
    const ACCOUNT: &str = "123456789012";
    const OPERATION_ARN: &str =
        "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/01789060589767-da9ddd4f";

    /// The resource `authorize_request` evaluates for a bulk operation
    /// body, from the same derivation function it calls. `authorize_request`
    /// itself takes an `AppState`, which these tests cannot build (it needs
    /// a storage engine the server crate's tests do not have), so the tests
    /// pin the derivation function; the function's answer is everything the
    /// bulk branch adds before the shared `check_authorization` call.
    fn test_identity() -> AuthIdentity {
        AuthIdentity::User {
            account_id: ACCOUNT.to_owned(),
            user_name: "tester".to_owned(),
        }
    }

    fn derived_resource(operation: &str, body: &serde_json::Value) -> String {
        bulk_operation_authz_resource(operation, body, REGION, ACCOUNT, &test_identity())
            .expect("this body derives a resource")
            .expect("this operation is a bulk operation")
    }

    fn derived_error(operation: &str, body: &serde_json::Value) -> DynamoDbError {
        bulk_operation_authz_resource(operation, body, REGION, ACCOUNT, &test_identity())
            .expect_err("this body must be refused before evaluation")
    }

    /// Evaluate one action against one resource under the given policy.
    fn decide(
        policy: &str,
        action: &str,
        resource: &str,
    ) -> extenddb_auth::policy::evaluator::AuthzDecision {
        use extenddb_auth::policy::context::{RequestContext, RequestParams};
        use extenddb_auth::policy::document::PolicyDocument;
        use extenddb_auth::policy::evaluator::evaluate_policies_arc;
        use std::collections::HashMap;

        let policies = vec![std::sync::Arc::new(
            PolicyDocument::from_json(policy).expect("the policy parses"),
        )];
        let context = RequestContext::build(
            HashMap::new(),
            HashMap::new(),
            false,
            RequestParams::default(),
        );
        evaluate_policies_arc(&policies, None, None, action, resource, &context)
    }

    #[test]
    fn a_deny_on_the_table_arn_binds_when_table_name_is_the_arn() {
        use extenddb_auth::policy::evaluator::AuthzDecision;

        let arn = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/orders");
        let policy = format!(
            r#"{{
                "Version": "2012-10-17",
                "Statement": [
                    {{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}},
                    {{"Effect": "Deny", "Action": "dynamodb:BulkPutItems",
                      "Resource": "{arn}"}}
                ]
            }}"#
        );
        let evaluate = |table_name: &str| {
            decide(
                &policy,
                "dynamodb:BulkPutItems",
                &derived_resource("StartBulkPutItems", &json!({ "TableName": table_name })),
            )
        };

        // The ARN spelling resolves to the bare name, so the resource the
        // policy is evaluated against is the table's ARN and the explicit
        // Deny binds.
        assert_eq!(evaluate(&arn), AuthzDecision::Deny);
        // The bare spelling reaches the same decision.
        assert_eq!(evaluate("orders"), AuthzDecision::Deny);
        // Without the resolution the ARN would be embedded inside another
        // ARN, missing the Deny; this is the gap the resolution closes.
        assert_eq!(
            decide(
                &policy,
                "dynamodb:BulkPutItems",
                &build_resource_arn(REGION, ACCOUNT, Some(&arn)),
            ),
            AuthzDecision::Allow
        );
    }

    #[test]
    fn start_allowed_on_table_a_is_denied_on_table_b_for_both_spellings() {
        use extenddb_auth::policy::evaluator::AuthzDecision;

        let policy = format!(
            r#"{{
                "Version": "2012-10-17",
                "Statement": [
                    {{"Effect": "Allow", "Action": "dynamodb:BulkPutItems",
                      "Resource": "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/alpha"}}
                ]
            }}"#
        );
        let evaluate = |table_name: &str| {
            decide(
                &policy,
                "dynamodb:BulkPutItems",
                &derived_resource("StartBulkPutItems", &json!({ "TableName": table_name })),
            )
        };

        assert_eq!(evaluate("alpha"), AuthzDecision::Allow);
        assert_eq!(
            evaluate(&format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/alpha")),
            AuthzDecision::Allow
        );
        assert_eq!(evaluate("beta"), AuthzDecision::Deny);
        assert_eq!(
            evaluate(&format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/beta")),
            AuthzDecision::Deny
        );
    }

    #[test]
    fn describe_and_cancel_evaluate_the_operation_arn_itself() {
        for operation in ["DescribeBulkOperation", "CancelBulkOperation"] {
            assert_eq!(
                derived_resource(operation, &json!({ "BulkOperationArn": OPERATION_ARN })),
                OPERATION_ARN
            );
        }
    }

    #[test]
    fn a_foreign_account_arn_is_denied_with_no_resource_derived() {
        // The derivation answers the denial itself (measured) and produces
        // no resource, so the caller has nothing to evaluate a policy
        // against and the answer never depends on whether the foreign
        // operation exists.
        let foreign_account =
            "arn:aws:dynamodb:us-east-1:999999999999:bulk-operation/01789060589767-da9ddd4f";
        for operation in ["DescribeBulkOperation", "CancelBulkOperation"] {
            match derived_error(operation, &json!({ "BulkOperationArn": foreign_account })) {
                DynamoDbError::AccessDeniedException(message) => assert_eq!(
                    message,
                    format!(
                        "User: arn:aws:iam::{ACCOUNT}:user/tester is not authorized \
                         to perform: dynamodb:{operation} on resource: {foreign_account}"
                    )
                ),
                other => panic!("expected AccessDeniedException, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_foreign_region_arn_is_not_found_with_no_resource_derived() {
        let foreign_region =
            "arn:aws:dynamodb:eu-west-1:123456789012:bulk-operation/01789060589767-da9ddd4f";
        for operation in ["DescribeBulkOperation", "CancelBulkOperation"] {
            match derived_error(operation, &json!({ "BulkOperationArn": foreign_region })) {
                DynamoDbError::ResourceNotFoundException(message) => assert_eq!(
                    message,
                    format!("Bulk operation with arn: {foreign_region} does not exist")
                ),
                other => panic!("expected ResourceNotFoundException, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_layer_one_or_layer_two_invalid_arn_answers_its_message() {
        let layer_two =
            "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/not-a-valid-id-here";
        for operation in ["DescribeBulkOperation", "CancelBulkOperation"] {
            match derived_error(operation, &json!({ "BulkOperationArn": "orders" })) {
                DynamoDbError::ValidationException(message) => {
                    assert_eq!(message, "Invalid Bulk operation ARN");
                }
                other => panic!("expected ValidationException, got {other:?}"),
            }
            match derived_error(operation, &json!({ "BulkOperationArn": layer_two })) {
                DynamoDbError::ValidationException(message) => {
                    assert_eq!(message, "Invalid Request: Bulk operation ARN is invalid.");
                }
                other => panic!("expected ValidationException, got {other:?}"),
            }
        }
    }

    #[test]
    fn list_is_evaluated_against_the_account_bulk_operation_wildcard() {
        use extenddb_auth::policy::evaluator::AuthzDecision;

        let resource = derived_resource("ListBulkOperations", &json!({}));
        assert_eq!(
            resource,
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/*")
        );

        // A policy granting the account's bulk-operation wildcard covers the
        // resource; a policy granting one operation ARN does not, so a List
        // grant has to be written against the wildcard.
        let wildcard_grant = format!(
            r#"{{"Version": "2012-10-17", "Statement": [
                {{"Effect": "Allow", "Action": "dynamodb:ListBulkOperations",
                  "Resource": "arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/*"}}]}}"#
        );
        let single_grant = format!(
            r#"{{"Version": "2012-10-17", "Statement": [
                {{"Effect": "Allow", "Action": "dynamodb:ListBulkOperations",
                  "Resource": "{OPERATION_ARN}"}}]}}"#
        );
        assert_eq!(
            decide(&wildcard_grant, "dynamodb:ListBulkOperations", &resource),
            AuthzDecision::Allow
        );
        assert_eq!(
            decide(&single_grant, "dynamodb:ListBulkOperations", &resource),
            AuthzDecision::Deny
        );
    }

    #[test]
    fn a_body_without_the_field_evaluates_the_operation_wildcard() {
        // The handler rejects such a body after authorization; the
        // derivation follows the generic path's convention for a missing
        // TableName and evaluates the wildcard rather than refusing.
        assert_eq!(
            derived_resource("StartBulkPutItems", &json!({})),
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/*")
        );
        for operation in ["DescribeBulkOperation", "CancelBulkOperation"] {
            assert_eq!(
                derived_resource(operation, &json!({ "BulkOperationArn": 7 })),
                format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/*")
            );
        }
    }

    #[test]
    fn other_operations_derive_no_bulk_resource() {
        for operation in [
            "GetItem",
            "PutItem",
            "Query",
            "ListTables",
            "BatchWriteItem",
        ] {
            assert_eq!(
                bulk_operation_authz_resource(
                    operation,
                    &json!({ "TableName": "orders" }),
                    REGION,
                    ACCOUNT,
                    &test_identity()
                )
                .expect("no error for non-bulk operations"),
                None,
                "{operation} must stay on its existing path"
            );
        }
    }
}
