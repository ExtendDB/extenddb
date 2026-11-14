// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The item authorizer the handler gives the engine for one request.
//!
//! The PartiQL operations name their tables inside their statements, so
//! `authorize_request` does not authorize them; the engine authorizes each
//! statement through this authorizer before it reads or writes (see
//! `extenddb_engine::ItemAuthorizer`). It evaluates the caller's policies
//! with `check_authorization`, against this deployment's ARN of the table or
//! index, with the request-level condition keys every other operation
//! carries and the item-level keys of the statement.

use std::sync::Arc;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::RequestParams;
use extenddb_core::types::AttributeValue;
use extenddb_engine::{ItemAccess, ItemAuthorization, ItemAuthorizer};

use crate::AppState;
use crate::authorization;
use crate::authz_resource::{self, AuthzTarget};
use crate::leading_keys::{NumberForm, extract_typed_value};

/// The item authorizer of a request signed by `identity`, from `source_ip`
/// with `user_agent`. In developer mode it allows every access, as
/// `authorize_request` skips the IAM decision there.
pub(crate) fn for_request(
    state: &Arc<AppState>,
    identity: AuthIdentity,
    account_id: &Arc<str>,
    source_ip: String,
    user_agent: Option<String>,
) -> Arc<dyn ItemAuthorizer> {
    if state.dev_mode {
        return Arc::new(AllowEveryAccess);
    }
    Arc::new(PolicyAuthorizer {
        state: Arc::clone(state),
        identity,
        account_id: Arc::clone(account_id),
        source_ip,
        user_agent,
    })
}

/// Developer mode: the request is authenticated, and no policy is evaluated.
struct AllowEveryAccess;

impl ItemAuthorizer for AllowEveryAccess {
    fn authorize<'a>(&'a self, _access: &'a ItemAccess) -> ItemAuthorization<'a> {
        Box::pin(async { Ok(()) })
    }
}

/// Evaluates the caller's policies for each access.
struct PolicyAuthorizer {
    state: Arc<AppState>,
    identity: AuthIdentity,
    account_id: Arc<str>,
    source_ip: String,
    user_agent: Option<String>,
}

impl ItemAuthorizer for PolicyAuthorizer {
    fn authorize<'a>(&'a self, access: &'a ItemAccess) -> ItemAuthorization<'a> {
        Box::pin(async move {
            // A statement names its table by name, so the denial names the
            // canonical ARN.
            let target = AuthzTarget::new(
                access.action,
                resource_arn(access, &self.state.region, &self.account_id),
            );
            let params = RequestParams {
                region: Some(self.state.region.to_string()),
                secure_transport: self.state.tls_enabled,
                source_ip: Some(self.source_ip.clone()),
                user_agent: self.user_agent.clone(),
                ..item_params(access)
            };
            Box::pin(authorization::check_authorization(
                self.state.authz_cache.as_ref(),
                &self.identity,
                &target,
                false,
                params,
            ))
            .await
        })
    }
}

/// The resource an access is authorized against: this deployment's ARN of
/// its table, or of its index for a read of an index, which a read of the
/// index is authorized against alone (measured 2026-11-03). The table name
/// has passed the table name rule, so it holds no `/` or `:`.
fn resource_arn(access: &ItemAccess, region: &str, account_id: &str) -> String {
    let table = authz_resource::table_resource_arn(&access.table, region, account_id);
    match &access.index {
        Some(index) => format!("{table}/index/{index}"),
        None => table,
    }
}

/// The item-level condition keys of an access.
fn item_params(access: &ItemAccess) -> RequestParams {
    RequestParams {
        leading_keys: access.partition_keys.as_deref().and_then(leading_keys),
        attributes: access.attributes.clone(),
        select: access.select.map(ToOwned::to_owned),
        return_values: access.return_values.map(ToOwned::to_owned),
        enclosing_operation: access.enclosing_operation.map(ToOwned::to_owned),
        full_table_scan: access.full_table_scan,
        ..RequestParams::default()
    }
}

/// The `dynamodb:LeadingKeys` values of partition key values, or `None`
/// when one cannot be read, which leaves the key absent as it does for the
/// classic operations. A number is written as Java's `BigDecimal` prints the
/// text the statement or the parameter gives it (`NumberForm::Decimal`):
/// under `ForAllValues` `["10"]`, `pk = 10` and `pk = 010` run and `pk =
/// 10.0`, `pk = 1E1` and the parameter `{"N": "10.0"}` are refused, in all
/// three operations (measured 2026-11-03).
fn leading_keys(values: &[AttributeValue]) -> Option<Vec<String>> {
    values
        .iter()
        .map(|value| {
            let wire = serde_json::to_value(value).ok()?;
            extract_typed_value(&wire, NumberForm::Decimal)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access(table: &str, index: Option<&str>) -> ItemAccess {
        ItemAccess {
            action: "PartiQLSelect",
            table: table.to_owned(),
            index: index.map(ToOwned::to_owned),
            ..ItemAccess::default()
        }
    }

    #[test]
    fn a_table_read_is_authorized_against_the_table_arn() {
        assert_eq!(
            resource_arn(&access("orders", None), "us-east-1", "123456789012"),
            "arn:aws:dynamodb:us-east-1:123456789012:table/orders"
        );
    }

    #[test]
    fn an_index_read_is_authorized_against_the_index_arn() {
        assert_eq!(
            resource_arn(&access("orders", Some("gix")), "eu-west-1", "123456789012"),
            "arn:aws:dynamodb:eu-west-1:123456789012:table/orders/index/gix"
        );
    }

    #[test]
    fn a_number_partition_value_keeps_the_scale_it_is_written_with() {
        // Measured 2026-11-03 under ForAllValues ["10"] and ["10.0"].
        for (text, written) in [
            ("10", "10"),
            ("010", "10"),
            ("10.0", "10.0"),
            ("1E1", "1E+1"),
            ("010.0", "10.0"),
            ("-10", "-10"),
        ] {
            assert_eq!(
                leading_keys(&[AttributeValue::N(text.to_owned())]),
                Some(vec![written.to_owned()]),
                "{text}"
            );
        }
    }

    #[test]
    fn string_and_binary_partition_values_are_written_as_sent() {
        let values = [
            AttributeValue::S("alice".to_owned()),
            AttributeValue::B(vec![1, 2]),
        ];
        assert_eq!(
            leading_keys(&values),
            Some(vec!["alice".to_owned(), "AQI=".to_owned()])
        );
    }

    #[test]
    fn a_number_that_cannot_be_read_leaves_the_key_absent() {
        let values = [
            AttributeValue::S("a".to_owned()),
            AttributeValue::N("not a number".to_owned()),
        ];
        assert_eq!(leading_keys(&values), None);
    }

    #[test]
    fn the_item_keys_of_an_access_reach_the_policy_request() {
        let access = ItemAccess {
            action: "PartiQLUpdate",
            table: "t".to_owned(),
            partition_keys: Some(vec![AttributeValue::S("a".to_owned())]),
            attributes: Some(vec!["pk".to_owned(), "q".to_owned()]),
            return_values: Some("ALL_NEW"),
            enclosing_operation: Some("ExecuteTransaction"),
            full_table_scan: Some(false),
            ..ItemAccess::default()
        };
        let params = item_params(&access);
        assert_eq!(params.leading_keys, Some(vec!["a".to_owned()]));
        assert_eq!(
            params.attributes,
            Some(vec!["pk".to_owned(), "q".to_owned()])
        );
        assert_eq!(params.return_values.as_deref(), Some("ALL_NEW"));
        assert_eq!(
            params.enclosing_operation.as_deref(),
            Some("ExecuteTransaction")
        );
        assert_eq!(params.full_table_scan, Some(false));
        assert_eq!(params.select, None);
    }
}
