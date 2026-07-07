// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` implementation for the DynamoDB-at-home backend.
//!
//! TTL is handled natively by DynamoDB — no ExtendDB TTL worker is needed.
//! Tag methods resolve the ExtendDB ARN to the underlying DynamoDB table ARN
//! via `DescribeTable`, then forward to the DynamoDB Tagging API.
//! Table listing uses `ListTables` filtered to the account prefix.

use futures::future::BoxFuture;

use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::error::StorageError;
use extenddb_storage::{MetadataEngine, TtlTableInfo};

use crate::DynamoEngine;

impl MetadataEngine for DynamoEngine {
    // ── TTL ───────────────────────────────────────────────────────────────────

    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let physical = self.namer.physical(&account_id, &table_name);
            let out = self
                .client
                .describe_time_to_live()
                .table_name(physical)
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;

            let (time_to_live_status, attribute_name) = match out.time_to_live_description() {
                Some(sdk_ttl) => {
                    let status = match sdk_ttl.time_to_live_status() {
                        Some(aws_sdk_dynamodb::types::TimeToLiveStatus::Enabled)
                        | Some(aws_sdk_dynamodb::types::TimeToLiveStatus::Enabling) => {
                            TimeToLiveStatus::Enabled
                        }
                        _ => TimeToLiveStatus::Disabled,
                    };
                    (status, sdk_ttl.attribute_name().map(str::to_owned))
                }
                None => (TimeToLiveStatus::Disabled, None),
            };
            Ok(TimeToLiveDescription {
                time_to_live_status,
                attribute_name,
            })
        })
    }

    fn update_ttl(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let attribute_name = attribute_name.to_owned();
        Box::pin(async move {
            let physical = self.namer.physical(&account_id, &table_name);
            let spec = aws_sdk_dynamodb::types::TimeToLiveSpecification::builder()
                .enabled(enabled)
                .attribute_name(attribute_name)
                .build()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            self.client
                .update_time_to_live()
                .table_name(physical)
                .time_to_live_specification(spec)
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;
            Ok(())
        })
    }

    // ── Tags ──────────────────────────────────────────────────────────────────
    //
    // ExtendDB ARNs follow the same format as real DynamoDB ARNs:
    //   arn:aws:dynamodb:<region>:<account_id>:table/<logical_table_name>
    //
    // We parse the incoming ARN to recover (account_id, logical_table_name),
    // resolve the physical table name, then call DescribeTable to get the
    // real DynamoDB table ARN, which is what the Tagging API requires.

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_owned();
        let tags = tags.to_owned();
        Box::pin(async move {
            let real_arn = self.resolve_table_arn_from_extenddb_arn(&arn).await?;
            let sdk_tags: Result<Vec<_>, _> = tags
                .iter()
                .map(|t| {
                    aws_sdk_dynamodb::types::Tag::builder()
                        .key(t.key.clone())
                        .value(t.value.clone())
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))
                })
                .collect();
            self.client
                .tag_resource()
                .resource_arn(real_arn)
                .set_tags(Some(sdk_tags?))
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;
            Ok(())
        })
    }

    fn untag_resource(
        &self,
        arn: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_owned();
        let tag_keys = tag_keys.to_owned();
        Box::pin(async move {
            let real_arn = self.resolve_table_arn_from_extenddb_arn(&arn).await?;
            let mut req = self.client.untag_resource().resource_arn(real_arn);
            for key in &tag_keys {
                req = req.tag_keys(key.clone());
            }
            req.send().await.map_err(crate::errors::from_sdk_error)?;
            Ok(())
        })
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            let real_arn = self.resolve_table_arn_from_extenddb_arn(&arn).await?;
            let out = self
                .client
                .list_tags_of_resource()
                .resource_arn(real_arn)
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;
            let tags = out
                .tags()
                .iter()
                .map(|t| Tag {
                    key: t.key().to_owned(),
                    value: t.value().to_owned(),
                })
                .collect();
            Ok(tags)
        })
    }

    // ── Table listing ─────────────────────────────────────────────────────────

    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let prefix = self.namer.account_prefix(&account_id);
            let mut table_names = Vec::new();
            let mut exclusive_start: Option<String> = None;

            loop {
                let mut req = self.client.list_tables();
                if let Some(start) = exclusive_start.take() {
                    req = req.exclusive_start_table_name(start);
                }
                let out = req.send().await.map_err(crate::errors::from_sdk_error)?;

                for phys in out.table_names() {
                    if phys.starts_with(&prefix)
                        && let Ok(logical) = self.namer.logical(&account_id, phys)
                    {
                        table_names.push(logical);
                    }
                }

                match out.last_evaluated_table_name() {
                    Some(last) => exclusive_start = Some(last.to_owned()),
                    None => break,
                }
            }

            Ok(table_names)
        })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move {
            // Collect all physical table names across all accounts.
            // Physical table format: <prefix><account_id>_<table_name>
            // We detect the account_id by stripping the fixed prefix and splitting on '_'.
            let table_prefix = &self.namer.account_prefix(""); // prefix without account part: just self.prefix
            let mut pairs = Vec::new();
            let mut exclusive_start: Option<String> = None;

            loop {
                let mut req = self.client.list_tables();
                if let Some(start) = exclusive_start.take() {
                    req = req.exclusive_start_table_name(start);
                }
                let out = req.send().await.map_err(crate::errors::from_sdk_error)?;

                for phys in out.table_names() {
                    if let Some((account_id, table_name)) = parse_physical_table(phys, table_prefix)
                    {
                        pairs.push((account_id, table_name));
                    }
                }

                match out.last_evaluated_table_name() {
                    Some(last) => exclusive_start = Some(last.to_owned()),
                    None => break,
                }
            }

            Ok(pairs)
        })
    }

    fn refresh_table_size(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // DynamoDB maintains table size and item counts natively via DescribeTable.
        // ExtendDB's refresh_table_size exists for backends (Postgres) that cache
        // these metrics separately. Here, there is nothing to recompute.
        Box::pin(async { Ok(()) })
    }

    // ── TTL worker no-ops ─────────────────────────────────────────────────────
    //
    // DynamoDB performs TTL deletion itself, asynchronously, in the background.
    // ExtendDB's TTL worker (which calls these methods) has nothing to do for
    // this backend. All methods below return empty/unit successes immediately.

    fn tables_with_ttl(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        // DynamoDB handles TTL expiry internally; no table list needed by ExtendDB worker.
        Box::pin(async { Ok(vec![]) })
    }

    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        // DynamoDB handles TTL expiry internally; no cross-account table list needed.
        Box::pin(async { Ok(vec![]) })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        // DynamoDB handles TTL expiry internally; no TTL-index-ready concept applies.
        Box::pin(async { Ok(vec![]) })
    }

    fn create_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // DynamoDB manages its own TTL machinery; no secondary index for expiry needed.
        Box::pin(async { Ok(()) })
    }

    fn drop_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // DynamoDB manages its own TTL machinery; no index to drop.
        Box::pin(async { Ok(()) })
    }

    fn find_expired_items_indexed(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        // DynamoDB expires items itself; ExtendDB's TTL worker does not scan for expired items.
        Box::pin(async { Ok(vec![]) })
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

impl DynamoEngine {
    /// Parse an ExtendDB table ARN to extract `(account_id, logical_table_name)`.
    ///
    /// ExtendDB ARNs use the same structure as real DynamoDB ARNs:
    ///   `arn:aws:dynamodb:<region>:<account_id>:table/<logical_table_name>`
    ///
    /// We then resolve the physical name and call DescribeTable to get the
    /// real DynamoDB ARN (needed for the Tagging API).
    ///
    /// # Concern
    ///
    /// This is best-effort: if the incoming ARN is a stream or index ARN rather
    /// than a table ARN, parsing will fail and an error will be returned.
    /// The implementation only handles `arn:aws:dynamodb:...:table/<name>`.
    async fn resolve_table_arn_from_extenddb_arn(&self, arn: &str) -> Result<String, StorageError> {
        // ARN format: arn:aws:dynamodb:<region>:<account_id>:table/<logical_name>
        // Split on ':' → ["arn", "aws", "dynamodb", "<region>", "<account_id>", "table/<name>"]
        let segments: Vec<&str> = arn.splitn(6, ':').collect();
        if segments.len() < 6 {
            return Err(StorageError::Validation(format!(
                "Invalid ExtendDB ARN (too few segments): {arn}"
            )));
        }
        let account_id = segments[4];
        let resource = segments[5]; // e.g. "table/MyTable"
        let logical_table_name = resource.strip_prefix("table/").ok_or_else(|| {
            StorageError::Validation(format!(
                "Only table ARNs are supported for tag operations (got resource '{resource}')"
            ))
        })?;

        let physical = self.namer.physical(account_id, logical_table_name);
        let out = self
            .client
            .describe_table()
            .table_name(&physical)
            .send()
            .await
            .map_err(crate::errors::from_sdk_error)?;

        out.table()
            .and_then(|t| t.table_arn())
            .map(str::to_owned)
            .ok_or_else(|| {
                StorageError::Internal(format!(
                    "DescribeTable for '{physical}' returned no TableArn"
                ))
            })
    }
}

/// Parse a physical table name (`<prefix><account_id>_<table_name>`) back into
/// `(account_id, logical_table_name)`.
///
/// `table_prefix` is the fixed prefix without the account part (e.g. `"athome_"`).
///
/// Returns `None` if the name does not match the expected pattern.
fn parse_physical_table(physical: &str, table_prefix: &str) -> Option<(String, String)> {
    let rest = physical.strip_prefix(table_prefix)?;
    // rest = "<account_id>_<table_name>"
    // account_ids are 12 digits, but we split at the first '_' following the prefix.
    let sep = rest.find('_')?;
    let account_id = &rest[..sep];
    let table_name = &rest[sep + 1..];
    if account_id.is_empty() || table_name.is_empty() {
        return None;
    }
    Some((account_id.to_owned(), table_name.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_physical_table_round_trips() {
        assert_eq!(
            parse_physical_table("athome_123456789012_Orders", "athome_"),
            Some(("123456789012".to_owned(), "Orders".to_owned()))
        );
    }

    #[test]
    fn parse_physical_table_handles_underscores_in_name() {
        assert_eq!(
            parse_physical_table("athome_123456789012_my_orders_v2", "athome_"),
            Some(("123456789012".to_owned(), "my_orders_v2".to_owned()))
        );
    }

    #[test]
    fn parse_physical_table_rejects_wrong_prefix() {
        assert_eq!(
            parse_physical_table("other_123456789012_Orders", "athome_"),
            None
        );
    }

    #[test]
    fn parse_physical_table_rejects_no_separator() {
        assert_eq!(parse_physical_table("athome_nodash", "athome_"), None);
    }
}
