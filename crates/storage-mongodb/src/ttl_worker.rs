// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL cleanup background worker for `MongoDB`.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::metrics::MetricsCollector;
use extenddb_core::types::UserIdentity;
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, MetadataEngine, TableEngine};

use crate::MongoEngine;

const SCAN_INTERVAL: Duration = Duration::from_secs(60);
const BATCH_SIZE: usize = 100;

pub(crate) async fn ttl_cleanup_worker(storage: Arc<MongoEngine>, metrics: Arc<MetricsCollector>) {
    let region_arc: Arc<str> = Arc::from(storage.region.as_str());

    loop {
        tokio::time::sleep(SCAN_INTERVAL).await;
        retry_pending_indexes(&storage).await;
        sweep_expired_items(&storage, &metrics, &region_arc).await;
    }
}

async fn retry_pending_indexes(storage: &MongoEngine) {
    let Ok(pending) = MetadataEngine::all_tables_with_ttl(storage).await else {
        return;
    };
    let Ok(ready) = MetadataEngine::all_tables_with_ttl_index_ready(storage).await else {
        return;
    };
    let ready_set: std::collections::HashSet<(&str, &str)> = ready
        .iter()
        .map(|(a, t, _)| (a.as_str(), t.as_str()))
        .collect();
    for (account_id, table_name, ttl_attr) in &pending {
        if !ready_set.contains(&(account_id.as_str(), table_name.as_str())) {
            if let Err(e) =
                MetadataEngine::create_ttl_index(storage, account_id, table_name, ttl_attr).await
            {
                tracing::debug!("TTL worker: index creation retry failed for {table_name}: {e}");
            } else {
                tracing::info!("TTL worker: index created for {table_name}");
            }
        }
    }
}

async fn sweep_expired_items(storage: &MongoEngine, metrics: &MetricsCollector, region: &Arc<str>) {
    let ttl_identity = UserIdentity {
        identity_type: "Service".to_owned(),
        principal_id: "dynamodb.amazonaws.com".to_owned(),
    };

    let tables = match MetadataEngine::all_tables_with_ttl_index_ready(storage).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!("TTL worker: failed to list tables: {e}");
            return;
        }
    };

    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    for (account_id, table_name, ttl_attribute) in &tables {
        let items = match MetadataEngine::find_expired_items_indexed(
            storage,
            account_id,
            table_name,
            ttl_attribute,
            BATCH_SIZE,
        )
        .await
        {
            Ok(items) => items,
            Err(e) => {
                tracing::warn!("TTL worker: find expired failed for {table_name}: {e}");
                continue;
            }
        };

        if items.is_empty() {
            continue;
        }

        let key_info = match TableEngine::table_key_info(storage, account_id, table_name).await {
            Ok(ki) => ki,
            Err(e) => {
                tracing::warn!("TTL worker: key info failed for {table_name}: {e}");
                continue;
            }
        };

        let view_type = stream_view_type(&key_info);
        let (condition_expr, maps) = build_ttl_condition(ttl_attribute, now_epoch);

        let mut deleted = 0usize;
        for item in &items {
            let staleness = item
                .get(ttl_attribute.as_str())
                .and_then(|av| {
                    if let extenddb_core::types::AttributeValue::N(n) = av {
                        n.parse::<u64>().ok()
                    } else {
                        None
                    }
                })
                .map(|ttl_val| now_epoch.saturating_sub(ttl_val));

            let key: extenddb_core::types::Item = key_info
                .key_schema
                .iter()
                .filter_map(|ks| {
                    item.get(&ks.attribute_name)
                        .map(|v| (ks.attribute_name.clone(), v.clone()))
                })
                .collect();

            let return_old = view_type.is_some();
            let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: Some(ttl_identity.clone()),
                region: region.clone(),
            });
            match DataEngine::delete_item(
                storage,
                &key_info,
                &key,
                return_old,
                Some(&condition_expr),
                &maps,
                stream.as_ref(),
            )
            .await
            {
                Err(StorageError::ConditionFailed(_)) => {}
                Err(e) => {
                    tracing::warn!("TTL worker: delete failed for {table_name}: {e}");
                }
                Ok(_old_item) => {
                    deleted += 1;
                    metrics.record_ttl_deletion(table_name);
                    if let Some(s) = staleness {
                        #[allow(clippy::cast_precision_loss)]
                        metrics.record_ttl_staleness(table_name, s as f64);
                    }
                }
            }
        }

        if deleted > 0 {
            tracing::info!("TTL worker: deleted {deleted} expired items from {table_name}");
        }
    }
}

fn stream_view_type(
    key_info: &extenddb_core::types::TableKeyInfo,
) -> Option<extenddb_core::types::StreamViewType> {
    key_info.stream_specification.as_ref().and_then(|spec| {
        if spec.stream_enabled {
            spec.stream_view_type
        } else {
            None
        }
    })
}

fn build_ttl_condition(
    ttl_attribute: &str,
    now_epoch: u64,
) -> (
    extenddb_core::expression::Expr,
    extenddb_core::expression::ExpressionMaps,
) {
    use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement};
    use std::collections::HashMap;

    let ttl_path = vec![PathElement::Attribute("#ttl".to_owned())];
    let condition_expr = Expr::And(
        Box::new(Expr::Function {
            name: "attribute_exists".to_owned(),
            args: vec![Expr::Path(ttl_path.clone())],
        }),
        Box::new(Expr::Compare {
            left: Box::new(Expr::Path(ttl_path)),
            op: CompareOp::Le,
            right: Box::new(Expr::Placeholder("now".to_owned())),
        }),
    );

    let mut names = HashMap::new();
    names.insert("ttl".to_owned(), ttl_attribute.to_owned());
    let mut values = HashMap::new();
    values.insert(
        "now".to_owned(),
        extenddb_core::types::AttributeValue::N(now_epoch.to_string()),
    );

    (condition_expr, ExpressionMaps::new(names, values))
}
