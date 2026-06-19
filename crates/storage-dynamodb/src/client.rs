// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Builds an aws-sdk-dynamodb client from [`DynamoStorageConfig`].
//!
//! Setting `endpoint_url` is what lets this point at DynamoDB Local — or at
//! another ExtendDB endpoint, which is a feature, not a bug.

use crate::config::DynamoStorageConfig;

/// Build an [`aws_sdk_dynamodb::Client`] from the given config.
///
/// If `endpoint_url` is set, the client is directed there instead of the
/// standard AWS DynamoDB endpoint — enabling DynamoDB Local or another
/// ExtendDB node as the storage target.
pub async fn build_client(cfg: &DynamoStorageConfig) -> aws_sdk_dynamodb::Client {
    use aws_config::BehaviorVersion;
    use aws_config::Region;

    let mut loader =
        aws_config::defaults(BehaviorVersion::latest()).region(Region::new(cfg.region.clone()));

    if let Some(ep) = &cfg.endpoint_url {
        loader = loader.endpoint_url(ep.clone());
    }

    let shared = loader.load().await;
    aws_sdk_dynamodb::Client::new(&shared)
}
