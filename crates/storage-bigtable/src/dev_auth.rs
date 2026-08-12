//! Dev-mode auth bypass for the bigtable backend.
//!
//! Skips SigV4 validation entirely. Every request authenticates as a fixed
//! IAM user (the dev_account_id / dev_user_name). Paired with
//! `BigtableCatalogStore`'s dev_mode flag which returns an allow-all policy
//! for the same identity so authz also passes.
//!
//! **Never enable on a production deployment.** Effectively no authentication.

use async_trait::async_trait;
use axum::http::HeaderMap;
use extenddb_auth::{AuthIdentity, AuthProvider};
use extenddb_core::error::DynamoDbError;

pub const DEV_USER_NAME: &str = "__dev_admin__";

pub struct DevAuthProvider {
    account_id: String,
    user_name: String,
}

impl DevAuthProvider {
    pub fn new(account_id: String) -> Self {
        Self {
            account_id,
            user_name: DEV_USER_NAME.to_string(),
        }
    }
}

#[async_trait]
impl AuthProvider for DevAuthProvider {
    async fn authenticate(
        &self,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError> {
        Ok(AuthIdentity::User {
            account_id: self.account_id.clone(),
            user_name: self.user_name.clone(),
        })
    }
}
