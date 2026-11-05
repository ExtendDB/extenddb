// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The startup credential check: the standard AWS credential chain asked
//! once, within five seconds, and the outcome logged under the store's
//! name, with a warning for resolved keys the SDK cannot renew.

use std::time::Duration;

use aws_config::SdkConfig;
use aws_credential_types::credential_feature::AwsCredentialFeature;
use aws_sdk_s3::config::{Credentials, ProvideCredentials};

/// How long the startup credential check waits for the credential chain.
const CREDENTIAL_CHECK: Duration = Duration::from_secs(5);

/// What the startup warning says about credentials that resolve later.
/// The SDK parses the shared config and credentials files once, when the
/// chain first reads them, and keeps the result. Until credentials
/// resolve, the other providers of the chain are asked again each time a
/// request needs credentials; once they resolve, the SDK's identity cache
/// keeps them until they expire, or for 15 minutes when they carry no
/// expiry.
pub const CREDENTIALS_LATER: &str = "The shared config and credentials files are read \
     once, at startup, so a change to them needs a restart; until credentials resolve, the \
     environment, web identity, container and instance metadata providers are asked again for \
     each request that needs them";

/// How one store names itself in its startup lines.
#[derive(Clone, Copy, Debug)]
pub struct Startup<'a> {
    /// The store's name, as its log lines give it, such as `S3 bulk store`.
    pub label: &'a str,
    /// What the store's callers meet while its credentials do not resolve,
    /// as one clause, such as `Start answers InternalServerError until
    /// they resolve`.
    pub until_resolved: &'a str,
}

/// Resolve the credentials once, log the outcome, and return the resolved
/// credentials.
pub(crate) async fn check_credentials(
    sdk: &SdkConfig,
    startup: &Startup<'_>,
) -> Option<Credentials> {
    let Startup {
        label,
        until_resolved,
    } = *startup;
    let Some(provider) = sdk.credentials_provider() else {
        tracing::warn!("{label} has no credential provider");
        return None;
    };
    match tokio::time::timeout(CREDENTIAL_CHECK, provider.provide_credentials()).await {
        Ok(Ok(credentials)) => {
            tracing::info!("{label} credentials resolved");
            Some(credentials)
        }
        Ok(Err(error)) => {
            tracing::warn!(
                error = %aws_sdk_s3::error::DisplayErrorContext(&error),
                "{label} credentials cannot be resolved; {until_resolved}. {CREDENTIALS_LATER}"
            );
            None
        }
        Err(_) => {
            tracing::warn!(
                "{label} credentials were not resolved within five seconds; {until_resolved}. \
                 {CREDENTIALS_LATER}"
            );
            None
        }
    }
}

/// Warn when the resolved credentials are temporary keys the SDK cannot
/// renew: a session token and no expiry, as environment variables or keys
/// written into a shared file give them. The SDK asks such a source again
/// and gets the same keys, so once they expire every request fails until
/// the server restarts. Credentials with an expiry (a role, web identity,
/// IAM Identity Center, a `credential_process` that prints one) are renewed
/// before they expire, and long-term keys carry no session token. Keys a
/// `credential_process` printed without an expiry are no warning either:
/// the SDK's identity cache keeps them for 15 minutes and then runs the
/// process again.
pub(crate) fn warn_of_temporary_keys(credentials: &Credentials, label: &str) {
    let from_a_process = credentials
        .get_property::<Vec<AwsCredentialFeature>>()
        .is_some_and(|features| features.contains(&AwsCredentialFeature::CredentialsProcess));
    if credentials.session_token().is_some() && credentials.expiry().is_none() && !from_a_process {
        tracing::warn!(
            "The {label} credentials are temporary keys the AWS SDK cannot renew. When they \
             expire, its S3 requests fail until the server restarts with new keys. Give the \
             server a role, web identity, IAM Identity Center or a credential_process profile \
             instead"
        );
    }
}
