// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The startup check of an S3 store's credentials against the access keys
//! this server accepts from its clients, which the wiring of every S3
//! store calls with the store's name and the key its clients resolved.

use extenddb_auth::CredentialStore;
use tracing::instrument::WithSubscriber;
use tracing::subscriber::NoSubscriber;

/// Warn when the S3 store named `label` signs with an access key this
/// server also accepts from its clients: a key in the credential store,
/// created or imported, or the dev-mode pair when dev mode is on. AWS S3
/// refuses a key ExtendDB created, and takes an imported AWS key as the
/// identity of that key, so the store signs as one of the clients. The key
/// is never logged: a credential store logs the key of a lookup that
/// fails, so the lookup runs with its log lines dropped, and a lookup
/// error counts as no match.
pub(crate) async fn warn_of_a_client_key(
    label: &str,
    access_key_id: &str,
    credential_store: &dyn CredentialStore,
    dev_mode: bool,
) {
    let lookup = credential_store
        .lookup_credential(access_key_id)
        .with_subscriber(NoSubscriber::default())
        .await;
    let in_store = matches!(lookup, Ok(Some(_)));
    let dev_pair = dev_mode
        && (access_key_id == crate::dev_credentials::EXAMPLE_ACCESS_KEY_ID
            || crate::dev_credentials::resolve()
                .is_ok_and(|pairs| pairs.iter().any(|pair| pair.access_key_id == access_key_id)));
    if in_store || dev_pair {
        tracing::warn!(
            "The {label} credentials are an access key this server also accepts from its \
             clients. S3 refuses a key ExtendDB created, and accepts an imported AWS key as \
             that key's identity. Start the server without the clients' AWS variables or profile"
        );
    }
}

#[cfg(test)]
mod tests;
