// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared cryptographic helpers for the management API: the secret key,
//! session key ID and session token that `assume_role` issues. The secret is
//! encrypted through the catalog store's key holder
//! (`extenddb_storage::secret_keys::SecretKeys`).

pub use extenddb_storage::management_store::{
    generate_secret_key, generate_session_key_id, generate_session_token,
};
