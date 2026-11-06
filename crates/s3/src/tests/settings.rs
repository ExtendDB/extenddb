// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The settings the store's clients are built with: the pinned behaviour
//! version.

use crate::behavior_version;
use crate::client::loader;

/// The SDK configuration the store's clients are built from carries the
/// pinned behaviour version. The workspace enables the SDK's
/// `behavior-version-latest` feature, which gives a client built without a
/// version the newest one, so the pin is checked on the store's own loader:
/// a loader that stopped setting it would float with the SDK release once
/// the SDK's newest version moves past the pinned one.
#[tokio::test]
async fn the_store_configuration_carries_the_pinned_behaviour_version() {
    let sdk = loader(&super::settings(None)).load().await;
    assert_eq!(sdk.behavior_version(), Some(behavior_version()));
}
