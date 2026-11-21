// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The deadlines of a store's clients: the connect and read timeouts every
//! client is built with.

use std::time::Duration;

use super::*;
use crate::{CONNECT_TIMEOUT, READ_TIMEOUT, clients_from};

#[test]
fn the_store_clients_carry_the_connect_and_read_deadlines() {
    assert_eq!(CONNECT_TIMEOUT, Duration::from_millis(3100));
    assert_eq!(READ_TIMEOUT, Duration::from_secs(60));
    for endpoint in [None, Some("http://127.0.0.1:9")] {
        let clients = clients_from(&settings(endpoint), &sdk_config(), |builder| builder);
        let deadlines = clients
            .config()
            .timeout_config()
            .expect("deadlines are set");
        assert_eq!(
            deadlines.connect_timeout(),
            Some(CONNECT_TIMEOUT),
            "{endpoint:?}"
        );
        assert_eq!(deadlines.read_timeout(), Some(READ_TIMEOUT), "{endpoint:?}");
        // A body is bounded by stalled-stream protection, not by a wall
        // clock over the whole call.
        assert_eq!(deadlines.operation_timeout(), None, "{endpoint:?}");
        assert_eq!(deadlines.operation_attempt_timeout(), None, "{endpoint:?}");
    }
}
