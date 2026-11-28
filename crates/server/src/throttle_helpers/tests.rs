// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the per partition throttling key.

use super::*;
use serde_json::json;

#[test]
fn the_throttling_key_is_read_by_the_member_the_engine_reads_it_by() {
    // The same rule as the engine's deserializer: a member whose name is
    // no type descriptor and a descriptor of JSON null are ignored, in
    // either order; no descriptor left or two gives no key.
    let bob = Some("bob".to_owned());
    for (value, expected) in [
        (json!({"S": "bob"}), bob.clone()),
        (json!({"A": "alice", "S": "bob"}), bob.clone()),
        (json!({"S": "bob", "A": "alice"}), bob.clone()),
        (json!({"N": null, "S": "bob"}), bob.clone()),
        (json!({"S": "bob", "N": null}), bob.clone()),
        (json!({"A": "alice"}), None),
        (json!({"S": null}), None),
        (json!({"S": "alice", "N": "1"}), None),
    ] {
        let get = json!({"TableName": "t", "Key": {"pk": value}});
        assert_eq!(
            extract_partition_value(&get, "GetItem"),
            expected,
            "{value}"
        );
        let put = json!({"TableName": "t", "Item": {"pk": value}});
        assert_eq!(
            extract_partition_value(&put, "PutItem"),
            expected,
            "{value}"
        );
    }
    assert_eq!(
        extract_partition_value(&json!({"TableName": "t"}), "Scan"),
        None
    );
}
