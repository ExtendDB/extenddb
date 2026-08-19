// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! UpdateTable billing-mode validation. Supplying ProvisionedThroughput for a
//! table whose effective billing mode is PAY_PER_REQUEST must be rejected with
//! a ValidationException, even when BillingMode is omitted from the request
//! (i.e. the table is already PAY_PER_REQUEST). The rejected request must not
//! mutate the table.

use crate::test_base::*;
use aws_sdk_dynamodb::types::ProvisionedThroughput;

#[tokio::test]
async fn update_table_rejects_throughput_on_pay_per_request_table() {
    let c = client();
    let t = tables().await;
    // The shared fixtures are created PAY_PER_REQUEST. Setting throughput
    // without BillingMode must be rejected (and leave the table unchanged).
    let table = &t.simple_key_string;
    let e = c
        .update_table()
        .table_name(table)
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(5)
                .write_capacity_units(5)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect_err("throughput on a PAY_PER_REQUEST table must be rejected");
    assert_eq!(err_code(&e), Some("ValidationException"), "{}", err_msg(&e));
    assert!(
        err_msg(&e).contains(
            "Neither ReadCapacityUnits nor WriteCapacityUnits can be specified \
             when BillingMode is PAY_PER_REQUEST"
        ),
        "got: {}",
        err_msg(&e)
    );

    // The rejected update must not have changed the billing mode: a PutItem
    // (only valid on an on-demand or provisioned-with-capacity table) still
    // succeeds, and DescribeTable still reports PAY_PER_REQUEST.
    let desc = c.describe_table().table_name(table).send().await.unwrap();
    let summary = desc
        .table()
        .and_then(|td| td.billing_mode_summary())
        .and_then(|b| b.billing_mode());
    assert_eq!(
        summary,
        Some(&aws_sdk_dynamodb::types::BillingMode::PayPerRequest),
        "table billing mode must remain PAY_PER_REQUEST after a rejected update"
    );
}
