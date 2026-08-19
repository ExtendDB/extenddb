# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateTable billing-mode validation.

Supplying ProvisionedThroughput for a table whose effective billing mode is
PAY_PER_REQUEST must be rejected with a ValidationException, even when
BillingMode is omitted from the request (i.e. the table is already
PAY_PER_REQUEST). A rejected request must not mutate the table.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError


def test_update_table_rejects_throughput_on_pay_per_request_table(
    dynamodb_client, create_and_cleanup_table
):
    # create_and_cleanup_table defaults to PAY_PER_REQUEST.
    name = create_and_cleanup_table()["TableDescription"]["TableName"]

    with pytest.raises(ClientError) as ei:
        dynamodb_client.update_table(
            TableName=name,
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        )
    err = ei.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert (
        "Neither ReadCapacityUnits nor WriteCapacityUnits can be specified "
        "when BillingMode is PAY_PER_REQUEST" in err["Message"]
    )

    # The rejected update must not have changed the billing mode.
    desc = dynamodb_client.describe_table(TableName=name)["Table"]
    assert (
        desc.get("BillingModeSummary", {}).get("BillingMode")
        == "PAY_PER_REQUEST"
    )
