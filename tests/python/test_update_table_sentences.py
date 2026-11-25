# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateTable refusals whose sentence the service fixes.

Each row asserts the code and the whole message as DynamoDB returns them
(measured 2026-10-24, 2026-10-25 and 2026-11-25). The service answers the rows
of the NEVER_CREATED tables before it looks the table up, so they go to a name
no test creates. The other rows need a table.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import wait_for_active

NEVER_CREATED = "never-created-by-g"

STREAM_VIEW_TYPE_REQUIRED = (
    "One or more parameter values were invalid: "
    "If stream is being enabled then UpdateViewType is required"
)
NO_DEFINITION = "Invalid KeySchema: Some index key attribute have no definition"
STREAM_NO_VIEW = {"StreamEnabled": True}
WARM = {"ReadUnitsPerSecond": 13000, "WriteUnitsPerSecond": 5000}


def _validation_message(exc_info) -> str:
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    return err["Message"]


def _gsi_create(name: str, keys: list[tuple[str, str]], projection: str) -> list[dict]:
    return [
        {
            "Create": {
                "IndexName": name,
                "KeySchema": [{"AttributeName": a, "KeyType": t} for a, t in keys],
                "Projection": {"ProjectionType": projection},
            }
        }
    ]


def _definitions(*names: str) -> list[dict]:
    return [{"AttributeName": n, "AttributeType": "S"} for n in names]


BY_G = _gsi_create("by-g", [("g", "HASH")], "ALL")
IDX1_G = _gsi_create("idx1", [("g", "HASH")], "KEYS_ONLY")


class TestStreamSpecification:
    """StreamEnabled without StreamViewType. The view type check comes before
    the already-enabled check."""

    @pytest.mark.parametrize(
        "table_spec",
        [
            pytest.param({}, id="never-streamed"),
            pytest.param(
                {
                    "StreamSpecification": {
                        "StreamEnabled": True,
                        "StreamViewType": "NEW_AND_OLD_IMAGES",
                    }
                },
                id="stream-enabled",
            ),
        ],
    )
    def test_the_view_type_is_required(self, table_factory, dynamodb_client, table_spec):
        name = table_factory(**table_spec)
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(TableName=name, StreamSpecification=STREAM_NO_VIEW)
        assert _validation_message(exc_info) == STREAM_VIEW_TYPE_REQUIRED


class TestValidationOrder:
    """Which refusal a request with two or more defects gets. A defective
    index Create and the billing rules come before the view type, and both the
    index Create and the view type come before the WarmThroughput rule."""

    NEVER_CREATED_ROWS = [
        pytest.param(
            {"StreamSpecification": STREAM_NO_VIEW, "GlobalSecondaryIndexUpdates": BY_G},
            "One or more parameter values were invalid: "
            "AttributeDefinitions is not specified for index: by-g",
            id="index-without-definitions-before-the-view-type",
        ),
        pytest.param(
            {"StreamSpecification": STREAM_NO_VIEW, "BillingMode": "PROVISIONED"},
            "One or more parameter values were invalid: "
            "ProvisionedThroughput must be specified when BillingMode is PROVISIONED",
            id="missing-throughput-before-the-view-type",
        ),
        pytest.param(
            {"StreamSpecification": STREAM_NO_VIEW, "WarmThroughput": WARM},
            STREAM_VIEW_TYPE_REQUIRED,
            id="view-type-before-warm-throughput",
        ),
        pytest.param(
            {
                "StreamSpecification": STREAM_NO_VIEW,
                "WarmThroughput": WARM,
                "DeletionProtectionEnabled": True,
            },
            STREAM_VIEW_TYPE_REQUIRED,
            id="view-type-before-warm-throughput-with-deletion-protection",
        ),
        pytest.param(
            {
                "StreamSpecification": STREAM_NO_VIEW,
                "WarmThroughput": WARM,
                "GlobalSecondaryIndexUpdates": IDX1_G,
            },
            "One or more parameter values were invalid: "
            "AttributeDefinitions is not specified for index: idx1",
            id="index-before-view-type-and-warm-throughput",
        ),
        pytest.param(
            {
                "WarmThroughput": WARM,
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexUpdates": IDX1_G,
            },
            "One or more parameter values were invalid: "
            "AttributeDefinitions is not specified for index: idx1",
            id="index-before-warm-throughput-with-billing-mode",
        ),
    ]

    @pytest.mark.parametrize("members, expected", NEVER_CREATED_ROWS)
    def test_the_service_refusal_comes_first(self, dynamodb_client, members, expected):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(TableName=NEVER_CREATED, **members)
        assert _validation_message(exc_info) == expected


class TestIndexKeyAttributeUndefined:
    """A secondary index key attribute that AttributeDefinitions does not
    define."""

    TABLE_ROWS = [
        pytest.param(
            {},
            "One or more parameter values were invalid: "
            "AttributeDefinitions is not specified for index: by-g",
            id="member-absent-names-the-index",
        ),
        pytest.param(
            {"AttributeDefinitions": _definitions("pk")},
            "One or more parameter values were invalid: "
            "Some index key attributes are not defined in AttributeDefinitions. "
            "Keys: [g], AttributeDefinitions: [pk]",
            id="member-lacking-the-key-lists-keys-and-definitions",
        ),
        pytest.param(
            {"AttributeDefinitions": []},
            NO_DEFINITION,
            id="member-empty",
        ),
    ]

    @pytest.mark.parametrize("members, expected", TABLE_ROWS)
    def test_update_table_on_a_table(self, table_factory, dynamodb_client, members, expected):
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=name, GlobalSecondaryIndexUpdates=BY_G, **members
            )
        assert _validation_message(exc_info) == expected
        # The refusal left the table as it was.
        wait_for_active(dynamodb_client, name)
        desc = dynamodb_client.describe_table(TableName=name)["Table"]
        assert "GlobalSecondaryIndexes" not in desc

    NEVER_CREATED_ROWS = [
        pytest.param(
            "create_table",
            {
                "AttributeDefinitions": _definitions("pk"),
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexes": [
                    {
                        "IndexName": "by-g",
                        "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
            },
            "One or more parameter values were invalid: "
            "Some index key attributes are not defined in AttributeDefinitions. "
            "Keys: [g], AttributeDefinitions: [pk]",
            id="create-table-lists-keys-and-definitions",
        ),
    ]

    @pytest.mark.parametrize("operation, members, expected", NEVER_CREATED_ROWS)
    def test_on_a_name_no_table_has(self, dynamodb_client, operation, members, expected):
        with pytest.raises(ClientError) as exc_info:
            getattr(dynamodb_client, operation)(TableName=NEVER_CREATED, **members)
        assert _validation_message(exc_info) == expected
