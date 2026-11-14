# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateTable refusals whose sentence the service fixes.

Each row asserts the code and the whole message as DynamoDB returns them
(measured 2026-10-24 and 2026-10-25).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import wait_for_active

STREAM_VIEW_TYPE_REQUIRED = (
    "One or more parameter values were invalid: "
    "If stream is being enabled then UpdateViewType is required"
)


def _validation_message(exc_info) -> str:
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    return err["Message"]


class TestStreamSpecification:
    """StreamEnabled without StreamViewType."""

    def test_never_streamed_table(self, table_factory, dynamodb_client):
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=name, StreamSpecification={"StreamEnabled": True}
            )
        assert _validation_message(exc_info) == STREAM_VIEW_TYPE_REQUIRED

    def test_table_with_an_enabled_stream(self, table_factory, dynamodb_client):
        """The view type check comes before the already-enabled check."""
        name = table_factory(
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            }
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=name, StreamSpecification={"StreamEnabled": True}
            )
        assert _validation_message(exc_info) == STREAM_VIEW_TYPE_REQUIRED


class TestValidationOrder:
    """Which refusal a request with two defects gets (measured 2026-10-25)."""

    def test_index_without_definitions_precedes_the_view_type(self, table_factory, dynamodb_client):
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=name,
                StreamSpecification={"StreamEnabled": True},
                GlobalSecondaryIndexUpdates=[
                    {
                        "Create": {
                            "IndexName": "by-g",
                            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                            "Projection": {"ProjectionType": "ALL"},
                        }
                    }
                ],
            )
        assert _validation_message(exc_info) == (
            "One or more parameter values were invalid: "
            "AttributeDefinitions is not specified for index: by-g"
        )

    def test_missing_throughput_precedes_the_view_type(self, table_factory, dynamodb_client):
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=name,
                StreamSpecification={"StreamEnabled": True},
                BillingMode="PROVISIONED",
            )
        assert _validation_message(exc_info) == (
            "One or more parameter values were invalid: "
            "ProvisionedThroughput must be specified when BillingMode is PROVISIONED"
        )


class TestIndexKeyAttributeUndefined:
    """A secondary index key attribute that AttributeDefinitions does not define."""

    @staticmethod
    def _create_by_g():
        return [
            {
                "Create": {
                    "IndexName": "by-g",
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            }
        ]

    def test_update_table_without_the_member_names_the_index(self, table_factory, dynamodb_client):
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=name,
                GlobalSecondaryIndexUpdates=self._create_by_g(),
            )
        assert _validation_message(exc_info) == (
            "One or more parameter values were invalid: "
            "AttributeDefinitions is not specified for index: by-g"
        )
        # The refusal left the table as it was.
        wait_for_active(dynamodb_client, name)
        desc = dynamodb_client.describe_table(TableName=name)["Table"]
        assert "GlobalSecondaryIndexes" not in desc

    def test_update_table_with_the_member_lacking_the_key_lists_keys_and_definitions(
        self, table_factory, dynamodb_client
    ):
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                GlobalSecondaryIndexUpdates=self._create_by_g(),
            )
        assert _validation_message(exc_info) == (
            "One or more parameter values were invalid: "
            "Some index key attributes are not defined in AttributeDefinitions. "
            "Keys: [g], AttributeDefinitions: [pk]"
        )

    def test_update_table_with_an_empty_member_has_its_own_sentence(self, table_factory, dynamodb_client):
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=name,
                AttributeDefinitions=[],
                GlobalSecondaryIndexUpdates=self._create_by_g(),
            )
        assert _validation_message(exc_info) == "Invalid KeySchema: Some index key attribute have no definition"

    def test_create_table_lists_keys_and_definitions(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName="never-created-by-g",
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "by-g",
                        "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
            )
        assert _validation_message(exc_info) == (
            "One or more parameter values were invalid: "
            "Some index key attributes are not defined in AttributeDefinitions. "
            "Keys: [g], AttributeDefinitions: [pk]"
        )
