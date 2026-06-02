# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Table operations: create, describe, list, update, delete, status transitions.

Tests run identically against real DynamoDB and extenddb. Failures always fail
the suite — there is no "expected failure" mode.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted


class TestCreateTable:
    """CreateTable API tests."""

    def test_create_hash_only(self, table_factory, dynamodb_client):
        """Create a table with hash key only."""
        name = table_factory()
        resp = dynamodb_client.describe_table(TableName=name)
        table = resp["Table"]
        assert table["TableStatus"] == "ACTIVE"
        assert table["TableName"] == name
        assert len(table["KeySchema"]) == 1
        assert table["KeySchema"][0]["KeyType"] == "HASH"

    def test_create_hash_range(self, table_factory, dynamodb_client):
        """Create a table with hash and range key."""
        name = table_factory(range_key="sk")
        resp = dynamodb_client.describe_table(TableName=name)
        table = resp["Table"]
        assert len(table["KeySchema"]) == 2
        key_types = {ks["KeyType"] for ks in table["KeySchema"]}
        assert key_types == {"HASH", "RANGE"}

    def test_create_with_number_keys(self, table_factory, dynamodb_client):
        """Create a table with N-type keys."""
        name = table_factory(hash_key="id", hash_type="N", range_key="ts", range_type="N")
        resp = dynamodb_client.describe_table(TableName=name)
        attr_types = {a["AttributeName"]: a["AttributeType"]
                      for a in resp["Table"]["AttributeDefinitions"]}
        assert attr_types["id"] == "N"
        assert attr_types["ts"] == "N"

    def test_create_with_binary_key(self, table_factory, dynamodb_client):
        """Create a table with B-type hash key."""
        name = table_factory(hash_key="bk", hash_type="B")
        resp = dynamodb_client.describe_table(TableName=name)
        attr_types = {a["AttributeName"]: a["AttributeType"]
                      for a in resp["Table"]["AttributeDefinitions"]}
        assert attr_types["bk"] == "B"

    def test_create_pay_per_request(self, table_factory, dynamodb_client):
        """PAY_PER_REQUEST billing mode."""
        name = table_factory()
        resp = dynamodb_client.describe_table(TableName=name)
        billing = resp["Table"].get("BillingModeSummary", {})
        assert billing.get("BillingMode") == "PAY_PER_REQUEST"

    def test_create_provisioned(self, table_factory, dynamodb_client):
        """PROVISIONED billing mode."""
        name = table_factory(
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        resp = dynamodb_client.describe_table(TableName=name)
        pt = resp["Table"]["ProvisionedThroughput"]
        assert pt["ReadCapacityUnits"] == 5
        assert pt["WriteCapacityUnits"] == 5

    def test_create_duplicate_fails(self, table_factory, dynamodb_client):
        """Creating a table that already exists returns ResourceInUseException."""
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceInUseException"

    def test_create_invalid_key_type(self, dynamodb_client):
        """Invalid key type in KeySchema — boto3 rejects before sending."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=unique_name("tbl"),
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "INVALID"}],
                BillingMode="PAY_PER_REQUEST",
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"

    def test_create_missing_attribute_definition(self, dynamodb_client):
        """KeySchema references an attribute not in AttributeDefinitions."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=unique_name("tbl"),
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


class TestDescribeTable:
    """DescribeTable API tests."""

    def test_describe_existing(self, table_factory, dynamodb_client):
        """Describe an existing table returns full metadata."""
        name = table_factory(range_key="sk")
        resp = dynamodb_client.describe_table(TableName=name)
        table = resp["Table"]
        assert table["TableName"] == name
        assert "TableArn" in table
        assert "TableId" in table
        assert "CreationDateTime" in table
        assert table["TableStatus"] == "ACTIVE"
        assert table["ItemCount"] >= 0
        assert table["TableSizeBytes"] >= 0

    def test_describe_nonexistent(self, dynamodb_client):
        """Describe a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.describe_table(TableName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestListTables:
    """ListTables API tests."""

    def test_list_includes_created(self, table_factory, dynamodb_client):
        """A newly created table appears in ListTables."""
        name = table_factory()
        resp = dynamodb_client.list_tables()
        assert name in resp["TableNames"]

    def test_list_pagination(self, table_factory, dynamodb_client):
        """ListTables with Limit returns ExclusiveStartTableName for pagination."""
        names = [table_factory() for _ in range(3)]
        resp = dynamodb_client.list_tables(Limit=1)
        assert len(resp["TableNames"]) == 1
        assert "LastEvaluatedTableName" in resp

        # Paginate through all
        all_names: list[str] = list(resp["TableNames"])
        while "LastEvaluatedTableName" in resp:
            resp = dynamodb_client.list_tables(
                Limit=1,
                ExclusiveStartTableName=resp["LastEvaluatedTableName"],
            )
            all_names.extend(resp["TableNames"])

        for n in names:
            assert n in all_names


class TestDeleteTable:
    """DeleteTable API tests."""

    def test_delete_existing(self, table_factory, dynamodb_client):
        """Delete an existing table succeeds."""
        name = table_factory()
        resp = dynamodb_client.delete_table(TableName=name)
        assert resp["TableDescription"]["TableName"] == name
        wait_for_deleted(dynamodb_client, name)

        # Verify it's gone
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.describe_table(TableName=name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_nonexistent(self, dynamodb_client):
        """Delete a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_table(TableName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestUpdateTable:
    """UpdateTable API tests."""

    def test_update_billing_mode(self, table_factory, dynamodb_client):
        """Switch from PAY_PER_REQUEST to PROVISIONED."""
        name = table_factory()
        dynamodb_client.update_table(
            TableName=name,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 5},
        )
        wait_for_active(dynamodb_client, name)
        resp = dynamodb_client.describe_table(TableName=name)
        pt = resp["Table"]["ProvisionedThroughput"]
        assert pt["ReadCapacityUnits"] == 10
        assert pt["WriteCapacityUnits"] == 5

    def test_update_nonexistent(self, dynamodb_client):
        """Update a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=f"nonexistent-{uuid.uuid4().hex[:8]}",
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
