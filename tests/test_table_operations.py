# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Phase 1 table operations tests — dual-target against real DynamoDB and extenddb.

Covers: CreateTable, DescribeTable, ListTables, DeleteTable, and key error paths.
REQ-TEST-001, REQ-TEST-002, REQ-TEST-003, REQ-TEST-004
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError
from conftest import wait_for_active
class TestCreateTable:
    """CreateTable operation tests."""

    def test_create_simple_hash_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        result = create_and_cleanup_table(unique_table_name)
        desc = result["TableDescription"]
        assert desc["TableName"] == unique_table_name
        assert desc["TableStatus"] in ("ACTIVE", "CREATING")
        assert len(desc["KeySchema"]) == 1
        assert desc["KeySchema"][0]["KeyType"] == "HASH"
        assert desc["TableArn"].endswith(f"table/{unique_table_name}")

    def test_create_hash_range_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        result = create_and_cleanup_table(
            unique_table_name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
        )
        desc = result["TableDescription"]
        assert len(desc["KeySchema"]) == 2
        assert desc["KeySchema"][1]["KeyType"] == "RANGE"

    def test_create_provisioned_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        result = create_and_cleanup_table(
            unique_table_name,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        desc = result["TableDescription"]
        pt = desc["ProvisionedThroughput"]
        assert pt["ReadCapacityUnits"] == 5
        assert pt["WriteCapacityUnits"] == 5

    def test_create_duplicate_table_fails(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(unique_table_name)
        with pytest.raises(ClientError) as exc_info:
            create_and_cleanup_table(unique_table_name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceInUseException"

    def test_create_table_name_too_short(self, dynamodb_client_no_validation):
        """Table name shorter than 3 chars is rejected by the service."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.create_table(
                TableName="ab",
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_create_table_invalid_characters(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName="invalid table!",
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
class TestDescribeTable:
    """DescribeTable operation tests."""

    def test_describe_existing_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(unique_table_name)
        result = dynamodb_client.describe_table(TableName=unique_table_name)
        desc = result["Table"]
        assert desc["TableName"] == unique_table_name
        assert desc["TableStatus"] in ("ACTIVE", "CREATING")
        assert "TableArn" in desc
        assert "TableId" in desc
        assert "CreationDateTime" in desc
        assert "TableSizeBytes" in desc
        assert "ItemCount" in desc

    def test_describe_nonexistent_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.describe_table(TableName="nonexistent-table-xyz-999")
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
class TestListTables:
    """ListTables operation tests."""

    def test_list_tables_returns_created_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(unique_table_name)
        # Paginate through all tables — the default page size is 100 and
        # the test database may contain more tables than that.
        collected: list[str] = []
        kwargs: dict = {}
        while True:
            result = dynamodb_client.list_tables(**kwargs)
            collected.extend(result["TableNames"])
            if "LastEvaluatedTableName" not in result:
                break
            kwargs["ExclusiveStartTableName"] = result["LastEvaluatedTableName"]
        assert unique_table_name in collected

    def test_list_tables_with_limit(self, create_and_cleanup_table, dynamodb_client):
        result = dynamodb_client.list_tables(Limit=1)
        assert len(result["TableNames"]) <= 1

    def test_list_tables_pagination(self, create_and_cleanup_table, dynamodb_client):
        # Create 3 tables, paginate with limit=1
        names = []
        for _ in range(3):
            name = f"extenddb-test-page-{uuid.uuid4().hex[:8]}"
            create_and_cleanup_table(name)
            names.append(name)

        collected = []
        kwargs: dict = {"Limit": 1}
        while True:
            result = dynamodb_client.list_tables(**kwargs)
            collected.extend(result["TableNames"])
            if "LastEvaluatedTableName" not in result:
                break
            kwargs["ExclusiveStartTableName"] = result["LastEvaluatedTableName"]

        # All 3 created tables should appear in the full listing
        for name in names:
            assert name in collected
class TestDeleteTable:
    """DeleteTable operation tests."""

    def test_delete_existing_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(unique_table_name)
        result = dynamodb_client.delete_table(TableName=unique_table_name)
        desc = result["TableDescription"]
        assert desc["TableName"] == unique_table_name
        assert desc["TableStatus"] in ("DELETING", "ACTIVE")

    def test_delete_nonexistent_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_table(TableName="nonexistent-table-xyz-999")
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"

    def test_delete_protected_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(
            unique_table_name,
            DeletionProtectionEnabled=True,
        )
        wait_for_active(dynamodb_client, unique_table_name)
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_table(TableName=unique_table_name)
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "deletion protection" in err["Message"].lower() or "protected" in err["Message"].lower()

        # Cleanup: disable protection so fixture can delete
        dynamodb_client.update_table(
            TableName=unique_table_name,
            DeletionProtectionEnabled=False,
        )


# ---------------------------------------------------------------------------
# CreateTable validation additions (covers commits since 6b98234dcf)
# ---------------------------------------------------------------------------


class TestCreateTableValidation:
    """CreateTable validation edge cases from recent fixes."""

    def test_create_table_duplicate_key_schema(self, dynamodb_client):
        """CreateTable with duplicate attribute in KeySchema is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=f"extenddb-test-{__import__('uuid').uuid4().hex[:8]}",
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                ],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "pk", "KeyType": "RANGE"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_create_table_three_key_schema_elements(self, dynamodb_client):
        """CreateTable with >2 KeySchema elements is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=f"extenddb-test-{__import__('uuid').uuid4().hex[:8]}",
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "extra", "AttributeType": "S"},
                ],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                    {"AttributeName": "extra", "KeyType": "RANGE"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_create_table_lsi_without_range_key(self, dynamodb_client):
        """CreateTable with LSI on a hash-only table is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=f"extenddb-test-{__import__('uuid').uuid4().hex[:8]}",
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "lsi_sk", "AttributeType": "N"},
                ],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ],
                LocalSecondaryIndexes=[
                    {
                        "IndexName": "bad-lsi",
                        "KeySchema": [
                            {"AttributeName": "pk", "KeyType": "HASH"},
                            {"AttributeName": "lsi_sk", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_create_table_invalid_billing_mode(self, dynamodb_client):
        """CreateTable with invalid BillingMode string is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName=f"extenddb-test-{__import__('uuid').uuid4().hex[:8]}",
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                ],
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ],
                BillingMode="INVALID_MODE",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# ListTables sort order
# ---------------------------------------------------------------------------


class TestListTablesOrder:
    """ListTables returns tables in alphabetical order."""

    def test_list_tables_alphabetical_order(self, create_and_cleanup_table, dynamodb_client):
        """Tables are returned in alphabetical (lexicographic) order."""
        # Create tables with names that sort in a known order.
        prefix = f"extenddb-order-{uuid.uuid4().hex[:4]}"
        names = [f"{prefix}-charlie", f"{prefix}-alpha", f"{prefix}-bravo"]
        for name in names:
            create_and_cleanup_table(name)

        # Collect all tables.
        collected: list[str] = []
        kwargs: dict = {}
        while True:
            result = dynamodb_client.list_tables(**kwargs)
            collected.extend(result["TableNames"])
            if "LastEvaluatedTableName" not in result:
                break
            kwargs["ExclusiveStartTableName"] = result["LastEvaluatedTableName"]

        # Filter to just our test tables.
        our_tables = [t for t in collected if t.startswith(prefix)]
        assert our_tables == sorted(our_tables)


# ---------------------------------------------------------------------------
# UpdateTable validation
# ---------------------------------------------------------------------------


class TestUpdateTable:
    """UpdateTable validation edge cases from recent fixes."""

    def test_update_table_pay_per_request_with_throughput(
        self, create_and_cleanup_table, dynamodb_client, unique_table_name
    ):
        """UpdateTable with PAY_PER_REQUEST + ProvisionedThroughput is rejected."""
        create_and_cleanup_table(unique_table_name)
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=unique_table_name,
                BillingMode="PAY_PER_REQUEST",
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_update_table_zero_throughput(
        self, create_and_cleanup_table, dynamodb_client_no_validation, unique_table_name
    ):
        """UpdateTable with throughput=0 is rejected by the service."""
        create_and_cleanup_table(
            unique_table_name,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.update_table(
                TableName=unique_table_name,
                ProvisionedThroughput={"ReadCapacityUnits": 0, "WriteCapacityUnits": 5},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_update_table_remove_nonexistent_gsi(
        self, create_and_cleanup_table, dynamodb_client, unique_table_name
    ):
        """UpdateTable trying to delete a GSI that doesn't exist is rejected."""
        create_and_cleanup_table(unique_table_name)
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_table(
                TableName=unique_table_name,
                GlobalSecondaryIndexUpdates=[
                    {"Delete": {"IndexName": "nonexistent-gsi"}},
                ],
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException" or err["Code"] == "ResourceNotFoundException"

    def test_update_table_billing_mode_switch(
        self, create_and_cleanup_table, dynamodb_client, unique_table_name
    ):
        """Switch from PROVISIONED to PAY_PER_REQUEST succeeds."""
        create_and_cleanup_table(
            unique_table_name,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        resp = dynamodb_client.update_table(
            TableName=unique_table_name,
            BillingMode="PAY_PER_REQUEST",
        )
        # Should succeed — the table transitions billing mode.
        assert resp["TableDescription"]["TableName"] == unique_table_name



class TestUpdateTableGsiDeletion:
    """UpdateTable GSI deletion error handling."""

    def test_delete_nonexistent_gsi_returns_resource_not_found(
        self, dynamodb_client, create_and_cleanup_table
    ):
        """Deleting a non-existent GSI returns ResourceNotFoundException."""
        resp = create_and_cleanup_table()
        name = resp["TableDescription"]["TableName"]
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_table(
                TableName=name,
                GlobalSecondaryIndexUpdates=[
                    {"Delete": {"IndexName": "does_not_exist"}}
                ],
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
        assert "Requested resource not found" in err["Message"]


class TestContainsDuplicateOperand:
    """contains() with duplicate operand error message."""

    def test_contains_duplicate_path_error_message(
        self, dynamodb_client, create_and_cleanup_table
    ):
        """contains(#a, #a) with same path reports correct error with resolved name."""
        resp = create_and_cleanup_table()
        name = resp["TableDescription"]["TableName"]
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "val": {"S": "hello"}}
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "k2"}},
                ConditionExpression="contains(#a, #a)",
                ExpressionAttributeNames={"#a": "val"},
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "first operand must be distinct" in err["Message"]
        assert "[val]" in err["Message"]
