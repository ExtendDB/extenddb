# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Table ARN accepted as TableName — dual-target against real DynamoDB and extenddb.

A data-plane request may reference a table by its full ARN in place of the bare
table name. Amazon DynamoDB and DynamoDB Local both resolve the ARN to the table
name and serve the request. These tests exercise that resolution across the
table-name-bearing API surface (item, query/scan, batch, transact), plus the
rejection of an index/non-table ARN.

The ARN is taken from the table's own DescribeTable output, so it carries the
correct account and region for whichever target the suite runs against.

REQ-TEST-002, REQ-TEST-003
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Hash-only table for the class; yields (name, arn)."""
    with scoped_table(dynamodb_client) as name:
        arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]
        yield name, arn


@pytest.fixture(scope="class")
def second_table(dynamodb_client):
    """A second hash-only table for multi-table batch cases; yields (name, arn)."""
    with scoped_table(dynamodb_client) as name:
        arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]
        yield name, arn


class TestTableArnAsName:
    """A full table ARN may be supplied wherever a TableName is expected."""

    def test_get_item_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        item = {"pk": {"S": "g1"}, "v": {"S": "hello"}}
        dynamodb_client.put_item(TableName=name, Item=item)
        resp = dynamodb_client.get_item(TableName=arn, Key={"pk": {"S": "g1"}})
        assert resp["Item"] == item

    def test_put_item_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        item = {"pk": {"S": "p1"}, "v": {"S": "world"}}
        dynamodb_client.put_item(TableName=arn, Item=item)
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "p1"}})
        assert resp["Item"] == item

    def test_update_item_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "u1"}})
        dynamodb_client.update_item(
            TableName=arn,
            Key={"pk": {"S": "u1"}},
            UpdateExpression="SET v = :v",
            ExpressionAttributeValues={":v": {"S": "updated"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "u1"}})
        assert resp["Item"]["v"] == {"S": "updated"}

    def test_delete_item_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "d1"}})
        dynamodb_client.delete_item(TableName=arn, Key={"pk": {"S": "d1"}})
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "d1"}})
        assert "Item" not in resp

    def test_query_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "q1"}, "v": {"S": "x"}})
        resp = dynamodb_client.query(
            TableName=arn,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "q1"}},
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["pk"] == {"S": "q1"}

    def test_scan_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "s1"}})
        resp = dynamodb_client.scan(
            TableName=arn,
            FilterExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "s1"}},
        )
        assert resp["Count"] == 1

    def test_batch_get_item_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "bg1"}, "v": {"S": "b"}})
        resp = dynamodb_client.batch_get_item(
            RequestItems={arn: {"Keys": [{"pk": {"S": "bg1"}}]}}
        )
        # The response echoes the caller's supplied key verbatim (the ARN),
        # matching Amazon DynamoDB.
        assert resp["Responses"][arn] == [{"pk": {"S": "bg1"}, "v": {"S": "b"}}]

    def test_consumed_capacity_echoes_supplied_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "cc1"}})
        resp = dynamodb_client.get_item(
            TableName=arn,
            Key={"pk": {"S": "cc1"}},
            ReturnConsumedCapacity="TOTAL",
        )
        # ConsumedCapacity.TableName echoes exactly what the caller supplied.
        assert resp["ConsumedCapacity"]["TableName"] == arn

    def test_batch_write_item_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        dynamodb_client.batch_write_item(
            RequestItems={arn: [{"PutRequest": {"Item": {"pk": {"S": "bw1"}}}}]}
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "bw1"}})
        assert resp["Item"]["pk"] == {"S": "bw1"}

    def test_transact_write_and_get_by_arn(self, dynamodb_client, hash_table):
        name, arn = hash_table
        dynamodb_client.transact_write_items(
            TransactItems=[{"Put": {"TableName": arn, "Item": {"pk": {"S": "tw1"}}}}]
        )
        resp = dynamodb_client.transact_get_items(
            TransactItems=[{"Get": {"TableName": arn, "Key": {"pk": {"S": "tw1"}}}}]
        )
        assert resp["Responses"][0]["Item"]["pk"] == {"S": "tw1"}

    def test_index_arn_as_table_name_rejected(self, dynamodb_client, hash_table):
        _name, arn = hash_table
        # An index ARN (or any non-table resource) is not a valid TableName.
        # The message text differs across implementations; assert the class.
        with pytest.raises(ClientError) as exc:
            dynamodb_client.get_item(
                TableName=f"{arn}/index/some-index", Key={"pk": {"S": "g1"}}
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_bare_nonexistent_name_still_not_found(self, dynamodb_client):
        # Control: a plain (non-ARN) name for a missing table is still a 404.
        # The ARN path must not swallow genuine resource-not-found errors.
        missing = f"extenddb-missing-{uuid.uuid4().hex[:12]}"
        with pytest.raises(ClientError) as exc:
            dynamodb_client.get_item(TableName=missing, Key={"pk": {"S": "x"}})
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_batch_get_arn_and_bare_same_table_collapse(self, dynamodb_client, hash_table):
        # An ARN key and the bare key for the same table are duplicate
        # references. DynamoDB collapses them to a single entry rather than
        # rejecting; both keys request the same item so the result is stable.
        name, arn = hash_table
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "col1"}, "v": {"S": "c"}})
        resp = dynamodb_client.batch_get_item(
            RequestItems={
                arn: {"Keys": [{"pk": {"S": "col1"}}]},
                name: {"Keys": [{"pk": {"S": "col1"}}]},
            }
        )
        responses = resp["Responses"]
        assert len(responses) == 1
        (items,) = responses.values()
        assert items == [{"pk": {"S": "col1"}, "v": {"S": "c"}}]

    def test_batch_write_arn_and_bare_distinct_tables(
        self, dynamodb_client, hash_table, second_table
    ):
        # Distinct tables addressed by an ARN key and a bare key in one batch
        # must both be written (no collision, both entries preserved).
        name_a, arn_a = hash_table
        name_b, _arn_b = second_table
        dynamodb_client.batch_write_item(
            RequestItems={
                arn_a: [{"PutRequest": {"Item": {"pk": {"S": "mt-a"}}}}],
                name_b: [{"PutRequest": {"Item": {"pk": {"S": "mt-b"}}}}],
            }
        )
        assert "Item" in dynamodb_client.get_item(TableName=name_a, Key={"pk": {"S": "mt-a"}})
        assert "Item" in dynamodb_client.get_item(TableName=name_b, Key={"pk": {"S": "mt-b"}})

    def test_batch_get_mixed_arn_and_bare_echo_selectivity(
        self, dynamodb_client, hash_table, second_table
    ):
        # One table addressed by ARN, one by bare name, in a single batch: the
        # response echoes the ARN for the ARN table and the bare name for the
        # bare table, simultaneously (per-reference echo, matching DynamoDB).
        name_a, arn_a = hash_table
        name_b, _arn_b = second_table
        dynamodb_client.put_item(TableName=name_a, Item={"pk": {"S": "mx-a"}})
        dynamodb_client.put_item(TableName=name_b, Item={"pk": {"S": "mx-b"}})
        resp = dynamodb_client.batch_get_item(
            RequestItems={
                arn_a: {"Keys": [{"pk": {"S": "mx-a"}}]},
                name_b: {"Keys": [{"pk": {"S": "mx-b"}}]},
            }
        )
        responses = resp["Responses"]
        assert responses[arn_a] == [{"pk": {"S": "mx-a"}}]
        assert responses[name_b] == [{"pk": {"S": "mx-b"}}]

    def test_transact_write_condition_check_and_update_by_arn(self, dynamodb_client, hash_table):
        # ConditionCheck and Update sub-operations also accept an ARN TableName.
        name, arn = hash_table
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "tc1"}, "n": {"N": "1"}})
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "tc2"}, "n": {"N": "1"}})
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "ConditionCheck": {
                        "TableName": arn,
                        "Key": {"pk": {"S": "tc1"}},
                        "ConditionExpression": "attribute_exists(pk)",
                    }
                },
                {
                    "Update": {
                        "TableName": arn,
                        "Key": {"pk": {"S": "tc2"}},
                        "UpdateExpression": "SET n = :two",
                        "ExpressionAttributeValues": {":two": {"N": "2"}},
                    }
                },
            ]
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "tc2"}})
        assert resp["Item"]["n"] == {"N": "2"}
