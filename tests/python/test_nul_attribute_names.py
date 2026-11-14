# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""U+0000 and U+0001 in the attribute names of a table definition.

DynamoDB accepts U+0000 inside a key schema attribute name (the table's or a
secondary index's), an attribute definition and a projected attribute name,
and DescribeTable returns each name byte for byte. Measured against the
service on 2026-09-18 (partition key and GSI key rows) and 2026-10-24 (LSI key
and projected name row), and again on 2026-11-04 in us-east-1 for those four
rows. The SDK's parameter validation checks no pattern on an attribute name,
so the request reaches the server with the name as written.

On PostgreSQL the catalog stores these names through an escape (U+0000 as
U+0001 U+0001, U+0001 as U+0001 U+0002). The U+0001 row holds the stored
forms themselves as names: a raw U+0001 pair, which the decoder must not read
as U+0000, and U+0001 U+0002. DynamoDB returned those names byte for byte
too (measured in us-east-1 on 2026-11-04).
"""

from __future__ import annotations

import contextlib
import time

NUL = "\u0000"
SOH = "\u0001"
STX = "\u0002"


def _defs(table: dict) -> dict:
    return {d["AttributeName"]: d["AttributeType"] for d in table["AttributeDefinitions"]}


def _wait_for_table_and_indexes(client, name: str, timeout: float = 900.0) -> dict:
    """Poll DescribeTable until the table and every GSI on it are ACTIVE, and
    return the description.

    A GSI that UpdateTable adds is still CREATING after the table is ACTIVE
    again, and one that UpdateTable deletes is listed as DELETING until it is
    gone. `helpers.wait_for_active` reads only the table status.
    """
    deadline = time.monotonic() + timeout
    delay = 0.1
    while True:
        table = client.describe_table(TableName=name)["Table"]
        indexes = {g["IndexName"]: g["IndexStatus"] for g in table.get("GlobalSecondaryIndexes", [])}
        if table["TableStatus"] == "ACTIVE" and all(s == "ACTIVE" for s in indexes.values()):
            return table
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"{name}: table {table['TableStatus']}, indexes {indexes} after {timeout:.0f} s"
            )
        time.sleep(delay)
        delay = min(delay * 2, 5.0)


class TestNulInKeyAttributeNames:
    def test_partition_key_attribute_name_with_nul_is_stored_and_returned(
        self, dynamodb_client, table_factory
    ):
        pk = f"p{NUL}k"
        name = table_factory(hash_key=pk)
        table = dynamodb_client.describe_table(TableName=name)["Table"]
        assert table["TableStatus"] == "ACTIVE"
        assert table["KeySchema"] == [{"AttributeName": pk, "KeyType": "HASH"}]
        assert table["AttributeDefinitions"] == [{"AttributeName": pk, "AttributeType": "S"}]

    def test_gsi_key_attribute_name_with_nul_is_stored_and_returned(
        self, dynamodb_client, table_factory
    ):
        gk = f"g{NUL}k"
        name = table_factory(
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": gk, "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": gk, "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        table = dynamodb_client.describe_table(TableName=name)["Table"]
        gsi = table["GlobalSecondaryIndexes"][0]
        assert gsi["IndexName"] == "gsi1"
        assert gsi["KeySchema"] == [{"AttributeName": gk, "KeyType": "HASH"}]
        assert _defs(table) == {"pk": "S", gk: "S"}

    def test_gsi_on_nul_attribute_can_be_removed_and_added_again(
        self, dynamodb_client, table_factory
    ):
        gk = f"g{NUL}k"
        name = table_factory(
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": gk, "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": gk, "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        # The fixture's cleanup ignores a DeleteTable that fails, and three
        # runs of this test against DynamoDB that ended while gsi2 was still
        # CREATING left their table behind. So the test waits for every index
        # before it ends, whether it passes or fails.
        try:
            dynamodb_client.update_table(
                TableName=name,
                GlobalSecondaryIndexUpdates=[{"Delete": {"IndexName": "gsi1"}}],
            )
            table = _wait_for_table_and_indexes(dynamodb_client, name)
            assert not table.get("GlobalSecondaryIndexes")
            dynamodb_client.update_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": gk, "AttributeType": "S"}],
                GlobalSecondaryIndexUpdates=[
                    {
                        "Create": {
                            "IndexName": "gsi2",
                            "KeySchema": [{"AttributeName": gk, "KeyType": "HASH"}],
                            "Projection": {"ProjectionType": "ALL"},
                        }
                    }
                ],
            )
            table = _wait_for_table_and_indexes(dynamodb_client, name)
            gsi = table["GlobalSecondaryIndexes"][0]
            assert gsi["IndexName"] == "gsi2"
            assert gsi["KeySchema"] == [{"AttributeName": gk, "KeyType": "HASH"}]
            assert _defs(table) == {"pk": "S", gk: "S"}
        finally:
            with contextlib.suppress(AssertionError):
                _wait_for_table_and_indexes(dynamodb_client, name)

    def test_lsi_key_and_gsi_projected_attribute_names_with_nul_are_stored_and_returned(
        self, dynamodb_client, table_factory
    ):
        """An LSI keyed on a U+0000 attribute and a GSI whose NonKeyAttributes
        entry holds U+0000: both returned byte for byte (measured 2026-10-24
        and 2026-11-04)."""
        lk = f"l{NUL}k"
        nk = f"n{NUL}k"
        name = table_factory(
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gk", "AttributeType": "S"},
                {"AttributeName": lk, "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "lsi1",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": lk, "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": [nk]},
                }
            ],
        )
        table = dynamodb_client.describe_table(TableName=name)["Table"]
        lsi = table["LocalSecondaryIndexes"][0]
        assert lsi["KeySchema"] == [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": lk, "KeyType": "RANGE"},
        ]
        gsi = table["GlobalSecondaryIndexes"][0]
        assert gsi["Projection"] == {"ProjectionType": "INCLUDE", "NonKeyAttributes": [nk]}
        assert _defs(table) == {"pk": "S", "sk": "S", "gk": "S", lk: "S"}


class TestSohInKeyAttributeNames:
    def test_key_and_projected_attribute_names_with_u0001_are_stored_and_returned(
        self, dynamodb_client, table_factory
    ):
        """A raw U+0001 pair in the partition key and the GSI key, and
        U+0001 U+0002 in the sort key and a projected name."""
        pk = f"a{SOH}{SOH}z"
        sk = f"b{SOH}{STX}y"
        gk = f"g{SOH}{SOH}k"
        nk = f"n{SOH}{STX}"
        name = table_factory(
            AttributeDefinitions=[
                {"AttributeName": pk, "AttributeType": "S"},
                {"AttributeName": sk, "AttributeType": "S"},
                {"AttributeName": gk, "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": pk, "KeyType": "HASH"},
                {"AttributeName": sk, "KeyType": "RANGE"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": gk, "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": [nk]},
                }
            ],
        )
        table = dynamodb_client.describe_table(TableName=name)["Table"]
        assert table["KeySchema"] == [
            {"AttributeName": pk, "KeyType": "HASH"},
            {"AttributeName": sk, "KeyType": "RANGE"},
        ]
        assert _defs(table) == {pk: "S", sk: "S", gk: "S"}
        gsi = table["GlobalSecondaryIndexes"][0]
        assert gsi["KeySchema"] == [{"AttributeName": gk, "KeyType": "HASH"}]
        assert gsi["Projection"] == {"ProjectionType": "INCLUDE", "NonKeyAttributes": [nk]}
