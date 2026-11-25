# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for PartiQL reads through an index that does not
project every attribute, and for the FROM path rules every statement shares.

Rules pinned here, each measured on the service (2026-11-02):
  * a keyed read of the index (an equality or IN on its partition key at the
    top of the WHERE) is refused when its filter names an attribute the index
    does not project, on a GSI and on an LSI; an unkeyed read is a scan that
    evaluates the filter against the index rows;
  * a GSI read whose projection names such an attribute is refused, keyed or
    not, and the filter refusal is reported first;
  * an LSI read whose projection names such an attribute reads every row it
    walks back from the base table, serves the projection and the filter from
    that item, and charges each read on the table arm of ConsumedCapacity;
  * a begins_with with a number prefix gets the operand-type message, in a
    filter and on the LSI sort key, whether or not the read reaches back;
  * a FROM path with an empty component or more than two components is
    refused once the statement has parsed and before the table is looked up,
    and an UPDATE, DELETE or INSERT naming an index is refused the same way.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an ExtendDB server; unset, the
tests run against the live service. The table carries an
``extenddb-measure-ixr-`` prefix and is deleted in a finally.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest
from botocore.exceptions import ClientError

NOT_WELL_FORMED = "Statement wasn't well formed, can't be processed: "
EMPTY_COMPONENT = "Path component cannot be an empty string"
TOO_MANY_COMPONENTS = "A path may contain at most 2 components in the FROM clause"
ON_INDEX = "This operation is not supported on an index"
INVALID = "One or more parameter values were invalid: "


def _filter_refusal(index: str, names: str) -> str:
    return f"{INVALID}Secondary index {index} does not project one or more filter attributes: [{names}]"


def _projection_refusal(index: str, names: str) -> str:
    return f"{INVALID}Global secondary index {index} does not project [{names}]"


def _insert_refusal(table: str) -> str:
    # The span is the table name token: "INSERT INTO " is 12 characters, and
    # the token includes its two double quotes.
    return (
        "FROM clause may only contain a single table name in data manipulation "
        f"statements at 1:13:{len(table) + 2}"
    )


def _is_service() -> bool:
    return not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()


def _interval() -> float:
    return 0.3 if _is_service() else 0.03


# Items. Partition "a" holds the rows the LSIs index (a/1, a/2) and one row no
# index holds (a/3); b/1 sits in a second GSI partition. "note" and "label" are
# projected by no index.
ITEMS = [
    {
        "pk": {"S": "a"}, "sk": {"S": "1"}, "color": {"S": "red"},
        "rank": {"S": "r1"}, "weight": {"S": "w1"}, "dims": {"S": "S"},
        "note": {"S": "n1"}, "label": {"S": "l1"},
    },
    {
        "pk": {"S": "a"}, "sk": {"S": "2"}, "color": {"S": "red"},
        "rank": {"S": "r2"}, "weight": {"S": "w2"}, "dims": {"S": "M"},
        "note": {"S": "n2"}, "label": {"S": "l2"},
    },
    {"pk": {"S": "a"}, "sk": {"S": "3"}, "note": {"S": "n3"}},
    {
        "pk": {"S": "b"}, "sk": {"S": "1"}, "color": {"S": "blue"},
        "dims": {"S": "L"}, "note": {"S": "n4"},
    },
]


def _wait_active(client, name: str, timeout: float = 300.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        d = client.describe_table(TableName=name)["Table"]
        gsis = d.get("GlobalSecondaryIndexes", [])
        if d["TableStatus"] == "ACTIVE" and all(g.get("IndexStatus") == "ACTIVE" for g in gsis):
            return
        time.sleep(_interval())
    raise TimeoutError(f"{name} not ACTIVE within {timeout}s")


def _wait_deleted(client, name: str, timeout: float = 300.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client.describe_table(TableName=name)
        except client.exceptions.ResourceNotFoundException:
            return
        time.sleep(_interval())


def _wait_gsi(client, name: str, color: str, want: int, timeout: float = 60.0) -> None:
    """A GSI fills asynchronously on both targets; poll until it holds the rows."""
    statement = f"SELECT pk FROM \"{name}\".\"by-color\" WHERE color = '{color}'"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(client.execute_statement(Statement=statement).get("Items", [])) >= want:
            return
        time.sleep(_interval())
    raise TimeoutError(f"by-color did not reach {want} rows for {color}")


@pytest.fixture(scope="module")
def table(dynamodb_client):
    name = f"extenddb-measure-ixr-{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": n, "AttributeType": "S"}
            for n in ("pk", "sk", "color", "rank", "weight")
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-color",
                "KeySchema": [{"AttributeName": "color", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["dims"]},
            }
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "by-rank",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "rank", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            },
            {
                "IndexName": "by-weight",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "weight", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["dims"]},
            },
        ],
    )
    try:
        _wait_active(dynamodb_client, name)
        for item in ITEMS:
            dynamodb_client.put_item(TableName=name, Item=item)
        _wait_gsi(dynamodb_client, name, "red", 2)
        _wait_gsi(dynamodb_client, name, "blue", 1)
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
            _wait_deleted(dynamodb_client, name)
        except ClientError:
            pass


def _refusal(client, statement: str, **kwargs) -> str:
    with pytest.raises(ClientError) as ei:
        client.execute_statement(Statement=statement, **kwargs)
    err = ei.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def _items(client, statement: str, **kwargs) -> list:
    return client.execute_statement(Statement=statement, **kwargs).get("Items", [])


def _keys(items) -> list:
    return sorted(f"{i['pk']['S']}/{i['sk']['S']}" for i in items)


# ---- the filter of a keyed read --------------------------------------------

@pytest.mark.parametrize(
    "where",
    [
        "color = 'red' AND note = 'n1'",
        "color = 'red' AND NOT note = 'n1'",
        "color = 'red' AND note IS MISSING",
        "color = 'red' AND note IS NOT MISSING",
        "color = 'red' AND begins_with(note, 'n')",
        "color IN ['red', 'blue'] AND note = 'n1'",
        "color = 'red' AND (note = 'n1' OR dims = 'S')",
        "color = 'red' AND note.child = 'x'",
        "'red' = color AND note = 'n1'",
    ],
)
def test_keyed_gsi_filter_on_an_unprojected_attribute_is_refused(dynamodb_client, table, where):
    message = _refusal(dynamodb_client, f'SELECT pk FROM "{table}"."by-color" WHERE {where}')
    assert message == _filter_refusal("by-color", "note")


@pytest.mark.parametrize(
    "where",
    [
        "pk = 'a' AND note = 'n1'",
        "pk = 'a' AND rank BETWEEN 'r1' AND 'r2' AND note = 'n1'",
        "pk = 'a' AND rank > 'r0' AND note = 'n1'",
        "pk = 'a' AND begins_with(rank, 'r') AND note = 'n1'",
        "pk = 'a' AND rank IN ['r1', 'r2'] AND note = 'n1'",
    ],
)
def test_keyed_lsi_filter_on_an_unprojected_attribute_is_refused(dynamodb_client, table, where):
    message = _refusal(dynamodb_client, f'SELECT pk FROM "{table}"."by-rank" WHERE {where}')
    assert message == _filter_refusal("by-rank", "note")


def test_a_not_equal_sort_condition_makes_the_lsi_read_a_scan(dynamodb_client, table):
    statement = f"SELECT pk FROM \"{table}\".\"by-rank\" WHERE pk = 'a' AND rank <> 'r1' AND note = 'n1'"
    assert _items(dynamodb_client, statement) == []


def test_filter_refusal_lists_each_attribute_in_statement_order(dynamodb_client, table):
    message = _refusal(
        dynamodb_client,
        f"SELECT pk FROM \"{table}\".\"by-color\" WHERE color = 'red' AND note = 'n1' AND label = 'l1'",
    )
    assert message == _filter_refusal("by-color", "note, label")


@pytest.mark.parametrize(
    "where",
    [
        "note = 'n1'",
        "note = 'n1' AND note = 'n2'",
        "color <> 'blue' AND note = 'n1'",
        "begins_with(color, 're') AND note = 'n1'",
        "color = 'red' AND color = 'blue' AND note = 'n1'",
        "color = 'red' AND NOT color = 'blue' AND note = 'n1'",
        "pk = 'a' AND note = 'n1'",
    ],
)
def test_unkeyed_gsi_filter_is_evaluated_on_index_rows(dynamodb_client, table, where):
    # The index rows carry no "note", so nothing matches.
    assert _items(dynamodb_client, f'SELECT pk FROM "{table}"."by-color" WHERE {where}') == []


def test_unprojected_filter_inside_an_or_branch_is_accepted(dynamodb_client, table):
    items = _items(
        dynamodb_client,
        f"SELECT pk, sk FROM \"{table}\".\"by-color\" "
        "WHERE (color = 'red' AND note = 'n1') OR color = 'blue'",
    )
    assert _keys(items) == ["b/1"]


def test_keyed_filter_on_a_projected_attribute_is_accepted(dynamodb_client, table):
    items = _items(
        dynamodb_client,
        f"SELECT pk, sk FROM \"{table}\".\"by-color\" WHERE color = 'red' AND dims = 'S'",
    )
    assert _keys(items) == ["a/1"]


# ---- the projection of a GSI read ------------------------------------------

@pytest.mark.parametrize(
    "statement",
    [
        "SELECT note FROM \"{t}\".\"by-color\" WHERE color = 'red'",
        "SELECT note FROM \"{t}\".\"by-color\"",
        "SELECT pk, note FROM \"{t}\".\"by-color\" WHERE note = 'n1'",
        "SELECT note.child FROM \"{t}\".\"by-color\" WHERE color = 'red'",
    ],
)
def test_gsi_projection_of_an_unprojected_attribute_is_refused(dynamodb_client, table, statement):
    message = _refusal(dynamodb_client, statement.format(t=table))
    assert message == _projection_refusal("by-color", "note")


def test_projection_refusal_lists_each_attribute_in_statement_order(dynamodb_client, table):
    message = _refusal(
        dynamodb_client,
        f"SELECT note, label FROM \"{table}\".\"by-color\" WHERE color = 'red'",
    )
    assert message == _projection_refusal("by-color", "note, label")


def test_keyed_read_reports_the_filter_refusal_first(dynamodb_client, table):
    message = _refusal(
        dynamodb_client,
        f"SELECT note FROM \"{table}\".\"by-color\" WHERE color = 'red' AND label = 'l1'",
    )
    assert message == _filter_refusal("by-color", "label")


# ---- an LSI read that reaches back to the base table ------------------------

def _capacity(resp) -> tuple:
    cc = resp["ConsumedCapacity"]
    lsi = cc.get("LocalSecondaryIndexes", {})
    return (
        cc["CapacityUnits"],
        cc.get("Table", {}).get("CapacityUnits"),
        {name: arm["CapacityUnits"] for name, arm in lsi.items()},
    )


def test_lsi_serves_an_unprojected_attribute_from_the_base_table(dynamodb_client, table):
    resp = dynamodb_client.execute_statement(
        Statement=f"SELECT note FROM \"{table}\".\"by-rank\" WHERE pk = 'a'",
        ReturnConsumedCapacity="INDEXES",
    )
    assert resp["Items"] == [{"note": {"S": "n1"}}, {"note": {"S": "n2"}}]
    # Two rows walked: half a read unit each on the table arm, eventually
    # consistent; the index arm is the ordinary read of the two index rows.
    assert _capacity(resp) == (1.5, 1.0, {"by-rank": 0.5})


def test_reach_back_charge_follows_the_rows_walked(dynamodb_client, table):
    resp = dynamodb_client.execute_statement(
        Statement=f"SELECT note FROM \"{table}\".\"by-rank\" WHERE pk = 'a'",
        ReturnConsumedCapacity="INDEXES",
        Limit=1,
    )
    assert resp["Items"] == [{"note": {"S": "n1"}}]
    assert "NextToken" in resp
    assert _capacity(resp) == (1.0, 0.5, {"by-rank": 0.5})


def test_reach_back_charges_rows_the_filter_drops(dynamodb_client, table):
    resp = dynamodb_client.execute_statement(
        Statement=f"SELECT note FROM \"{table}\".\"by-weight\" WHERE pk = 'a' AND dims = 'XL'",
        ReturnConsumedCapacity="INDEXES",
    )
    assert resp["Items"] == []
    assert _capacity(resp) == (1.5, 1.0, {"by-weight": 0.5})


def test_reach_back_strong_read_doubles_both_arms(dynamodb_client, table):
    resp = dynamodb_client.execute_statement(
        Statement=f"SELECT note FROM \"{table}\".\"by-rank\" WHERE pk = 'a'",
        ReturnConsumedCapacity="INDEXES",
        ConsistentRead=True,
    )
    assert _capacity(resp) == (3.0, 2.0, {"by-rank": 1.0})


def test_reach_back_total_capacity_includes_the_table_reads(dynamodb_client, table):
    resp = dynamodb_client.execute_statement(
        Statement=f"SELECT note FROM \"{table}\".\"by-rank\" WHERE pk = 'a'",
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"]["CapacityUnits"] == 1.5
    assert "Table" not in resp["ConsumedCapacity"]


def test_reach_back_serves_key_and_unprojected_attributes_together(dynamodb_client, table):
    items = _items(
        dynamodb_client,
        f"SELECT sk, rank, label FROM \"{table}\".\"by-rank\" WHERE pk = 'a'",
    )
    assert items == [
        {"sk": {"S": "1"}, "rank": {"S": "r1"}, "label": {"S": "l1"}},
        {"sk": {"S": "2"}, "rank": {"S": "r2"}, "label": {"S": "l2"}},
    ]


def test_select_star_on_a_keys_only_lsi_does_not_reach_back(dynamodb_client, table):
    resp = dynamodb_client.execute_statement(
        Statement=f"SELECT * FROM \"{table}\".\"by-rank\" WHERE pk = 'a'",
        ReturnConsumedCapacity="INDEXES",
    )
    assert sorted(resp["Items"][0]) == ["pk", "rank", "sk"]
    assert _capacity(resp) == (0.5, 0.0, {"by-rank": 0.5})


def test_keyed_reach_back_still_refuses_an_unprojected_filter(dynamodb_client, table):
    message = _refusal(
        dynamodb_client,
        f"SELECT note FROM \"{table}\".\"by-rank\" WHERE pk = 'a' AND note = 'n1'",
    )
    assert message == _filter_refusal("by-rank", "note")


def test_unkeyed_reach_back_serves_the_base_items(dynamodb_client, table):
    items = _items(dynamodb_client, f'SELECT note FROM "{table}"."by-rank"')
    assert sorted(i["note"]["S"] for i in items) == ["n1", "n2"]


def test_unkeyed_reach_back_filters_the_base_items(dynamodb_client, table):
    items = _items(
        dynamodb_client,
        f"SELECT note FROM \"{table}\".\"by-rank\" WHERE note = 'n1'",
    )
    assert items == [{"note": {"S": "n1"}}]


def test_unkeyed_read_without_reach_back_filters_the_index_rows(dynamodb_client, table):
    items = _items(
        dynamodb_client,
        f"SELECT pk FROM \"{table}\".\"by-rank\" WHERE note = 'n1'",
    )
    assert items == []


BEGINS_WITH_NUMBER = (
    "Incorrect operand type for operator or function; operator or function: begins_with, "
    "operand type: N"
)


@pytest.mark.parametrize("projection", ["note", "*"], ids=["reach-back", "index-rows"])
def test_a_number_begins_with_prefix_in_an_lsi_filter(dynamodb_client, table, projection):
    message = _refusal(
        dynamodb_client,
        f"SELECT {projection} FROM \"{table}\".\"by-rank\" WHERE begins_with(note, 1)",
    )
    assert message == BEGINS_WITH_NUMBER


@pytest.mark.parametrize("projection", ["note", "*"], ids=["reach-back", "index-rows"])
def test_a_number_begins_with_prefix_on_the_lsi_sort_key(dynamodb_client, table, projection):
    message = _refusal(
        dynamodb_client,
        f"SELECT {projection} FROM \"{table}\".\"by-rank\" "
        "WHERE pk = 'a' AND begins_with(rank, 1)",
    )
    assert message == BEGINS_WITH_NUMBER


# ---- the FROM path ----------------------------------------------------------

def test_three_component_path_is_refused(dynamodb_client, table):
    message = _refusal(
        dynamodb_client, f"SELECT * FROM \"{table}\".\"by-color\".\"x\" WHERE color = 'red'"
    )
    assert message == TOO_MANY_COMPONENTS


def test_empty_index_component_is_refused(dynamodb_client, table):
    message = _refusal(dynamodb_client, f"SELECT * FROM \"{table}\".\"\" WHERE pk = 'a'")
    assert message == EMPTY_COMPONENT


def test_empty_table_component_is_refused(dynamodb_client):
    assert _refusal(dynamodb_client, "SELECT * FROM \"\" WHERE pk = 'a'") == EMPTY_COMPONENT


def test_empty_component_is_reported_ahead_of_the_length(dynamodb_client, table):
    message = _refusal(dynamodb_client, f"SELECT * FROM \"{table}\".\"by-color\".\"\" WHERE pk = 'a'")
    assert message == EMPTY_COMPONENT


def test_path_is_refused_before_the_table_is_looked_up(dynamodb_client):
    missing = f"extenddb-no-such-{uuid.uuid4().hex[:8]}"
    message = _refusal(dynamodb_client, f"SELECT * FROM \"{missing}\".\"a\".\"b\" WHERE pk = 'a'")
    assert message == TOO_MANY_COMPONENTS


def test_syntax_error_is_reported_ahead_of_the_path(dynamodb_client, table):
    message = _refusal(
        dynamodb_client, f"SELECT * FROM \"{table}\".\"a\".\"b\" WHERE pk = 'a'; SELECT 1"
    )
    assert message.startswith(NOT_WELL_FORMED)


def test_path_is_refused_ahead_of_the_limit_clause(dynamodb_client, table):
    message = _refusal(dynamodb_client, f"SELECT * FROM \"{table}\".\"a\".\"b\" LIMIT 1")
    assert message == TOO_MANY_COMPONENTS


# ---- write statements naming an index ---------------------------------------

@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE \"{t}\".\"by-color\" SET dims = 'Z' WHERE pk = 'a' AND sk = '1'",
        "UPDATE \"{t}\".\"no-such-index\" SET dims = 'Z' WHERE pk = 'a' AND sk = '1'",
        "DELETE FROM \"{t}\".\"by-color\" WHERE pk = 'a' AND sk = '1'",
        "DELETE FROM \"{t}\".\"by-rank\" WHERE pk = 'a' AND sk = '1'",
    ],
)
def test_update_and_delete_on_an_index_are_refused(dynamodb_client, table, statement):
    assert _refusal(dynamodb_client, statement.format(t=table)) == ON_INDEX


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE \"{t}\".\"by-color\" WHERE pk = 'a' AND sk = '1'",
        "INSERT INTO \"{t}\".\"by-color\" VALUE {{'pk': 'w', 'sk': 'w'",
    ],
)
def test_write_syntax_error_is_reported_ahead_of_the_index(dynamodb_client, table, statement):
    assert _refusal(dynamodb_client, statement.format(t=table)).startswith(NOT_WELL_FORMED)


def test_update_on_an_index_is_refused_before_the_table_is_looked_up(dynamodb_client):
    missing = f"extenddb-no-such-{uuid.uuid4().hex[:8]}"
    message = _refusal(
        dynamodb_client, f"UPDATE \"{missing}\".\"idx\" SET a = 1 WHERE pk = 'a' AND sk = '1'"
    )
    assert message == ON_INDEX


@pytest.mark.parametrize(
    ("statement", "want"),
    [
        ("UPDATE \"{t}\".\"a\".\"b\" SET a = 1 WHERE pk = 'a' AND sk = '1'", TOO_MANY_COMPONENTS),
        ("DELETE FROM \"{t}\".\"\" WHERE pk = 'a' AND sk = '1'", EMPTY_COMPONENT),
        ("UPDATE \"\" SET a = 1 WHERE pk = 'a' AND sk = '1'", EMPTY_COMPONENT),
    ],
)
def test_write_paths_take_the_select_path_refusals(dynamodb_client, table, statement, want):
    assert _refusal(dynamodb_client, statement.format(t=table)) == want


@pytest.mark.parametrize("target", ['"{t}"."by-color"', '"{t}"."no-such-index"', '"{t}"."a"."b"'])
def test_insert_into_a_path_is_refused_at_the_table_name(dynamodb_client, table, target):
    statement = f"INSERT INTO {target.format(t=table)} VALUE {{'pk': 'w', 'sk': 'w'}}"
    assert _refusal(dynamodb_client, statement) == _insert_refusal(table)


def test_insert_into_a_path_is_refused_before_the_table_is_looked_up(dynamodb_client):
    missing = f"extenddb-no-such-{uuid.uuid4().hex[:8]}"
    statement = f"INSERT INTO \"{missing}\".\"i\" VALUE {{'pk': 'w', 'sk': 'w'}}"
    assert _refusal(dynamodb_client, statement) == _insert_refusal(missing)


def test_refused_writes_leave_the_table_unchanged(dynamodb_client, table):
    got = dynamodb_client.get_item(
        TableName=table, Key={"pk": {"S": "a"}, "sk": {"S": "1"}}, ConsistentRead=True
    )["Item"]
    assert got == ITEMS[0]
    absent = dynamodb_client.get_item(
        TableName=table, Key={"pk": {"S": "w"}, "sk": {"S": "w"}}, ConsistentRead=True
    )
    assert "Item" not in absent
