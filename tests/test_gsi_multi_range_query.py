# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query on a GSI with one HASH attribute and several RANGE attributes.

The index key schema is HASH ``h`` and RANGE ``a``, ``b``, ``c``, ``d``. Amazon
DynamoDB orders an index partition lexicographically over all four RANGE
attributes, in key-schema order, and serves a reverse Query as the exact
mirror. A key condition may name a leading run of the RANGE attributes, with
an equality on every one before the last and any sort-key operator on the
last; a gap, or an operator before a later condition, is refused. Clause
order in the expression is free.

The item values are chosen so that ties on ``a`` are broken by ``b``, ties on
``b`` by ``c`` and ties on ``c`` by ``d``, and so that the base key order of
the items is the reverse of their index order. An item without ``c`` is not
in the index.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import configured_backend, _is_real_dynamodb, scoped_table, wait_for_gsi_items

INDEX = "gsi"
TWO_HASH_INDEX = "two-hash"
RANGE_ATTRS = ("a", "b", "c", "d")

# PK -> (a, b, c, d). p6 has no c, so the index does not hold it.
ITEMS = {
    "p1": ("T", "live", "b", "r1"),
    "p2": ("T", "live", "a", "r2"),
    "p3": ("T", "live", "a", "r1"),
    "p4": ("T", "draft", "c", "r1"),
    "p5": ("F", "live", "a", "r0"),
    "p6": ("T", "live", None, "r1"),
}
INDEXED = 5


def _s(name: str) -> dict:
    return {"AttributeName": name, "AttributeType": "S"}


@pytest.fixture(scope="module")
def multi_range_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            _s("PK"),
            _s("h"),
            *(_s(n) for n in RANGE_ATTRS),
            _s("h1"),
            _s("h2"),
            {"AttributeName": "r", "AttributeType": "N"},
        ],
        key_schema=[{"AttributeName": "PK", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": INDEX,
                "KeySchema": [
                    {"AttributeName": "h", "KeyType": "HASH"},
                    *({"AttributeName": n, "KeyType": "RANGE"} for n in RANGE_ATTRS),
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": TWO_HASH_INDEX,
                "KeySchema": [
                    {"AttributeName": "h1", "KeyType": "HASH"},
                    {"AttributeName": "h2", "KeyType": "HASH"},
                    {"AttributeName": "r", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    ) as name:
        for pk, values in ITEMS.items():
            item = {"PK": {"S": pk}, "h": {"S": "o1"}}
            for attr, value in zip(RANGE_ATTRS, values):
                if value is not None:
                    item[attr] = {"S": value}
            dynamodb_client.put_item(TableName=name, Item=item)
        indexed = wait_for_gsi_items(
            lambda: _query_all(dynamodb_client, name, "h = :h", {":h": "o1"}),
            INDEXED,
        )
        assert len(indexed) == INDEXED, indexed
        yield name


FIRST_RANGE_ONLY_MARK = "applies a key condition to the first RANGE attribute"
FIRST_RANGE_ONLY_BACKENDS = ("mongodb", "cassandra")


@pytest.fixture(scope="module")
def first_range_only(dynamodb_client, multi_range_table) -> bool:
    """True when the backend under test refuses a key condition on a later
    RANGE attribute.

    The MongoDB and Cassandra backends store and filter the first RANGE
    attribute of a multi-attribute index only and refuse the rest; the
    service, SQLite and PostgreSQL serve them. The backend comes from
    `EXTENDDB_TEST_BACKEND` or the server's config file, as tests/test_ttl.py
    reads it, and the server's reply must match it: a backend that serves
    these conditions and starts refusing them fails here instead of skipping
    the served-result tests.
    """
    backend = None if _is_real_dynamodb() else configured_backend()
    expected = backend in FIRST_RANGE_ONLY_BACKENDS
    try:
        dynamodb_client.query(
            TableName=multi_range_table,
            IndexName=INDEX,
            KeyConditionExpression="h = :h AND a = :a AND b = :b",
            ExpressionAttributeValues=_values({":h": "o1", ":a": "T", ":b": "live"}),
        )
        refused = None
    except ClientError as exc:
        refused = exc.response["Error"]
    if expected and (refused is None or FIRST_RANGE_ONLY_MARK not in refused["Message"]):
        pytest.fail(f"backend {backend} should refuse a condition on b, got {refused or 'items'}")
    if not expected and refused is not None:
        pytest.fail(f"backend {backend or 'unknown'} should serve a condition on b, got {refused}")
    return expected


@pytest.fixture
def needs_all_range_attributes(first_range_only):
    if first_range_only:
        pytest.skip("this backend applies key conditions to the first RANGE attribute only")


def _values(values: dict[str, str]) -> dict:
    return {k: {"S": v} for k, v in values.items()}


def _query(client, table: str, kce: str, values: dict[str, str], **kwargs) -> dict:
    return client.query(
        TableName=table,
        IndexName=INDEX,
        KeyConditionExpression=kce,
        ExpressionAttributeValues=_values(values),
        **kwargs,
    )


def _query_all(client, table: str, kce: str, values: dict[str, str], **kwargs) -> list[str]:
    """Every page of the query, as the PK of each item in result order."""
    pks: list[str] = []
    start = None
    while True:
        extra = dict(kwargs)
        if start is not None:
            extra["ExclusiveStartKey"] = start
        resp = _query(client, table, kce, values, **extra)
        pks.extend(item["PK"]["S"] for item in resp["Items"])
        start = resp.get("LastEvaluatedKey")
        if start is None:
            return pks


def _refused(client, table: str, kce: str, values: dict[str, str]) -> dict:
    with pytest.raises(ClientError) as exc_info:
        _query(client, table, kce, values)
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err


@pytest.mark.usefixtures("needs_all_range_attributes")
class TestOrder:
    """Results are ordered over every RANGE attribute in key-schema order."""

    def test_hash_only_orders_over_all_range_attributes(self, dynamodb_client, multi_range_table):
        pks = _query_all(dynamodb_client, multi_range_table, "h = :h", {":h": "o1"})
        assert pks == ["p5", "p4", "p3", "p2", "p1"]

    def test_hash_only_reverse_is_the_mirror(self, dynamodb_client, multi_range_table):
        pks = _query_all(
            dynamodb_client,
            multi_range_table,
            "h = :h",
            {":h": "o1"},
            ScanIndexForward=False,
        )
        assert pks == ["p1", "p2", "p3", "p4", "p5"]

    def test_first_range_equality_orders_by_the_rest(self, dynamodb_client, multi_range_table):
        pks = _query_all(
            dynamodb_client,
            multi_range_table,
            "h = :h AND a = :a",
            {":h": "o1", ":a": "T"},
        )
        assert pks == ["p4", "p3", "p2", "p1"]

    def test_first_range_equality_reverse(self, dynamodb_client, multi_range_table):
        pks = _query_all(
            dynamodb_client,
            multi_range_table,
            "h = :h AND a = :a",
            {":h": "o1", ":a": "T"},
            ScanIndexForward=False,
        )
        assert pks == ["p1", "p2", "p3", "p4"]

    @pytest.mark.parametrize("forward", [True, False])
    def test_one_item_pages_resume_over_all_range_attributes(
        self, dynamodb_client, multi_range_table, forward
    ):
        pks = _query_all(
            dynamodb_client,
            multi_range_table,
            "h = :h",
            {":h": "o1"},
            Limit=1,
            ScanIndexForward=forward,
        )
        want = ["p5", "p4", "p3", "p2", "p1"]
        assert pks == (want if forward else want[::-1])

    def test_limit_page_and_last_evaluated_key_shape(self, dynamodb_client, multi_range_table):
        resp = _query(
            dynamodb_client,
            multi_range_table,
            "h = :h AND a = :a",
            {":h": "o1", ":a": "T"},
            Limit=2,
        )
        assert [item["PK"]["S"] for item in resp["Items"]] == ["p4", "p3"]
        lek = resp["LastEvaluatedKey"]
        assert set(lek) == {"PK", "h", "a", "b", "c", "d"}
        assert lek["PK"] == {"S": "p3"}


@pytest.mark.usefixtures("needs_all_range_attributes")
class TestAcceptedKeyConditions:
    """A leading run of RANGE attributes, an operator only on the last one."""

    @pytest.mark.parametrize(
        ("kce", "values", "want"),
        [
            (
                "h = :h AND a = :a AND b = :b",
                {":h": "o1", ":a": "T", ":b": "live"},
                ["p3", "p2", "p1"],
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c",
                {":h": "o1", ":a": "T", ":b": "live", ":c": "a"},
                ["p3", "p2"],
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d = :d",
                {":h": "o1", ":a": "T", ":b": "live", ":c": "a", ":d": "r1"},
                ["p3"],
            ),
            (
                "h = :h AND a = :a AND b = :b AND c > :c",
                {":h": "o1", ":a": "T", ":b": "live", ":c": "a"},
                ["p1"],
            ),
            (
                "h = :h AND a = :a AND b < :b",
                {":h": "o1", ":a": "T", ":b": "live"},
                ["p4"],
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND begins_with(d, :d)",
                {":h": "o1", ":a": "T", ":b": "live", ":c": "a", ":d": "r"},
                ["p3", "p2"],
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d BETWEEN :x AND :y",
                {":h": "o1", ":a": "T", ":b": "live", ":c": "a", ":x": "r1", ":y": "r1"},
                ["p3"],
            ),
        ],
        ids=[
            "eq-a-b",
            "eq-a-b-c",
            "eq-all-four",
            "gt-on-c",
            "lt-on-b",
            "begins-with-on-d",
            "between-on-d",
        ],
    )
    def test_condition_is_served(self, dynamodb_client, multi_range_table, kce, values, want):
        assert _query_all(dynamodb_client, multi_range_table, kce, values) == want

    @pytest.mark.parametrize(
        "kce",
        [
            "b = :b AND h = :h AND a = :a",
            "a = :a AND b = :b AND h = :h",
        ],
        ids=["hash-in-the-middle", "hash-last"],
    )
    def test_clause_order_is_free(self, dynamodb_client, multi_range_table, kce):
        pks = _query_all(
            dynamodb_client,
            multi_range_table,
            kce,
            {":h": "o1", ":a": "T", ":b": "live"},
        )
        assert pks == ["p3", "p2", "p1"]


class TestRefusedKeyConditions:
    """A gap in the RANGE attributes, or an operator before a later condition."""

    @pytest.mark.parametrize(
        ("kce", "values", "message"),
        [
            (
                "h = :h AND b = :b",
                {":h": "o1", ":b": "live"},
                "RANGE key attributes a must have equality conditions specified in the "
                "query because a condition is present on key attribute b",
            ),
            (
                "h = :h AND a = :a AND c = :c",
                {":h": "o1", ":a": "T", ":c": "a"},
                "RANGE key attributes b must have equality conditions specified in the "
                "query because a condition is present on key attribute c",
            ),
            (
                "h = :h AND a = :a AND b > :b AND c = :c",
                {":h": "o1", ":a": "T", ":b": "draft", ":c": "a"},
                "RANGE key attributes b must have equality conditions specified in the "
                "query because a condition is present on key attribute c",
            ),
        ],
        ids=["gap-at-a", "gap-at-b", "operator-before-c"],
    )
    def test_refused_with_the_attribute_named(
        self, dynamodb_client, multi_range_table, kce, values, message
    ):
        err = _refused(dynamodb_client, multi_range_table, kce, values)
        assert err["Message"] == message

    def test_two_missing_range_attributes_are_refused(self, dynamodb_client, multi_range_table):
        err = _refused(
            dynamodb_client,
            multi_range_table,
            "h = :h AND c = :c",
            {":h": "o1", ":c": "a"},
        )
        assert "must have equality conditions specified in the query" in err["Message"]

    @pytest.mark.parametrize(
        "kce,values",
        [
            ("a = :a AND b = :b AND c = :c", {":a": "T", ":b": "live", ":c": "a"}),
            ("a = :a", {":a": "T"}),
        ],
        ids=["three-range-no-hash", "one-range-no-hash"],
    )
    def test_missing_hash_is_refused(self, dynamodb_client, multi_range_table, kce, values):
        err = _refused(dynamodb_client, multi_range_table, kce, values)
        assert err["Message"] == (
            "HASH key attributes h must have equality conditions specified in the query"
        )

    @pytest.mark.parametrize(
        "kce,values,missing",
        [
            ("h1 = :x", {":x": "1"}, "h2"),
            ("h2 = :x", {":x": "1"}, "h1"),
            ("h1 = :x AND r = :y", {":x": "1", ":y": "2"}, "h2"),
            ("r = :y", {":y": "2"}, "h1, h2"),
            ("h1 = :x AND h2 > :y", {":x": "1", ":y": "2"}, "h2"),
        ],
        ids=["h1-only", "h2-only", "h1-and-range", "range-only", "non-equality-on-h2"],
    )
    def test_two_hash_index_names_every_missing_hash_attribute(
        self, dynamodb_client, multi_range_table, kce, values, missing
    ):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=multi_range_table,
                IndexName=TWO_HASH_INDEX,
                KeyConditionExpression=kce,
                ExpressionAttributeValues=_values(values),
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"] == (
            f"HASH key attributes {missing} must have equality conditions specified in the query"
        )

    @pytest.mark.parametrize(
        "kce,values",
        [
            ("h = :x AND h = :y", {":x": "o1", ":y": "o2"}),
            ("h = :x AND h = :y AND a = :z", {":x": "o1", ":y": "o2", ":z": "T"}),
            ("h = :x AND a = :y AND a = :z", {":x": "o1", ":y": "T", ":z": "F"}),
            ("h = :h AND a = :a AND a > :x", {":h": "o1", ":a": "T", ":x": "A"}),
            ("h = :h AND a > :x AND a = :a", {":h": "o1", ":a": "T", ":x": "A"}),
            ("h = :h AND a = :a AND b = :b AND b > :lo", {":h": "o1", ":a": "T", ":b": "live", ":lo": "a"}),
            ("h = :h AND h > :x", {":h": "o1", ":x": "a"}),
        ],
        ids=[
            "hash-twice",
            "hash-twice-with-range",
            "range-twice",
            "range-eq-then-gt",
            "range-gt-then-eq",
            "second-range-eq-then-gt",
            "hash-eq-then-gt",
        ],
    )
    def test_a_second_condition_on_one_key_attribute_is_refused(
        self, dynamodb_client, multi_range_table, kce, values
    ):
        err = _refused(dynamodb_client, multi_range_table, kce, values)
        assert err["Message"] == (
            "Invalid KeyConditionExpression: KeyConditionExpressions must only contain "
            "one condition per key"
        )

    @pytest.mark.parametrize(
        "kce,values,missing",
        [
            ("h = :h AND z = :z", {":h": "o1", ":z": "zz"}, "a"),
            ("h = :h AND z = :z AND y = :y", {":h": "o1", ":z": "zz", ":y": "yy"}, "a"),
            ("h = :h AND z > :z", {":h": "o1", ":z": "zz"}, "a"),
            ("h = :h AND a = :a AND z = :z", {":h": "o1", ":a": "T", ":z": "zz"}, "b"),
            ("h = :h AND a = :a AND z > :z", {":h": "o1", ":a": "T", ":z": "zz"}, "b"),
            ("h = :h AND a > :a AND z = :z", {":h": "o1", ":a": "A", ":z": "zz"}, "b"),
            ("h = :h AND a = :a AND b = :b AND z > :z", {":h": "o1", ":a": "T", ":b": "live", ":z": "zz"}, "c"),
            ("h = :h AND PK = :p", {":h": "o1", ":p": "p1"}, "a"),
        ],
        ids=[
            "nonkey-eq",
            "two-nonkey-eq",
            "nonkey-gt",
            "nonkey-eq-after-a",
            "nonkey-gt-after-a",
            "nonkey-eq-after-a-gt",
            "nonkey-gt-after-a-b",
            "base-table-key",
        ],
    )
    def test_a_condition_on_a_non_key_attribute_names_the_first_uncovered_range_attribute(
        self, dynamodb_client, multi_range_table, kce, values, missing
    ):
        err = _refused(dynamodb_client, multi_range_table, kce, values)
        assert err["Message"] == f"Query condition missed key schema element: {missing}"

    def test_a_non_key_condition_on_a_two_hash_index_names_the_range_attribute(
        self, dynamodb_client, multi_range_table
    ):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=multi_range_table,
                IndexName=TWO_HASH_INDEX,
                KeyConditionExpression="h1 = :x AND h2 = :y AND z = :z",
                ExpressionAttributeValues=_values({":x": "1", ":y": "2", ":z": "3"}),
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"] == "Query condition missed key schema element: r"

    @pytest.mark.parametrize(
        "kce,values,count",
        [
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND z = :z",
                {":h": "o1", ":a": "T", ":b": "live", ":c": "a", ":d": "r1", ":z": "zz"},
                6,
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND z > :z",
                {":h": "o1", ":a": "T", ":b": "live", ":c": "a", ":d": "r1", ":z": "zz"},
                6,
            ),
        ],
        ids=["six-with-nonkey-eq", "six-with-nonkey-gt"],
    )
    def test_more_conditions_than_key_attributes_are_refused_by_count(
        self, dynamodb_client, multi_range_table, kce, values, count
    ):
        err = _refused(dynamodb_client, multi_range_table, kce, values)
        assert err["Message"] == (
            f"The number of query conditions ({count}) exceeds the number of key attributes "
            "defined in the schema (5)"
        )

    @pytest.mark.parametrize(
        "kce,values",
        [
            ("h = :h", {":h": {"N": "1"}}),
            ("h = :h AND a = :a", {":h": {"S": "o1"}, ":a": {"N": "1"}}),
            ("h = :h AND a = :a AND b = :b", {":h": {"S": "o1"}, ":a": {"S": "T"}, ":b": {"N": "1"}}),
            ("h = :h AND a = :a AND b > :b", {":h": {"S": "o1"}, ":a": {"S": "T"}, ":b": {"N": "1"}}),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND begins_with(d, :d)",
                {":h": {"S": "o1"}, ":a": {"S": "T"}, ":b": {"S": "live"}, ":c": {"S": "a"}, ":d": {"B": b"r"}},
            ),
            ("h = :h AND z = :z", {":h": {"N": "1"}, ":z": {"S": "zz"}}),
        ],
        ids=["hash-n", "a-n", "b-n", "b-gt-n", "d-begins-with-b", "hash-n-with-nonkey"],
    )
    def test_a_value_of_the_wrong_type_is_refused(self, dynamodb_client, multi_range_table, kce, values):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=multi_range_table,
                IndexName=INDEX,
                KeyConditionExpression=kce,
                ExpressionAttributeValues=values,
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"] == (
            "One or more parameter values were invalid: Condition parameter type does not match "
            "schema type"
        )

    @pytest.mark.parametrize(
        "index,kce,values,missing",
        [
            (INDEX, "h = :h AND PK = :p", {":h": {"S": "o1"}, ":p": {"N": "1"}}, "a"),
            (INDEX, "h = :h AND r = :r", {":h": {"S": "o1"}, ":r": {"S": "1"}}, "a"),
            (
                TWO_HASH_INDEX,
                "h1 = :x AND h2 = :y AND a = :a",
                {":x": {"S": "1"}, ":y": {"S": "2"}, ":a": {"N": "1"}},
                "r",
            ),
        ],
        ids=["base-table-key-n", "other-index-range-s", "two-hash-other-index-range-n"],
    )
    def test_a_value_on_an_attribute_outside_the_index_key_schema_is_not_type_checked(
        self, dynamodb_client, multi_range_table, index, kce, values, missing
    ):
        # The table defines the attribute (the base table key, or the other
        # index's key) with a different type, but the queried index's key schema
        # does not hold it, so the non-key rule answers. Measured 2026-10-30 on a
        # GSI with HASH GSI2PK and RANGE GSI2SK (one RANGE attribute):
        # `GSI2PK = :h AND PK = :k` answers `Query condition missed key schema
        # element: GSI2SK` with `:k` an S, PK's defined type, and with `:k` an N.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=multi_range_table,
                IndexName=index,
                KeyConditionExpression=kce,
                ExpressionAttributeValues=values,
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"] == f"Query condition missed key schema element: {missing}"


    def test_begins_with_on_a_number_attribute_is_refused(self, dynamodb_client, multi_range_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=multi_range_table,
                IndexName=TWO_HASH_INDEX,
                KeyConditionExpression="h1 = :x AND h2 = :y AND begins_with(r, :r)",
                ExpressionAttributeValues={":x": {"S": "1"}, ":y": {"S": "2"}, ":r": {"N": "1"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"] == (
            "Invalid KeyConditionExpression: Incorrect operand type for operator or function; "
            "operator or function: begins_with, operand type: N"
        )

    @pytest.mark.parametrize(
        "kce,values",
        [("h = :h AND #q = :a", {":h": "o1", ":a": "T"}), ("h = :h AND a = :a AND #q > :x", {":h": "o1", ":a": "T", ":x": "A"})],
        ids=["undefined-name-eq", "undefined-name-gt"],
    )
    def test_an_undefined_name_reference_is_refused(self, dynamodb_client, multi_range_table, kce, values):
        err = _refused(dynamodb_client, multi_range_table, kce, values)
        assert err["Message"] == (
            "Invalid KeyConditionExpression: An expression attribute name used in the document "
            "path is not defined; attribute name: #q"
        )


@pytest.mark.usefixtures("needs_all_range_attributes")
class TestLegacyKeyConditions:
    """`KeyConditions` on a multi-attribute index follow the same rules."""

    @staticmethod
    def _kc(**conds):
        return {
            attr: {"ComparisonOperator": op, "AttributeValueList": [{"S": v} for v in vals]}
            for attr, (op, *vals) in conds.items()
        }

    def test_all_equalities_are_served(self, dynamodb_client, multi_range_table):
        resp = dynamodb_client.query(
            TableName=multi_range_table,
            IndexName=INDEX,
            KeyConditions=self._kc(h=("EQ", "o1"), a=("EQ", "T"), b=("EQ", "live")),
        )
        assert [i["PK"]["S"] for i in resp["Items"]] == ["p3", "p2", "p1"]

    def test_trailing_operator_is_served(self, dynamodb_client, multi_range_table):
        resp = dynamodb_client.query(
            TableName=multi_range_table,
            IndexName=INDEX,
            KeyConditions=self._kc(h=("EQ", "o1"), a=("EQ", "T"), b=("GT", "draft")),
        )
        assert [i["PK"]["S"] for i in resp["Items"]] == ["p3", "p2", "p1"]

    @pytest.mark.parametrize(
        "conds,message",
        [
            (
                {"h": ("EQ", "o1"), "b": ("EQ", "live")},
                "RANGE key attributes a must have equality conditions specified in the query "
                "because a condition is present on key attribute b",
            ),
            (
                {"h": ("EQ", "o1"), "a": ("GT", "A"), "b": ("GT", "a")},
                "RANGE key attributes a must have equality conditions specified in the query "
                "because a condition is present on key attribute b",
            ),
            ({"h": ("EQ", "o1"), "z": ("EQ", "zz")}, "Query condition missed key schema element: a"),
            (
                {"a": ("EQ", "T")},
                "HASH key attributes h must have equality conditions specified in the query",
            ),
        ],
        ids=["gap-at-a", "two-operators", "nonkey", "no-hash"],
    )
    def test_refusals_match_the_expression_form(
        self, dynamodb_client, multi_range_table, conds, message
    ):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=multi_range_table, IndexName=INDEX, KeyConditions=self._kc(**conds)
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert err["Message"] == message


class TestFirstRangeOnlyBackends:
    """Backends that filter the first RANGE attribute only refuse the rest."""

    def test_a_condition_beyond_the_first_range_attribute_is_refused(
        self, dynamodb_client, multi_range_table, first_range_only
    ):
        if not first_range_only:
            pytest.skip("this target serves conditions on every RANGE attribute")
        for kce, values in [
            ("h = :h AND a = :a AND b = :b", {":h": "o1", ":a": "T", ":b": "live"}),
            ("h = :h AND a = :a AND b > :b", {":h": "o1", ":a": "T", ":b": "a"}),
        ]:
            err = _refused(dynamodb_client, multi_range_table, kce, values)
            assert err["Message"].startswith("Key condition on b: ")
            assert FIRST_RANGE_ONLY_MARK in err["Message"]
        # The first RANGE attribute alone is served.
        resp = _query(dynamodb_client, multi_range_table, "h = :h AND a = :a", {":h": "o1", ":a": "T"})
        assert resp["Count"] == 4
