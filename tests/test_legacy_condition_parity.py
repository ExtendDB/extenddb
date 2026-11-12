# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Legacy QueryFilter, ScanFilter and KeyConditions: the checks that need the
key schema, the number check on legacy values, and BatchGetItem capacity for
absent keys. Every expected response was measured against Amazon DynamoDB in
us-east-1 on 2026-10-24 (issue #477), and the cases marked 2026-10-31 and
2026-11-12 on those dates.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_gsi_items

INVALID_PARAMS = "One or more parameter values were invalid: "
SCHEMA_TYPE = INVALID_PARAMS + "Condition parameter type does not match schema type"
NOT_INDEXABLE = INVALID_PARAMS + "Attempted conditional constraint is not an indexable operation"
LEGACY_TYPE = INVALID_PARAMS + "ComparisonOperator {} is not valid for {} AttributeValue type"
LEGACY_COUNT = INVALID_PARAMS + "Invalid number of argument(s) for the {} ComparisonOperator"
LEGACY_RANGE = INVALID_PARAMS + "The BETWEEN condition was provided a range where the lower bound is greater than the upper bound"
NOT_SUPPORTED = "Query key condition not supported"
EMPTY_KEY = "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: {}"
FILTER_ON_KEY = "QueryFilter can only contain non-primary key attributes: Primary key attribute: {}"
NOT_A_NUMBER = "The parameter cannot be converted to a numeric value: {}"


def _cond(operator, values=None):
    cond = {"ComparisonOperator": operator}
    if values is not None:
        cond["AttributeValueList"] = values
    return cond


def _s(v):
    return {"S": v}


def _n(v):
    return {"N": v}


PK_X = {"pk": _cond("EQ", [_s("x")])}


def _validation_error(call):
    with pytest.raises(ClientError) as ei:
        call()
    err = ei.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


@pytest.fixture(scope="module")
def ss_table(dynamodb_client):
    """pk S, sk S, with GSIs on gpk S / gsk S (gsi1 ALL, kidx KEYS_ONLY) and two items under pk p1."""
    name = f"legacy_parity_{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
            {"AttributeName": "gsk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}, {"AttributeName": "gsk", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "kidx",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}, {"AttributeName": "gsk", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    dynamodb_client.put_item(TableName=name, Item={"pk": _s("p1"), "sk": _s("a"), "gpk": _s("g1"), "gsk": _s("a")})
    dynamodb_client.put_item(TableName=name, Item={"pk": _s("p1"), "sk": _s("b"), "gpk": _s("g1"), "gsk": _s("b")})
    wait_for_gsi_items(lambda: dynamodb_client.query(TableName=name, IndexName="gsi1", KeyConditions={"gpk": _cond("EQ", [_s("g1")])})["Items"], 2)
    yield name
    dynamodb_client.delete_table(TableName=name)


@pytest.fixture(scope="module")
def sn_table(dynamodb_client):
    """pk S, sk N, two items under pk p1."""
    name = f"legacy_parity_n_{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    dynamodb_client.put_item(TableName=name, Item={"pk": _s("p1"), "sk": _n("1")})
    dynamodb_client.put_item(TableName=name, Item={"pk": _s("p1"), "sk": _n("2")})
    yield name
    dynamodb_client.delete_table(TableName=name)


@pytest.fixture(scope="module")
def multi_table(dynamodb_client):
    """PK S, with a GSI midx on HASH h1, h2 and RANGE r1, r2 (all S) and one item."""
    name = f"legacy_parity_m_{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"} for a in ("PK", "h1", "h2", "r1", "r2")],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "midx",
                "KeySchema": [
                    {"AttributeName": "h1", "KeyType": "HASH"},
                    {"AttributeName": "h2", "KeyType": "HASH"},
                    {"AttributeName": "r1", "KeyType": "RANGE"},
                    {"AttributeName": "r2", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    dynamodb_client.put_item(TableName=name, Item={"PK": _s("i1"), "h1": _s("a"), "h2": _s("b"), "r1": _s("c"), "r2": _s("d")})
    wait_for_gsi_items(lambda: dynamodb_client.query(TableName=name, IndexName="midx", KeyConditions={"h1": _cond("EQ", [_s("a")]), "h2": _cond("EQ", [_s("b")])})["Items"], 1)
    yield name
    dynamodb_client.delete_table(TableName=name)


@pytest.fixture(scope="module")
def bb_table(dynamodb_client):
    """pk B, sk B, one item."""
    name = f"legacy_parity_b_{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "B"}, {"AttributeName": "sk", "AttributeType": "B"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    dynamodb_client.put_item(TableName=name, Item={"pk": {"B": b"\x01"}, "sk": {"B": b"\x02"}})
    yield name
    dynamodb_client.delete_table(TableName=name)


class TestLegacyNumberValues:
    """A legacy N value that is not a number is refused with the number
    message, ahead of the operator checks on the same entry and ahead of the
    table lookup; sets, lists and maps are searched too."""

    @pytest.mark.parametrize(
        ("operator", "values"),
        [
            ("EQ", [_n("abc")]),
            ("BEGINS_WITH", [_n("abc")]),
            ("FOO", [_n("abc")]),
            ("EQ", [_n("abc"), _s("x")]),
            ("EQ", [_s("x"), _n("abc")]),
            ("IN", [_s("x"), _n("abc")]),
            ("NULL", [_n("abc")]),
            ("BETWEEN", [_n("2"), _n("abc")]),
            ("BETWEEN", [_n("abc"), _n("2")]),
            ("EQ", [{"NS": ["1", "abc"]}]),
            ("EQ", [{"L": [_n("abc")]}]),
            ("EQ", [{"M": {"k": _n("abc")}}]),
            ("LT", [{"L": [_n("abc")]}]),
        ],
    )
    def test_scan_filter_number_is_checked_first(self, dynamodb_client_no_validation, ss_table, operator, values):
        msg = _validation_error(lambda: dynamodb_client_no_validation.scan(TableName=ss_table, ScanFilter={"a": _cond(operator, values)}))
        assert msg == NOT_A_NUMBER.format("abc")

    @pytest.mark.parametrize(("raw", "message"), [("", NOT_A_NUMBER.format("")), ("12e", NOT_A_NUMBER.format("12e")), (" 1", NOT_A_NUMBER.format(" 1")), ("1e400", "Number overflow. Attempting to store a number with magnitude larger than supported range"), ("1e-400", "Number underflow. Attempting to store a number with magnitude smaller than supported range"), ("1" * 39, "Attempting to store more than 38 significant digits in a Number")])
    def test_scan_filter_number_messages(self, dynamodb_client_no_validation, ss_table, raw, message):
        msg = _validation_error(lambda: dynamodb_client_no_validation.scan(TableName=ss_table, ScanFilter={"a": _cond("EQ", [_n(raw)])}))
        assert msg == message

    @pytest.mark.parametrize("value", [{"L": [_n("")]}, {"NS": [""]}, {"M": {"k": _n("")}}, {"NS": ["1", ""]}, {"L": [{"M": {"k": _n("")}}]}])
    def test_an_empty_nested_number_keeps_the_separator(self, dynamodb_client_no_validation, ss_table, value):
        # Measured 2026-11-12: the text of a top-level N "", in all three maps.
        client = dynamodb_client_no_validation
        cond = {"x": _cond("EQ", [value])}
        assert _validation_error(lambda: client.scan(TableName=ss_table, ScanFilter=cond)) == NOT_A_NUMBER.format("")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")])}, QueryFilter=cond)) == NOT_A_NUMBER.format("")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [value])})) == NOT_A_NUMBER.format("")

    def test_query_filter_and_key_conditions_numbers(self, dynamodb_client_no_validation, ss_table, sn_table):
        client = dynamodb_client_no_validation
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=PK_X, QueryFilter={"a": _cond("EQ", [_n("abc")])})) == NOT_A_NUMBER.format("abc")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_n("abc")])})) == NOT_A_NUMBER.format("abc")
        assert _validation_error(lambda: client.query(TableName=sn_table, KeyConditions={"pk": _cond("EQ", [_s("x")]), "sk": _cond("EQ", [_n("abc")])})) == NOT_A_NUMBER.format("abc")
        # A QueryFilter error is reported before a KeyConditions number.
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_n("abc")])}, QueryFilter={"a": _cond("BEGINS_WITH", [_n("1")])})) == LEGACY_TYPE.format("BEGINS_WITH", "N")

    def test_number_is_checked_before_the_table_lookup_and_select(self, dynamodb_client_no_validation):
        client = dynamodb_client_no_validation
        absent = f"absent_{uuid.uuid4().hex[:8]}"
        assert _validation_error(lambda: client.scan(TableName=absent, ScanFilter={"a": _cond("EQ", [_n("abc")])})) == NOT_A_NUMBER.format("abc")
        assert _validation_error(lambda: client.scan(TableName=absent, ScanFilter={"a": _cond("EQ", [_n("abc")])}, IndexName="nope")) == NOT_A_NUMBER.format("abc")

    @pytest.mark.parametrize(("segments", "absent_table"), [({"Segment": 0}, False), ({"Segment": 2, "TotalSegments": 2}, False), ({"Segment": 0}, True)])
    def test_number_is_checked_before_the_segment_checks(self, dynamodb_client_no_validation, ss_table, segments, absent_table):
        # Measured 2026-10-31: the number message ahead of the Segment and
        # TotalSegments pairing checks, on a table that does not exist too.
        table = f"absent_{uuid.uuid4().hex[:8]}" if absent_table else ss_table
        msg = _validation_error(lambda: dynamodb_client_no_validation.scan(TableName=table, ScanFilter={"x": _cond("EQ", [_n("abc")])}, **segments))
        assert msg == NOT_A_NUMBER.format("abc")

    def test_segment_check_answers_when_the_number_is_valid(self, dynamodb_client_no_validation, ss_table):
        # Measured 2026-10-31.
        msg = _validation_error(lambda: dynamodb_client_no_validation.scan(TableName=ss_table, ScanFilter={"x": _cond("EQ", [_n("5")])}, Segment=0))
        assert msg == "The TotalSegments parameter is required but was not present in the request when Segment parameter is present"

    def test_both_forms_conflict_is_reported_before_the_number(self, dynamodb_client_no_validation, ss_table):
        msg = _validation_error(lambda: dynamodb_client_no_validation.scan(TableName=ss_table, ScanFilter={"a": _cond("EQ", [_n("abc")])}, FilterExpression="b = :v", ExpressionAttributeValues={":v": _s("x")}))
        assert msg.startswith("Can not use both expression and non-expression parameters")

    def test_valid_numbers_pass(self, dynamodb_client, ss_table):
        assert dynamodb_client.scan(TableName=ss_table, ScanFilter={"a": _cond("EQ", [_n("-1.5e3")])})["Count"] == 0


class TestKeyConditionsSchemaChecks:
    """KeyConditions values are held to the key's AttributeDefinitions type,
    non-indexable operators are refused on either key at request time, empty
    key strings are refused after the schema checks, and the checks run in
    the measured order."""

    @pytest.mark.parametrize("value", [_n("1"), {"B": b"\x01"}, {"BOOL": True}, {"B": b""}])
    def test_hash_key_value_of_the_wrong_type(self, dynamodb_client_no_validation, ss_table, value):
        msg = _validation_error(lambda: dynamodb_client_no_validation.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [value])}))
        assert msg == SCHEMA_TYPE

    @pytest.mark.parametrize("cond", [_cond("EQ", [_n("1")]), _cond("LE", [{"B": b"\x01"}]), _cond("BETWEEN", [_n("1"), _n("2")]), _cond("BEGINS_WITH", [{"B": b"\x01"}]), _cond("BEGINS_WITH", [{"B": b""}])])
    def test_range_key_value_of_the_wrong_type_on_a_string_key(self, dynamodb_client_no_validation, ss_table, cond):
        msg = _validation_error(lambda: dynamodb_client_no_validation.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": cond}))
        assert msg == SCHEMA_TYPE

    @pytest.mark.parametrize("cond", [_cond("EQ", [_s("abc")]), _cond("BEGINS_WITH", [_s("1")]), _cond("BETWEEN", [_s("a"), _s("b")]), _cond("LT", [_s("5")])])
    def test_range_key_value_of_the_wrong_type_on_a_number_key(self, dynamodb_client, sn_table, cond):
        msg = _validation_error(lambda: dynamodb_client.query(TableName=sn_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": cond}))
        assert msg == SCHEMA_TYPE

    def test_well_typed_conditions_read_rows(self, dynamodb_client, sn_table, ss_table):
        assert dynamodb_client.query(TableName=sn_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": _cond("EQ", [_n("1")])})["Count"] == 1
        assert dynamodb_client.query(TableName=sn_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": _cond("BETWEEN", [_n("1"), _n("2")])})["Count"] == 2
        assert dynamodb_client.query(TableName=ss_table, IndexName="gsi1", KeyConditions={"gpk": _cond("EQ", [_s("g1")]), "gsk": _cond("EQ", [_s("a")])})["Count"] == 1

    def test_index_keys_are_held_to_the_index_schema(self, dynamodb_client, ss_table):
        assert _validation_error(lambda: dynamodb_client.query(TableName=ss_table, IndexName="gsi1", KeyConditions={"gpk": _cond("EQ", [_n("1")])})) == SCHEMA_TYPE
        assert _validation_error(lambda: dynamodb_client.query(TableName=ss_table, IndexName="gsi1", KeyConditions={"gpk": _cond("EQ", [_s("g1")]), "gsk": _cond("EQ", [_n("1")])})) == SCHEMA_TYPE
        assert _validation_error(lambda: dynamodb_client.query(TableName=ss_table, IndexName="gsi1", KeyConditions={"gpk": _cond("EQ", [_s("")])})) == EMPTY_KEY.format("gpk")
        assert _validation_error(lambda: dynamodb_client.query(TableName=ss_table, IndexName="gsi1", KeyConditions={"gpk": _cond("EQ", [_s("g1")]), "gsk": _cond("BEGINS_WITH", [_s("")])})) == EMPTY_KEY.format("gsk")

    @pytest.mark.parametrize(("operator", "values"), [("NE", [_s("x")]), ("IN", [_s("x")]), ("NULL", None), ("NOT_NULL", None), ("CONTAINS", [_s("x")])])
    def test_non_indexable_operator_on_the_hash_key(self, dynamodb_client_no_validation, ss_table, operator, values):
        msg = _validation_error(lambda: dynamodb_client_no_validation.query(TableName=ss_table, KeyConditions={"pk": _cond(operator, values)}))
        assert msg == NOT_INDEXABLE

    def test_non_indexable_is_reported_before_the_schema_checks(self, dynamodb_client, ss_table):
        client = dynamodb_client
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_n("1")]), "sk": _cond("NE", [_s("x")])})) == NOT_INDEXABLE
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("x")]), "sk": _cond("NE", [_n("1")])})) == NOT_INDEXABLE
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"sk": _cond("NE", [_s("x")])})) == NOT_INDEXABLE
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("")]), "sk": _cond("NE", [_s("x")])})) == NOT_INDEXABLE

    def test_hash_key_order_type_then_operator(self, dynamodb_client, ss_table):
        client = dynamodb_client
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("LT", [_n("1")])})) == SCHEMA_TYPE
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("LT", [_s("x")]), "sk": _cond("EQ", [_n("1")])})) == NOT_SUPPORTED
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("LT", [_s("")])})) == NOT_SUPPORTED
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("x"), _s("y")]), "sk": _cond("EQ", [_n("1")])})) == LEGACY_COUNT.format("EQ")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"sk": _cond("EQ", [_n("1")])})) == "Query condition missed key schema element: pk"

    def test_schema_type_is_reported_after_the_lookups_and_before_the_start_key(self, dynamodb_client, ss_table):
        client = dynamodb_client
        bad = {"pk": _cond("EQ", [_n("1")])}
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=bad, IndexName="nope")) == "The table does not have the specified index: nope"
        assert _validation_error(lambda: client.query(TableName=ss_table, IndexName="gsi1", ConsistentRead=True, KeyConditions={"gpk": _cond("EQ", [_n("1")])})) == "Consistent reads are not supported on global secondary indexes"
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=bad, ExclusiveStartKey={"zz": _s("x")})) == SCHEMA_TYPE
        with pytest.raises(ClientError) as ei:
            client.query(TableName=f"absent_{uuid.uuid4().hex[:8]}", KeyConditions=bad)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"

    @pytest.mark.parametrize("operator", ["EQ", "LE", "LT", "GE", "GT", "BEGINS_WITH"])
    def test_empty_string_on_the_range_key(self, dynamodb_client, ss_table, operator):
        msg = _validation_error(lambda: dynamodb_client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": _cond(operator, [_s("")])}))
        assert msg == EMPTY_KEY.format("sk")

    def test_empty_string_on_the_hash_key_and_between_bounds(self, dynamodb_client, ss_table):
        client = dynamodb_client
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("")])})) == EMPTY_KEY.format("pk")
        # Both keys empty: the service named pk in some sends of one request
        # and sk in others (measured 2026-11-12, in both map orders).
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("")]), "sk": _cond("EQ", [_s("")])})) in (EMPTY_KEY.format("pk"), EMPTY_KEY.format("sk"))
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": _cond("BETWEEN", [_s(""), _s("a")])})) == EMPTY_KEY.format("sk")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": _cond("BETWEEN", [_s(""), _s("")])})) == EMPTY_KEY.format("sk")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": _cond("BETWEEN", [_s("a"), _s("")])})) == LEGACY_RANGE

    def test_empty_string_is_reported_after_the_schema_checks(self, dynamodb_client, ss_table):
        client = dynamodb_client
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("")]), "sk": _cond("EQ", [_n("1")])})) == SCHEMA_TYPE
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_n("1")]), "sk": _cond("EQ", [_s("")])})) == SCHEMA_TYPE
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("")])}, QueryFilter={"a": _cond("BEGINS_WITH", [_n("1")])})) == LEGACY_TYPE.format("BEGINS_WITH", "N")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("")])}, ExclusiveStartKey={"zz": _s("x")})) == EMPTY_KEY.format("pk")
        # A QueryFilter on a key is the last check.
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("")])}, QueryFilter={"pk": _cond("EQ", [_s("x")])})) == EMPTY_KEY.format("pk")
        # An empty filter value on a non-key attribute passes.
        assert client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")])}, QueryFilter={"a": _cond("EQ", [_s("")])})["Count"] == 0


class TestQueryFilterOnKeyAttribute:
    """A legacy QueryFilter entry on a key of the queried table or index is
    refused, after every other Query error."""

    def test_table_keys_are_refused_hash_named_first(self, dynamodb_client, ss_table):
        client = dynamodb_client
        kc = {"pk": _cond("EQ", [_s("p1")])}
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter={"pk": _cond("EQ", [_s("p1")])})) == FILTER_ON_KEY.format("pk")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter={"sk": _cond("EQ", [_s("a")])})) == FILTER_ON_KEY.format("sk")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter={"sk": _cond("EQ", [_s("a")]), "pk": _cond("EQ", [_s("p1")])})) == FILTER_ON_KEY.format("pk")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter={"pk": _cond("NOT_NULL")})) == FILTER_ON_KEY.format("pk")
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter={"pk": _cond("EQ", [_s("p1")]), "zz": _cond("EQ", [_s("x")])}, ConditionalOperator="OR")) == FILTER_ON_KEY.format("pk")

    def test_index_query_refuses_index_keys_and_passes_table_keys(self, dynamodb_client, ss_table):
        client = dynamodb_client
        kc = {"gpk": _cond("EQ", [_s("g1")])}
        assert _validation_error(lambda: client.query(TableName=ss_table, IndexName="gsi1", KeyConditions=kc, QueryFilter={"gpk": _cond("EQ", [_s("g1")])})) == FILTER_ON_KEY.format("gpk")
        assert _validation_error(lambda: client.query(TableName=ss_table, IndexName="gsi1", KeyConditions=kc, QueryFilter={"gsk": _cond("EQ", [_s("a")])})) == FILTER_ON_KEY.format("gsk")
        assert client.query(TableName=ss_table, IndexName="gsi1", KeyConditions=kc, QueryFilter={"pk": _cond("EQ", [_s("x")])})["Count"] == 0
        assert client.query(TableName=ss_table, IndexName="gsi1", KeyConditions=kc, QueryFilter={"sk": _cond("EQ", [_s("a")])})["Count"] == 1
        assert client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")])}, QueryFilter={"gpk": _cond("EQ", [_s("g1")])})["Count"] == 2

    def test_reported_after_every_other_query_error(self, dynamodb_client, ss_table):
        client = dynamodb_client
        qf = {"pk": _cond("EQ", [_s("x")])}
        kc = {"pk": _cond("EQ", [_s("p1")])}
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter=qf, IndexName="nope")) == "The table does not have the specified index: nope"
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_n("1")])}, QueryFilter=qf)) == SCHEMA_TYPE
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"sk": _cond("EQ", [_s("a")])}, QueryFilter=qf)) == "Query condition missed key schema element: pk"
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": _cond("NE", [_s("a")])}, QueryFilter=qf)) == NOT_INDEXABLE
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("LT", [_s("p1")])}, QueryFilter=qf)) == NOT_SUPPORTED
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions={"pk": _cond("EQ", [_s("p1")]), "sk": _cond("EQ", [_s("")])}, QueryFilter=qf)) == EMPTY_KEY.format("sk")
        assert _validation_error(lambda: client.query(TableName=ss_table, QueryFilter=qf)) == "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request."
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter={"pk": _cond("EQ", [_s("x")]), "zz": _cond("BEGINS_WITH", [_n("1")])})) == LEGACY_TYPE.format("BEGINS_WITH", "N")
        assert _validation_error(lambda: client.query(TableName=ss_table, IndexName="gsi1", ConsistentRead=True, KeyConditions={"gpk": _cond("EQ", [_s("g1")])}, QueryFilter=qf)) == "Consistent reads are not supported on global secondary indexes"
        # The Select check comes first (measured 2026-10-31).
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter=qf, Select="SPECIFIC_ATTRIBUTES")) == "1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"
        assert _validation_error(lambda: client.query(TableName=ss_table, KeyConditions=kc, QueryFilter=qf, ExclusiveStartKey={"zz": _s("x")})) == FILTER_ON_KEY.format("pk")
        with pytest.raises(ClientError) as ei:
            client.query(TableName=f"absent_{uuid.uuid4().hex[:8]}", KeyConditions=kc, QueryFilter=qf)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_scan_filter_on_a_key_passes(self, dynamodb_client, ss_table):
        assert dynamodb_client.scan(TableName=ss_table, ScanFilter={"pk": _cond("EQ", [_s("p1")])})["Count"] == 2


def _eq(value):
    return _cond("EQ", [value])


SPECIFIC = "1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"
ALL_PROJECTED = "1 validation error detected: ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"
RANGE_R1_FOR_R2 = "RANGE key attributes r1 must have equality conditions specified in the query because a condition is present on key attribute r2"
MIDX = {"IndexName": "midx"}
KEYS_ONLY_ALL = {"IndexName": "kidx", "Select": "ALL_ATTRIBUTES"}
SK_NE = {"pk": _cond("EQ", [_s("p1")]), "sk": _cond("NE", [_s("a")])}
B_01 = {"B": b"\x01"}
EMPTY_B = "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: {}"

# Query requests measured on 2026-11-12 against tables of these fixtures'
# shapes, two sends each: (id, fixture, request, the answers the service
# gave). Two answers: the service named one key in some sends and the other
# key in the rest.
MEASURED_ORDER = [
    # A GSI with HASH h1, h2 and RANGE r1, r2: an empty key value is refused
    # after the key schema rules and the Select check, and before a
    # QueryFilter on a key.
    ("multi_empty_h1", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("")), "h2": _eq(_s("b"))}}, [EMPTY_KEY.format("h1")]),
    ("multi_empty_h2", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("a")), "h2": _eq(_s(""))}}, [EMPTY_KEY.format("h2")]),
    ("multi_empty_r1", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("a")), "h2": _eq(_s("b")), "r1": _eq(_s(""))}}, [EMPTY_KEY.format("r1")]),
    ("multi_empty_r1_begins_with", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("a")), "h2": _eq(_s("b")), "r1": _cond("BEGINS_WITH", [_s("")])}}, [EMPTY_KEY.format("r1")]),
    ("multi_empty_r1_between", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("a")), "h2": _eq(_s("b")), "r1": _cond("BETWEEN", [_s(""), _s("z")])}}, [EMPTY_KEY.format("r1")]),
    ("multi_empty_r2", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("a")), "h2": _eq(_s("b")), "r1": _eq(_s("c")), "r2": _eq(_s(""))}}, [EMPTY_KEY.format("r2")]),
    ("multi_empty_h1_and_h2", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("")), "h2": _eq(_s(""))}}, [EMPTY_KEY.format("h1"), EMPTY_KEY.format("h2")]),
    ("multi_empty_before_query_filter_on_key", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("")), "h2": _eq(_s("b"))}, "QueryFilter": {"h1": _eq(_s("x"))}}, [EMPTY_KEY.format("h1")]),
    ("multi_missing_hash_first", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s(""))}}, ["HASH key attributes h2 must have equality conditions specified in the query"]),
    ("multi_range_rule_first", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("a")), "h2": _eq(_s("b")), "r2": _eq(_s(""))}}, [RANGE_R1_FOR_R2]),
    ("multi_two_non_equalities_first", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("a")), "h2": _eq(_s("b")), "r1": _cond("GT", [_s("")]), "r2": _cond("GT", [_s("x")])}}, [RANGE_R1_FOR_R2]),
    ("multi_non_key_first", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("")), "h2": _eq(_s("b")), "zz": _eq(_s("x"))}}, ["Query condition missed key schema element: r1"]),
    ("multi_schema_type_first", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_n("1")), "h2": _eq(_s(""))}}, [SCHEMA_TYPE]),
    ("multi_select_first", "multi_table", {**MIDX, "KeyConditions": {"h1": _eq(_s("")), "h2": _eq(_s("b"))}, "Select": "SPECIFIC_ATTRIBUTES"}, [SPECIFIC]),
    # The Select checks are reported before an empty key value.
    ("select_before_empty_hash", "ss_table", {"KeyConditions": {"pk": _eq(_s(""))}, "Select": "SPECIFIC_ATTRIBUTES"}, [SPECIFIC]),
    ("select_before_empty_range", "ss_table", {"KeyConditions": {"pk": _eq(_s("p1")), "sk": _eq(_s(""))}, "Select": "SPECIFIC_ATTRIBUTES"}, [SPECIFIC]),
    ("all_projected_before_empty_hash", "ss_table", {"KeyConditions": {"pk": _eq(_s(""))}, "Select": "ALL_PROJECTED_ATTRIBUTES"}, [ALL_PROJECTED]),
    # Select ALL_ATTRIBUTES on a KEYS_ONLY GSI is reported after the key
    # condition, Select, empty key, QueryFilter key and start key checks.
    ("keys_only_query_filter_on_key_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditions": {"gpk": _eq(_s("g"))}, "QueryFilter": {"gpk": _eq(_s("g"))}}, [FILTER_ON_KEY.format("gpk")]),
    ("keys_only_query_filter_on_key_before_start_key", "ss_table", {**KEYS_ONLY_ALL, "KeyConditions": {"gpk": _eq(_s("g"))}, "QueryFilter": {"gpk": _eq(_s("g"))}, "ExclusiveStartKey": {"zz": _s("x")}}, [FILTER_ON_KEY.format("gpk")]),
    ("keys_only_empty_hash_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditions": {"gpk": _eq(_s(""))}}, [EMPTY_KEY.format("gpk")]),
    ("keys_only_empty_range_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditions": {"gpk": _eq(_s("g")), "gsk": _eq(_s(""))}}, [EMPTY_KEY.format("gsk")]),
    ("keys_only_schema_type_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditions": {"gpk": _eq(_n("1"))}}, [SCHEMA_TYPE]),
    ("keys_only_missing_hash_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditions": {"gsk": _eq(_s("a"))}}, ["Query condition missed key schema element: gpk"]),
    ("keys_only_hash_operator_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditions": {"gpk": _cond("LT", [_s("g")])}}, [NOT_SUPPORTED]),
    ("keys_only_expression_type_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditionExpression": "gpk = :v", "ExpressionAttributeValues": {":v": _n("1")}}, [SCHEMA_TYPE]),
    ("keys_only_expression_missing_hash_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditionExpression": "gsk = :v", "ExpressionAttributeValues": {":v": _s("a")}}, ["Query condition missed key schema element: gpk"]),
    ("keys_only_projection_first", "ss_table", {**KEYS_ONLY_ALL, "KeyConditionExpression": "gpk = :v", "ExpressionAttributeValues": {":v": _s("g")}, "ProjectionExpression": "x"}, ["1 validation error detected: Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES"]),
    ("keys_only_no_key_condition_first", "ss_table", {**KEYS_ONLY_ALL}, ["Either the KeyConditions or KeyConditionExpression parameter must be specified in the request."]),
    ("keys_only_select_all_attributes", "ss_table", {**KEYS_ONLY_ALL, "KeyConditions": {"gpk": _eq(_s("g"))}}, [INVALID_PARAMS + "Select type ALL_ATTRIBUTES is not supported for global secondary index kidx because its projection type is not ALL"]),
    # A non-indexable operator is refused before the table and index lookups,
    # the ConsistentRead check and the Select checks.
    ("non_indexable_on_a_missing_table", "missing_table", {"KeyConditions": SK_NE}, [NOT_INDEXABLE]),
    ("non_indexable_before_the_index_lookup", "ss_table", {"KeyConditions": SK_NE, "IndexName": "nope"}, [NOT_INDEXABLE]),
    ("non_indexable_before_consistent_read", "ss_table", {"KeyConditions": {"gpk": _eq(_s("g")), "gsk": _cond("NE", [_s("a")])}, "IndexName": "kidx", "ConsistentRead": True}, [NOT_INDEXABLE]),
    ("non_indexable_before_select", "ss_table", {"KeyConditions": SK_NE, "Select": "SPECIFIC_ATTRIBUTES"}, [NOT_INDEXABLE]),
    # B keys: the schema type check and the binary wording of the empty key
    # check.
    ("binary_hash_string_value", "bb_table", {"KeyConditions": {"pk": _eq(_s("x"))}}, [SCHEMA_TYPE]),
    ("binary_range_string_value", "bb_table", {"KeyConditions": {"pk": _eq(B_01), "sk": _eq(_s("x"))}}, [SCHEMA_TYPE]),
    ("binary_empty_hash", "bb_table", {"KeyConditions": {"pk": _eq({"B": b""})}}, [EMPTY_B.format("pk")]),
    ("binary_empty_range", "bb_table", {"KeyConditions": {"pk": _eq(B_01), "sk": _eq({"B": b""})}}, [EMPTY_B.format("sk")]),
    ("binary_empty_range_begins_with", "bb_table", {"KeyConditions": {"pk": _eq(B_01), "sk": _cond("BEGINS_WITH", [{"B": b""}])}}, [EMPTY_B.format("sk")]),
    ("binary_empty_between_bound", "bb_table", {"KeyConditions": {"pk": _eq(B_01), "sk": _cond("BETWEEN", [{"B": b""}, {"B": b"\x05"}])}}, [EMPTY_B.format("sk")]),
]


@pytest.fixture()
def missing_table():
    """The name of a table that does not exist."""
    return f"absent_{uuid.uuid4().hex[:8]}"


class TestMeasuredCheckOrder:
    """Which Query check the service reports when a request fails several."""

    @pytest.mark.parametrize(("fixture", "args", "answers"), [pytest.param(*row[1:], id=row[0]) for row in MEASURED_ORDER])
    def test_reported_check(self, request, dynamodb_client_no_validation, fixture, args, answers):
        table = request.getfixturevalue(fixture)
        assert _validation_error(lambda: dynamodb_client_no_validation.query(TableName=table, **args)) in answers

    def test_multi_attribute_index_reads_the_item(self, dynamodb_client, multi_table):
        # Measured 2026-11-12: Count 1.
        assert dynamodb_client.query(TableName=multi_table, **MIDX, KeyConditions={"h1": _eq(_s("a")), "h2": _eq(_s("b"))})["Count"] == 1

    def test_binary_key_reads_the_item(self, dynamodb_client, bb_table):
        # Measured 2026-11-12: Count 1.
        assert dynamodb_client.query(TableName=bb_table, KeyConditions={"pk": _eq(B_01)})["Count"] == 1


class TestBatchGetAbsentKeyCapacity:
    """BatchGetItem charges each absent key the minimum read and lists every
    requested table in ConsumedCapacity. The service lists the tables in no
    stable order, so the order is not asserted."""

    def test_absent_keys_are_charged(self, dynamodb_client, sn_table):
        client = dynamodb_client
        absent = {"pk": _s("nope"), "sk": _n("1")}
        absent2 = {"pk": _s("nope"), "sk": _n("2")}
        present = {"pk": _s("p1"), "sk": _n("1")}
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [absent]}}, ReturnConsumedCapacity="TOTAL")
        assert resp["Responses"] == {sn_table: []}
        assert resp["ConsumedCapacity"] == [{"TableName": sn_table, "CapacityUnits": 0.5}]
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [absent, absent2]}}, ReturnConsumedCapacity="TOTAL")
        assert resp["ConsumedCapacity"] == [{"TableName": sn_table, "CapacityUnits": 1.0}]
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [present, absent]}}, ReturnConsumedCapacity="TOTAL")
        assert len(resp["Responses"][sn_table]) == 1
        assert resp["ConsumedCapacity"] == [{"TableName": sn_table, "CapacityUnits": 1.0}]
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [absent], "ConsistentRead": True}}, ReturnConsumedCapacity="TOTAL")
        assert resp["ConsumedCapacity"] == [{"TableName": sn_table, "CapacityUnits": 1.0}]
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [absent, absent2], "ConsistentRead": True}}, ReturnConsumedCapacity="TOTAL")
        assert resp["ConsumedCapacity"] == [{"TableName": sn_table, "CapacityUnits": 2.0}]
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [absent], "ProjectionExpression": "pk"}}, ReturnConsumedCapacity="TOTAL")
        assert resp["ConsumedCapacity"] == [{"TableName": sn_table, "CapacityUnits": 0.5}]
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [{"pk": _s("nope"), "sk": _n(str(i))} for i in range(100)]}}, ReturnConsumedCapacity="TOTAL")
        assert resp["ConsumedCapacity"] == [{"TableName": sn_table, "CapacityUnits": 50.0}]

    def test_indexes_shape_and_none(self, dynamodb_client, sn_table):
        client = dynamodb_client
        absent = {"pk": _s("nope"), "sk": _n("1")}
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [absent]}}, ReturnConsumedCapacity="INDEXES")
        assert resp["ConsumedCapacity"] == [{"TableName": sn_table, "CapacityUnits": 0.5, "Table": {"CapacityUnits": 0.5}}]
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [absent]}}, ReturnConsumedCapacity="NONE")
        assert "ConsumedCapacity" not in resp
        resp = client.batch_get_item(RequestItems={sn_table: {"Keys": [absent]}})
        assert "ConsumedCapacity" not in resp

    def test_two_tables_all_absent_each_listed_once(self, dynamodb_client, ss_table, sn_table):
        client = dynamodb_client
        items = {sn_table: {"Keys": [{"pk": _s("nope"), "sk": _n("1")}]}, ss_table: {"Keys": [{"pk": _s("nope"), "sk": _s("x")}]}}
        for order in (list(items), list(reversed(list(items)))):
            resp = client.batch_get_item(RequestItems={t: items[t] for t in order}, ReturnConsumedCapacity="TOTAL")
            assert resp["Responses"] == {ss_table: [], sn_table: []}
            listed = sorted(resp["ConsumedCapacity"], key=lambda entry: entry["TableName"])
            assert listed == [{"TableName": t, "CapacityUnits": 0.5} for t in sorted(items)]
