# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Item size at the 409,600-byte limit: number byte costs, the UpdateItem rule,
the transacted Update rule, and the empty-binary index key refusal on UpdateItem.

Every figure here was measured against Amazon DynamoDB (us-east-1, 2026-10-19)
by an accept/refuse pair either side of the limit: an item of exactly N bytes is
accepted and one of N+1 is refused, so the pair measures a single attribute's
cost to the byte with two requests.

Numbers: ``1 + ceil(integer significant digits / 2) + ceil(fraction significant
digits / 2)``, plus one for a sign. The two halves round up separately, so
``1.5`` costs 3 where ``15`` costs 2 and ``3.14159`` costs 5 where ``123456``
costs 4.

UpdateItem is sized by the statement, not the finished item: a base of 3; 18
per SET or ADD clause and nothing per REMOVE or DELETE; a skeleton item holding
only the written paths (each map entry its name plus one, each map or list 3,
each list element one more, untouched siblings left out), where a SET or ADD
leaf costs the clause's value placeholders and a REMOVE or DELETE leaf costs
nothing; one more for a clause through a list index; and inside a SET operand
26 plus the name length per function call, 34 per "+", 37 per "-", and for a
document path operand 15 plus the name for its first element, 14 plus the name
for a later attribute, 15 for an index, with the attribute's stored value not
charged. The legacy AttributeUpdates form costs 3 plus the name and finished
value of each attribute it names that is present afterwards. The key and
untouched attributes are left out, so at a one-byte key the largest finished
item a plain SET can produce is 409,581 bytes, and a longer key or an
untouched attribute buys the headroom back, capped at 409,600, which the
finished item never exceeds. The service applies this rule in 31 of 33
commercial regions, us-east-1 among them. A transacted Update is sized on the
finished item alone: exactly 409,600 is stored, one byte over is a
cancellation. Both update paths word the refusal "Item size to update has
exceeded the maximum allowed size".

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

# The statement-sized UpdateItem rule and the empty-binary index-key refusal are
# enforced by the PostgreSQL and SQLite storage layers. The Cassandra layer
# sizes every update on the finished item against a fixed 409,600 bytes and the
# MongoDB layer applies no item-size limit on updates, both documented in
# docs/differences-from-dynamodb.md, so the update-specific classes below are
# skipped on those two backends. Number sizing lives in shared core and is
# asserted on every backend. When the backend is unset (a run against live
# DynamoDB through conf-run/live), every class runs.
on_default_backends_only = pytest.mark.skipif(
    os.environ.get("EXTENDDB_TEST_BACKEND", "") in {"mongodb", "cassandra"},
    reason="UpdateItem is sized on the finished item (Cassandra) or unbounded (MongoDB); "
    "see docs/differences-from-dynamodb.md",
)

MAX = 409_600
# A one-byte partition key "k": pk(2) + value(1) bytes the statement never writes.
KEY_K_BYTES = 3
PUT_WORDING = "Item size has exceeded the maximum allowed size"
UPDATE_WORDING = "Item size to update has exceeded the maximum allowed size"
UPDATE_BASE = 3
SET_COST = 19  # 18 for the clause plus the written attribute's map entry byte
REMOVE_COST = 2  # nothing for the clause plus the attribute's name and entry byte


@pytest.fixture(scope="class")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="class")
def gsi_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "bidx", "AttributeType": "B"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-bidx",
                "KeySchema": [{"AttributeName": "bidx", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
    ) as name:
        yield name


def _err(exc_info) -> dict:
    return exc_info.value.response["Error"]


class TestNumberByteCost:
    """A number's byte cost, measured by the accept/refuse pair at the limit."""

    # (literal, bytes) measured on the service.
    CASES = [
        ("0", 1), ("1", 2), ("15", 2), ("100", 2), ("1E125", 2), ("0.15", 2),
        ("1.5", 3), ("1.2", 3), ("1.200", 3), ("1010", 3), ("-42", 3),
        ("1.234", 4), ("100.5", 4), ("123456", 4), ("3.14159", 5),
    ]

    @pytest.mark.parametrize(("literal", "cost"), CASES)
    def test_number_costs_measured_bytes(self, dynamodb_client, table, literal, cost):
        pk = "n-" + literal.replace(".", "_").replace("-", "m")
        # pk(2) + pk value + n(1) + cost + p(1) + pad == MAX
        pad = MAX - (2 + len(pk) + 1 + cost + 1)
        dynamodb_client.put_item(
            TableName=table,
            Item={"pk": {"S": pk}, "n": {"N": literal}, "p": {"S": "x" * pad}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=table,
                Item={"pk": {"S": pk}, "n": {"N": literal}, "p": {"S": "x" * (pad + 1)}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == PUT_WORDING


@on_default_backends_only
class TestUpdateItemSizeRule:
    """UpdateItem is sized by the statement, not the finished item."""

    def _set(self, client, table, pk, finished_bytes, extra_expr="", extra_values=None):
        pad = finished_bytes - (2 + len(pk) + 1)  # pk name, pk value, "p" name
        values = {":v": {"S": "x" * pad}}
        values.update(extra_values or {})
        client.update_item(
            TableName=table,
            Key={"pk": {"S": pk}},
            UpdateExpression="SET p = :v" + extra_expr,
            ExpressionAttributeValues=values,
        )

    def test_one_set_at_a_one_byte_key_caps_below_the_limit(self, dynamodb_client, table):
        ceiling = MAX + KEY_K_BYTES - UPDATE_BASE - SET_COST  # 409,581
        self._set(dynamodb_client, table, "k", ceiling)
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as exc_info:
            self._set(dynamodb_client, table, "k", ceiling + 1)
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == UPDATE_WORDING

    def test_a_second_set_costs_nineteen_bytes(self, dynamodb_client, table):
        ceiling = MAX + KEY_K_BYTES - UPDATE_BASE - 2 * SET_COST  # 409,562
        extra = {":w": {"S": "y"}}
        # finished = pk(2)+1 + p(1)+pad + q(1)+1; the SET q costs 19, writes 2.
        pad = ceiling - (2 + 1 + 1 + 2)
        dynamodb_client.update_item(
            TableName=table, Key={"pk": {"S": "k"}},
            UpdateExpression="SET p = :v, q = :w",
            ExpressionAttributeValues={":v": {"S": "x" * pad}, **extra},
        )
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=table, Key={"pk": {"S": "k"}},
                UpdateExpression="SET p = :v, q = :w",
                ExpressionAttributeValues={":v": {"S": "x" * (pad + 1)}, **extra},
            )
        assert _err(exc_info)["Message"] == UPDATE_WORDING

    def test_a_remove_costs_two_bytes(self, dynamodb_client, table):
        ceiling = MAX + KEY_K_BYTES - UPDATE_BASE - SET_COST - REMOVE_COST  # 409,579
        pad = ceiling - (2 + 1 + 1)
        for size, ok in ((pad, True), (pad + 1, False)):
            dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, "r": {"S": "z"}})
            call = lambda: dynamodb_client.update_item(  # noqa: E731
                TableName=table, Key={"pk": {"S": "k"}},
                UpdateExpression="SET p = :v REMOVE r",
                ExpressionAttributeValues={":v": {"S": "x" * size}},
            )
            if ok:
                call()
            else:
                with pytest.raises(ClientError) as exc_info:
                    call()
                assert _err(exc_info)["Message"] == UPDATE_WORDING

    def test_an_untouched_attribute_buys_headroom_back_to_the_limit(self, dynamodb_client, table):
        # 100-byte attribute u is left alone; finished item may reach 409,600 exactly.
        for finished, ok in ((MAX, True), (MAX + 1, False)):
            dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, "u": {"S": "u" * 100}})
            pad = finished - (2 + 1 + 1 + 101)
            call = lambda: dynamodb_client.update_item(  # noqa: E731
                TableName=table, Key={"pk": {"S": "k"}},
                UpdateExpression="SET p = :v",
                ExpressionAttributeValues={":v": {"S": "x" * pad}},
            )
            if ok:
                call()
            else:
                with pytest.raises(ClientError) as exc_info:
                    call()
                assert _err(exc_info)["Message"] == UPDATE_WORDING

    def test_a_long_key_buys_headroom_back_to_the_limit(self, dynamodb_client, table):
        pk = "K" * 100
        self._set(dynamodb_client, table, pk, MAX)
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": pk}})
        with pytest.raises(ClientError) as exc_info:
            self._set(dynamodb_client, table, pk, MAX + 1)
        assert _err(exc_info)["Message"] == UPDATE_WORDING

    def test_an_alias_is_charged_as_the_attribute_it_names(self, dynamodb_client, table):
        # SET #twelvechars = :v writes attribute "p"; the alias costs nothing extra.
        ceiling = MAX + KEY_K_BYTES - UPDATE_BASE - SET_COST  # 409,581
        for finished, ok in ((ceiling, True), (ceiling + 1, False)):
            dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": "k"}})
            pad = finished - (2 + 1 + 1)
            call = lambda: dynamodb_client.update_item(  # noqa: E731
                TableName=table, Key={"pk": {"S": "k"}},
                UpdateExpression="SET #twelvechars = :v",
                ExpressionAttributeNames={"#twelvechars": "p"},
                ExpressionAttributeValues={":v": {"S": "x" * pad}},
            )
            if ok:
                call()
            else:
                with pytest.raises(ClientError) as exc_info:
                    call()
                assert _err(exc_info)["Message"] == UPDATE_WORDING

    def test_a_set_through_a_list_index_costs_one_byte_more(self, dynamodb_client, table):
        # Measured: SET d[0] = :v is charged 20 where SET d = :whole is charged 19.
        # finished = pk(2)+1 + d(1) + 3 + (pad + 1)
        ceiling = MAX + KEY_K_BYTES - UPDATE_BASE - SET_COST - 1  # 409,580
        for finished, ok in ((ceiling, True), (ceiling + 1, False)):
            dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, "d": {"L": [{"S": ""}]}})
            pad = finished - (2 + 1 + 1 + 3 + 1)
            call = lambda: dynamodb_client.update_item(  # noqa: E731
                TableName=table, Key={"pk": {"S": "k"}},
                UpdateExpression="SET d[0] = :v",
                ExpressionAttributeValues={":v": {"S": "x" * pad}},
            )
            if ok:
                call()
            else:
                with pytest.raises(ClientError) as exc_info:
                    call()
                assert _err(exc_info)["Message"] == UPDATE_WORDING

    def test_a_nested_path_set_is_charged_like_a_plain_set(self, dynamodb_client, table):
        ceiling = MAX + KEY_K_BYTES - UPDATE_BASE - SET_COST  # 409,581
        for finished, ok in ((ceiling, True), (ceiling + 1, False)):
            dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, "m": {"M": {"a": {"S": ""}}}})
            # finished = pk(2)+1 + m(1) + 3 + (a(1) + pad + 1)
            pad = finished - (2 + 1 + 1 + 3 + 1 + 1)
            call = lambda: dynamodb_client.update_item(  # noqa: E731
                TableName=table, Key={"pk": {"S": "k"}},
                UpdateExpression="SET m.a = :v",
                ExpressionAttributeValues={":v": {"S": "x" * pad}},
            )
            if ok:
                call()
            else:
                with pytest.raises(ClientError) as exc_info:
                    call()
                assert _err(exc_info)["Message"] == UPDATE_WORDING


def _s(n: int) -> dict:
    return {"S": "x" * n}


@on_default_backends_only
class TestUpdateStatementShapes:
    """The largest accepted padding at a one-byte key, per statement shape.

    Each row is (name, seed item or None, builder) where the builder returns the
    UpdateItem arguments for a given padding, and the figure is the largest
    padding the service accepted on 2026-10-19; the padding one larger is refused
    with the update wording. The seed is written with PutItem before each
    attempt. Shapes whose figure is the finished-item cap rather than the
    statement figure are marked so in the name.
    """

    SHAPES = [
        ("plain_set", None, 409_577,
         lambda n: {"UpdateExpression": "SET p = :v", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("two_sets_different_attributes", None, 409_556,
         lambda n: {"UpdateExpression": "SET p = :v, q = :w", "ExpressionAttributeValues": {":v": _s(n), ":w": {"S": "y"}}}),
        ("two_clauses_same_map_share_the_container", {"m": {"M": {}}}, 409_551,
         lambda n: {"UpdateExpression": "SET m.a = :v, m.b = :w", "ExpressionAttributeValues": {":v": _s(n), ":w": {"S": "y"}}}),
        ("three_clauses_same_map", {"m": {"M": {}}}, 409_530,
         lambda n: {"UpdateExpression": "SET m.a = :v, m.b = :w, m.c = :x", "ExpressionAttributeValues": {":v": _s(n), ":w": {"S": "y"}, ":x": {"S": "y"}}}),
        ("nested_two_deep", {"d": {"M": {"a": {"M": {}}}}}, 409_567,
         lambda n: {"UpdateExpression": "SET d.a.b = :v", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("map_operand", None, 409_572,
         lambda n: {"UpdateExpression": "SET p = :v", "ExpressionAttributeValues": {":v": {"M": {"a": _s(n)}}}}),
        ("list_index_siblings_not_charged_cap_binds", {"p": {"L": [{"S": "s" * 50}, {"S": "seed"}]}}, 409_541,
         lambda n: {"UpdateExpression": "SET p[1] = :v", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("if_not_exists_charges_the_operand", None, 409_522,
         lambda n: {"UpdateExpression": "SET p = if_not_exists(p, :v)", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("if_not_exists_charges_the_operand_when_present", {"p": {"S": "q"}}, 409_522,
         lambda n: {"UpdateExpression": "SET p = if_not_exists(p, :v)", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("if_not_exists_ten_byte_names", None, 409_504,
         lambda n: {"UpdateExpression": "SET pppppppppp = if_not_exists(pppppppppp, :v)", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("if_not_exists_other_inner_path", None, 409_513,
         lambda n: {"UpdateExpression": "SET p = if_not_exists(qqqqqqqqqq, :v)", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("if_not_exists_nested_inner_path", {"d": {"M": {}}}, 409_504,
         lambda n: {"UpdateExpression": "SET p = if_not_exists(d.leaf, :v)", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("if_not_exists_nested_target", {"d": {"M": {}}}, 409_496,
         lambda n: {"UpdateExpression": "SET d.leaf = if_not_exists(d.leaf, :v)", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("if_not_exists_indexed_inner_path", {"d": {"L": [{"S": "q"}]}}, 409_507,
         lambda n: {"UpdateExpression": "SET p = if_not_exists(d[0], :v)", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("list_append_two_values", None, 409_531,
         lambda n: {"UpdateExpression": "SET p = list_append(:v, :w)", "ExpressionAttributeValues": {":v": {"L": [_s(n)]}, ":w": {"L": [{"S": "y"}]}}}),
        ("list_append_path_value_not_charged", {"p": {"L": [{"S": "q"}]}}, 409_520,
         lambda n: {"UpdateExpression": "SET p = list_append(p, :v)", "ExpressionAttributeValues": {":v": {"L": [_s(n)]}}}),
        ("list_append_to_big_list_cap_binds", "big_list", 409_590,
         lambda n: {"UpdateExpression": "SET p = list_append(p, :v)", "ExpressionAttributeValues": {":v": {"L": [{"S": "y"}]}}}),
        ("nested_functions", None, 409_478,
         lambda n: {"UpdateExpression": "SET p = list_append(if_not_exists(p, :e), :v)", "ExpressionAttributeValues": {":e": {"L": []}, ":v": {"L": [_s(n)]}}}),
        ("addition_with_path_operand", {"n": {"N": "1"}}, 409_505,
         lambda n: {"UpdateExpression": "SET p = :v, n = n + :w", "ExpressionAttributeValues": {":v": _s(n), ":w": {"N": "1"}}}),
        ("addition_with_two_values", None, 409_519,
         lambda n: {"UpdateExpression": "SET p = :v, n = :w + :x", "ExpressionAttributeValues": {":v": _s(n), ":w": {"N": "1"}, ":x": {"N": "1"}}}),
        ("subtraction_with_two_values", None, 409_516,
         lambda n: {"UpdateExpression": "SET p = :v, n = :w - :x", "ExpressionAttributeValues": {":v": _s(n), ":w": {"N": "1"}, ":x": {"N": "1"}}}),
        ("add_number", None, 409_555,
         lambda n: {"UpdateExpression": "SET p = :v ADD n :w", "ExpressionAttributeValues": {":v": _s(n), ":w": {"N": "1"}}}),
        ("add_to_big_set_cap_binds", "big_set", 409_595,
         lambda n: {"UpdateExpression": "ADD s :v", "ExpressionAttributeValues": {":v": {"SS": ["y"]}}}),
        ("remove_nested_path", {"d": {"M": {"leaf": {"S": "y"}}}}, 409_567,
         lambda n: {"UpdateExpression": "SET p = :v REMOVE d.leaf", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("remove_through_list_index", {"d": {"L": [{"S": "y"}]}}, 409_570,
         lambda n: {"UpdateExpression": "SET p = :v REMOVE d[0]", "ExpressionAttributeValues": {":v": _s(n)}}),
        ("delete_operand_not_charged", {"s": {"SS": ["a", "b"]}}, 409_575,
         lambda n: {"UpdateExpression": "SET p = :v DELETE s :w", "ExpressionAttributeValues": {":v": _s(n), ":w": {"SS": ["a"]}}}),
        ("legacy_put", None, 409_596,
         lambda n: {"AttributeUpdates": {"p": {"Value": _s(n), "Action": "PUT"}}}),
        ("legacy_two_puts", None, 409_594,
         lambda n: {"AttributeUpdates": {"p": {"Value": _s(n), "Action": "PUT"}, "q": {"Value": {"S": "y"}, "Action": "PUT"}}}),
        ("legacy_put_ten_byte_name", None, 409_587,
         lambda n: {"AttributeUpdates": {"pppppppppp": {"Value": _s(n), "Action": "PUT"}}}),
        ("legacy_put_map", None, 409_591,
         lambda n: {"AttributeUpdates": {"p": {"Value": {"M": {"a": _s(n)}}, "Action": "PUT"}}}),
        ("legacy_put_and_delete_charges_the_finished_set", {"s": {"SS": ["a", "b"]}}, 409_594,
         lambda n: {"AttributeUpdates": {"p": {"Value": _s(n), "Action": "PUT"}, "s": {"Value": {"SS": ["a"]}, "Action": "DELETE"}}}),
        ("legacy_put_and_remove_charges_nothing_for_the_removed", {"s": {"SS": ["a", "b"]}}, 409_596,
         lambda n: {"AttributeUpdates": {"p": {"Value": _s(n), "Action": "PUT"}, "s": {"Action": "DELETE"}}}),
    ]

    @staticmethod
    def _seed(seed, n):
        if seed == "big_list":
            return {"p": {"L": [_s(n)]}}
        if seed == "big_set":
            return {"s": {"SS": ["x" * n]}}
        return seed

    @pytest.mark.parametrize("name,seed,ceiling,build", SHAPES, ids=[r[0] for r in SHAPES])
    def test_largest_accepted_padding(self, dynamodb_client, table, name, seed, ceiling, build):
        key = {"pk": {"S": "k"}}
        for n, ok in ((ceiling, True), (ceiling + 1, False)):
            dynamodb_client.delete_item(TableName=table, Key=key)
            seeded = self._seed(seed, n)
            try:
                if seeded:
                    dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k"}, **seeded})
                dynamodb_client.update_item(TableName=table, Key=key, **build(n))
                accepted = True
            except ClientError as exc:
                assert exc.response["Error"]["Code"] == "ValidationException"
                assert "size" in exc.response["Error"]["Message"], exc.response["Error"]["Message"]
                accepted = False
            assert accepted is ok, f"{name}: padding {n} {'accepted' if accepted else 'refused'}"


@on_default_backends_only
class TestTransactedUpdateSizeRule:
    """A transacted Update is sized on the finished item and cancels with the update wording."""

    def _txn(self, client, table, finished):
        pad = finished - (2 + 1 + 1)
        client.transact_write_items(TransactItems=[{
            "Update": {
                "TableName": table, "Key": {"pk": {"S": "t"}},
                "UpdateExpression": "SET p = :v",
                "ExpressionAttributeValues": {":v": {"S": "x" * pad}},
            }
        }])

    def test_exactly_the_limit_is_stored(self, dynamodb_client, table):
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": "t"}})
        self._txn(dynamodb_client, table, MAX)
        got = dynamodb_client.get_item(TableName=table, Key={"pk": {"S": "t"}}, ConsistentRead=True)
        assert len(got["Item"]["p"]["S"]) == MAX - 4

    def test_one_byte_over_cancels_with_the_update_wording(self, dynamodb_client, table):
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": "t"}})
        with pytest.raises(ClientError) as exc_info:
            self._txn(dynamodb_client, table, MAX + 1)
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        reasons = resp["CancellationReasons"]
        assert reasons[0]["Code"] == "ValidationError"
        assert reasons[0]["Message"] == UPDATE_WORDING


@on_default_backends_only
class TestEmptyBinaryIndexKeyOnUpdate:
    """UpdateItem setting a GSI key to an empty binary is refused with the update-form message."""

    def test_update_refuses_empty_binary_index_key(self, dynamodb_client, gsi_table):
        dynamodb_client.put_item(TableName=gsi_table, Item={"pk": {"S": "eb"}, "v": {"S": "1"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=gsi_table, Key={"pk": {"S": "eb"}},
                UpdateExpression="SET bidx = :b",
                ExpressionAttributeValues={":b": {"B": b""}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == (
            "One or more parameter values are not valid. The update expression attempted to "
            "update a secondary index key to a value that is not supported. The AttributeValue "
            "for a key attribute cannot contain an empty binary value."
        )
        got = dynamodb_client.get_item(TableName=gsi_table, Key={"pk": {"S": "eb"}}, ConsistentRead=True)
        assert "bidx" not in got["Item"]
