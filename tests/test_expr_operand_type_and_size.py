# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Operand-type validation for ordering comparisons, and size() on unsupported types.

Two related expression-evaluator behaviours, verified against Amazon DynamoDB:

Case A (missing validation): an ordering comparison (<, <=, >, >=) or BETWEEN
whose *literal* operand (an ExpressionAttributeValue) is a non-orderable type
(BOOL, NULL, or any set/list/map) is rejected up front with a
ValidationException, regardless of whether the branch is evaluated. A document
*path* that resolves to a non-orderable type is not validated: the comparison
just evaluates to false.

Case B (over-validation): size() applied to an attribute that resolves to a
type size() does not support (Number, Bool, Null) yields no value, so the
enclosing comparison evaluates to false. It must not raise.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

BASE_ITEM = {
    "pk": {"S": "p1"},
    "n": {"N": "5"},
    "num": {"N": "9"},
    "flag": {"BOOL": True},
    "s": {"S": "x"},
    "nu": {"NULL": True},
    "sset": {"SS": ["a", "b"]},
    "lst": {"L": [{"S": "a"}]},
}

# Non-orderable literal operands keyed by the DynamoDB type code DynamoDB reports.
NON_ORDERABLE = {
    "BOOL": {"BOOL": True},
    "NULL": {"NULL": True},
    "SS": {"SS": ["a", "b"]},
    "NS": {"NS": ["1", "2"]},
    "L": {"L": [{"S": "a"}]},
    "M": {"M": {"k": {"S": "a"}}},
}

ORDERING_OPS = ["<", "<=", ">", ">="]


@pytest.fixture(scope="class")
def probe_table(dynamodb_client):
    name = f"expr_operand_{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    from conftest import wait_for_active

    wait_for_active(dynamodb_client, name)
    dynamodb_client.put_item(TableName=name, Item=BASE_ITEM)
    yield name
    dynamodb_client.delete_table(TableName=name)


def _put_cond(client, table, condition, eav):
    """PutItem the base item under a condition. Rewrites identical data on success."""
    client.put_item(
        TableName=table,
        Item=BASE_ITEM,
        ConditionExpression=condition,
        ExpressionAttributeValues=eav,
    )


def _err(exc_info):
    return exc_info.value.response["Error"]


class TestOrderingOperandType:
    """Case A: ordering comparison / BETWEEN with a non-orderable literal -> ValidationException."""

    @pytest.mark.parametrize("op", ORDERING_OPS)
    @pytest.mark.parametrize("type_code", list(NON_ORDERABLE))
    def test_ordering_op_rejects_non_orderable_literal(
        self, dynamodb_client, probe_table, op, type_code
    ):
        with pytest.raises(ClientError) as ei:
            _put_cond(
                dynamodb_client,
                probe_table,
                f"n {op} :v",
                {":v": NON_ORDERABLE[type_code]},
            )
        err = _err(ei)
        assert err["Code"] == "ValidationException"
        assert "Incorrect operand type for operator or function" in err["Message"]
        assert f"operator or function: {op}" in err["Message"]
        assert f"operand type: {type_code}" in err["Message"]

    def test_between_rejects_non_orderable_literal_bounds(
        self, dynamodb_client, probe_table
    ):
        with pytest.raises(ClientError) as ei:
            _put_cond(
                dynamodb_client,
                probe_table,
                "n BETWEEN :a AND :b",
                {":a": {"BOOL": True}, ":b": {"BOOL": False}},
            )
        err = _err(ei)
        assert err["Code"] == "ValidationException"
        assert "operator or function: BETWEEN" in err["Message"]
        assert "operand type: BOOL" in err["Message"]

    def test_reports_offending_operand_regardless_of_position(
        self, dynamodb_client, probe_table
    ):
        # Right operand non-orderable.
        with pytest.raises(ClientError) as ei:
            _put_cond(
                dynamodb_client,
                probe_table,
                ":x > :v",
                {":x": {"N": "1"}, ":v": {"BOOL": True}},
            )
        assert "operand type: BOOL" in _err(ei)["Message"]
        # Left operand non-orderable.
        with pytest.raises(ClientError) as ei:
            _put_cond(
                dynamodb_client,
                probe_table,
                ":v > :x",
                {":v": {"BOOL": True}, ":x": {"N": "1"}},
            )
        assert "operand type: BOOL" in _err(ei)["Message"]

    def test_validated_upfront_across_short_circuit_and_not(
        self, dynamodb_client, probe_table
    ):
        # Whole-expression validation: the bad compare is rejected even when the
        # boolean structure would short-circuit past it, or NOT-wraps it.
        for cond in [
            "attribute_not_exists(zzz) OR n > :bool",
            "attribute_exists(zzz) AND n > :bool",
            "NOT (n > :bool)",
        ]:
            with pytest.raises(ClientError) as ei:
                _put_cond(dynamodb_client, probe_table, cond, {":bool": {"BOOL": True}})
            assert _err(ei)["Code"] == "ValidationException"
            assert "Incorrect operand type" in _err(ei)["Message"]

    def test_absent_path_with_non_orderable_literal_still_rejects(
        self, dynamodb_client, probe_table
    ):
        with pytest.raises(ClientError) as ei:
            _put_cond(
                dynamodb_client, probe_table, "nope > :bool", {":bool": {"BOOL": True}}
            )
        assert _err(ei)["Code"] == "ValidationException"

    def test_size_result_compared_to_non_orderable_literal_rejects(
        self, dynamodb_client, probe_table
    ):
        with pytest.raises(ClientError) as ei:
            _put_cond(
                dynamodb_client, probe_table, "size(s) > :bool", {":bool": {"BOOL": True}}
            )
        assert "operand type: BOOL" in _err(ei)["Message"]

    def test_filter_expression_carries_filter_prefix(self, dynamodb_client, probe_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.scan(
                TableName=probe_table,
                FilterExpression="n > :v",
                ExpressionAttributeValues={":v": {"BOOL": True}},
            )
        err = _err(ei)
        assert err["Code"] == "ValidationException"
        assert "Invalid FilterExpression: Incorrect operand type" in err["Message"]


class TestOrderingOperandControls:
    """Case A controls: must NOT raise ValidationException."""

    @pytest.mark.parametrize("op", ["=", "<>"])
    def test_equality_ops_accept_non_orderable_literal(
        self, dynamodb_client, probe_table, op
    ):
        # Equality/inequality are defined for all types: no ValidationException.
        try:
            _put_cond(dynamodb_client, probe_table, f"n {op} :v", {":v": {"BOOL": True}})
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ConditionalCheckFailedException"

    @pytest.mark.parametrize(
        "cond,eav",
        [
            ("flag > :zero", {":zero": {"N": "0"}}),  # stored BOOL path
            ("nu > :zero", {":zero": {"N": "0"}}),  # stored NULL path
            ("nope > :zero", {":zero": {"N": "0"}}),  # absent path
            ("flag > n", None),  # both document paths
            ("sset > n", None),  # stored SS path
        ],
    )
    def test_non_orderable_document_path_does_not_reject(
        self, dynamodb_client, probe_table, cond, eav
    ):
        # A path that resolves to a non-orderable type evaluates false (CCFE),
        # never a ValidationException.
        kwargs = {"ConditionExpression": cond}
        if eav is not None:
            kwargs["ExpressionAttributeValues"] = eav
        with pytest.raises(ClientError) as ei:
            dynamodb_client.put_item(TableName=probe_table, Item=BASE_ITEM, **kwargs)
        assert _err(ei)["Code"] == "ConditionalCheckFailedException"

    def test_orderable_literal_accepted(self, dynamodb_client, probe_table):
        # 5 > 0 is true: the conditional put succeeds, no exception.
        _put_cond(dynamodb_client, probe_table, "n > :zero", {":zero": {"N": "0"}})


class TestSizeUnsupportedTypes:
    """Case B: size() on Number/Bool/Null yields no value -> comparison false (CCFE), never a ValidationException."""

    @pytest.mark.parametrize("attr", ["flag", "nu", "num"])
    def test_size_on_unsupported_type_evaluates_false(
        self, dynamodb_client, probe_table, attr
    ):
        with pytest.raises(ClientError) as ei:
            _put_cond(
                dynamodb_client, probe_table, f"size({attr}) > :z", {":z": {"N": "0"}}
            )
        assert _err(ei)["Code"] == "ConditionalCheckFailedException"

    def test_size_on_supported_type_still_works(self, dynamodb_client, probe_table):
        # size(s) == 1 > 0 is true: the conditional put succeeds.
        _put_cond(dynamodb_client, probe_table, "size(s) > :z", {":z": {"N": "0"}})

    def test_size_on_set_still_works(self, dynamodb_client, probe_table):
        # size(sset) == 2 > 0 is true.
        _put_cond(dynamodb_client, probe_table, "size(sset) > :z", {":z": {"N": "0"}})

    def test_size_on_number_inequality_evaluates_true(self, dynamodb_client, probe_table):
        # size(num) yields no value; `<>` against a value is TRUE, so the
        # conditional put succeeds (locks in the None/Ne semantics the fix relies on).
        _put_cond(dynamodb_client, probe_table, "size(num) <> :z", {":z": {"N": "0"}})


class TestTransactionConditions:
    """Case A applies to TransactWriteItems condition expressions: a malformed
    operand type is a top-level ValidationException, not a cancellation reason."""

    BAD = {":v": {"BOOL": True}}

    def test_twi_put_rejects_non_orderable_literal(self, dynamodb_client, probe_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.transact_write_items(TransactItems=[{"Put": {
                "TableName": probe_table,
                "Item": {"pk": {"S": "p1"}, "n": {"N": "5"}},
                "ConditionExpression": "n > :v",
                "ExpressionAttributeValues": self.BAD}}])
        err = _err(ei)
        assert err["Code"] == "ValidationException"
        assert "Incorrect operand type" in err["Message"]
        assert "operand type: BOOL" in err["Message"]

    def test_twi_delete_rejects_non_orderable_literal(self, dynamodb_client, probe_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.transact_write_items(TransactItems=[{"Delete": {
                "TableName": probe_table,
                "Key": {"pk": {"S": "p1"}},
                "ConditionExpression": "n > :v",
                "ExpressionAttributeValues": self.BAD}}])
        assert _err(ei)["Code"] == "ValidationException"
        assert "operand type: BOOL" in _err(ei)["Message"]

    def test_twi_update_rejects_non_orderable_literal(self, dynamodb_client, probe_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.transact_write_items(TransactItems=[{"Update": {
                "TableName": probe_table,
                "Key": {"pk": {"S": "p1"}},
                "UpdateExpression": "SET z = :one",
                "ConditionExpression": "n > :v",
                "ExpressionAttributeValues": {":v": {"BOOL": True}, ":one": {"N": "1"}}}}])
        assert _err(ei)["Code"] == "ValidationException"
        assert "operand type: BOOL" in _err(ei)["Message"]

    def test_twi_condition_check_rejects_non_orderable_literal(self, dynamodb_client, probe_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.transact_write_items(TransactItems=[{"ConditionCheck": {
                "TableName": probe_table,
                "Key": {"pk": {"S": "p1"}},
                "ConditionExpression": "n > :v",
                "ExpressionAttributeValues": self.BAD}}])
        assert _err(ei)["Code"] == "ValidationException"
        assert "operand type: BOOL" in _err(ei)["Message"]

    def test_twi_genuine_condition_failure_is_transaction_cancelled(
        self, dynamodb_client, probe_table
    ):
        # A well-formed but failing condition is a cancellation, not a
        # ValidationException: the two must not be conflated.
        with pytest.raises(ClientError) as ei:
            dynamodb_client.transact_write_items(TransactItems=[{"Put": {
                "TableName": probe_table,
                "Item": {"pk": {"S": "p1"}, "n": {"N": "5"}},
                "ConditionExpression": "attribute_exists(zzz)"}}])
        assert _err(ei)["Code"] == "TransactionCanceledException"


class TestConditionApiSurface:
    """Operand-type validation reaches every condition/filter-bearing API, not just PutItem/Scan."""

    def test_update_item_condition_rejects_non_orderable_literal(
        self, dynamodb_client, probe_table
    ):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.update_item(
                TableName=probe_table,
                Key={"pk": {"S": "p1"}},
                UpdateExpression="SET s = :one",
                ConditionExpression="n > :v",
                ExpressionAttributeValues={":one": {"S": "y"}, ":v": {"BOOL": True}},
            )
        assert "operand type: BOOL" in _err(ei)["Message"]

    def test_delete_item_condition_rejects_non_orderable_literal(
        self, dynamodb_client, probe_table
    ):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.delete_item(
                TableName=probe_table,
                Key={"pk": {"S": "p1"}},
                ConditionExpression="n > :v",
                ExpressionAttributeValues={":v": {"BOOL": True}},
            )
        assert "operand type: BOOL" in _err(ei)["Message"]

    def test_query_filter_carries_filter_prefix(self, dynamodb_client, probe_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.query(
                TableName=probe_table,
                KeyConditionExpression="pk = :p",
                FilterExpression="n > :v",
                ExpressionAttributeValues={":p": {"S": "p1"}, ":v": {"BOOL": True}},
            )
        err = _err(ei)
        assert err["Code"] == "ValidationException"
        assert "Invalid FilterExpression: Incorrect operand type" in err["Message"]
