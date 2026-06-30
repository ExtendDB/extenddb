# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Request-validation precedence vs table existence.

Amazon DynamoDB validates a documented subset of request parameters *before*
it checks whether the target table exists. For a malformed request against a
table that does not exist, those checks make DynamoDB return a
``ValidationException`` rather than a ``ResourceNotFoundException``.

The pre-existence subset (confirmed by capturing Amazon DynamoDB directly):

* expression validation that ExtendDB performs (syntax / reserved-word /
  undefined-name / overlapping-path) for ConditionExpression,
  ProjectionExpression, UpdateExpression and FilterExpression;
* legacy ``Expected`` structural validation (e.g. ComparisonOperator alone);
* BatchGetItem request-structure validation (duplicate keys).

Scope note: a few cases that the conformance suite groups with this ordering
bug are actually *separate, missing-validation* bugs, not ordering bugs, so
they are out of scope here and are tracked separately:

* empty ConditionExpression / empty FilterExpression: ExtendDB accepts an empty
  string as "no expression" even against an existing table (Amazon DynamoDB
  rejects it). Root cause is the shared optional-expression parser, not
  ordering.
* legacy ``Expected`` with ``Exists: true`` and no ``Value``: ExtendDB accepts
  it against an existing table (Amazon DynamoDB requires a Value).

Some other checks are intentionally *post-existence* on Amazon DynamoDB and
must keep returning ``ResourceNotFoundException`` for an absent table. The most
visible one is the item-size limit (PutItem / BatchWriteItem). The control
tests at the bottom pin that boundary so the fix does not over-correct.

All expected behaviour here was captured from Amazon DynamoDB via the AWS CLI
(profile ``asomasun-admin``, us-east-1).
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

# A table that is never created, so it does not exist on either target.
ABSENT_TABLE = f"eddb-absent-{uuid.uuid4().hex[:12]}"


def _error(excinfo) -> tuple[str, str]:
    err = excinfo.value.response["Error"]
    return err["Code"], err["Message"]


def _assert_validation(excinfo, msg_contains: str) -> None:
    code, message = _error(excinfo)
    assert code == "ValidationException", f"expected ValidationException, got {code}: {message}"
    assert msg_contains in message, f"{msg_contains!r} not in {message!r}"


# Use the no-validation client so botocore does not reject the malformed
# parameters client-side; we want the *service* to do the rejecting.
@pytest.fixture()
def client(dynamodb_client_no_validation):
    return dynamodb_client_no_validation


class TestValidationBeforeExistence:
    """Malformed request to an absent table -> ValidationException."""

    def test_put_item_expected_comparison_operator_alone(self, client):
        with pytest.raises(ClientError) as ei:
            client.put_item(
                TableName=ABSENT_TABLE,
                Item={"a": {"S": "k"}},
                Expected={"a": {"ComparisonOperator": "EQ"}},
            )
        _assert_validation(ei, "One or more parameter values were invalid")

    def test_get_item_empty_projection_expression(self, client):
        with pytest.raises(ClientError) as ei:
            client.get_item(
                TableName=ABSENT_TABLE,
                Key={"a": {"S": "k"}},
                ProjectionExpression="",
            )
        _assert_validation(ei, "Invalid ProjectionExpression:")

    def test_get_item_syntax_error_projection_expression(self, client):
        with pytest.raises(ClientError) as ei:
            client.get_item(
                TableName=ABSENT_TABLE,
                Key={"a": {"S": "k"}},
                ProjectionExpression="a..b",
            )
        _assert_validation(ei, "Invalid ProjectionExpression:")

    def test_get_item_overlapping_projection_paths(self, client):
        with pytest.raises(ClientError) as ei:
            client.get_item(
                TableName=ABSENT_TABLE,
                Key={"a": {"S": "k"}},
                ProjectionExpression="a, a.b",
            )
        _assert_validation(ei, "Invalid ProjectionExpression:")

    def test_delete_item_expected_comparison_operator_alone(self, client):
        with pytest.raises(ClientError) as ei:
            client.delete_item(
                TableName=ABSENT_TABLE,
                Key={"a": {"S": "k"}},
                Expected={"a": {"ComparisonOperator": "EQ"}},
            )
        _assert_validation(ei, "One or more parameter values were invalid")

    def test_update_item_empty_update_expression(self, client):
        with pytest.raises(ClientError) as ei:
            client.update_item(
                TableName=ABSENT_TABLE,
                Key={"a": {"S": "k"}},
                UpdateExpression="",
            )
        _assert_validation(ei, "Invalid UpdateExpression:")

    def test_update_item_syntax_error_update_expression(self, client):
        with pytest.raises(ClientError) as ei:
            client.update_item(
                TableName=ABSENT_TABLE,
                Key={"a": {"S": "k"}},
                UpdateExpression="SET",
            )
        _assert_validation(ei, "Invalid UpdateExpression:")

    def test_update_item_reserved_keyword(self, client):
        with pytest.raises(ClientError) as ei:
            client.update_item(
                TableName=ABSENT_TABLE,
                Key={"a": {"S": "k"}},
                UpdateExpression="SET status = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        _assert_validation(ei, "Invalid UpdateExpression:")

    def test_update_item_undefined_attribute_name(self, client):
        with pytest.raises(ClientError) as ei:
            client.update_item(
                TableName=ABSENT_TABLE,
                Key={"a": {"S": "k"}},
                UpdateExpression="SET #x = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        # ExtendDB omits the "Invalid UpdateExpression:" prefix that Amazon
        # DynamoDB adds here (a separate message-fidelity gap); assert on the
        # shared substring instead. The point of this case is the error class.
        _assert_validation(ei, "not defined")

    def test_scan_syntax_error_filter_expression(self, client):
        with pytest.raises(ClientError) as ei:
            client.scan(
                TableName=ABSENT_TABLE,
                FilterExpression="a +",
            )
        _assert_validation(ei, "Invalid FilterExpression:")

    def test_batch_get_item_duplicate_keys(self, client):
        with pytest.raises(ClientError) as ei:
            client.batch_get_item(
                RequestItems={
                    ABSENT_TABLE: {"Keys": [{"a": {"S": "k"}}, {"a": {"S": "k"}}]}
                },
            )
        _assert_validation(ei, "duplicates")


class TestValidationAfterExistence:
    """Controls: checks that stay post-existence on Amazon DynamoDB.

    These must keep returning ResourceNotFoundException for an absent table so
    the precedence fix does not move item-content validation ahead of the
    existence check.
    """

    def _big_item(self) -> dict:
        return {"a": {"S": "k"}, "b": {"S": "x" * 400001}}

    def test_put_item_too_big_is_resource_not_found(self, client):
        with pytest.raises(ClientError) as ei:
            client.put_item(TableName=ABSENT_TABLE, Item=self._big_item())
        code, _ = _error(ei)
        assert code == "ResourceNotFoundException", f"got {code}"

    def test_batch_write_item_too_big_is_resource_not_found(self, client):
        with pytest.raises(ClientError) as ei:
            client.batch_write_item(
                RequestItems={
                    ABSENT_TABLE: [{"PutRequest": {"Item": self._big_item()}}]
                },
            )
        code, _ = _error(ei)
        assert code == "ResourceNotFoundException", f"got {code}"
