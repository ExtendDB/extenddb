# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""AttributeValue wire-format error class and message — all data-plane APIs.

The AWS Java SDK omits explicitly-null fields from the JSON it puts on the
wire, so call patterns like ``attrVal.setBOOL(null)`` and ``attrVal.setM(null)``
emit a literal ``{}`` AttributeValue. Real DynamoDB rejects these with a
``ValidationException`` whose message starts with
``"Supplied AttributeValue is empty"`` (error code ``EMPTY_ATTRIBUTE_VALUE`` in
the BigBird parity tests). The same shape is expected for AttributeValues
that contain more than one type-key (``MULTI_ATTRIBUTE_VALUE``) and for
``SS``/``NS``/``BS`` arrays whose elements include a JSON null.

Because every data-plane API ultimately runs the same ``AttributeValue``
deserializer over its inputs (Items, Keys, ExpressionAttributeValues,
ExclusiveStartKey, etc.), the same defect would otherwise surface across
``PutItem``, ``UpdateItem``, ``DeleteItem``, ``GetItem``, ``BatchGetItem``,
``BatchWriteItem``, ``TransactGetItems``, ``TransactWriteItems``, ``Query``,
and ``Scan``. This file exercises each API at the natural wire-format
injection point.

Tests post raw SigV4-signed JSON bodies to bypass boto3's client-side
parameter validation, since boto3 will refuse to serialize the malformed
payloads we need on the wire.

REQ-TEST-001, REQ-TEST-002, REQ-TEST-003
"""

from __future__ import annotations

import json
import os
import uuid

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from conftest import scoped_table


# Tests use the EXTENDDB_TEST_ENDPOINT used by the rest of the suite. When the
# var is unset (e.g., ad-hoc invocation), default to the local extenddb daemon.
ENDPOINT = os.environ.get("EXTENDDB_TEST_ENDPOINT", "http://localhost:8000").strip()


# Wire-format AttributeValues that DynamoDB rejects with a
# ValidationException. The bare `{}`, deeply-nested `{}`, and multi-key
# variants all surface as `EMPTY_ATTRIBUTE_VALUE` ("Supplied AttributeValue
# is empty, ..."). The SS/NS/BS-with-null variants share the same error
# *class* but have type-specific message text — for example, real DDB
# returns `"An string set may not have a null string as a member"` for the
# `{"SS":[..., null]}` case (yes, "An string set" is verbatim DDB wording).
#
# These tests assert the error class and HTTP status. The exact SS-null
# wording is locked in by `TestStringSetWithNullSpecificMessage` below.
EMPTY_AV_SCENARIOS = [
    pytest.param({}, id="bare_empty_map"),
    pytest.param({"L": [{}]}, id="empty_in_list"),
    pytest.param({"M": {"k": {}}}, id="empty_in_map"),
    pytest.param({"L": [{"L": [{}]}]}, id="empty_in_nested_list"),
    pytest.param({"M": {"outer": {"M": {"inner": {}}}}}, id="empty_in_nested_map"),
    pytest.param({"SS": ["a", None]}, id="ss_with_null_element"),
    pytest.param({"NS": ["1", None]}, id="ns_with_null_element"),
    pytest.param({"BS": ["QQ==", None]}, id="bs_with_null_element"),
]

# AttributeValue with more than one type-key. DynamoDB rejects with
# MULTI_ATTRIBUTE_VALUE wording, which is a separate but adjacent code path.
MULTI_AV = {"S": "hello", "N": "1"}


# ---------------------------------------------------------------------------
# Raw HTTP helpers — boto3 strips invalid payloads before sending, so we sign
# the request ourselves and post the bytes verbatim.
# ---------------------------------------------------------------------------


def _signed_post(operation: str, body: dict) -> requests.Response:
    """POST ``body`` to extenddb under the DynamoDB JSON-1.0 protocol.

    Mirrors the helper in ``test_import_export.py``. The request is SigV4-signed
    using the same env-var credentials the boto3-based tests pick up, so we get
    through any auth layer the daemon enforces.
    """
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    if access_key and secret_key:
        creds = Credentials(access_key, secret_key)
        aws_req = AWSRequest(method="POST", url=ENDPOINT, data=body_bytes, headers=headers)
        SigV4Auth(creds, "dynamodb", region).add_auth(aws_req)
        headers = dict(aws_req.headers)
    return requests.post(
        ENDPOINT,
        data=body_bytes,
        headers=headers,
        verify=not ENDPOINT.startswith("https://"),
    )


def _assert_empty_attribute_value(resp: requests.Response) -> None:
    """Assert the response is a 400 ValidationException for a malformed AttributeValue.

    Accepts either of two DynamoDB wordings:
    - ``EMPTY_ATTRIBUTE_VALUE`` ("Supplied AttributeValue is empty, ...") for
      bare-empty and nested-empty cases.
    - ``INVALID_PARAMETER_VALUE`` with the SS-specific suffix
      ("An string set may not have a null string as a member") for the
      ``{"SS":[..., null]}`` case.

    The exact SS-specific wording is locked in by the focused tests in
    ``TestStringSetWithNullSpecificMessage``.
    """
    assert resp.status_code == 400, (
        f"expected HTTP 400, got {resp.status_code}: {resp.text}"
    )
    payload = resp.json()
    error_type = payload.get("__type", "")
    message = payload.get("message", payload.get("Message", ""))
    assert "ValidationException" in error_type, (
        f"expected ValidationException, got {error_type}: {message}"
    )
    is_empty_av = (
        "Supplied AttributeValue is empty" in message
        and "must contain exactly one of the supported datatypes" in message
    )
    is_invalid_param = (
        "One or more parameter values were invalid" in message
        and (
            "may not have a null" in message
            or "may not contain null" in message
        )
    )
    assert is_empty_av or is_invalid_param, (
        f"expected EMPTY_ATTRIBUTE_VALUE or INVALID_PARAMETER_VALUE wording, got: {message}"
    )


def _assert_ss_with_null_specific_message(resp: requests.Response) -> None:
    """Assert the SS-with-null-element response uses the exact DDB wording.

    BigBird parity tests (``putItemTestListWithNullStringSet``,
    ``putItemTestMapWithNullStringSet``) check for the specific
    ``INVALID_PARAMETER_VALUE`` prefix plus
    ``"An string set may not have a null string as a member"`` (the awkward
    "An string set" wording is verbatim from real DDB).
    """
    assert resp.status_code == 400, resp.text
    payload = resp.json()
    error_type = payload.get("__type", "")
    message = payload.get("message", payload.get("Message", ""))
    assert "ValidationException" in error_type, (
        f"expected ValidationException, got {error_type}: {message}"
    )
    assert "One or more parameter values were invalid" in message, (
        f"expected INVALID_PARAMETER_VALUE prefix, got: {message}"
    )
    assert "An string set may not have a null string as a member" in message, (
        f"expected SS-specific suffix, got: {message}"
    )


def _assert_multi_attribute_value(resp: requests.Response) -> None:
    """Assert ValidationException with MULTI_ATTRIBUTE_VALUE wording."""
    assert resp.status_code == 400, resp.text
    payload = resp.json()
    error_type = payload.get("__type", "")
    message = payload.get("message", payload.get("Message", ""))
    assert "ValidationException" in error_type, (
        f"expected ValidationException, got {error_type}: {message}"
    )
    assert "Supplied AttributeValue has more than one datatypes set" in message, (
        f"expected MULTI_ATTRIBUTE_VALUE wording, got: {message}"
    )
    assert "must contain exactly one of the supported datatypes" in message, (
        f"expected DDB MULTI_ATTRIBUTE_VALUE suffix, got: {message}"
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def hash_table(dynamodb_client):
    """Hash-only table shared across the module, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def second_hash_table(dynamodb_client):
    """Second hash-only table for cross-table batch/transact tests."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[{"AttributeName": "id", "AttributeType": "S"}],
        key_schema=[{"AttributeName": "id", "KeyType": "HASH"}],
    ) as name:
        yield name


def _new_pk(prefix: str) -> str:
    """Generate a unique partition key value for a single test run."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# PutItem
# ---------------------------------------------------------------------------


class TestPutItemAttributeValueValidation:
    """PutItem rejects malformed AttributeValues in the Item map."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_item(self, hash_table, bad_av):
        body = {
            "TableName": hash_table,
            "Item": {"pk": {"S": _new_pk("put-empty")}, "bad": bad_av},
        }
        _assert_empty_attribute_value(_signed_post("PutItem", body))

    def test_multi_type_attribute_value_in_item(self, hash_table):
        body = {
            "TableName": hash_table,
            "Item": {"pk": {"S": _new_pk("put-multi")}, "bad": MULTI_AV},
        }
        _assert_multi_attribute_value(_signed_post("PutItem", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_expression_value(self, hash_table, bad_av):
        body = {
            "TableName": hash_table,
            "Item": {"pk": {"S": _new_pk("put-eav")}, "x": {"S": "y"}},
            "ConditionExpression": "attribute_not_exists(#a) OR #a = :v",
            "ExpressionAttributeNames": {"#a": "x"},
            "ExpressionAttributeValues": {":v": bad_av},
        }
        _assert_empty_attribute_value(_signed_post("PutItem", body))


# ---------------------------------------------------------------------------
# UpdateItem
# ---------------------------------------------------------------------------


class TestUpdateItemAttributeValueValidation:
    """UpdateItem rejects malformed AttributeValues in Key and EAV."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_key(self, hash_table, bad_av):
        body = {
            "TableName": hash_table,
            "Key": {"pk": bad_av},
            "UpdateExpression": "SET #a = :v",
            "ExpressionAttributeNames": {"#a": "data"},
            "ExpressionAttributeValues": {":v": {"S": "ok"}},
        }
        _assert_empty_attribute_value(_signed_post("UpdateItem", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_expression_value(self, hash_table, bad_av):
        body = {
            "TableName": hash_table,
            "Key": {"pk": {"S": _new_pk("upd-eav")}},
            "UpdateExpression": "SET #a = :v",
            "ExpressionAttributeNames": {"#a": "data"},
            "ExpressionAttributeValues": {":v": bad_av},
        }
        _assert_empty_attribute_value(_signed_post("UpdateItem", body))

    def test_multi_type_attribute_value_in_key(self, hash_table):
        body = {
            "TableName": hash_table,
            "Key": {"pk": MULTI_AV},
            "UpdateExpression": "SET #a = :v",
            "ExpressionAttributeNames": {"#a": "data"},
            "ExpressionAttributeValues": {":v": {"S": "ok"}},
        }
        _assert_multi_attribute_value(_signed_post("UpdateItem", body))


# ---------------------------------------------------------------------------
# DeleteItem
# ---------------------------------------------------------------------------


class TestDeleteItemAttributeValueValidation:
    """DeleteItem rejects malformed AttributeValues in Key and EAV."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_key(self, hash_table, bad_av):
        body = {"TableName": hash_table, "Key": {"pk": bad_av}}
        _assert_empty_attribute_value(_signed_post("DeleteItem", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_expression_value(self, hash_table, bad_av):
        body = {
            "TableName": hash_table,
            "Key": {"pk": {"S": _new_pk("del-eav")}},
            "ConditionExpression": "#a = :v",
            "ExpressionAttributeNames": {"#a": "x"},
            "ExpressionAttributeValues": {":v": bad_av},
        }
        _assert_empty_attribute_value(_signed_post("DeleteItem", body))


# ---------------------------------------------------------------------------
# GetItem
# ---------------------------------------------------------------------------


class TestGetItemAttributeValueValidation:
    """GetItem rejects malformed AttributeValues in Key."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_key(self, hash_table, bad_av):
        body = {"TableName": hash_table, "Key": {"pk": bad_av}}
        _assert_empty_attribute_value(_signed_post("GetItem", body))

    def test_multi_type_attribute_value_in_key(self, hash_table):
        body = {"TableName": hash_table, "Key": {"pk": MULTI_AV}}
        _assert_multi_attribute_value(_signed_post("GetItem", body))


# ---------------------------------------------------------------------------
# BatchGetItem
# ---------------------------------------------------------------------------


class TestBatchGetItemAttributeValueValidation:
    """BatchGetItem rejects malformed AttributeValues in per-table Keys."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_key(self, hash_table, bad_av):
        body = {"RequestItems": {hash_table: {"Keys": [{"pk": bad_av}]}}}
        _assert_empty_attribute_value(_signed_post("BatchGetItem", body))


# ---------------------------------------------------------------------------
# BatchWriteItem
# ---------------------------------------------------------------------------


class TestBatchWriteItemAttributeValueValidation:
    """BatchWriteItem rejects malformed AttributeValues in PutRequest.Item / DeleteRequest.Key."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_put_item(self, hash_table, bad_av):
        body = {
            "RequestItems": {
                hash_table: [
                    {"PutRequest": {"Item": {"pk": {"S": _new_pk("bwi-put")}, "bad": bad_av}}}
                ]
            }
        }
        _assert_empty_attribute_value(_signed_post("BatchWriteItem", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_delete_key(self, hash_table, bad_av):
        body = {
            "RequestItems": {
                hash_table: [{"DeleteRequest": {"Key": {"pk": bad_av}}}]
            }
        }
        _assert_empty_attribute_value(_signed_post("BatchWriteItem", body))


# ---------------------------------------------------------------------------
# TransactGetItems
# ---------------------------------------------------------------------------


class TestTransactGetItemsAttributeValueValidation:
    """TransactGetItems rejects malformed AttributeValues in per-op Get.Key."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_get_key(self, hash_table, bad_av):
        body = {
            "TransactItems": [
                {"Get": {"TableName": hash_table, "Key": {"pk": bad_av}}}
            ]
        }
        _assert_empty_attribute_value(_signed_post("TransactGetItems", body))


# ---------------------------------------------------------------------------
# TransactWriteItems
# ---------------------------------------------------------------------------


class TestTransactWriteItemsAttributeValueValidation:
    """TransactWriteItems rejects malformed AttributeValues in any sub-op."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_put_item(self, hash_table, bad_av):
        body = {
            "TransactItems": [
                {
                    "Put": {
                        "TableName": hash_table,
                        "Item": {"pk": {"S": _new_pk("twi-put")}, "bad": bad_av},
                    }
                }
            ]
        }
        _assert_empty_attribute_value(_signed_post("TransactWriteItems", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_update_key(self, hash_table, bad_av):
        body = {
            "TransactItems": [
                {
                    "Update": {
                        "TableName": hash_table,
                        "Key": {"pk": bad_av},
                        "UpdateExpression": "SET #a = :v",
                        "ExpressionAttributeNames": {"#a": "data"},
                        "ExpressionAttributeValues": {":v": {"S": "ok"}},
                    }
                }
            ]
        }
        _assert_empty_attribute_value(_signed_post("TransactWriteItems", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_delete_key(self, hash_table, bad_av):
        body = {
            "TransactItems": [
                {"Delete": {"TableName": hash_table, "Key": {"pk": bad_av}}}
            ]
        }
        _assert_empty_attribute_value(_signed_post("TransactWriteItems", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_condition_check_key(self, hash_table, bad_av):
        body = {
            "TransactItems": [
                {
                    "ConditionCheck": {
                        "TableName": hash_table,
                        "Key": {"pk": bad_av},
                        "ConditionExpression": "attribute_exists(pk)",
                    }
                }
            ]
        }
        _assert_empty_attribute_value(_signed_post("TransactWriteItems", body))


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


class TestQueryAttributeValueValidation:
    """Query rejects malformed AttributeValues in EAV and ExclusiveStartKey."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_expression_value(self, hash_table, bad_av):
        body = {
            "TableName": hash_table,
            "KeyConditionExpression": "#a = :v",
            "ExpressionAttributeNames": {"#a": "pk"},
            "ExpressionAttributeValues": {":v": bad_av},
        }
        _assert_empty_attribute_value(_signed_post("Query", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_exclusive_start_key(self, hash_table, bad_av):
        body = {
            "TableName": hash_table,
            "KeyConditionExpression": "#a = :v",
            "ExpressionAttributeNames": {"#a": "pk"},
            "ExpressionAttributeValues": {":v": {"S": _new_pk("query-esk")}},
            "ExclusiveStartKey": {"pk": bad_av},
        }
        _assert_empty_attribute_value(_signed_post("Query", body))


# ---------------------------------------------------------------------------
# Scan
# ---------------------------------------------------------------------------


class TestScanAttributeValueValidation:
    """Scan rejects malformed AttributeValues in EAV and ExclusiveStartKey."""

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_expression_value(self, hash_table, bad_av):
        body = {
            "TableName": hash_table,
            "FilterExpression": "#a = :v",
            "ExpressionAttributeNames": {"#a": "data"},
            "ExpressionAttributeValues": {":v": bad_av},
        }
        _assert_empty_attribute_value(_signed_post("Scan", body))

    @pytest.mark.parametrize("bad_av", EMPTY_AV_SCENARIOS)
    def test_empty_attribute_value_in_exclusive_start_key(self, hash_table, bad_av):
        body = {"TableName": hash_table, "ExclusiveStartKey": {"pk": bad_av}}
        _assert_empty_attribute_value(_signed_post("Scan", body))


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# StringSet-with-null exact-wording lockdown
#
# Real DynamoDB returns a type-specific INVALID_PARAMETER_VALUE message for
# the ``{"SS":[..., null]}`` case rather than the generic
# EMPTY_ATTRIBUTE_VALUE wording. BigBird parity tests
# (``putItemTestListWithNullStringSet``, ``putItemTestMapWithNullStringSet``)
# match on the suffix verbatim, so we lock the wording in here on a few
# representative APIs. Other APIs route through the same deserializer so
# they automatically inherit the fix; the broader EMPTY_AV_SCENARIOS suite
# above covers them under the more permissive helper.
# ---------------------------------------------------------------------------


class TestStringSetWithNullSpecificMessage:
    """SS arrays containing a JSON null produce DDB's INVALID_PARAMETER_VALUE wording."""

    def test_put_item_top_level_ss_with_null(self, hash_table):
        body = {
            "TableName": hash_table,
            "Item": {
                "pk": {"S": _new_pk("ss-null-put-top")},
                "bad_set": {"SS": ["a", None]},
            },
        }
        _assert_ss_with_null_specific_message(_signed_post("PutItem", body))

    def test_put_item_ss_with_null_inside_list(self, hash_table):
        # Mirrors BigBird ``putItemTestListWithNullStringSet`` shape.
        body = {
            "TableName": hash_table,
            "Item": {
                "pk": {"S": _new_pk("ss-null-put-list")},
                "bad_attr": {"L": [{"SS": ["a", None]}]},
            },
        }
        _assert_ss_with_null_specific_message(_signed_post("PutItem", body))

    def test_put_item_ss_with_null_inside_map(self, hash_table):
        # Mirrors BigBird ``putItemTestMapWithNullStringSet`` shape.
        body = {
            "TableName": hash_table,
            "Item": {
                "pk": {"S": _new_pk("ss-null-put-map")},
                "bad_attr": {"M": {"k": {"SS": ["a", None]}}},
            },
        }
        _assert_ss_with_null_specific_message(_signed_post("PutItem", body))

    def test_update_item_ss_with_null_in_expression_value(self, hash_table):
        body = {
            "TableName": hash_table,
            "Key": {"pk": {"S": _new_pk("ss-null-update")}},
            "UpdateExpression": "SET #a = :v",
            "ExpressionAttributeNames": {"#a": "tags"},
            "ExpressionAttributeValues": {":v": {"SS": ["a", None]}},
        }
        _assert_ss_with_null_specific_message(_signed_post("UpdateItem", body))

    def test_batch_write_item_ss_with_null_in_put_item(self, hash_table):
        body = {
            "RequestItems": {
                hash_table: [
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": _new_pk("ss-null-bwi")},
                                "bad_set": {"SS": ["a", None]},
                            }
                        }
                    }
                ]
            }
        }
        _assert_ss_with_null_specific_message(_signed_post("BatchWriteItem", body))


# ---------------------------------------------------------------------------
# Sanity — legitimate falsy / empty-collection AttributeValues must still work.
# Guards against an over-eager fix that rejects valid payloads alongside the
# truly malformed ones. boto3 sends these correctly so we use the high-level
# client here for parity with the rest of the suite.
# ---------------------------------------------------------------------------


class TestSanityValidPayloadsStillAccepted:
    """Legitimate falsy AttributeValues continue to round-trip cleanly."""

    def test_bool_false_round_trip(self, dynamodb_client, hash_table):
        pk = _new_pk("sanity-bool-false")
        item = {"pk": {"S": pk}, "is_active": {"BOOL": False}}
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": pk}})
        assert resp["Item"] == item

    def test_null_true_round_trip(self, dynamodb_client, hash_table):
        pk = _new_pk("sanity-null")
        item = {"pk": {"S": pk}, "deleted_at": {"NULL": True}}
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": pk}})
        assert resp["Item"] == item

    def test_empty_list_and_empty_map_round_trip(self, dynamodb_client, hash_table):
        pk = _new_pk("sanity-empty-collections")
        item = {
            "pk": {"S": pk},
            "empty_list": {"L": []},
            "empty_map": {"M": {}},
        }
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": pk}})
        assert resp["Item"] == item
