# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The order of the tag checks against the ARN checks and the table definition.

TagResource and UntagResource check, in this order: the ARN prefix, the
resource kind, the account, the partition, service and region of a stream ARN,
the tag keys and values, a table ARN's table segment, partition, service and
region, the table's existence, and last a key that reads ``aws:`` once U+017F
is read as ``s``. CreateTable checks the TableName, then the Tags, then the
rest of the request. Every expected response in this file was measured against
DynamoDB in us-east-1 on 2026-10-24 or on 2026-11-20.

REQ-TEST-001
"""

from __future__ import annotations

import uuid

import pytest

from conftest import scoped_table
from test_tag_validation import (
    LONG_S_MESSAGE,
    assert_create_table_refused,
    assert_error,
    assert_validation,
    key_message,
    prefix_message,
    value_message,
)

K_BAD = [{"Key": "k!", "Value": "v"}]
K_OK = [{"Key": "valid-key", "Value": "v"}]
STREAM = "stream/2026-01-01T00:00:00.000"


def with_field(arn: str, index: int, value: str) -> str:
    fields = arn.split(":", 5)
    fields[index] = value
    return ":".join(fields)


@pytest.fixture(scope="module")
def tag_table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]
        region = arn.split(":")[3]
        yield {
            "name": name,
            "arn": arn,
            "other_region": "eu-west-1" if region != "eu-west-1" else "us-west-2",
            "other_account": "111122223333" if arn.split(":")[4] != "111122223333" else "222233334444",
        }


def absent(t) -> str:
    return t["arn"].replace(t["name"], f"absent-{uuid.uuid4().hex[:8]}")


class TestTagsAgainstEachOther:
    """Which of two broken tag inputs the response reports."""

    @pytest.mark.parametrize(
        "tags,expected",
        [
            ([{"Key": "aws:k!", "Value": "v"}], key_message("aws:k!")),
            ([{"Key": "aws:" + "k" * 130, "Value": "v"}], key_message("aws:" + "k" * 130)),
            ([{"Key": "k!", "Value": "v!"}], key_message("k!")),
            ([{"Key": "ok1", "Value": "v!"}, {"Key": "k!", "Value": "ok2"}], value_message("v!")),
            ([{"Key": "aw\u017f:foo", "Value": "v"}, {"Key": "k!", "Value": "v"}], key_message("k!")),
        ],
        ids=[
            "characters-before-prefix",
            "length-before-prefix",
            "key-before-value",
            "request-order",
            "grammar-before-long-s-prefix",
        ],
    )
    def test_reported_input(self, dynamodb_client_no_validation, tag_table, tags, expected):
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=tags
            ),
            expected,
        )


# Each ARN shape with an invalid key: the answer the service gives, so whether
# the ARN check runs before or after the tags. Built from the probe table's ARN.
ARN_BEFORE_OR_AFTER_TAGS = [
    # The account and the resource kind come before the tags.
    ("other-account", lambda t: with_field(t["arn"], 4, t["other_account"]), "account"),
    (
        "foreign-service-other-account",
        lambda t: f"arn:aws:s3:{t['arn'].split(':')[3]}:{t['other_account']}:table/x/",
        "account",
    ),
    ("index", lambda t: f"{t['arn']}/index/gsi1", "index"),
    ("export", lambda t: f"{t['arn']}/export/01700000000000-abcdef12", "not-dynamodb"),
    ("import", lambda t: f"{t['arn']}/import/01700000000000-abcdef12", "not-dynamodb"),
    # A stream ARN outside this deployment comes before the tags.
    ("stream-other-region", lambda t: with_field(f"{t['arn']}/{STREAM}", 3, t["other_region"]), "stream"),
    ("stream-aws-cn", lambda t: with_field(f"{t['arn']}/{STREAM}", 1, "aws-cn"), "stream"),
    ("stream-s3", lambda t: with_field(f"{t['arn']}/{STREAM}", 2, "s3"), "stream"),
    ("stream-bad-table-segment", lambda t: t["arn"].replace(t["name"], "bad!name") + f"/{STREAM}", "stream"),
    # The tags come before everything else.
    ("stream-absent-table", lambda t: f"{absent(t)}/{STREAM}", "tag"),
    ("absent-table", absent, "tag"),
    ("other-region", lambda t: with_field(t["arn"], 3, t["other_region"]), "tag"),
    ("aws-cn", lambda t: with_field(t["arn"], 1, "aws-cn"), "tag"),
    ("foreign-service-table", lambda t: with_field(t["arn"], 2, "s3"), "tag"),
    ("bad-table-segment", lambda t: t["arn"].replace(t["name"], "bad!name"), "tag"),
    (
        "bad-table-segment-other-region",
        lambda t: with_field(t["arn"].replace(t["name"], "bad!name"), 3, t["other_region"]),
        "tag",
    ),
]


def arn_answer(kind: str, arn: str) -> tuple[str, str]:
    return {
        "account": ("AccessDeniedException", "Access is denied"),
        "index": (
            "ValidationException",
            "One or more parameter values were invalid: Invalid resource arn provided, only "
            f"table or stream is accepted. Provided resource arn: {arn}",
        ),
        "not-dynamodb": (
            "ValidationException",
            f"One or more parameter values were invalid: Provided Arn is not a DynamoDB resource arn: {arn}",
        ),
        "stream": ("ValidationException", "Invalid StreamArn"),
        "tag": ("ValidationException", key_message("k!")),
    }[kind]


@pytest.mark.parametrize("op", ["tag_resource", "untag_resource"])
@pytest.mark.parametrize(
    "label,build,kind", ARN_BEFORE_OR_AFTER_TAGS, ids=[r[0] for r in ARN_BEFORE_OR_AFTER_TAGS]
)
def test_arn_checks_against_an_invalid_key(
    dynamodb_client_no_validation, tag_table, op, label, build, kind
):
    arn = build(tag_table)
    client = dynamodb_client_no_validation
    calls = {
        "tag_resource": lambda: client.tag_resource(ResourceArn=arn, Tags=K_BAD),
        "untag_resource": lambda: client.untag_resource(ResourceArn=arn, TagKeys=["k!"]),
    }
    assert_error(calls[op], *arn_answer(kind, arn))


@pytest.mark.parametrize(
    "tags,expected",
    [
        ([{"Key": "ok", "Value": "v!"}], value_message("v!")),
        ([{"Key": "aws:foo", "Value": "v"}], prefix_message("aws:foo")),
    ],
    ids=["value", "prefix"],
)
def test_a_bad_table_segment_reports_the_tag_value_and_prefix(
    dynamodb_client_no_validation, tag_table, tags, expected
):
    arn = tag_table["arn"].replace(tag_table["name"], "bad!name")
    assert_validation(
        lambda: dynamodb_client_no_validation.tag_resource(ResourceArn=arn, Tags=tags), expected
    )


# With a valid key every operation still runs both halves of the ARN check.
# The long-s key is valid until the table is found, so it reports the ARN too.
VALID_KEY_SHAPES = [
    ("cross_region", lambda t: with_field(t["arn"], 3, t["other_region"])),
    ("cross_partition", lambda t: with_field(t["arn"], 1, "aws-cn")),
    ("wrong_service", lambda t: with_field(t["arn"], 2, "s3")),
    ("bad_table_segment", lambda t: t["arn"].replace(t["name"], "bad!name")),
    ("absent", absent),
    ("cross_account", lambda t: with_field(t["arn"], 4, t["other_account"])),
]


def valid_key_answer(shape: str, arn: str) -> tuple[str, str]:
    if shape == "absent":
        return ("ResourceNotFoundException", f"Requested resource not found: ResourceArn: {arn} not found")
    if shape == "cross_account":
        return ("AccessDeniedException", "Access is denied")
    return ("ValidationException", f"Invalid TableArn: Invalid ResourceArn provided as input {arn}")


@pytest.mark.parametrize(
    "op", ["tag_resource", "tag_resource_long_s", "untag_resource", "list_tags_of_resource"]
)
@pytest.mark.parametrize("shape,build", VALID_KEY_SHAPES, ids=[r[0] for r in VALID_KEY_SHAPES])
def test_a_valid_key_still_checks_the_arn(dynamodb_client_no_validation, tag_table, op, shape, build):
    arn = build(tag_table)
    client = dynamodb_client_no_validation
    calls = {
        "tag_resource": lambda: client.tag_resource(ResourceArn=arn, Tags=K_OK),
        "tag_resource_long_s": lambda: client.tag_resource(
            ResourceArn=arn, Tags=[{"Key": "aw\u017f:foo", "Value": "v"}]
        ),
        "untag_resource": lambda: client.untag_resource(ResourceArn=arn, TagKeys=["valid-key"]),
        "list_tags_of_resource": lambda: client.list_tags_of_resource(ResourceArn=arn),
    }
    assert_error(calls[op], *valid_key_answer(shape, arn))


# CreateTable: each definition error with an invalid tag reports the tag, and
# a long-s key reports itself before the key schema checks and after the enum
# checks.
UNDEFINED_KEY = [{"AttributeName": "other", "AttributeType": "S"}]
CREATE_TABLE_ORDER = [
    ("undefined-key-attribute", {"AttributeDefinitions": UNDEFINED_KEY}, K_BAD, key_message("k!")),
    (
        "undefined-key-attribute-value",
        {"AttributeDefinitions": UNDEFINED_KEY},
        [{"Key": "ok", "Value": "v!"}],
        value_message("v!"),
    ),
    (
        "undefined-key-attribute-prefix",
        {"AttributeDefinitions": UNDEFINED_KEY},
        [{"Key": "aws:owner", "Value": "v"}],
        prefix_message("aws:owner"),
    ),
    ("provisioned-without-throughput", {"BillingMode": "PROVISIONED"}, K_BAD, key_message("k!")),
    ("billing-mode-enum", {"BillingMode": "FOO"}, K_BAD, key_message("k!")),
    (
        "attribute-type-enum",
        {"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "X"}]},
        K_BAD,
        key_message("k!"),
    ),
    ("missing-key-schema", {"KeySchema": None}, K_BAD, key_message("k!")),
    (
        "two-hash-keys",
        {
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "p2", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "p2", "KeyType": "HASH"},
            ],
        },
        K_BAD,
        key_message("k!"),
    ),
    ("stream-without-view-type", {"StreamSpecification": {"StreamEnabled": True}}, K_BAD, key_message("k!")),
    (
        "long-s-before-undefined-key-attribute",
        {"AttributeDefinitions": UNDEFINED_KEY},
        [{"Key": "aw\u017f:foo", "Value": "v"}],
        LONG_S_MESSAGE,
    ),
    (
        "billing-mode-enum-before-long-s",
        {"BillingMode": "FOO"},
        [{"Key": "aw\u017f:foo", "Value": "v"}],
        "1 validation error detected: Value 'FOO' at 'billingMode' failed to satisfy constraint: "
        "Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]",
    ),
]


@pytest.mark.parametrize(
    "label,definition,tags,expected", CREATE_TABLE_ORDER, ids=[r[0] for r in CREATE_TABLE_ORDER]
)
def test_create_table_checks_the_tags_before_the_definition(
    dynamodb_client_no_validation, label, definition, tags, expected
):
    assert_create_table_refused(dynamodb_client_no_validation, expected, Tags=tags, **definition)


TOO_SHORT = (
    "1 validation error detected: Value 'ab' at 'tableName' failed to satisfy constraint: "
    "Member must have length greater than or equal to 3"
)


@pytest.mark.parametrize(
    "request_name,definition,tags,expected",
    [
        ("ab", {}, K_BAD, TOO_SHORT),
        (None, {}, K_BAD, "The parameter 'TableName' is required but was not present in the request"),
        # Without tags the TableName length still comes first.
        ("ab", {"BillingMode": "FOO"}, None, TOO_SHORT),
        ("ab", {"KeySchema": None}, None, TOO_SHORT),
    ],
    ids=["too-short", "missing", "too-short-billing-mode-enum", "too-short-missing-key-schema"],
)
def test_create_table_checks_the_table_name_first(
    dynamodb_client_no_validation, request_name, definition, tags, expected
):
    params = {
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "TableName": request_name,
        "Tags": tags,
    }
    params.update(definition)
    params = {k: v for k, v in params.items() if v is not None}
    assert_validation(lambda: dynamodb_client_no_validation.create_table(**params), expected)
