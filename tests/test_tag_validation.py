# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tag key and value validation on TagResource, UntagResource and CreateTable.

The service accepts a tag key or value built from Unicode letters, numbers and
separators plus ``_ . : / = + - @``, up to 128 characters for a key and 256 for
a value, and refuses a key prefixed with ``aws:`` in any case. Every expected
response in this file was measured against DynamoDB in us-east-1 on
2026-10-24 and recorded in the builder report's Service measurements section,
including the order of the checks: the ARN prefix, then the resource kind, then
the account, then the tag keys and values, then the region, then the table's
existence.

The requests go through ``dynamodb_client_no_validation`` because botocore
enforces the model's own 1-to-128 and 0-to-256 length bounds on the client, and
the point of these tests is what the server answers.

REQ-TEST-001
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table, wait_for_active

# Characters outside the tag grammar. Measured one at a time in a key and in a
# value: every one is a ValidationException.
REJECTED_CHARACTERS = [
    "!",
    "#",
    "$",
    "%",
    "&",
    "*",
    "(",
    ",",
    ";",
    "<",
    "'",
    '"',
    "\\",
    "`",
    "~",
    "^",
    "|",
    "{",
    "[",
    "?",
    "\t",
    "\n",
    "\U0001f600",
    "\u0301",
    "\u093e",
]

# Keys and values built only from characters the service accepts, with the
# label each one measures. Applied in one TagResource request.
ACCEPTED_TAGS = [
    ("ascii-charset", "Abc123 _.:/=+-@", "Abc123 _.:/=+-@"),
    ("letter-e-acute", "k\u00e9y", "v\u00e9x"),
    ("letter-sharp-s", "k\u00dfy", "v\u00dfx"),
    ("letter-cjk", "k\u4e2dy", "v\u4e2dx"),
    ("letter-cyrillic", "k\u0434y", "v\u0434x"),
    ("separator-no-break-space", "k\u00a0y", "v\u00a0x"),
    ("separator-line", "k\u2028y", "v\u2028x"),
    ("number-arabic-indic", "k\u0663y", "v\u0663x"),
    ("number-roman-numeral", "k\u2167y", "v\u2167x"),
    ("leading-space", " lead", " lead"),
    ("trailing-space", "trail ", "trail "),
    ("space-only-key", " ", " "),
    ("key-aws-bare", "aws", "v"),
    ("key-aws-not-at-start", "xaws:foo", "v"),
    ("value-aws-prefix", "vaws", "aws:foo"),
    ("key-128-characters", "k" * 128, "v"),
    ("key-128-two-byte-characters", "\u00e9" * 128, "v"),
    ("value-256-characters", "vlen", "v" * 256),
    ("value-empty", "vempty", ""),
]


def key_message(key: str) -> str:
    return f"The Tag Key provided is invalid, Key: {key}"


def value_message(value: str) -> str:
    return f"The Tag Value provided is invalid, Value: {value}"


def prefix_message(key: str) -> str:
    return f"Tag Key cannot be prefixed with aws:, Key: {key}"


def assert_validation(call, expected_message: str) -> None:
    """Assert the call fails with ValidationException and that exact message."""
    with pytest.raises(ClientError) as exc:
        call()
    err = exc.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == expected_message, err


@pytest.fixture(scope="class")
def tag_table(dynamodb_client):
    """One table for the rejection cases, which apply no tag mutation."""
    with scoped_table(dynamodb_client) as name:
        arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]
        yield {"name": name, "arn": arn}


class TestTagKeyCharacters:
    """A character outside the grammar is refused in a tag key."""

    @pytest.mark.parametrize("char", REJECTED_CHARACTERS, ids=lambda c: f"u{ord(c):04x}")
    def test_tag_resource_rejects_key_character(
        self, dynamodb_client_no_validation, tag_table, char
    ):
        key = f"k{char}y"
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": key, "Value": "v"}]
            ),
            key_message(key),
        )

    @pytest.mark.parametrize("char", REJECTED_CHARACTERS, ids=lambda c: f"u{ord(c):04x}")
    def test_tag_resource_rejects_value_character(
        self, dynamodb_client_no_validation, tag_table, char
    ):
        value = f"v{char}x"
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": "vchar", "Value": value}]
            ),
            value_message(value),
        )


class TestTagLength:
    """128 characters of key and 256 of value are accepted, one more is not."""

    def test_key_of_129_characters_rejected(self, dynamodb_client_no_validation, tag_table):
        key = "k" * 129
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": key, "Value": "v"}]
            ),
            key_message(key),
        )

    def test_key_of_129_two_byte_characters_rejected(
        self, dynamodb_client_no_validation, tag_table
    ):
        """The limit counts characters, so 129 two-byte characters is over it."""
        key = "\u00e9" * 129
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": key, "Value": "v"}]
            ),
            key_message(key),
        )

    def test_empty_key_rejected(self, dynamodb_client_no_validation, tag_table):
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": "", "Value": "v"}]
            ),
            key_message(""),
        )

    def test_value_of_257_characters_rejected(self, dynamodb_client_no_validation, tag_table):
        value = "v" * 257
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": "vlen", "Value": value}]
            ),
            value_message(value),
        )


class TestReservedPrefix:
    """A key prefixed with aws: is refused, whatever its case."""

    @pytest.mark.parametrize("key", ["aws:foo", "AWS:foo", "Aws:foo", "aws:"])
    def test_tag_resource_rejects_reserved_prefix(
        self, dynamodb_client_no_validation, tag_table, key
    ):
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": key, "Value": "v"}]
            ),
            prefix_message(key),
        )

    def test_untag_resource_rejects_reserved_prefix(
        self, dynamodb_client_no_validation, tag_table
    ):
        assert_validation(
            lambda: dynamodb_client_no_validation.untag_resource(
                ResourceArn=tag_table["arn"], TagKeys=["aws:foo"]
            ),
            prefix_message("aws:foo"),
        )


class TestCheckOrder:
    """Which of two broken inputs the response reports."""

    def test_characters_are_checked_before_the_reserved_prefix(
        self, dynamodb_client_no_validation, tag_table
    ):
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": "aws:k!", "Value": "v"}]
            ),
            key_message("aws:k!"),
        )

    def test_length_is_checked_before_the_reserved_prefix(
        self, dynamodb_client_no_validation, tag_table
    ):
        key = "aws:" + "k" * 130
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": key, "Value": "v"}]
            ),
            key_message(key),
        )

    def test_key_is_checked_before_its_value(self, dynamodb_client_no_validation, tag_table):
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": "k!", "Value": "v!"}]
            ),
            key_message("k!"),
        )

    def test_tags_are_checked_in_request_order(self, dynamodb_client_no_validation, tag_table):
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"],
                Tags=[{"Key": "ok1", "Value": "v!"}, {"Key": "k!", "Value": "ok2"}],
            ),
            value_message("v!"),
        )

    def test_tags_are_checked_before_the_table_exists(
        self, dynamodb_client_no_validation, tag_table
    ):
        """An invalid key on an ARN naming no table reports the key."""
        arn = tag_table["arn"].replace(tag_table["name"], f"absent-{uuid.uuid4().hex[:8]}")
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=arn, Tags=[{"Key": "k!", "Value": "v"}]
            ),
            key_message("k!"),
        )

    def test_untag_keys_are_checked_before_the_table_exists(
        self, dynamodb_client_no_validation, tag_table
    ):
        arn = tag_table["arn"].replace(tag_table["name"], f"absent-{uuid.uuid4().hex[:8]}")
        assert_validation(
            lambda: dynamodb_client_no_validation.untag_resource(
                ResourceArn=arn, TagKeys=["k!"]
            ),
            key_message("k!"),
        )

    def test_tags_are_checked_before_the_region(self, dynamodb_client_no_validation, tag_table):
        """An invalid key on a cross-region table ARN reports the key."""
        fields = tag_table["arn"].split(":")
        fields[3] = "eu-west-1" if fields[3] != "eu-west-1" else "us-west-2"
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=":".join(fields), Tags=[{"Key": "k!", "Value": "v"}]
            ),
            key_message("k!"),
        )

    def test_tags_are_checked_before_the_partition(
        self, dynamodb_client_no_validation, tag_table
    ):
        fields = tag_table["arn"].split(":")
        fields[1] = "aws-cn"
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=":".join(fields), Tags=[{"Key": "k!", "Value": "v"}]
            ),
            key_message("k!"),
        )

    def test_the_account_is_checked_before_the_tags(
        self, dynamodb_client_no_validation, tag_table
    ):
        """A cross-account ARN is denied without reporting the tag."""
        fields = tag_table["arn"].split(":")
        fields[4] = "111111111111" if fields[4] != "111111111111" else "222222222222"
        with pytest.raises(ClientError) as exc:
            dynamodb_client_no_validation.tag_resource(
                ResourceArn=":".join(fields), Tags=[{"Key": "k!", "Value": "v"}]
            )
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"

    def test_the_resource_kind_is_checked_before_the_tags(
        self, dynamodb_client_no_validation, tag_table
    ):
        """An index ARN is reported as the wrong resource, not as a bad tag."""
        arn = f"{tag_table['arn']}/index/gsi1"
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=arn, Tags=[{"Key": "k!", "Value": "v"}]
            ),
            "One or more parameter values were invalid: Invalid resource arn provided, "
            f"only table or stream is accepted. Provided resource arn: {arn}",
        )


class TestAcceptedGrammar:
    """Every character class the service accepts is stored and read back."""

    def test_accepted_keys_and_values_are_stored(self, dynamodb_client_no_validation):
        with scoped_table(dynamodb_client_no_validation) as name:
            arn = dynamodb_client_no_validation.describe_table(TableName=name)["Table"][
                "TableArn"
            ]
            dynamodb_client_no_validation.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": k, "Value": v} for _, k, v in ACCEPTED_TAGS],
            )
            stored = {
                t["Key"]: t["Value"]
                for t in dynamodb_client_no_validation.list_tags_of_resource(ResourceArn=arn)[
                    "Tags"
                ]
            }
            for label, key, value in ACCEPTED_TAGS:
                assert key in stored, label
                assert stored[key] == value, label

    def test_untag_accepts_a_key_in_the_grammar(self, dynamodb_client_no_validation, tag_table):
        """A key within the grammar is accepted even when no such tag is set."""
        dynamodb_client_no_validation.untag_resource(
            ResourceArn=tag_table["arn"], TagKeys=["k\u00e9y", "aws", "k" * 128]
        )


class TestUntagKeyCharacters:
    """UntagResource applies the same key rules as TagResource."""

    @pytest.mark.parametrize("char", ["!", "#", "\t", "\U0001f600", "\u093e"])
    def test_untag_rejects_key_character(
        self, dynamodb_client_no_validation, tag_table, char
    ):
        key = f"k{char}y"
        assert_validation(
            lambda: dynamodb_client_no_validation.untag_resource(
                ResourceArn=tag_table["arn"], TagKeys=[key]
            ),
            key_message(key),
        )

    def test_untag_rejects_empty_key(self, dynamodb_client_no_validation, tag_table):
        assert_validation(
            lambda: dynamodb_client_no_validation.untag_resource(
                ResourceArn=tag_table["arn"], TagKeys=[""]
            ),
            key_message(""),
        )

    def test_untag_rejects_key_of_129_characters(
        self, dynamodb_client_no_validation, tag_table
    ):
        key = "k" * 129
        assert_validation(
            lambda: dynamodb_client_no_validation.untag_resource(
                ResourceArn=tag_table["arn"], TagKeys=[key]
            ),
            key_message(key),
        )

    def test_untag_reports_the_first_invalid_key(
        self, dynamodb_client_no_validation, tag_table
    ):
        assert_validation(
            lambda: dynamodb_client_no_validation.untag_resource(
                ResourceArn=tag_table["arn"], TagKeys=["ok", "zk!", "ak#"]
            ),
            key_message("zk!"),
        )


class TestCreateTableTags:
    """CreateTable.Tags are stored in the same rows, so the same rules apply."""

    def test_create_table_rejects_an_invalid_tag_key(self, dynamodb_client_no_validation):
        name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
        assert_validation(
            lambda: dynamodb_client_no_validation.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
                Tags=[{"Key": "k!", "Value": "v"}],
            ),
            key_message("k!"),
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client_no_validation.describe_table(TableName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_table_rejects_an_invalid_tag_value(self, dynamodb_client_no_validation):
        name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
        assert_validation(
            lambda: dynamodb_client_no_validation.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
                Tags=[{"Key": "ok", "Value": "v!"}],
            ),
            value_message("v!"),
        )

    def test_create_table_rejects_the_reserved_prefix(self, dynamodb_client_no_validation):
        name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
        assert_validation(
            lambda: dynamodb_client_no_validation.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
                Tags=[{"Key": "aws:owner", "Value": "v"}],
            ),
            prefix_message("aws:owner"),
        )

    def test_create_table_keeps_accepting_tags_in_the_grammar(
        self, dynamodb_client_no_validation
    ):
        name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
        dynamodb_client_no_validation.create_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "k\u00e9y", "Value": ""}],
        )
        try:
            wait_for_active(dynamodb_client_no_validation, name)
            arn = dynamodb_client_no_validation.describe_table(TableName=name)["Table"][
                "TableArn"
            ]
            stored = {
                t["Key"]: t["Value"]
                for t in dynamodb_client_no_validation.list_tags_of_resource(ResourceArn=arn)[
                    "Tags"
                ]
            }
            assert stored["env"] == "test"
            assert stored["k\u00e9y"] == ""
        finally:
            try:
                dynamodb_client_no_validation.delete_table(TableName=name)
            except ClientError:
                pass
