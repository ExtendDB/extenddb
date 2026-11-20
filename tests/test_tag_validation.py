# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tag key and value grammar on TagResource, UntagResource and CreateTable.

The service accepts a tag key or value built from Unicode 13.0 letters, numbers
and separators plus ``_ . : / = + - @``, up to 128 UTF-16 code units for a key
and 256 for a value. It refuses a key prefixed with ``aws:`` in any case, and,
in a later check, a key that reads ``aws:`` once U+017F is read as ``s``. Every
expected response in this file was measured against DynamoDB in us-east-1 on
2026-10-24 or on 2026-11-20. ``test_tag_check_order.py`` holds the order of
these checks against the ARN checks and the table definition.

The requests go through ``dynamodb_client_no_validation`` because botocore
enforces the model's own 1-to-128 and 0-to-256 length bounds on the client, and
the point of these tests is what the server answers.

REQ-TEST-001
"""

from __future__ import annotations

import time
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
    # Letters and digits that Unicode 14.0 to 17.0 assign.
    "\u0870",
    "\U00016ac0",
    "\U0001e290",
    "\U00031350",
    "\U0002ebf0",
    "\u1c89",
    "\U000323b0",
]

# Keys and values built only from characters the service accepts, with the
# label each one measures. Applied in one TagResource request.
ACCEPTED_TAGS = [
    ("ascii-charset", "Abc123 _.:/=+-@", "Abc123 _.:/=+-@"),
    ("letter-e-acute", "k\u00e9y", "v\u00e9x"),
    ("letter-sharp-s", "k\u00dfy", "v\u00dfx"),
    ("letter-cjk", "k\u4e2dy", "v\u4e2dx"),
    ("letter-cyrillic", "k\u0434y", "v\u0434x"),
    ("letter-unicode-13-arabic", "k\u08bey", "v\u08bex"),
    ("letter-unicode-13-yezidi", "k\U00010e80y", "v\U00010e80x"),
    ("letter-unicode-13-cjk", "k\U00030000y", "v\U00030000x"),
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
    ("value-long-s-prefix", "vlongs", "aw\u017f:foo"),
    ("key-128-characters", "k" * 128, "v"),
    ("key-128-two-byte-characters", "\u00e9" * 128, "v"),
    ("key-64-four-byte-characters", "\U00020000" * 64, "v"),
    ("key-128-utf16-units", "a" * 126 + "\U00020000", "v"),
    ("value-256-characters", "vlen", "v" * 256),
    ("value-256-two-byte-characters", "vlen2", "\u00e9" * 256),
    ("value-128-four-byte-characters", "vlen4", "\U00020000" * 128),
    ("value-empty", "vempty", ""),
]

# Keys and values one UTF-16 code unit over the limit, and the empty key. The
# limits count UTF-16 code units: U+20000 is two of them, e-acute one.
OVER_LENGTH_KEYS = [
    ("empty", ""),
    ("129-characters", "k" * 129),
    ("129-two-byte-characters", "\u00e9" * 129),
    ("65-four-byte-characters", "\U00020000" * 65),
    ("128-characters-129-utf16-units", "a" * 127 + "\U00020000"),
]
OVER_LENGTH_VALUES = [
    ("257-characters", "v" * 257),
    ("257-two-byte-characters", "\u00e9" * 257),
    ("129-four-byte-characters", "\U00020000" * 129),
]

LONG_S_MESSAGE = "One or more parameter values were invalid"


def key_message(key: str) -> str:
    return f"The Tag Key provided is invalid, Key: {key}"


def value_message(value: str) -> str:
    return f"The Tag Value provided is invalid, Value: {value}"


def prefix_message(key: str) -> str:
    return f"Tag Key cannot be prefixed with aws:, Key: {key}"


def assert_error(call, code: str, message: str) -> None:
    """Assert the call fails with that error code and that exact message."""
    with pytest.raises(ClientError) as exc:
        call()
    err = exc.value.response["Error"]
    assert (err["Code"], err["Message"]) == (code, message), err


def assert_validation(call, expected_message: str) -> None:
    """Assert the call fails with ValidationException and that exact message."""
    assert_error(call, "ValidationException", expected_message)


def assert_create_table_refused(client, expected_message: str, **params) -> None:
    """CreateTable under a new name fails with that message and leaves no table.

    A server that accepts the request still has its table deleted.
    """
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    definition = {
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
    }
    definition.update(params)
    request = {k: v for k, v in definition.items() if v is not None}
    try:
        assert_validation(lambda: client.create_table(TableName=name, **request), expected_message)
        with pytest.raises(ClientError) as exc:
            client.describe_table(TableName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try:
            wait_for_active(client, name)
            client.delete_table(TableName=name)
        except ClientError:
            pass


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
    """128 UTF-16 code units of key and 256 of value are accepted, one more is not."""

    @pytest.mark.parametrize("label,key", OVER_LENGTH_KEYS, ids=[r[0] for r in OVER_LENGTH_KEYS])
    def test_tag_resource_rejects_key(self, dynamodb_client_no_validation, tag_table, label, key):
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": key, "Value": "v"}]
            ),
            key_message(key),
        )

    @pytest.mark.parametrize("label,key", OVER_LENGTH_KEYS, ids=[r[0] for r in OVER_LENGTH_KEYS])
    def test_untag_resource_rejects_key(
        self, dynamodb_client_no_validation, tag_table, label, key
    ):
        assert_validation(
            lambda: dynamodb_client_no_validation.untag_resource(
                ResourceArn=tag_table["arn"], TagKeys=[key]
            ),
            key_message(key),
        )

    @pytest.mark.parametrize(
        "label,value", OVER_LENGTH_VALUES, ids=[r[0] for r in OVER_LENGTH_VALUES]
    )
    def test_tag_resource_rejects_value(
        self, dynamodb_client_no_validation, tag_table, label, value
    ):
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

    @pytest.mark.parametrize("key", ["aw\u017f:foo", "AW\u017f:foo"], ids=["lower", "upper"])
    def test_tag_resource_rejects_a_long_s_prefix(
        self, dynamodb_client_no_validation, tag_table, key
    ):
        """U+017F passes the prefix rule above and is refused by a later check."""
        assert_validation(
            lambda: dynamodb_client_no_validation.tag_resource(
                ResourceArn=tag_table["arn"], Tags=[{"Key": key, "Value": "v"}]
            ),
            LONG_S_MESSAGE,
        )

    def test_untag_resource_accepts_a_long_s_prefix(
        self, dynamodb_client_no_validation, tag_table
    ):
        dynamodb_client_no_validation.untag_resource(
            ResourceArn=tag_table["arn"], TagKeys=["aw\u017f:foo"]
        )


class TestAcceptedGrammar:
    """Every character class the service accepts is stored and read back."""

    def test_accepted_keys_and_values_are_stored(self, dynamodb_client_no_validation):
        """The service applies the tags after TagResource returns. A
        ListTagsOfResource sent at once listed none of them, and DeleteTable
        answered ResourceInUseException ("Table tags are being updated") while
        the update ran (measured 2026-11-20)."""
        client = dynamodb_client_no_validation
        name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
        client.create_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        try:
            wait_for_active(client, name)
            arn = client.describe_table(TableName=name)["Table"]["TableArn"]
            client.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": k, "Value": v} for _, k, v in ACCEPTED_TAGS],
            )
            deadline = time.monotonic() + 60
            while True:
                tags = client.list_tags_of_resource(ResourceArn=arn)["Tags"]
                if len(tags) >= len(ACCEPTED_TAGS) or time.monotonic() > deadline:
                    break
                time.sleep(1)
            stored = {t["Key"]: t["Value"] for t in tags}
            for label, key, value in ACCEPTED_TAGS:
                assert key in stored, label
                assert stored[key] == value, label
        finally:
            deadline = time.monotonic() + 120
            while True:
                try:
                    client.delete_table(TableName=name)
                    break
                except ClientError as e:
                    code = e.response["Error"]["Code"]
                    if code != "ResourceInUseException" or time.monotonic() > deadline:
                        raise
                    time.sleep(2)

    def test_untag_accepts_a_key_in_the_grammar(self, dynamodb_client_no_validation, tag_table):
        """A key within the grammar is accepted even when no such tag is set."""
        dynamodb_client_no_validation.untag_resource(
            ResourceArn=tag_table["arn"],
            TagKeys=["k\u00e9y", "aws", "k" * 128, "\U00020000" * 64],
        )


class TestUntagKeyCharacters:
    """UntagResource applies the same key rules as TagResource."""

    @pytest.mark.parametrize("char", ["!", "#", "\t", "\U0001f600", "\u093e", "\u0870"])
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

    def test_untag_reports_the_first_invalid_key(
        self, dynamodb_client_no_validation, tag_table
    ):
        assert_validation(
            lambda: dynamodb_client_no_validation.untag_resource(
                ResourceArn=tag_table["arn"], TagKeys=["ok", "zk!", "ak#"]
            ),
            key_message("zk!"),
        )


# CreateTable.Tags are stored in the same rows, so the same rules apply. Each
# refusal leaves no table.
CREATE_TABLE_REFUSED_TAGS = [
    ("invalid-key", {"Key": "k!", "Value": "v"}, key_message("k!")),
    ("invalid-value", {"Key": "ok", "Value": "v!"}, value_message("v!")),
    ("reserved-prefix", {"Key": "aws:owner", "Value": "v"}, prefix_message("aws:owner")),
    ("long-s-prefix", {"Key": "aw\u017f:foo", "Value": "v"}, LONG_S_MESSAGE),
    (
        "key-65-four-byte-characters",
        {"Key": "\U00020000" * 65, "Value": "v"},
        key_message("\U00020000" * 65),
    ),
]


class TestCreateTableTags:
    """CreateTable.Tags follow the TagResource rules."""

    @pytest.mark.parametrize(
        "label,tag,expected",
        CREATE_TABLE_REFUSED_TAGS,
        ids=[r[0] for r in CREATE_TABLE_REFUSED_TAGS],
    )
    def test_create_table_rejects_the_tag(
        self, dynamodb_client_no_validation, label, tag, expected
    ):
        assert_create_table_refused(dynamodb_client_no_validation, expected, Tags=[tag])

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
