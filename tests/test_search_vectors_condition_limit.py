# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""SearchVectors condition count limit and the order of the expression checks.

Measured against Amazon DynamoDB on 2026-10-29 and 2026-10-30 in us-east-1, on
a table whose vector index declares a HASH ``h`` and 18 inline filters ``f1``
to ``f18``. Each request shape was sent twice with the same response both
times.

The limit is 19 comparisons. The 20th gets ``Invalid SearchConditionExpression:
SearchConditionExpression cannot have more than 1 partition key and more than
18 inline filter key comparisons``.

The service validates in two phases:

1. The whole expression: syntax, then each comparison's attribute name
   (undefined ``#alias``, bare reserved word) and value placeholder (undefined
   ``:value``), in written order. OR, NOT and IN are refused here too. A fault
   in this phase gets its own message even in the 20th or 21st comparison.
2. Each comparison in written order: the 20th gets the count message. Before
   it, the comparator, a right side without a colon, a nested path, and a
   repeated attribute get their own messages, in that order.

Unused values are checked after both phases.

For syntax errors, undefined aliases, nested paths and logical operators,
ExtendDB's wording differs from the service's. Those tests assert that the
check, not the count, answers.

Tests post raw SigV4-signed JSON. Against the service, SearchVectors is sent to
the ``search-dynamodb`` host the SDKs use for it. The module skips when the
target backend does not support vector indexes, unless
``EXTENDDB_EXPECT_VECTORS=1``, which makes that a failure.

REQ-TEST-001, REQ-TEST-002, REQ-TEST-003
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid

import boto3
import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

_ENDPOINT_VAR = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ENDPOINT = _ENDPOINT_VAR or f"https://dynamodb.{_REGION}.amazonaws.com/"
# The helpers in test_vector_index_query_scan.py post every operation to one
# endpoint. The service serves SearchVectors from its own host, so this module
# keeps its own copy that routes by operation.
SEARCH_ENDPOINT = _ENDPOINT_VAR or f"https://search-dynamodb.{_REGION}.amazonaws.com/"

PREFIX = "Invalid SearchConditionExpression"
TOO_MANY = (
    "Invalid SearchConditionExpression: SearchConditionExpression cannot have "
    "more than 1 partition key and more than 18 inline filter key comparisons"
)
DUPLICATE = (
    "Invalid SearchConditionExpression: SearchConditionExpression must only "
    "contain one condition per attribute"
)
COMPARATOR = (
    "Invalid SearchConditionExpression: Invalid comparator used in "
    "SearchConditionExpression"
)
RESERVED_BUCKET = (
    "Invalid SearchConditionExpression: Attribute name is a reserved keyword; "
    "reserved keyword: bucket"
)
NOT_IN_SCHEMA = (
    "SearchConditionExpression must not contain any attributes that is not in "
    "SearchSchema. Invalid attribute: x"
)
NESTED = "SearchConditionExpression cannot have conditions on nested attributes"
OPERATOR = "Invalid operator used in SearchConditionExpression"

INLINE = [f"f{i}" for i in range(1, 19)]
NINETEEN = ["h", *INLINE]


def _undefined_value(placeholder: str) -> str:
    return (
        "Invalid SearchConditionExpression: An expression attribute value used in "
        f"expression is not defined; attribute value: {placeholder}"
    )


def _expect_vectors() -> bool | None:
    """``EXTENDDB_EXPECT_VECTORS``: 1 requires vector support, 0 forbids it."""
    value = os.environ.get("EXTENDDB_EXPECT_VECTORS")
    if value is None or not _ENDPOINT_VAR:
        return None
    if value not in ("0", "1"):
        pytest.fail(f"EXTENDDB_EXPECT_VECTORS must be 0 or 1, got {value!r}")
    return value == "1"


def _signed_post(operation: str, body: dict) -> requests.Response:
    """POST ``body`` under the DynamoDB JSON-1.0 protocol, SigV4-signed."""
    url = SEARCH_ENDPOINT if operation == "SearchVectors" else ENDPOINT
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    creds = boto3.Session().get_credentials()
    if creds is not None:
        aws_req = AWSRequest(method="POST", url=url, data=body_bytes, headers=headers)
        SigV4Auth(creds.get_frozen_credentials(), "dynamodb", _REGION).add_auth(aws_req)
        headers = dict(aws_req.headers)
    verify = True if not _ENDPOINT_VAR else not url.startswith("https://")
    return requests.post(url, data=body_bytes, headers=headers, verify=verify, timeout=30)


def _error_message(resp: requests.Response) -> str:
    payload = resp.json()
    return payload.get("message", payload.get("Message", ""))


def _wait_for_vector_table_active(name: str) -> None:
    """Poll until the table and its vector indexes are all ACTIVE.

    The service takes minutes to build a vector index; ExtendDB takes well
    under a second.
    """
    timeout = 180.0 if _ENDPOINT_VAR else 1800.0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = _signed_post("DescribeTable", {"TableName": name})
        if resp.status_code == 200:
            table = resp.json()["Table"]
            if table["TableStatus"] == "ACTIVE" and all(
                vi.get("IndexStatus") == "ACTIVE" for vi in table.get("VectorIndexes", [])
            ):
                return
        time.sleep(0.05 if _ENDPOINT_VAR else 10.0)
    raise TimeoutError(f"table {name} did not become ACTIVE within {timeout}s")


def _delete_table(name: str) -> None:
    """Delete the fixture table, retrying only on connection errors."""
    for attempt in range(3):
        try:
            resp = _signed_post("DeleteTable", {"TableName": name})
        except requests.exceptions.ConnectionError:
            if attempt == 2:
                raise
            time.sleep(5.0)
            continue
        assert resp.status_code == 200, f"DeleteTable {name} failed: {resp.text}"
        return


@pytest.fixture(scope="module")
def search_table():
    """A table whose vector index declares a HASH and 18 inline filters."""
    expected = _expect_vectors()
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    attrs = ["pk", "h", *INLINE]
    resp = _signed_post(
        "CreateTable",
        {
            "TableName": name,
            "AttributeDefinitions": [
                {"AttributeName": a, "AttributeType": "S"} for a in attrs
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {
                    "IndexName": "vidx",
                    "Dimensions": 4,
                    "DistanceFunction": "COSINE",
                    "VectorAttribute": {"AttributeName": "emb"},
                    "SearchSchema": [
                        {"AttributeName": "h", "SearchSchemaElementType": "HASH"},
                        *(
                            {"AttributeName": a, "SearchSchemaElementType": "INLINE_FILTER"}
                            for a in INLINE
                        ),
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        },
    )
    if resp.status_code == 400 and "not supported" in _error_message(resp):
        if expected is True:
            pytest.fail(
                "EXTENDDB_EXPECT_VECTORS=1 but the backend refuses vector indexes: "
                + _error_message(resp)
            )
        pytest.skip("target backend does not support vector indexes")
    assert resp.status_code == 200, f"CreateTable failed: {resp.text}"
    try:
        assert expected is not False, (
            "EXTENDDB_EXPECT_VECTORS=0 but the backend accepted a vector index"
        )
        _wait_for_vector_table_active(name)
        # A target may accept CreateTable while ignoring VectorIndexes, and the
        # wait above then passes with no index to search.
        describe = _signed_post("DescribeTable", {"TableName": name})
        assert describe.status_code == 200, describe.text
        index_names = [
            vi.get("IndexName") for vi in describe.json()["Table"].get("VectorIndexes", [])
        ]
        if "vidx" not in index_names:
            if expected is True:
                pytest.fail("EXTENDDB_EXPECT_VECTORS=1 but the vector index was not created")
            pytest.skip("target accepted CreateTable but did not create the vector index")
        yield name
    finally:
        _delete_table(name)


def _comparisons(attrs: list[str]) -> list[str]:
    return [f"{a} = :v{i}" for i, a in enumerate(attrs)]


def _search_expr(
    table: str,
    comparisons: list[str],
    *,
    undefined: tuple[str, ...] = (),
    unused: tuple[str, ...] = (),
    names: dict[str, str] | None = None,
    joiner: str = " AND ",
) -> requests.Response:
    """Search with ``comparisons`` joined by AND.

    Every ``:vN`` written is defined except those in ``undefined``.
    """
    expr = joiner.join(comparisons)
    values = {
        tok: {"S": f"x{tok[2:]}"}
        for tok in re.split(r"[\s()<>=!,]+", expr)
        if tok.startswith(":v") and tok not in undefined
    }
    values.update({u: {"S": "unused"} for u in unused})
    body = {
        "TableName": table,
        "IndexName": "vidx",
        "SearchVector": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}],
        "TopK": 5,
        "SearchConditionExpression": expr,
        "ExpressionAttributeValues": values,
    }
    if names:
        body["ExpressionAttributeNames"] = names
    return _signed_post("SearchVectors", body)


def _search(table: str, attrs: list[str]) -> requests.Response:
    return _search_expr(table, _comparisons(attrs))


def _base(n: int = 19) -> list[str]:
    return _comparisons(NINETEEN[:n])


def _with(position: int, comparison: str, n: int = 19) -> list[str]:
    """The first ``n`` base comparisons with the one at ``position`` replaced."""
    parts = _base(n)
    parts[position - 1] = comparison
    return parts


def _refused_with(resp: requests.Response, message: str) -> None:
    assert resp.status_code == 400, resp.text
    assert resp.json()["__type"].endswith("ValidationException"), resp.text
    assert _error_message(resp) == message


def _refused_not_for_count(resp: requests.Response, *fragments: str) -> None:
    """Refused by a check other than the count, whose message has ``fragments``."""
    assert resp.status_code == 400, resp.text
    assert resp.json()["__type"].endswith("ValidationException"), resp.text
    message = _error_message(resp)
    assert message != TOO_MANY, resp.text
    for fragment in fragments:
        assert fragment in message, resp.text


class TestSearchConditionLimit:
    """The count limit itself."""

    def test_nineteen_conditions_are_accepted(self, search_table):
        resp = _search(search_table, NINETEEN)
        assert resp.status_code == 200, resp.text

    def test_twentieth_condition_repeating_an_attribute_gets_count_message(
        self, search_table
    ):
        _refused_with(_search(search_table, [*NINETEEN, "f1"]), TOO_MANY)

    def test_twentieth_condition_outside_schema_gets_count_message(self, search_table):
        _refused_with(_search(search_table, [*NINETEEN, "x"]), TOO_MANY)

    def test_twenty_one_conditions_get_count_message(self, search_table):
        _refused_with(_search(search_table, [*NINETEEN, "x", "y"]), TOO_MANY)

    def test_count_is_checked_before_an_early_out_of_schema_attribute(
        self, search_table
    ):
        _refused_with(_search(search_table, ["h", "x", *INLINE]), TOO_MANY)

    def test_out_of_schema_attribute_within_limit_gets_schema_message(
        self, search_table
    ):
        _refused_with(_search(search_table, [*NINETEEN[:-1], "x"]), NOT_IN_SCHEMA)

    def test_repeat_within_limit_gets_duplicate_message(self, search_table):
        _refused_with(_search(search_table, [*NINETEEN[:-1], "f1"]), DUPLICATE)

    def test_twenty_plus_an_unused_value_gets_count_message(self, search_table):
        _refused_with(
            _search_expr(search_table, [*_base(), "x = :v19"], unused=(":unused",)),
            TOO_MANY,
        )

    def test_count_ignores_case_and_spacing(self, search_table):
        lower = [*_base(), "x = :v19"]
        _refused_with(_search_expr(search_table, lower, joiner=" and "), TOO_MANY)
        tight = [c.replace(" = ", "=") for c in lower]
        _refused_with(_search_expr(search_table, tight, joiner="  AND  "), TOO_MANY)
        resp = _search_expr(search_table, _base(), joiner=" and ")
        assert resp.status_code == 200, resp.text


class TestWholeExpressionPhase:
    """Faults that answer before the count, wherever they sit."""

    def test_undefined_value_in_twentieth(self, search_table):
        resp = _search_expr(search_table, [*_base(), "x = :v19"], undefined=(":v19",))
        _refused_with(resp, _undefined_value(":v19"))

    def test_undefined_value_in_twenty_first(self, search_table):
        resp = _search_expr(
            search_table, [*_base(), "x = :v19", "y = :v20"], undefined=(":v20",)
        )
        _refused_with(resp, _undefined_value(":v20"))

    def test_reserved_word_in_twentieth(self, search_table):
        _refused_with(
            _search_expr(search_table, [*_base(), "bucket = :v19"]), RESERVED_BUCKET
        )

    def test_reserved_word_in_twenty_first(self, search_table):
        _refused_with(
            _search_expr(search_table, [*_base(), "x = :v19", "bucket = :v20"]),
            RESERVED_BUCKET,
        )

    def test_reserved_path_element_in_twentieth(self, search_table):
        _refused_with(
            _search_expr(search_table, [*_base(), "a.bucket = :v19"]), RESERVED_BUCKET
        )

    def test_undefined_alias_in_twentieth(self, search_table):
        _refused_not_for_count(
            _search_expr(search_table, [*_base(), "#nope = :v19"]),
            "expression attribute name used in the",
            "#nope",
        )

    def test_undefined_alias_in_twenty_first(self, search_table):
        _refused_not_for_count(
            _search_expr(search_table, [*_base(), "x = :v19", "#nope = :v20"]),
            "expression attribute name used in the",
            "#nope",
        )

    def test_undefined_alias_path_element_in_twentieth(self, search_table):
        _refused_not_for_count(
            _search_expr(search_table, [*_base(), "a.#nope = :v19"]),
            "expression attribute name used in the",
            "#nope",
        )

    @pytest.mark.parametrize(
        "tail",
        [
            pytest.param("", id="19-then-trailing-AND"),
            pytest.param(" AND x = :v19 AND", id="20-then-trailing-AND"),
            pytest.param(" AND x", id="19-then-AND-x"),
            pytest.param(" AND x :v19", id="20th-missing-equals"),
            pytest.param(" AND x =", id="20th-missing-value"),
            pytest.param(" AND x = :v19 junk", id="20th-extra-token"),
            pytest.param(" AND AND", id="19-then-AND-AND"),
            pytest.param(" AND x != :v19", id="20th-not-equals"),
        ],
    )
    def test_syntax_error_past_the_limit(self, search_table, tail):
        expr = " AND ".join(_base()) + (tail or " AND")
        values = {
            tok: {"S": "v"}
            for tok in re.split(r"[\s()<>=!,]+", expr)
            if tok.startswith(":v")
        }
        resp = _signed_post(
            "SearchVectors",
            {
                "TableName": search_table,
                "IndexName": "vidx",
                "SearchVector": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}],
                "TopK": 5,
                "SearchConditionExpression": expr,
                "ExpressionAttributeValues": values,
            },
        )
        _refused_not_for_count(resp, PREFIX)

    def test_syntax_is_checked_before_values(self, search_table):
        expr = " AND ".join(_base(18)) + " AND"
        values = {f":v{i}": {"S": "v"} for i in range(18) if i != 1}
        resp = _signed_post(
            "SearchVectors",
            {
                "TableName": search_table,
                "IndexName": "vidx",
                "SearchVector": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}],
                "TopK": 5,
                "SearchConditionExpression": expr,
                "ExpressionAttributeValues": values,
            },
        )
        _refused_not_for_count(resp, PREFIX)
        assert _error_message(resp) != _undefined_value(":v1"), resp.text

    @pytest.mark.parametrize(
        "comparisons",
        [
            pytest.param(
                [*_base()[:-1], _base()[-1] + " OR x = :v19"], id="OR-before-20th"
            ),
            pytest.param(
                [_base()[0] + " OR " + _base()[1], *_base()[2:], "x = :v19"],
                id="OR-before-2nd",
            ),
            pytest.param([*_base(), "NOT x = :v19"], id="NOT-in-20th"),
            pytest.param([*_base(), "x IN (:v19)"], id="IN-in-20th"),
        ],
    )
    def test_logical_operator_past_the_limit(self, search_table, comparisons):
        _refused_not_for_count(_search_expr(search_table, comparisons), OPERATOR)

    def test_names_and_values_resolve_in_written_order(self, search_table):
        reserved_first = _with(2, "bucket = :v1")
        reserved_first[2] = "f2 = :nope"
        _refused_with(_search_expr(search_table, reserved_first), RESERVED_BUCKET)
        value_first = _with(2, "f1 = :nope")
        value_first[2] = "bucket = :v2"
        _refused_with(_search_expr(search_table, value_first), _undefined_value(":nope"))
        _refused_with(
            _search_expr(search_table, _with(2, "bucket = :nope")), RESERVED_BUCKET
        )


class TestPerComparisonPhase:
    """Faults that the count preempts in the 20th comparison."""

    @pytest.mark.parametrize(
        "comparison,names",
        [
            pytest.param("a.b = :v19", None, id="nested"),
            pytest.param("#nn = :v19", {"#nn": "a.b"}, id="nested-through-alias"),
            pytest.param("x = v19", None, id="value-without-colon"),
            pytest.param("x < :v19", None, id="less-than"),
            pytest.param("x <> :v19", None, id="not-equal-comparator"),
            pytest.param("x <= :v19", None, id="less-or-equal"),
            pytest.param("x = :v0", None, id="placeholder-reused"),
        ],
    )
    def test_twentieth_gets_count_message(self, search_table, comparison, names):
        _refused_with(
            _search_expr(search_table, [*_base(), comparison], names=names), TOO_MANY
        )

    def test_repeat_at_third_of_twenty_gets_duplicate_message(self, search_table):
        _refused_with(
            _search_expr(search_table, [*_with(3, "f1 = :v2"), "x = :v19"]), DUPLICATE
        )

    def test_less_than_at_second_gets_comparator_message(self, search_table):
        _refused_with(
            _search_expr(search_table, [*_with(2, "f1 < :v1"), "x = :v19"]), COMPARATOR
        )
        _refused_with(_search_expr(search_table, _with(2, "f1 < :v1")), COMPARATOR)

    def test_nested_at_second_gets_nested_message(self, search_table):
        _refused_not_for_count(
            _search_expr(search_table, [*_with(2, "a.b = :v1"), "x = :v19"]), NESTED
        )

    def test_comparator_is_checked_before_nested_and_repeat(self, search_table):
        _refused_with(_search_expr(search_table, _with(2, "a.b < :v1")), COMPARATOR)
        _refused_with(_search_expr(search_table, _with(3, "f1 < :v2")), COMPARATOR)
        nested_then_less = _with(2, "a.b = :v1")
        nested_then_less[2] = "f2 < :v2"
        _refused_not_for_count(_search_expr(search_table, nested_then_less), NESTED)

    def test_value_shape_is_checked_before_nested_and_repeat(self, search_table):
        for comparisons in (_with(2, "a.b = v1"), _with(3, "f1 = v2")):
            resp = _search_expr(search_table, comparisons)
            _refused_not_for_count(resp, PREFIX)
            assert _error_message(resp) not in (NESTED, DUPLICATE), resp.text
