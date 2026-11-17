# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""SearchVectors with the extended SearchConditionExpression grammar.

For an account the grammar is enabled for, a search condition takes ``<>``,
``<``, ``<=``, ``>``, ``>=``, BETWEEN, IN, OR, NOT, parentheses,
``attribute_exists`` and ``attribute_not_exists``, and more than one
condition on an attribute, up to 50 conditions.

None of the expected responses here has been measured against the live
service yet. Each one is a prediction of the service's answer, and a live run
on an enabled account confirms or corrects it before these tests are relied
on.

Against ExtendDB the module turns the grammar on for every account through the
management API, as the admin in ``EXTENDDB_ADMIN_USER`` and
``EXTENDDB_ADMIN_PASSWORD``, and restores the previous value afterwards. A
build without the feature refuses the setting. The module then probes one
inequality search, and skips when the target answers it with the
equality-only message, which is the answer of a build without the feature and
of an account the service has not enabled.

On a backend without vector search, every SearchVectors request is refused by
the backend, the extended grammar included; ``TestBackendWithoutVectors``
pins that.

Tests post raw SigV4-signed JSON. Against the service, SearchVectors is sent to
the ``search-dynamodb`` host the SDKs use for it.
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
SEARCH_ENDPOINT = _ENDPOINT_VAR or f"https://search-dynamodb.{_REGION}.amazonaws.com/"
SETTING = "search_inequality_filters"

P = "Invalid SearchConditionExpression: "
EQUALITY_ONLY = P + "Invalid comparator used in SearchConditionExpression"
NOT_SUPPORTED = "SearchVectors is not supported by this storage backend"
HASH_NOT_EQUALS = (
    "SearchConditionExpression must use = on the HASH attribute, not inside OR or "
    "NOT; HASH attribute: Country"
)
HASH_TWICE = (
    "SearchConditionExpression must contain only one condition on the HASH "
    "attribute; to search more than one HASH value, make one SearchVectors call "
    "for each value"
)
HASH_MISSING = (
    "SearchConditionExpression must have all HASH attributes in configured SearchSchema"
)


def _number_only(operator: str, attribute: str, attribute_type: str) -> str:
    return (
        "SearchConditionExpression allows this operator only on Number (N) "
        f"attributes; operator: {operator}, attribute: {attribute}, attribute type: "
        f"{attribute_type}"
    )


def _not_in_schema(attribute: str) -> str:
    return (
        "SearchConditionExpression must not contain any attributes that is not in "
        f"SearchSchema. Invalid attribute: {attribute}"
    )


def _wrong_type(attribute: str, given: str, declared: str) -> str:
    return (
        f"Type of '{attribute}' attribute in SearchConditionExpression ({given}) does "
        f"not match type in search schema ({declared})"
    )


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


def _message(resp: requests.Response) -> str:
    payload = resp.json()
    return payload.get("message", payload.get("Message", ""))


def _settings_api():
    """The settings URL and admin credentials, or ``None`` against the service."""
    password = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not _ENDPOINT_VAR or not password:
        return None
    user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip() or "admin"
    return f"{_ENDPOINT_VAR.rstrip('/')}/management/settings/{SETTING}", (user, password)


def _set_setting(value: str | None) -> bool:
    """Write the setting; ``False`` when the target cannot take it."""
    api = _settings_api()
    if api is None:
        return False
    url, auth = api
    if value is None:
        value = "off"
    resp = requests.put(url, auth=auth, json={"value": value}, timeout=30, verify=False)
    return resp.status_code == 200


def _previous_setting() -> str | None:
    api = _settings_api()
    if api is None:
        return None
    url, auth = api
    resp = requests.get(url, auth=auth, timeout=30, verify=False)
    return resp.json().get("value") if resp.status_code == 200 else None


# id: (Country, Category, Price, Tag); None is an absent attribute.
ITEMS = {
    "i1": ("US", "a", "5", "AQ=="),
    "i2": ("US", "b", "10", "Ag=="),
    "i3": ("US", "c", "15", None),
    "i4": ("US", "a", None, None),
    "i5": ("US", None, "-2.5", None),
    "i6": ("US", "a", "100", None),
    "i7": ("US", "b", "1E2", None),
    "i8": ("CA", "a", "5", None),
    "i9": ("US", "c", "99999999999999999999999999999999999999", None),
    "i10": ("US", "b", "0.1", None),
}

VALUES = {
    ":us": {"S": "US"},
    ":ca": {"S": "CA"},
    ":a": {"S": "a"},
    ":b": {"S": "b"},
    ":p0": {"N": "0"},
    ":p5": {"N": "5"},
    ":p10": {"N": "10"},
    ":p15": {"N": "15"},
    ":p100": {"N": "100"},
    ":big": {"N": "99999999999999999999999999999999999998"},
    ":t1": {"B": "AQ=="},
    ":yes": {"BOOL": True},
}

H = "Country = :us AND "
US = {k for k, v in ITEMS.items() if v[0] == "US"}


def _wait_active(name: str) -> None:
    deadline = time.monotonic() + (180.0 if _ENDPOINT_VAR else 1800.0)
    while time.monotonic() < deadline:
        resp = _signed_post("DescribeTable", {"TableName": name})
        if resp.status_code == 200:
            table = resp.json()["Table"]
            if table["TableStatus"] == "ACTIVE" and all(
                vi.get("IndexStatus") == "ACTIVE" for vi in table.get("VectorIndexes", [])
            ):
                return
        time.sleep(0.05 if _ENDPOINT_VAR else 10.0)
    raise TimeoutError(f"table {name} did not become ACTIVE")


def _search(table: str, expr: str, top_k: int = 100, names=None) -> requests.Response:
    used = set(re.findall(r":[A-Za-z0-9_]+", expr))
    body = {
        "TableName": table,
        "IndexName": "vix",
        "SearchVector": [{"N": "1"}, {"N": "0"}, {"N": "0"}, {"N": "0"}],
        "TopK": top_k,
        "SearchConditionExpression": expr,
        "ExpressionAttributeValues": {k: v for k, v in VALUES.items() if k in used},
    }
    if names:
        body["ExpressionAttributeNames"] = names
    return _signed_post("SearchVectors", body)


def _ids(resp: requests.Response) -> set[str]:
    assert resp.status_code == 200, resp.text
    return {r["Item"]["id"]["S"] for r in resp.json()["SearchResults"]}


def _refused(resp: requests.Response, message: str) -> None:
    assert resp.status_code == 400, resp.text
    assert resp.json()["__type"].endswith("ValidationException"), resp.text
    assert _message(resp) == message


@pytest.fixture(scope="module")
def grammar_table():
    """Table with HASH Country (S), inline filters Category (S), Price (N), Tag (B)."""
    previous = _previous_setting()
    managed = _set_setting("all")
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    created = False
    try:
        resp = _signed_post(
            "CreateTable",
            {
                "TableName": name,
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"},
                    {"AttributeName": "Country", "AttributeType": "S"},
                    {"AttributeName": "Category", "AttributeType": "S"},
                    {"AttributeName": "Price", "AttributeType": "N"},
                    {"AttributeName": "Tag", "AttributeType": "B"},
                ],
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "VectorIndexes": [
                    {
                        "IndexName": "vix",
                        "Dimensions": 4,
                        "DistanceFunction": "COSINE",
                        "VectorAttribute": {"AttributeName": "emb"},
                        "SearchSchema": [
                            {"AttributeName": "Country", "SearchSchemaElementType": "HASH"},
                            {"AttributeName": "Category", "SearchSchemaElementType": "INLINE_FILTER"},
                            {"AttributeName": "Price", "SearchSchemaElementType": "INLINE_FILTER"},
                            {"AttributeName": "Tag", "SearchSchemaElementType": "INLINE_FILTER"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
            },
        )
        if resp.status_code == 400 and "not supported" in _message(resp):
            pytest.skip("target backend does not support vector indexes")
        assert resp.status_code == 200, resp.text
        created = True
        _wait_active(name)
        for rank, (item_id, (country, category, price, tag)) in enumerate(ITEMS.items()):
            item = {
                "id": {"S": item_id},
                "Country": {"S": country},
                # distinct distances from the query, nearest first in id order
                "emb": {"L": [{"N": "1"}, {"N": str(rank / 10)}, {"N": "0"}, {"N": "0"}]},
            }
            if category is not None:
                item["Category"] = {"S": category}
            if price is not None:
                item["Price"] = {"N": price}
            if tag is not None:
                item["Tag"] = {"B": tag}
            put = _signed_post("PutItem", {"TableName": name, "Item": item})
            assert put.status_code == 200, put.text
        probe = _search(name, H + "Price > :p5")
        if probe.status_code == 400 and _message(probe) == EQUALITY_ONLY:
            pytest.skip("the extended grammar is off for this account")
        # Vector index maintenance may lag the write by the propagation delay.
        deadline = time.monotonic() + 30
        while _ids(_search(name, H + "Price <> :p0")) != US:
            if time.monotonic() > deadline:
                raise TimeoutError("items did not reach the vector index")
            time.sleep(0.1)
        yield name
    finally:
        if created:
            _signed_post("DeleteTable", {"TableName": name})
        if managed:
            _set_setting(previous)




class TestOperators:
    def test_each_comparator_on_a_number(self, grammar_table):
        assert _ids(_search(grammar_table, H + "Price = :p10")) == {"i2"}
        assert _ids(_search(grammar_table, H + "Price < :p10")) == {"i1", "i5", "i10"}
        assert _ids(_search(grammar_table, H + "Price <= :p10")) == {"i1", "i2", "i5", "i10"}
        assert _ids(_search(grammar_table, H + "Price > :p10")) == {"i3", "i6", "i7", "i9"}
        assert _ids(_search(grammar_table, H + "Price >= :p10")) == {"i2", "i3", "i6", "i7", "i9"}
        assert _ids(_search(grammar_table, H + ":p10 > Price")) == {"i1", "i5", "i10"}

    def test_ordering_on_a_string_or_binary_is_refused(self, grammar_table):
        for op in ("<", "<=", ">", ">="):
            _refused(
                _search(grammar_table, H + f"Category {op} :a"),
                _number_only(op, "Category", "S"),
            )
        _refused(_search(grammar_table, H + "Tag < :t1"), _number_only("<", "Tag", "B"))

    def test_not_equal_and_equal_on_strings_and_binaries(self, grammar_table):
        assert _ids(_search(grammar_table, H + "Category = :a")) == {"i1", "i4", "i6"}
        assert _ids(_search(grammar_table, H + "Tag = :t1")) == {"i1"}
        assert "i1" not in _ids(_search(grammar_table, H + "Tag <> :t1"))

    def test_a_range_on_one_attribute(self, grammar_table):
        assert _ids(_search(grammar_table, H + "Price > :p5 AND Price < :p15")) == {"i2"}
        assert _ids(_search(grammar_table, H + "Price >= :p5 AND Price = :p5")) == {"i1"}
        assert _ids(_search(grammar_table, H + "Price = :p5 AND Price = :p10")) == set()


class TestEvaluation:
    """Not yet measured: a missing attribute, ``1E2`` against ``100``, 38 digits."""

    def test_a_missing_attribute_under_not_equal_and_not(self, grammar_table):
        assert "i4" in _ids(_search(grammar_table, H + "Price <> :p10"))
        assert "i4" in _ids(_search(grammar_table, H + "NOT (Price = :p10)"))
        assert "i5" in _ids(_search(grammar_table, H + "Category <> :a"))
        assert "i4" not in _ids(_search(grammar_table, H + "Price > :p0"))

    def test_existence_functions(self, grammar_table):
        assert _ids(_search(grammar_table, H + "attribute_not_exists(Price)")) == {"i4"}
        assert _ids(_search(grammar_table, H + "attribute_exists(Price)")) == US - {"i4"}

    def test_numbers_compare_as_decimals(self, grammar_table):
        assert _ids(_search(grammar_table, H + "Price = :p100")) == {"i6", "i7"}
        assert _ids(_search(grammar_table, H + "Price > :big")) == {"i9"}
        assert _ids(_search(grammar_table, H + "Price < :p0")) == {"i5"}
        assert _ids(_search(grammar_table, H + "Price > :p0 AND Price < :p5")) == {"i10"}

    def test_the_hash_value_scopes_the_search(self, grammar_table):
        assert _ids(_search(grammar_table, "Country = :ca AND Price = :p5")) == {"i8"}

    def test_the_filter_applies_before_top_k(self, grammar_table):
        resp = _search(grammar_table, H + "Price > :p5", top_k=2)
        assert [r["Item"]["id"]["S"] for r in resp.json()["SearchResults"]] == ["i2", "i3"]


class TestLogic:
    def test_or_not_and_parentheses(self, grammar_table):
        assert _ids(_search(grammar_table, H + "(Price = :p5 OR Price = :p10)")) == {"i1", "i2"}
        assert _ids(_search(grammar_table, H + "NOT NOT Price = :p5")) == {"i1"}
        assert _ids(
            _search(grammar_table, H + "(Category = :a OR (Price > :p5 AND NOT Price = :p15))")
        ) == {"i1", "i2", "i4", "i6", "i7", "i9"}

    def test_keywords_in_lower_case(self, grammar_table):
        assert "i1" in _ids(
            _search(grammar_table, "Country = :us and (Price = :p5 or not Price = :p10)")
        )

    def test_in_and_between(self, grammar_table):
        assert _ids(_search(grammar_table, H + "Price IN (:p5, :p10)")) == {"i1", "i2"}
        assert _ids(_search(grammar_table, H + "Category IN (:a, :b)")) == {
            "i1", "i2", "i4", "i6", "i7", "i10"
        }
        assert _ids(_search(grammar_table, H + "Price BETWEEN :p5 AND :p10")) == {"i1", "i2"}
        assert _ids(_search(grammar_table, H + "Price BETWEEN :p5 AND :p5")) == {"i1"}
        assert "i1" not in _ids(_search(grammar_table, H + "NOT Price BETWEEN :p5 AND :p10"))


class TestRefusals:
    def test_hash_rules(self, grammar_table):
        _refused(_search(grammar_table, "Category = :a"), HASH_MISSING)
        _refused(_search(grammar_table, "Country <> :us"), HASH_NOT_EQUALS)
        _refused(_search(grammar_table, "Country = :us AND Country > :ca"), HASH_NOT_EQUALS)
        _refused(_search(grammar_table, "Country = :us AND Country = :ca"), HASH_TWICE)
        _refused(_search(grammar_table, "Country IN (:us)"), HASH_NOT_EQUALS)
        _refused(_search(grammar_table, "Country = :us OR Price = :p5"), HASH_NOT_EQUALS)
        _refused(_search(grammar_table, "Country > :ca"), HASH_NOT_EQUALS)
        _refused(
            _search(
                grammar_table,
                "(Country = :us AND Price = :p5) OR (Country = :us AND Price = :p10)",
            ),
            HASH_NOT_EQUALS,
        )
        _refused(_search(grammar_table, "Country = :p5"), _wrong_type("Country", "N", "S"))

    def test_grammar_rules(self, grammar_table):
        _refused(
            _search(grammar_table, H + "Category IN (:a, Price)"),
            P + "The IN operator list must contain only expression attribute values "
            "(:value), not attribute names",
        )
        _refused(
            _search(grammar_table, H + "begins_with(Category, :a)"),
            P + "The function is not allowed in a search condition; function: begins_with",
        )
        _refused(
            _search(grammar_table, H + "attribute_exists(Info.Price)"),
            P + "SearchConditionExpression cannot have conditions on nested attributes",
        )
        _refused(
            _search(grammar_table, H + "Price > :yes"),
            P + "Incorrect operand type for operator or function; operator or "
            "function: >, operand type: BOOL",
        )
        _refused(
            _search(grammar_table, H + "Price BETWEEN :p10 AND :p5"),
            P + "The BETWEEN operator requires upper bound to be greater than or equal "
            "to lower bound; lower bound operand: AttributeValue: {N:10}, upper bound "
            "operand: AttributeValue: {N:5}",
        )
        _refused(
            _search(grammar_table, H + "Price = "),
            P + 'Syntax error; token: "<EOF>", near: "= "',
        )

    def test_schema_checks_in_written_order(self, grammar_table):
        _refused(_search(grammar_table, H + "Color = :a AND Category > :a"), _not_in_schema("Color"))
        _refused(
            _search(grammar_table, H + "Category > :a AND Color = :a"),
            _number_only(">", "Category", "S"),
        )
        _refused(
            _search(grammar_table, H + "Category > :p5"), _number_only(">", "Category", "S")
        )
        _refused(_search(grammar_table, H + "Price = :a"), _wrong_type("Price", "S", "N"))
        _refused(
            _search(grammar_table, H + "Category IN (:a, :p5)"),
            _wrong_type("Category", "N", "S"),
        )
        _refused(
            _search(grammar_table, H + "#c > :a", names={"#c": "Category"}),
            _number_only(">", "Category", "S"),
        )

    def test_the_condition_limit(self, grammar_table):
        conditions = [f"Price > :p{i % 2 * 5}" for i in range(49)]
        assert _search(grammar_table, H + " AND ".join(conditions)).status_code == 200
        _refused(
            _search(grammar_table, H + " AND ".join([*conditions, "Price > :p0"])),
            P + "The expression contains too many conditions; number of conditions: "
            "51, maximum: 50",
        )


class TestBackendWithoutVectors:
    def test_the_backend_refuses_before_the_grammar_runs(self):
        """On a backend without vector search the refusal is the backend's."""
        if not _ENDPOINT_VAR:
            pytest.skip("requires EXTENDDB_TEST_ENDPOINT")
        previous = _previous_setting()
        managed = _set_setting("all")
        try:
            resp = _search(f"extenddb-test-{uuid.uuid4().hex[:12]}", H + "Price > :p5")
            if resp.status_code == 400 and _message(resp) == NOT_SUPPORTED:
                return
            pytest.skip("target backend supports vector search")
        finally:
            if managed:
                _set_setting(previous)
