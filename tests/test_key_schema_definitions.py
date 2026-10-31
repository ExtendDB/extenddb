# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Key schemas whose attributes are missing from AttributeDefinitions.

Amazon DynamoDB, measured on 2026-10-25 and 2026-10-26 at 01:30 UTC
(us-east-1), each request twice, on CreateTable and on an UpdateTable GSI
Create:

- A name defined twice answers ``Attribute Name is duplicated: <name>``,
  naming the first name whose second definition comes first, before every
  key check.
- The table's key schema is checked first, then each LSI's, then each GSI's.
  A schema with more keys than there are definitions answers ``Invalid
  KeySchema: Some index key attribute have no definition``. Otherwise a key
  with no definition answers ``Some index key attributes are not defined in
  AttributeDefinitions`` listing every key of that schema and every
  definition.

The service orders both lists in a way that follows neither the request nor
the alphabet, and ExtendDB lists them in request order, so these tests compare
the lists as sets.

On UpdateTable a definition that redefines the table's own key with another
type is accepted beside a valid Create, and the table keeps its type
(measured 2026-10-25).

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import re

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

INVALID = "Invalid KeySchema: Some index key attribute have no definition"
LISTED = re.compile(
    r"^One or more parameter values were invalid: Some index key attributes are not defined in "
    r"AttributeDefinitions\. Keys: \[([^\]]*)\], AttributeDefinitions: \[([^\]]*)\]$"
)


def defs(spec: str) -> list[dict]:
    return [{"AttributeName": p.split(":")[0], "AttributeType": p.split(":")[1]} for p in spec.split()]


def ks(hash_key: str, range_key: str | None = None) -> list[dict]:
    out = [{"AttributeName": hash_key, "KeyType": "HASH"}]
    if range_key:
        out.append({"AttributeName": range_key, "KeyType": "RANGE"})
    return out


def _message(call) -> str:
    with pytest.raises(ClientError) as exc_info:
        call()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def _listed(message: str) -> tuple[set[str], set[str]]:
    m = LISTED.match(message)
    assert m, message
    return set(m.group(1).split(", ")), set(m.group(2).split(", "))


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _update(client, table, spec, hash_key, range_key=None):
    return lambda: client.update_table(
        TableName=table,
        AttributeDefinitions=defs(spec),
        GlobalSecondaryIndexUpdates=[{"Create": {
            "IndexName": "never-created",
            "KeySchema": ks(hash_key, range_key),
            "Projection": {"ProjectionType": "KEYS_ONLY"},
        }}],
    )


def _create(client, spec, table_keys, gsis=(), lsis=()):
    kwargs = {
        "TableName": "never-created-key-definitions",
        "AttributeDefinitions": defs(spec),
        "KeySchema": table_keys,
        "BillingMode": "PAY_PER_REQUEST",
    }
    if gsis:
        kwargs["GlobalSecondaryIndexes"] = [
            {"IndexName": f"gsi{i}", "KeySchema": k, "Projection": {"ProjectionType": "KEYS_ONLY"}}
            for i, k in enumerate(gsis)
        ]
    if lsis:
        kwargs["LocalSecondaryIndexes"] = [
            {"IndexName": f"lsi{i}", "KeySchema": k, "Projection": {"ProjectionType": "KEYS_ONLY"}}
            for i, k in enumerate(lsis)
        ]
    return lambda: client.create_table(**kwargs)


# UpdateTable GSI Create: (definitions, index hash, index range, keys listed, definitions listed)
UPDATE_LISTED = [
    ("range_missing", "pk:S g:S", "g", "r", {"g", "r"}, {"pk", "g"}),
    ("hash_missing", "pk:S r:S", "g", "r", {"g", "r"}, {"pk", "r"}),
    ("both_missing_two_defs", "pk:S z:S", "g", "r", {"g", "r"}, {"pk", "z"}),
    ("both_missing_three_defs", "pk:S y:S z:S", "g", "r", {"g", "r"}, {"pk", "y", "z"}),
    ("hash_only_missing", "pk:S", "g", None, {"g"}, {"pk"}),
    ("other_types", "pk:S n:N b:B", "n", "x", {"n", "x"}, {"pk", "n", "b"}),
]


@pytest.mark.parametrize(("case", "spec", "h", "r", "keys", "names"), UPDATE_LISTED,
                         ids=[c[0] for c in UPDATE_LISTED])
def test_update_table_lists_every_key_and_definition(dynamodb_client, table, case, spec, h, r, keys, names):
    message = _message(_update(dynamodb_client, table, spec, h, r))
    assert _listed(message) == (keys, names)


@pytest.mark.parametrize(("spec", "h", "r"), [("pk:S", "g", "r"), ("pk:S", "pk", "r")],
                         ids=["both_missing_one_def", "range_missing_one_def"])
def test_update_table_with_fewer_definitions_than_keys(dynamodb_client, table, spec, h, r):
    assert _message(_update(dynamodb_client, table, spec, h, r)) == INVALID


@pytest.mark.parametrize(("spec", "h", "r", "name"), [
    ("h:S h:S", "h", None, "h"),
    ("h:S h:S", "h", "r", "h"),
    ("b:S a:S b:S a:S h:S", "h", "r", "b"),
    ("h:S h:N", "h", "r", "h"),
], ids=["index_key", "and_a_missing_key", "first_repeated", "two_types"])
def test_update_table_refuses_a_name_defined_twice(dynamodb_client, table, spec, h, r, name):
    assert _message(_update(dynamodb_client, table, spec, h, r)) == f"Attribute Name is duplicated: {name}"


# CreateTable: (definitions, table keys, GSIs, LSIs, keys listed or None for the
# key schema sentence, definitions listed)
CREATE = [
    ("gsi_range_missing", "pk:S g:S", ks("pk"), [ks("g", "r")], [], {"g", "r"}, {"pk", "g"}),
    ("gsi_both_missing", "pk:S", ks("pk"), [ks("g", "r")], [], None, None),
    ("table_sort_missing_one_def", "pk:S", ks("pk", "sk"), [], [], None, None),
    ("table_sort_missing", "pk:S x:S", ks("pk", "sk"), [], [], {"pk", "sk"}, {"pk", "x"}),
    ("table_pk_missing", "x:S", ks("pk"), [], [], {"pk"}, {"x"}),
    ("table_before_gsi", "pk:S g:S", ks("pk", "sk"), [ks("g", "r")], [], {"pk", "sk"}, {"pk", "g"}),
    ("table_before_lsi", "pk:S x:S", ks("pk", "sk"), [], [ks("pk", "l")], {"pk", "sk"}, {"pk", "x"}),
    ("lsi_missing", "pk:S sk:S", ks("pk", "sk"), [], [ks("pk", "l")], {"pk", "l"}, {"pk", "sk"}),
    ("lsi_before_gsi", "pk:S sk:S g:S", ks("pk", "sk"), [ks("g", "r")], [ks("pk", "l")],
     {"pk", "l"}, {"pk", "sk", "g"}),
    ("first_gsi_first", "pk:S g:S h:S", ks("pk"), [ks("g", "r"), ks("h", "q")], [],
     {"g", "r"}, {"pk", "g", "h"}),
]


@pytest.mark.parametrize(("case", "spec", "table_keys", "gsis", "lsis", "keys", "names"), CREATE,
                         ids=[c[0] for c in CREATE])
def test_create_table_checks_each_key_schema_in_turn(dynamodb_client, case, spec, table_keys, gsis, lsis,
                                                     keys, names):
    message = _message(_create(dynamodb_client, spec, table_keys, gsis, lsis))
    if keys is None:
        assert message == INVALID
    else:
        assert _listed(message) == (keys, names)


@pytest.mark.parametrize(("spec", "table_keys", "gsis", "name"), [
    ("pk:S pk:S", ks("pk"), [], "pk"),
    ("pk:S pk:N", ks("pk"), [], "pk"),
    ("pk:S pk:S", ks("pk", "sk"), [], "pk"),
    ("pk:S g:S g:S", ks("pk"), [ks("g")], "g"),
    ("b:S a:S b:S a:S pk:S", ks("pk"), [], "b"),
], ids=["table_key", "two_types", "and_a_missing_key", "index_key", "first_repeated"])
def test_create_table_refuses_a_name_defined_twice(dynamodb_client, spec, table_keys, gsis, name):
    message = _message(_create(dynamodb_client, spec, table_keys, gsis))
    assert message == f"Attribute Name is duplicated: {name}"


def test_update_table_keeps_the_type_of_a_redefined_table_key(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.update_table(
            TableName=name,
            AttributeDefinitions=defs("pk:N g:S"),
            GlobalSecondaryIndexUpdates=[{"Create": {
                "IndexName": "by-g",
                "KeySchema": ks("g"),
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }}],
        )
        desc = dynamodb_client.describe_table(TableName=name)["Table"]
        stored = {d["AttributeName"]: d["AttributeType"] for d in desc["AttributeDefinitions"]}
        assert stored == {"pk": "S", "g": "S"}
