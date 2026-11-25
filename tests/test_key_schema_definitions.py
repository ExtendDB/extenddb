# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Key schemas whose attributes are missing from AttributeDefinitions.

Amazon DynamoDB, measured on 2026-10-25, 2026-10-26 and 2026-11-25
(us-east-1), each request twice, on CreateTable and on an UpdateTable GSI
Create:

- The keySchema length constraint comes before the key schema rules below,
  and on CreateTable so does a table billing fault.
- Then the table's key schema, or the created index's, against the
  definitions: more keys than there are definitions answers ``Invalid
  KeySchema: Some index key attribute have no definition``, and a HASH and a
  RANGE element on one name answers ``Both the Hash Key and the Range Key
  element in the KeySchema have the same name``.
- Then a name defined twice answers ``Attribute Name is duplicated: <name>``,
  naming the name whose second definition comes first.
- On CreateTable each key schema follows in turn, the table's, each LSI's and
  each GSI's: more keys than definitions answers ``Invalid KeySchema``, and a
  key with no definition answers ``Some index key attributes are not defined
  in AttributeDefinitions`` listing every key of that schema and every
  definition. The service checks an index's definitions before its shape, and
  ExtendDB checks every shape first. No row here has both faults in one index.

The service lists both in the order of a Java HashMap, which
crates/core/src/validation/key_definitions.rs models, so the rows compare the
whole message.

On UpdateTable a definition that redefines the table's own key with another
type is accepted beside a valid Create, and the table keeps its type
(measured 2026-10-25).

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

NEVER_CREATED = "never-created-key-definitions"
INVALID = "Invalid KeySchema: Some index key attribute have no definition"
SAME_NAME = "Both the Hash Key and the Range Key element in the KeySchema have the same name"


def listed(keys: str, names: str) -> str:
    return ("One or more parameter values were invalid: Some index key attributes are not defined in "
            f"AttributeDefinitions. Keys: [{keys}], AttributeDefinitions: [{names}]")


def defs(spec: str) -> list[dict]:
    return [{"AttributeName": p.split(":")[0], "AttributeType": p.split(":")[1]} for p in spec.split()]


def ks(hash_key: str, range_key: str | None = None) -> list[dict]:
    out = [{"AttributeName": hash_key, "KeyType": "HASH"}]
    if range_key:
        out.append({"AttributeName": range_key, "KeyType": "RANGE"})
    return out


def keys(spec: str) -> list[dict]:
    """A key schema in any order, as ``name:HASH name:RANGE``."""
    return [{"AttributeName": p.split(":")[0], "KeyType": p.split(":")[1]} for p in spec.split()]


def _message(call) -> str:
    with pytest.raises(ClientError) as exc_info:
        call()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _update(client, table, spec, key_schema):
    return lambda: client.update_table(
        TableName=table,
        AttributeDefinitions=defs(spec),
        GlobalSecondaryIndexUpdates=[{"Create": {
            "IndexName": "never-created",
            "KeySchema": key_schema,
            "Projection": {"ProjectionType": "KEYS_ONLY"},
        }}],
    )


def _create(client, spec, table_keys, gsis=(), lsis=()):
    kwargs = {
        "TableName": NEVER_CREATED,
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


# UpdateTable GSI Create: (definitions, index hash, index range, message)
UPDATE_LISTED = [
    ("range_missing", "pk:S g:S", "g", "r", listed("r, g", "pk, g")),
    ("hash_missing", "pk:S r:S", "g", "r", listed("r, g", "r, pk")),
    ("both_missing_two_defs", "pk:S z:S", "g", "r", listed("r, g", "z, pk")),
    ("both_missing_three_defs", "pk:S y:S z:S", "g", "r", listed("r, g", "y, z, pk")),
    ("hash_only_missing", "pk:S", "g", None, listed("g", "pk")),
    ("other_types", "pk:S n:N b:B", "n", "x", listed("x, n", "n, b, pk")),
    ("service_order", "pk:S x:S", "g", "r", listed("r, g", "x, pk")),
]


@pytest.mark.parametrize(("case", "spec", "h", "r", "expected"), UPDATE_LISTED,
                         ids=[c[0] for c in UPDATE_LISTED])
def test_update_table_lists_every_key_and_definition(dynamodb_client, table, case, spec, h, r, expected):
    assert _message(_update(dynamodb_client, table, spec, ks(h, r))) == expected


@pytest.mark.parametrize(("spec", "h", "r"), [("pk:S", "g", "r"), ("pk:S", "pk", "r")],
                         ids=["both_missing_one_def", "range_missing_one_def"])
def test_update_table_with_fewer_definitions_than_keys(dynamodb_client, table, spec, h, r):
    assert _message(_update(dynamodb_client, table, spec, ks(h, r))) == INVALID


@pytest.mark.parametrize(("spec", "h", "r", "name"), [
    ("h:S h:S", "h", None, "h"),
    ("h:S h:S", "h", "r", "h"),
    ("b:S a:S b:S a:S h:S", "h", "r", "b"),
    ("h:S h:N", "h", "r", "h"),
], ids=["index_key", "and_a_missing_key", "first_repeated", "two_types"])
def test_update_table_refuses_a_name_defined_twice(dynamodb_client, table, spec, h, r, name):
    message = _message(_update(dynamodb_client, table, spec, ks(h, r)))
    assert message == f"Attribute Name is duplicated: {name}"


# UpdateTable GSI Create on a table name that does not exist, where two rules
# apply: (definitions, index key schema, message). The definition count and the
# HASH and RANGE names come before a name defined twice.
UPDATE_ORDER = [
    ("count_before_duplicate", "g:S g:S", "g:HASH x:RANGE y:RANGE", INVALID),
    ("count_before_duplicate_other_names", "a:S a:S", "a:HASH b:RANGE c:RANGE", INVALID),
    ("same_name_before_duplicate", "g:S g:S", "g:HASH g:RANGE", SAME_NAME),
    ("count_before_same_name", "g:S", "g:HASH g:RANGE", INVALID),
    ("same_name", "g:S x:S", "g:HASH g:RANGE", SAME_NAME),
]


@pytest.mark.parametrize(("case", "spec", "key_spec", "expected"), UPDATE_ORDER,
                         ids=[c[0] for c in UPDATE_ORDER])
def test_update_table_checks_in_the_service_order(dynamodb_client, case, spec, key_spec, expected):
    assert _message(_update(dynamodb_client, NEVER_CREATED, spec, keys(key_spec))) == expected


# CreateTable: (definitions, table keys, GSIs, LSIs, message)
CREATE = [
    ("gsi_range_missing", "pk:S g:S", ks("pk"), [ks("g", "r")], [], listed("r, g", "pk, g")),
    ("gsi_both_missing", "pk:S", ks("pk"), [ks("g", "r")], [], INVALID),
    ("table_sort_missing_one_def", "pk:S", ks("pk", "sk"), [], [], INVALID),
    ("table_sort_missing", "pk:S x:S", ks("pk", "sk"), [], [], listed("sk, pk", "x, pk")),
    ("table_pk_missing", "x:S", ks("pk"), [], [], listed("pk", "x")),
    ("table_before_gsi", "pk:S g:S", ks("pk", "sk"), [ks("g", "r")], [], listed("sk, pk", "pk, g")),
    ("table_before_lsi", "pk:S x:S", ks("pk", "sk"), [], [ks("pk", "l")], listed("sk, pk", "x, pk")),
    ("lsi_missing", "pk:S sk:S", ks("pk", "sk"), [], [ks("pk", "l")], listed("l, pk", "sk, pk")),
    ("lsi_before_gsi", "pk:S sk:S g:S", ks("pk", "sk"), [ks("g", "r")], [ks("pk", "l")],
     listed("l, pk", "sk, pk, g")),
    ("first_gsi_first", "pk:S g:S h:S", ks("pk"), [ks("g", "r"), ks("h", "q")], [],
     listed("r, g", "h, pk, g")),
]


@pytest.mark.parametrize(("case", "spec", "table_keys", "gsis", "lsis", "expected"), CREATE,
                         ids=[c[0] for c in CREATE])
def test_create_table_checks_each_key_schema_in_turn(dynamodb_client, case, spec, table_keys, gsis, lsis,
                                                     expected):
    assert _message(_create(dynamodb_client, spec, table_keys, gsis, lsis)) == expected


KEY_SCHEMA_LENGTH = ("1 validation error detected: Value '[KeySchemaElement(attributeName=a, keyType={}), "
                     "KeySchemaElement(attributeName=b, keyType={}), KeySchemaElement(attributeName=c, "
                     "keyType=RANGE)]' at 'keySchema' failed to satisfy constraint: Member must have length "
                     "less than or equal to 2")

# CreateTable where two rules apply: (definitions, table keys, GSIs, LSIs,
# message). The keySchema length comes first, then the definition count, then
# the HASH and RANGE names, then a name defined twice, then the key schema
# shapes.
CREATE_ORDER = [
    ("length_before_count", "a:S b:S", keys("a:HASH b:RANGE c:RANGE"), [], [],
     KEY_SCHEMA_LENGTH.format("HASH", "RANGE")),
    ("length_before_shape", "a:S b:S c:S", keys("a:RANGE b:HASH c:RANGE"), [], [],
     KEY_SCHEMA_LENGTH.format("RANGE", "HASH")),
    ("count_before_shape", "pk:S", keys("sk:RANGE pk:HASH"), [], [], INVALID),
    ("count_before_same_name", "pk:S", keys("pk:HASH pk:RANGE"), [], [], INVALID),
    ("same_name", "pk:S x:S", keys("pk:HASH pk:RANGE"), [], [], SAME_NAME),
    ("same_name_before_duplicate", "pk:S pk:S", keys("pk:HASH pk:RANGE"), [], [], SAME_NAME),
    ("duplicate_before_table_hash_first", "pk:S sk:S sk:S", keys("sk:RANGE pk:HASH"), [], [],
     "Attribute Name is duplicated: sk"),
    ("duplicate_before_table_range_second", "pk:S sk:S sk:S", keys("pk:HASH sk:HASH"), [], [],
     "Attribute Name is duplicated: sk"),
    ("duplicate_before_gsi_shape", "pk:S g:S g:S r:S", ks("pk"), [keys("r:RANGE g:HASH")], [],
     "Attribute Name is duplicated: g"),
    ("duplicate_before_lsi_shape", "pk:S sk:S l:S l:S", ks("pk", "sk"), [], [keys("l:RANGE pk:HASH")],
     "Attribute Name is duplicated: l"),
]


@pytest.mark.parametrize(("case", "spec", "table_keys", "gsis", "lsis", "expected"), CREATE_ORDER,
                         ids=[c[0] for c in CREATE_ORDER])
def test_create_table_checks_in_the_service_order(dynamodb_client, case, spec, table_keys, gsis, lsis,
                                                  expected):
    assert _message(_create(dynamodb_client, spec, table_keys, gsis, lsis)) == expected


BILLING = ("One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits "
           "can be specified when BillingMode is PAY_PER_REQUEST")


@pytest.mark.parametrize(("spec", "table_keys"), [
    ("pk:S pk:S", ks("pk")),
    ("pk:S x:S", ks("pk", "sk")),
    ("pk:S", ks("pk", "sk")),
    ("pk:S sk:S", keys("sk:RANGE pk:HASH")),
], ids=["duplicate", "missing_key", "fewer_definitions", "range_first"])
def test_create_table_reports_a_billing_fault_first(dynamodb_client, spec, table_keys):
    def call():
        dynamodb_client.create_table(
            TableName=NEVER_CREATED, AttributeDefinitions=defs(spec), KeySchema=table_keys,
            BillingMode="PAY_PER_REQUEST", ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1})
    assert _message(call) == BILLING


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


def _wait_for_index(client, table, index, timeout=120.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_table(TableName=table)["Table"]
        status = {i["IndexName"]: i["IndexStatus"] for i in desc.get("GlobalSecondaryIndexes", [])}
        if desc["TableStatus"] == "ACTIVE" and status.get(index) == "ACTIVE":
            return
        time.sleep(0.5)
    raise TimeoutError(f"{index} on {table} not ACTIVE after {timeout} s")


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
        # The table cannot be deleted while the index is being created.
        _wait_for_index(dynamodb_client, name, "by-g")
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "a"}, "g": {"S": "b"}})
