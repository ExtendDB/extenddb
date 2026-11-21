# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""CreateTable refusals of a malformed table or index KeySchema, a local index
fault, an unused attribute definition and a PROVISIONED table without
ProvisionedThroughput, with the texts the service gives.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError


def _key_schema(spec: str) -> list[dict]:
    """``"pk HASH, sk RANGE"`` as a KeySchema."""
    return [{"AttributeName": n, "KeyType": t} for n, t in (e.split() for e in spec.split(", "))]


def _too_long(spec: str, limit: int) -> str:
    """The keySchema length refusal, its elements written as the service writes them."""
    elements = ", ".join(
        f"KeySchemaElement(attributeName={n}, keyType={t})" for n, t in (e.split() for e in spec.split(", "))
    )
    return (
        f"1 validation error detected: Value '[{elements}]' at 'keySchema' failed to satisfy "
        f"constraint: Member must have length less than or equal to {limit}"
    )


_INVALID = "One or more parameter values were invalid: "
_FIRST_NOT_HASH = "Invalid KeySchema: The first KeySchemaElement is not a HASH key type"
_HASH_BEFORE_RANGE = f"{_INVALID}All HASH key attributes must precede RANGE key attributes in the KeySchema"
_UNUSED = (
    f"{_INVALID}Number of attributes in KeySchema does not exactly match number of attributes "
    "defined in AttributeDefinitions"
)
_PK_AND_UNUSED = {"AttributeDefinitions": [{"AttributeName": n, "AttributeType": "S"} for n in ("pk", "unused")]}
_NINE = "h0 HASH, h1 HASH, h2 HASH, h3 HASH, r0 RANGE, r1 RANGE, r2 RANGE, r3 RANGE, r4 RANGE"
_PK_SK = "pk HASH, sk RANGE"

# CreateTable faults the service refuses with ValidationException and these texts
# (us-east-1, measured 13 and 14 Nov 2026). Each row: an id, the table KeySchema,
# the GSIs and the LSIs as (name, KeySchema) pairs, the other request members, and
# the message. The table is on demand and every key attribute is defined as S
# unless the row says otherwise.
CREATE_TABLE_REFUSALS = [
    ("table-first-range", "sk RANGE, pk HASH", [], [], {}, _FIRST_NOT_HASH),
    ("gsi-first-range", "pk HASH", [("idx", "b RANGE, a HASH")], [], {}, _FIRST_NOT_HASH),
    ("gsi-single-range", "pk HASH", [("idx", "r0 RANGE")], [], {}, _FIRST_NOT_HASH),
    ("lsi-first-range", _PK_SK, [], [("idx", "l RANGE, pk HASH")], {}, _FIRST_NOT_HASH),
    ("lsi-single-range", _PK_SK, [], [("idx", "l1 RANGE")], {}, _FIRST_NOT_HASH),
    ("lsi-hash-only", _PK_SK, [], [("idx", "pk HASH")], {},
     f"{_INVALID}Index KeySchema does not have a range key for index: idx"),
    ("lsi-second-hash", _PK_SK, [], [("idx", "pk HASH, l1 HASH")], {},
     "Invalid KeySchema: The second KeySchemaElement is not a RANGE key type"),
    ("gsi-hash-after-range", "pk HASH", [("idx", "a HASH, b RANGE, c HASH")], [], {}, _HASH_BEFORE_RANGE),
    ("gsi-hash-after-two-ranges", "pk HASH", [("idx", "a HASH, b RANGE, c RANGE, d HASH")], [], {}, _HASH_BEFORE_RANGE),
    ("gsi-hash-range-hash-after-two-hashes", "pk HASH", [("idx", "a HASH, b HASH, c RANGE, d HASH")], [], {},
     _HASH_BEFORE_RANGE),
    ("gsi-alternating", "pk HASH", [("idx", "a HASH, b RANGE, c HASH, d RANGE")], [], {}, _HASH_BEFORE_RANGE),
    ("gsi-five-hash", "pk HASH", [("idx", "h0 HASH, h1 HASH, h2 HASH, h3 HASH, h4 HASH")], [], {},
     f"{_INVALID}The KeySchema exceeds the maximum allowed number of HASH key attributes"),
    ("gsi-five-range", "pk HASH", [("idx", "h0 HASH, r0 RANGE, r1 RANGE, r2 RANGE, r3 RANGE, r4 RANGE")], [], {},
     f"{_INVALID}The KeySchema exceeds the maximum allowed number of RANGE key attributes"),
    ("gsi-nine-elements", "pk HASH", [("idx", _NINE)], [], {}, _too_long(_NINE, 8)),
    ("lsi-three-elements", _PK_SK, [], [("idx", "pk HASH, l1 RANGE, l2 RANGE")], {},
     _too_long("pk HASH, l1 RANGE, l2 RANGE", 2)),
    ("lsi-three-elements-hash-last", _PK_SK, [], [("idx", "pk HASH, b RANGE, c HASH")], {},
     _too_long("pk HASH, b RANGE, c HASH", 2)),
    ("lsi-other-hash-key", _PK_SK, [], [("byOther", "other HASH, sk RANGE")], {},
     f"{_INVALID}Index KeySchema does not have the same leading hash key as table KeySchema for index: "
     "byOther. index hash key: other, table hash key: pk"),
    ("six-lsis", _PK_SK, [], [(f"lsi{n}", f"pk HASH, alt{n} RANGE") for n in range(6)], {},
     f"{_INVALID}Number of LocalSecondaryIndexes exceeds per-table limit of 5"),
    ("unused-definition", "pk HASH", [], [], _PK_AND_UNUSED, _UNUSED),
    ("unused-definition-with-stream", "pk HASH", [], [],
     {**_PK_AND_UNUSED, "StreamSpecification": {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}},
     _UNUSED),
    ("provisioned-without-throughput", "pk HASH", [], [], {"BillingMode": "PROVISIONED"},
     f"{_INVALID}ReadCapacityUnits and WriteCapacityUnits must both be specified when BillingMode is PROVISIONED"),
]


@pytest.mark.parametrize("row", CREATE_TABLE_REFUSALS, ids=[row[0] for row in CREATE_TABLE_REFUSALS])
def test_create_table_refusal_texts(create_and_cleanup_table, unique_table_name, row):
    """A key schema, local index, definition or throughput fault gets the service's text."""
    _, table_keys, gsis, lsis, members, message = row

    def index(name: str, keys: str) -> dict:
        return {"IndexName": name, "KeySchema": _key_schema(keys), "Projection": {"ProjectionType": "ALL"}}

    indexes = {
        "GlobalSecondaryIndexes": [index(*g) for g in gsis],
        "LocalSecondaryIndexes": [index(*l) for l in lsis],
    }
    key_schemas = [_key_schema(table_keys), *(i["KeySchema"] for v in indexes.values() for i in v)]
    key_names = dict.fromkeys(e["AttributeName"] for ks in key_schemas for e in ks)
    request = {
        "TableName": unique_table_name,
        "AttributeDefinitions": [{"AttributeName": n, "AttributeType": "S"} for n in key_names],
        "KeySchema": key_schemas[0],
        "BillingMode": "PAY_PER_REQUEST",
        **{kind: v for kind, v in indexes.items() if v},
        **members,
    }
    # The fixture removes the table should a request be accepted.
    with pytest.raises(ClientError) as exc_info:
        create_and_cleanup_table(**request)
    err = exc_info.value.response["Error"]
    assert (err["Code"], err["Message"]) == ("ValidationException", message)
