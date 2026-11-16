# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A table with a global secondary index for the PartiQL batch and
transaction tests, and the items those tests write in it.

The table has the key pk(S)/sk(S) and the index `gsi` on g(S)/gs(S) with
projection ALL, on demand. EXTENDDB_TEST_GSI_TABLE names a table to use in
place of a new one: it is created when it is missing and kept afterwards.
Without it the table is new and is deleted at the end. Every key a test
writes carries the run's suffix (see `key`), so a kept table holds no item a
test reads from an earlier run.
"""

from __future__ import annotations

import contextlib
import os
import uuid

from conftest import wait_for_active

INDEX = "gsi"

RUN = uuid.uuid4().hex[:8]


def key(name: str) -> str:
    """The partition key `name` takes in this run."""
    return f"{name}-{RUN}"


def item(pk: str, sk: str = "b", gs: str = "1", **extra) -> dict:
    """The item the tests write: `{pk, sk, a: 1, b: 2, x: 0, m: {k: 1, 'my
    key': 1}, g: pk, gs}`, with `extra` attributes added."""
    value = {
        "pk": {"S": pk},
        "sk": {"S": sk},
        "a": {"N": "1"},
        "b": {"N": "2"},
        "x": {"N": "0"},
        "m": {"M": {"k": {"N": "1"}, "my key": {"N": "1"}}},
        "g": {"S": pk},
        "gs": {"S": gs},
    }
    value.update(extra)
    return value


def put(client, table: str, name: str, **extra) -> str:
    """Write the test item under the key `name` takes in this run, and
    return that key."""
    pk = key(name)
    client.put_item(TableName=table, Item=item(pk, **extra))
    return pk


def get(client, table: str, pk: str):
    """The item stored under `pk`, read consistently, or None."""
    return client.get_item(
        TableName=table, Key={"pk": {"S": pk}, "sk": {"S": "b"}}, ConsistentRead=True
    ).get("Item")


@contextlib.contextmanager
def gsi_table(client, prefix: str):
    """The name of the table, created for the test module unless
    EXTENDDB_TEST_GSI_TABLE names one."""
    kept = os.environ.get("EXTENDDB_TEST_GSI_TABLE", "").strip()
    name = kept or f"{prefix}-{uuid.uuid4().hex[:12]}"
    if not kept or not _exists(client, name):
        client.create_table(
            TableName=name,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": n, "AttributeType": "S"} for n in ("pk", "sk", "g", "gs")
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": INDEX,
                    "KeySchema": [
                        {"AttributeName": "g", "KeyType": "HASH"},
                        {"AttributeName": "gs", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
    wait_for_active(client, name)
    try:
        yield name
    finally:
        if not kept:
            with contextlib.suppress(client.exceptions.ResourceNotFoundException):
                client.delete_table(TableName=name)


def _exists(client, name: str) -> bool:
    try:
        client.describe_table(TableName=name)
    except client.exceptions.ResourceNotFoundException:
        return False
    return True
