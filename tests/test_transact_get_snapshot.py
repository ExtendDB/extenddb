# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""TransactGetItems reads every item from one point in time.

One writer moves two items to the same generation inside a single
TransactWriteItems while readers fetch both with TransactGetItems. Every
successful read must see both items at the same generation; a mismatch is a
torn read (half of a committed transaction observed). The readers must also
see the writer make progress: a frozen view (one generation forever) is
internally consistent and would otherwise pass.

DynamoDB may cancel a TransactGetItems that overlaps an in-flight write with
TransactionConflict; that is a valid outcome and is not counted. Any other
cancellation reason is a failure.

The table has a composite key so the sort-key read path runs inside the
snapshot transaction too.
"""

from __future__ import annotations

import threading
import time

from botocore.exceptions import ClientError

from conftest import scoped_table
from test_concurrency import _make_client

DURATION_S = 5.0
READERS = 4
PK = "snap"
# Two items under one partition key, told apart by the sort key.
SKS = ("a", "b")
COMPOSITE_ATTRS = [
    {"AttributeName": "pk", "AttributeType": "S"},
    {"AttributeName": "sk", "AttributeType": "S"},
]
COMPOSITE_KEY = [
    {"AttributeName": "pk", "KeyType": "HASH"},
    {"AttributeName": "sk", "KeyType": "RANGE"},
]


def _key(sk: str) -> dict:
    return {"pk": {"S": PK}, "sk": {"S": sk}}


def _only_conflict_cancellations(err: ClientError) -> bool:
    """True when every cancellation reason is TransactionConflict or None.

    Any other reason (ValidationError, ConditionalCheckFailed, ...) is a
    regression the test must not hide behind "conflicts are allowed".
    """
    reasons = err.response.get("CancellationReasons") or []
    return all(r.get("Code") in (None, "None", "TransactionConflict") for r in reasons)


def test_transact_get_items_never_returns_a_torn_read(dynamodb_client):
    with scoped_table(dynamodb_client, COMPOSITE_ATTRS, COMPOSITE_KEY) as table:
        for sk in SKS:
            dynamodb_client.put_item(TableName=table, Item={**_key(sk), "gen": {"N": "0"}})

        deadline = time.monotonic() + DURATION_S
        torn: list[list[str]] = []
        gens_seen: set[str] = set()
        reads = [0]
        writes = [0]
        unexpected_cancellations: list[list] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def writer() -> None:
            client = _make_client()
            gen = 0
            try:
                while time.monotonic() < deadline:
                    gen += 1
                    client.transact_write_items(
                        TransactItems=[
                            {"Put": {"TableName": table, "Item": {**_key(sk), "gen": {"N": str(gen)}}}}
                            for sk in SKS
                        ]
                    )
                    with lock:
                        writes[0] += 1
            except BaseException as e:  # noqa: BLE001 - surfaced below
                errors.append(e)

        def reader() -> None:
            client = _make_client()
            try:
                while time.monotonic() < deadline:
                    try:
                        resp = client.transact_get_items(
                            TransactItems=[
                                {"Get": {"TableName": table, "Key": _key(sk)}} for sk in SKS
                            ]
                        )
                    except ClientError as e:
                        if e.response["Error"]["Code"] != "TransactionCanceledException":
                            raise
                        if not _only_conflict_cancellations(e):
                            with lock:
                                unexpected_cancellations.append(
                                    e.response.get("CancellationReasons")
                                )
                        continue
                    gens = [r["Item"]["gen"]["N"] for r in resp["Responses"]]
                    with lock:
                        reads[0] += 1
                        gens_seen.update(gens)
                        if len(set(gens)) != 1:
                            torn.append(gens)
            except BaseException as e:  # noqa: BLE001 - surfaced below
                errors.append(e)

        threads = [threading.Thread(target=writer)] + [
            threading.Thread(target=reader) for _ in range(READERS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, errors[0]
        assert not unexpected_cancellations, (
            f"{len(unexpected_cancellations)} TransactGetItems were cancelled for a reason "
            f"other than TransactionConflict, e.g. {unexpected_cancellations[:3]}"
        )
        assert writes[0] > 10, f"writer made too little progress ({writes[0]} transactions)"
        assert reads[0] > 10, f"readers made too little progress ({reads[0]} reads)"
        assert not torn, f"{len(torn)} of {reads[0]} TransactGetItems were torn, e.g. {torn[:5]}"
        # The readers must have watched the writer move, or a frozen snapshot
        # (or a cache) would pass every check above.
        assert len(gens_seen) > 1, (
            f"readers saw only generation {gens_seen} across {reads[0]} reads while the "
            f"writer committed {writes[0]} transactions"
        )
