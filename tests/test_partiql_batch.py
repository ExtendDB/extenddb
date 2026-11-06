# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""PartiQL BatchExecuteStatement over the wire.

Dual-target: identical assertions run against real DynamoDB and against an
ExtendDB fix server (selected by EXTENDDB_TEST_ENDPOINT, see conftest). Every
expected response and error Code+Message was measured on the live service; the
records are in conf-run/partiql/char-batch-txn.md (Part 1).

A batch is all reads or all writes. Reads behave like BatchGetItem
(Responses[i] = {TableName, Item}); writes carry per-statement Error entries and
real partial success. The one shared composite pk(S)/sk(S) table is reused with
a distinct partition key per test for isolation.
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import wait_for_active
from partiql_gsi_table import get as _gget
from partiql_gsi_table import gsi_table, item, key, put


@pytest.fixture(scope="session")
def dynamodb_client(endpoint_url):
    """DynamoDB client honoring EXTENDDB_TEST_PROFILE for the live-service run."""
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    profile = os.environ.get("EXTENDDB_TEST_PROFILE") or None
    session = boto3.Session(profile_name=profile, region_name=region)
    kwargs: dict = {"service_name": "dynamodb"}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
            kwargs["config"] = Config(parameter_validation=True)
    return session.client(**kwargs)


@pytest.fixture(scope="module")
def btable(dynamodb_client):
    """A composite pk(S)/sk(S) table, PAY_PER_REQUEST."""
    name = f"extenddb-measure-2c-batch-{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    try:
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except dynamodb_client.exceptions.ResourceNotFoundException:
            pass


def _batch(client, statements, **kwargs):
    return client.batch_execute_statement(Statements=statements, **kwargs)


def _error(excinfo):
    err = excinfo.value.response["Error"]
    return err["Code"], err["Message"]


def _seed(client, table, pk, sk="s1", a=1):
    client.put_item(
        TableName=table,
        Item={"pk": {"S": pk}, "sk": {"S": sk}, "a": {"N": str(a)}},
    )


def _get(client, table, pk, sk="s1"):
    return client.get_item(
        TableName=table, Key={"pk": {"S": pk}, "sk": {"S": sk}}
    ).get("Item")


# --- B1 / B2: reads --------------------------------------------------------


def test_all_reads_success_shape(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "p1", a=1)
    _seed(dynamodb_client, btable, "p2", a=2)
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='p1' AND sk='s1'"},
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='p2' AND sk='s1'"},
        ],
    )
    assert resp["Responses"] == [
        {"TableName": btable, "Item": {"pk": {"S": "p1"}, "sk": {"S": "s1"}, "a": {"N": "1"}}},
        {"TableName": btable, "Item": {"pk": {"S": "p2"}, "sk": {"S": "s1"}, "a": {"N": "2"}}},
    ]


def test_read_absent_key_omits_item(dynamodb_client, btable):
    resp = _batch(
        dynamodb_client,
        [{"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='nope' AND sk='nope'"}],
    )
    assert resp["Responses"] == [{"TableName": btable}]


def test_read_partial_key_is_per_statement_validation_error(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "pp")
    resp = _batch(
        dynamodb_client,
        [{"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='pp'"}],
    )
    assert resp["Responses"] == [
        {
            "Error": {
                "Code": "ValidationError",
                "Message": "Select statements within BatchExecuteStatement must specify the primary key in the where clause.",
            }
        }
    ]


# --- B4 / B6: writes -------------------------------------------------------


def test_all_writes_insert_success_shape(dynamodb_client, btable):
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'w1','sk':'s1','a':10}}"},
            {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'w2','sk':'s1','a':20}}"},
        ],
    )
    assert resp["Responses"] == [{"TableName": btable}, {"TableName": btable}]
    assert _get(dynamodb_client, btable, "w1")["a"] == {"N": "10"}
    assert _get(dynamodb_client, btable, "w2")["a"] == {"N": "20"}


def test_partial_success_good_insert_and_duplicate(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "exist", a=1)
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'fresh','sk':'s1','a':1}}"},
            {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'exist','sk':'s1','a':999}}"},
        ],
    )
    assert resp["Responses"] == [
        {"TableName": btable},
        {
            "Error": {"Code": "DuplicateItem", "Message": "Duplicate primary key exists in table"},
            "TableName": btable,
        },
    ]
    # The first INSERT committed; the duplicate left the existing item untouched.
    assert _get(dynamodb_client, btable, "fresh")["a"] == {"N": "1"}
    assert _get(dynamodb_client, btable, "exist")["a"] == {"N": "1"}


# --- B5: read/write mix (request-level) -----------------------------------


def test_mixed_read_and_write_rejects_whole_request(dynamodb_client, btable):
    with pytest.raises(ClientError) as excinfo:
        _batch(
            dynamodb_client,
            [
                {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='p1' AND sk='s1'"},
                {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'x','sk':'s1'}}"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Read and write requests together in the same batch is not supported."


# --- B7: duplicate key in one batch (request-level) -----------------------


def test_duplicate_key_two_updates_rejects(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "dupu")
    with pytest.raises(ClientError) as excinfo:
        _batch(
            dynamodb_client,
            [
                {"Statement": f"UPDATE \"{btable}\" SET a=1 WHERE pk='dupu' AND sk='s1'"},
                {"Statement": f"UPDATE \"{btable}\" SET a=2 WHERE pk='dupu' AND sk='s1'"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Provided list of item keys contains duplicates"


def test_duplicate_key_insert_and_delete_rejects(dynamodb_client, btable):
    with pytest.raises(ClientError) as excinfo:
        _batch(
            dynamodb_client,
            [
                {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'dupd','sk':'s1'}}"},
                {"Statement": f"DELETE FROM \"{btable}\" WHERE pk='dupd' AND sk='s1'"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Provided list of item keys contains duplicates"


# --- B8: ReturnConsumedCapacity (a list) ----------------------------------


def test_consumed_capacity_reads_indexes_is_a_list(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "c1")
    _seed(dynamodb_client, btable, "c2")
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='c1' AND sk='s1'"},
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='c2' AND sk='s1'"},
        ],
        ReturnConsumedCapacity="INDEXES",
    )
    assert resp["ConsumedCapacity"] == [
        {"TableName": btable, "CapacityUnits": 1.0, "Table": {"CapacityUnits": 1.0}}
    ]


def test_consumed_capacity_write_total_is_a_list(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "cw")
    resp = _batch(
        dynamodb_client,
        [{"Statement": f"UPDATE \"{btable}\" SET a=2 WHERE pk='cw' AND sk='s1'"}],
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"] == [{"TableName": btable, "CapacityUnits": 1.0}]


# --- B9: statement-count limit --------------------------------------------


def test_twenty_five_statements_ok(dynamodb_client, btable):
    statements = [
        {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='n{i}' AND sk='s1'"}
        for i in range(25)
    ]
    resp = _batch(dynamodb_client, statements)
    assert len(resp["Responses"]) == 25


def test_twenty_six_statements_rejected(dynamodb_client, btable):
    statements = [
        {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='n{i}' AND sk='s1'"}
        for i in range(26)
    ]
    with pytest.raises(ClientError) as excinfo:
        _batch(dynamodb_client, statements)
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message.endswith("Member must have length less than or equal to 25")


# --- B10: per-statement syntax / resource errors --------------------------


def test_syntax_error_is_per_statement(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "ok1")
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='ok1' AND sk='s1'"},
            {"Statement": f"SELCT * FROM \"{btable}\""},
        ],
    )
    assert resp["Responses"][0]["Item"]["pk"] == {"S": "ok1"}
    assert resp["Responses"][1] == {
        "Error": {
            "Code": "ValidationError",
            "Message": "Statement wasn't well formed, can't be processed: Expected data manipulation",
        }
    }


def test_unknown_table_is_per_statement_resource_not_found(dynamodb_client, btable):
    resp = _batch(
        dynamodb_client,
        [
            {
                "Statement": "INSERT INTO \"extenddb-measure-2c-no-such-table\" VALUE {'pk':'x','sk':'s1'}"
            }
        ],
    )
    assert resp["Responses"] == [
        {"Error": {"Code": "ResourceNotFound", "Message": "Requested resource not found"}}
    ]


# --- RETURNING on a write member ------------------------------------------
# A member honours RETURNING and carries the row as Responses[i].Item, with
# the projection ExecuteStatement puts in Items. An empty projection leaves
# Item out (measured 2026-11-02).


def test_member_delete_returning_all_old_carries_the_deleted_item(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "ret-del", a=7)
    resp = _batch(
        dynamodb_client,
        [
            {
                "Statement": f"DELETE FROM \"{btable}\" WHERE pk='ret-del' AND sk='s1' RETURNING ALL OLD *"
            }
        ],
    )
    assert resp["Responses"] == [
        {
            "TableName": btable,
            "Item": {"pk": {"S": "ret-del"}, "sk": {"S": "s1"}, "a": {"N": "7"}},
        }
    ]
    assert _get(dynamodb_client, btable, "ret-del") is None


def test_member_update_returning_all_new_carries_the_whole_item(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "ret-allnew", a=1)
    resp = _batch(
        dynamodb_client,
        [
            {
                "Statement": f"UPDATE \"{btable}\" SET a=2 WHERE pk='ret-allnew' AND sk='s1' RETURNING ALL NEW *"
            }
        ],
    )
    assert resp["Responses"] == [
        {
            "TableName": btable,
            "Item": {"pk": {"S": "ret-allnew"}, "sk": {"S": "s1"}, "a": {"N": "2"}},
        }
    ]


def test_member_update_returning_modified_new_carries_the_changed_attribute(
    dynamodb_client, btable
):
    _seed(dynamodb_client, btable, "ret-modnew", a=1)
    resp = _batch(
        dynamodb_client,
        [
            {
                "Statement": f"UPDATE \"{btable}\" SET a=2 WHERE pk='ret-modnew' AND sk='s1' RETURNING MODIFIED NEW *"
            }
        ],
    )
    assert resp["Responses"] == [{"TableName": btable, "Item": {"a": {"N": "2"}}}]


def test_member_update_with_an_empty_modified_projection_omits_item(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "ret-modempty", a=1)
    resp = _batch(
        dynamodb_client,
        [
            {
                "Statement": f"UPDATE \"{btable}\" REMOVE a WHERE pk='ret-modempty' AND sk='s1' RETURNING MODIFIED NEW *"
            }
        ],
    )
    assert resp["Responses"] == [{"TableName": btable}]
    assert "a" not in _get(dynamodb_client, btable, "ret-modempty")


# --- ConsistentRead per member --------------------------------------------
# ConsistentRead is rated per member: a consistent read of a small item costs
# a full unit, an eventual one half, and a mixed batch sums the two (measured
# 2026-11-02).


def _read_units(resp):
    return sum(entry["CapacityUnits"] for entry in resp["ConsumedCapacity"])


def test_consistent_read_is_rated_per_member(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "cr1")
    _seed(dynamodb_client, btable, "cr2")

    def member(pk, **extra):
        return {
            "Statement": f"SELECT * FROM \"{btable}\" WHERE pk='{pk}' AND sk='s1'",
            **extra,
        }

    def units(members):
        return _read_units(_batch(dynamodb_client, members, ReturnConsumedCapacity="TOTAL"))

    assert units([member("cr1")]) == 0.5
    assert units([member("cr1", ConsistentRead=False)]) == 0.5
    assert units([member("cr1", ConsistentRead=True)]) == 1.0
    assert units([member("cr1", ConsistentRead=True), member("cr2")]) == 1.5


# --- ReturnValuesOnConditionCheckFailure on a member ------------------------
# With ALL_OLD, a member whose condition fails carries the existing item on its
# Error, never on the entry; NONE and an absent option carry none (measured
# 2026-11-02).


@pytest.mark.parametrize("setting", ["ALL_OLD", "NONE", None])
def test_condition_failure_item_rides_on_the_member_error(dynamodb_client, btable, setting):
    pk = f"rvccf-{(setting or 'absent').lower()}"
    _seed(dynamodb_client, btable, pk, a=1)
    member = {"Statement": f"UPDATE \"{btable}\" SET a=2 WHERE pk='{pk}' AND sk='s1' AND a=99"}
    if setting is not None:
        member["ReturnValuesOnConditionCheckFailure"] = setting
    resp = _batch(dynamodb_client, [member])
    error = {"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"}
    if setting == "ALL_OLD":
        error["Item"] = {"pk": {"S": pk}, "sk": {"S": "s1"}, "a": {"N": "1"}}
    assert resp["Responses"] == [{"Error": error, "TableName": btable}]
    assert _get(dynamodb_client, btable, pk)["a"] == {"N": "1"}


# --- empty Statements (request-level) -------------------------------------


def test_empty_statements_list_rejected(dynamodb_client_no_validation):
    with pytest.raises(ClientError) as excinfo:
        dynamodb_client_no_validation.batch_execute_statement(Statements=[])
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "1 validation error detected: Value '[]' at 'statements' failed to satisfy "
        "constraint: Member must have length greater than or equal to 1"
    )


# --- DELETE on a missing key with a non-key predicate -----------------------
# The predicate is a condition on an item that exists, so a member naming a
# missing key is a silent no-op, as the statement is on its own.


def test_member_delete_missing_key_with_predicate_is_a_no_op(dynamodb_client, btable):
    resp = _batch(
        dynamodb_client,
        [
            {
                "Statement": f"DELETE FROM \"{btable}\" WHERE pk='ghost-pred' AND sk='s1' AND a=1"
            }
        ],
    )
    assert resp["Responses"] == [{"TableName": btable}]


# --- statements that fail before they run -----------------------------------
# The service translates each statement into the read or the write it runs
# as. A statement whose translation fails gets its own error and counts as
# neither a read nor a write, nor in the check for two statements on one item.
# A statement that translates and fails the check of its request counts as a
# read or a write, and not in the check for two statements on one item. The
# errors of the item operation a statement runs as name its table (measured
# 2026-11-05 against a table with a global secondary index).

MIXED = "Read and write requests together in the same batch is not supported."
DUPLICATES = "Provided list of item keys contains duplicates"
PARAMETERS = "Number of parameters in request and statement don't match."
NOT_FOUND = {"Error": {"Code": "ResourceNotFound", "Message": "Requested resource not found"}}
KEY_TYPE = "Key attribute's data type should match its data type in table's schema: Key sk"
INDEX_KEY_TYPE = (
    "One or more parameter values were invalid: Type mismatch for Index Key g Expected: S "
    "Actual: N IndexName: gsi"
)


def _key_update(name):
    return (
        f"One or more parameter values were invalid: Cannot update attribute {name}. "
        "This attribute is part of the key"
    )


def _validation(message, table=None):
    entry = {"Error": {"Code": "ValidationError", "Message": message}}
    if table is not None:
        entry["TableName"] = table
    return entry


@pytest.fixture(scope="module")
def gtable(dynamodb_client):
    """The table with the index `gsi`, holding the item of the reads."""
    with gsi_table(dynamodb_client, "extenddb-measure-2c-batchg") as name:
        put(dynamodb_client, name, "seed")
        yield name


def _sql(template, table, **names):
    """`template` with `{t}` as the quoted table and each other field as the
    key that name takes in this run."""
    return template.format(t=f'"{table}"', **{n: key(v) for n, v in names.items()})


def _seed_read(table):
    return _sql("SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b'", table, s="seed")


def _seed_answer(table):
    return {"TableName": table, "Item": item(key("seed"))}


@pytest.mark.parametrize(
    "statement, error",
    [
        ("SELECT * FROM nosuch_tbl WHERE pk = 'a' AND sk = 'b'", NOT_FOUND),
        (
            "SELECT * FROM {t}.\"nosuch\" WHERE g = 'a' AND gs = '1'",
            _validation("The table does not have the specified index"),
        ),
        ("SELECT * FROM {t} WHERE pk = '{s}' AND sk = 1", _validation(KEY_TYPE)),
        (
            "SELECT a, a FROM {t} WHERE pk = '{s}' AND sk = 'b'",
            _validation("Duplicate identifiers in select clause: a"),
        ),
        ("SELECT * FROM {t} WHERE pk = ? AND sk = 'b'", _validation(PARAMETERS)),
    ],
    ids=["missing-table", "missing-index", "key-type", "repeated-identifier", "parameter"],
)
def test_a_read_that_does_not_translate_is_no_read(dynamodb_client, gtable, statement, error):
    """A read that fails its translation leaves the batch to its writes."""
    inserted = f"r-untranslated-{uuid.uuid4().hex[:6]}"
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": _sql(statement, gtable, s="seed")},
            {"Statement": _sql("INSERT INTO {t} VALUE {{'pk':'{k}','sk':'b','a':1}}", gtable,
                               k=inserted)},
        ],
    )
    assert resp["Responses"] == [error, {"TableName": gtable}]
    assert _gget(dynamodb_client, gtable, key(inserted)) is not None


@pytest.mark.parametrize(
    "statement, error",
    [
        (
            "INSERT INTO {t} VALUE {{'pk':'{k}'}}",
            _validation("Key attribute should be present in the item: Key sk"),
        ),
        ("INSERT INTO {t} VALUE {{'pk':'{k}','sk':1}}", _validation(KEY_TYPE)),
        (
            "UPDATE {t} SET a = 9 WHERE pk = '{k}'",
            _validation("Where clause does not contain a mandatory equality on all key attributes"),
        ),
        ("INSERT INTO {t} VALUE {{'pk':'{k}','sk':'b','x':?}}", _validation(PARAMETERS)),
        (
            "DELETE FROM {t} WHERE pk = '{k}' AND sk = 'b' RETURNING ALL NEW *",
            _validation(
                "Invalid returning clause: RETURNING ALL NEW *. Only RETURNING ALL OLD * is "
                "allowed in DELETE statements."
            ),
        ),
        ("UPDATE nosuch_tbl SET a = 9 WHERE pk = 'a' AND sk = 'b'", NOT_FOUND),
        (
            "UPDATE {t} SET a = 1, a = 2 WHERE pk = '{k}' AND sk = 'b'",
            _validation(
                "Two document paths overlap with each other; must remove or rewrite one of "
                "these paths; path one: [a], path two: [a]"
            ),
        ),
    ],
    ids=[
        "no-sort-key",
        "key-type",
        "partial-key",
        "parameter",
        "returning",
        "missing-table",
        "overlapping-paths",
    ],
)
def test_a_write_that_does_not_translate_is_no_write(dynamodb_client, gtable, statement, error):
    """A write that fails its translation leaves the batch to its reads, and
    writes nothing."""
    pk = put(dynamodb_client, gtable, "w-untranslated")
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": _sql(statement, gtable, k="w-untranslated")},
            {"Statement": _seed_read(gtable)},
        ],
    )
    assert resp["Responses"] == [error, _seed_answer(gtable)]
    assert _gget(dynamodb_client, gtable, pk) == item(pk)


def test_a_failed_read_beside_a_write_lets_the_write_run(dynamodb_client, gtable):
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": "SELECT * FROM nosuch_tbl WHERE pk = 'a' AND sk = 'b'"},
            {"Statement": _sql("INSERT INTO {t} VALUE {{'pk':'{k}','sk':'b','a':1}}", gtable,
                               k="cb1")},
        ],
    )
    assert resp["Responses"] == [NOT_FOUND, {"TableName": gtable}]
    pk = key("cb1")
    assert _gget(dynamodb_client, gtable, pk) == {
        "pk": {"S": pk},
        "sk": {"S": "b"},
        "a": {"N": "1"},
    }


@pytest.mark.parametrize(
    "statements",
    [
        ["SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b'",
         "UPDATE {t} SET a = 9 WHERE pk = '{k}' AND sk = 'b'"],
        ["SELECT * FROM {t} WHERE pk = '{s}'",
         "INSERT INTO {t} VALUE {{'pk':'{n}','sk':'b','a':1}}"],
        ["SELECT * FROM {t}.\"gsi\" WHERE g = '{s}' AND gs = '1'",
         "INSERT INTO {t} VALUE {{'pk':'{n}','sk':'b','a':1}}"],
        ["SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b'",
         "UPDATE {t} SET pk = 'z' WHERE pk = '{k}' AND sk = 'b'"],
        ["SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b'",
         "UPDATE {t} SET a = 9 WHERE pk = '{n}' AND sk = 'b'"],
        ["SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b'",
         "UPDATE {t} SET g = 1 WHERE pk = '{k}' AND sk = 'b'"],
        ["SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b'",
         "UPDATE {t} SET a.b = 1 WHERE pk = '{k}' AND sk = 'b'"],
    ],
    ids=[
        "update",
        "partial-key-read",
        "index-read",
        "key-update",
        "missing-item",
        "index-key-type",
        "document-path",
    ],
)
def test_a_statement_that_translates_counts_toward_the_mix(dynamodb_client, gtable, statements):
    """A statement that translates is a read or a write, whatever its
    request then answers."""
    pk = put(dynamodb_client, gtable, "w-counted")
    with pytest.raises(ClientError) as excinfo:
        _batch(
            dynamodb_client,
            [{"Statement": _sql(s, gtable, s="seed", k="w-counted", n="w-counted-new")}
             for s in statements],
        )
    assert _error(excinfo) == ("ValidationException", MIXED)
    assert _gget(dynamodb_client, gtable, pk) == item(pk)
    assert _gget(dynamodb_client, gtable, key("w-counted-new")) is None


@pytest.mark.parametrize(
    "first, error",
    [
        ("INSERT INTO {t} VALUE {{'pk':'{k}','sk':'b','x':?}}", _validation(PARAMETERS)),
        ("UPDATE {t} SET a = ? WHERE pk = '{k}' AND sk = 'b'", _validation(PARAMETERS)),
        ("UPDATE {t} SET a = 9 WHERE pk = '{k}' AND sk = 'b' AND a = ?", _validation(PARAMETERS)),
        ("UPDATE {t} SET pk = 'z' WHERE pk = '{k}' AND sk = 'b'", "key-update:pk"),
        ("UPDATE {t} REMOVE sk WHERE pk = '{k}' AND sk = 'b'", "key-update:sk"),
        ("UPDATE {t} SET g = 1 WHERE pk = '{k}' AND sk = 'b'", "index-key-type"),
        ("INSERT INTO {t} VALUE {{'pk':'{k}','sk':'b','g':1}}", "index-key-type"),
        (
            "UPDATE {t} SET a = 1, a = 2 WHERE pk = '{k}' AND sk = 'b'",
            _validation(
                "Two document paths overlap with each other; must remove or rewrite one of "
                "these paths; path one: [a], path two: [a]"
            ),
        ),
    ],
    ids=[
        "insert-parameter",
        "set-parameter",
        "condition-parameter",
        "set-partition-key",
        "remove-sort-key",
        "set-index-key-type",
        "insert-index-key-type",
        "overlapping-paths",
    ],
)
def test_a_write_refused_before_its_item_is_not_a_duplicate(
    dynamodb_client, gtable, first, error
):
    """A write refused before it reaches its item leaves the item to the
    other statement on it."""
    pk = put(dynamodb_client, gtable, "dup-refused")
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": _sql(first, gtable, k="dup-refused")},
            {"Statement": _sql("DELETE FROM {t} WHERE pk = '{k}' AND sk = 'b'", gtable,
                               k="dup-refused")},
        ],
    )
    if error == "index-key-type":
        error = _validation(INDEX_KEY_TYPE, gtable)
    elif isinstance(error, str):
        error = _validation(_key_update(error.split(":")[1]), gtable)
    assert resp["Responses"] == [error, {"TableName": gtable}]
    assert _gget(dynamodb_client, gtable, pk) is None


def test_a_delete_with_another_returning_clause_is_not_a_duplicate(dynamodb_client, gtable):
    pk = put(dynamodb_client, gtable, "dup-returning")
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": _sql(
                "DELETE FROM {t} WHERE pk = '{k}' AND sk = 'b' RETURNING ALL NEW *", gtable,
                k="dup-returning")},
            {"Statement": _sql("UPDATE {t} SET a = 9 WHERE pk = '{k}' AND sk = 'b'", gtable,
                               k="dup-returning")},
        ],
    )
    assert resp["Responses"] == [
        _validation(
            "Invalid returning clause: RETURNING ALL NEW *. Only RETURNING ALL OLD * is "
            "allowed in DELETE statements."
        ),
        {"TableName": gtable},
    ]
    assert _gget(dynamodb_client, gtable, pk)["a"] == {"N": "9"}


@pytest.mark.parametrize(
    "first, error",
    [
        (
            "SELECT a, a FROM {t} WHERE pk = '{s}' AND sk = 'b'",
            _validation("Duplicate identifiers in select clause: a"),
        ),
        (
            "SELECT * FROM {t}.\"nosuch\" WHERE pk = '{s}' AND sk = 'b'",
            _validation("The table does not have the specified index"),
        ),
        (
            "SELECT * FROM {t}.\"gsi\" WHERE pk = '{s}' AND sk = 'b'",
            _validation(
                "Select statements within BatchExecuteStatement must specify the primary key "
                "in the where clause."
            ),
        ),
    ],
    ids=["repeated-identifier", "missing-index", "index-read"],
)
def test_a_read_refused_before_its_item_is_not_a_duplicate(
    dynamodb_client, gtable, first, error
):
    resp = _batch(
        dynamodb_client,
        [{"Statement": _sql(first, gtable, s="seed")}, {"Statement": _seed_read(gtable)}],
    )
    assert resp["Responses"] == [error, _seed_answer(gtable)]


@pytest.mark.parametrize(
    "first",
    [
        "UPDATE {t} SET a = 9 WHERE pk = '{k}' AND sk = 'b'",
        "UPDATE {t} SET a = 9 WHERE pk = '{k}' AND sk = 'b' AND a = 99",
        "UPDATE {t} SET b = m + 1 WHERE pk = '{k}' AND sk = 'b'",
        "UPDATE {t} SET a.b = 1 WHERE pk = '{k}' AND sk = 'b'",
    ],
    ids=["update", "failed-condition", "operand-type", "document-path"],
)
def test_two_writes_that_reach_one_item_are_duplicates(dynamodb_client, gtable, first):
    pk = put(dynamodb_client, gtable, "dup-reached")
    with pytest.raises(ClientError) as excinfo:
        _batch(
            dynamodb_client,
            [
                {"Statement": _sql(first, gtable, k="dup-reached")},
                {"Statement": _sql("DELETE FROM {t} WHERE pk = '{k}' AND sk = 'b'", gtable,
                                   k="dup-reached")},
            ],
        )
    assert _error(excinfo) == ("ValidationException", DUPLICATES)
    assert _gget(dynamodb_client, gtable, pk) == item(pk)


@pytest.mark.parametrize(
    "statement, message",
    [
        ("UPDATE {t} SET pk = 'z' WHERE pk = '{k}' AND sk = 'b'", _key_update("pk")),
        ("UPDATE {t} REMOVE sk WHERE pk = '{k}' AND sk = 'b'", _key_update("sk")),
        (
            "UPDATE {t} SET b = m + 1 WHERE pk = '{k}' AND sk = 'b'",
            "An operand in the update expression has an incorrect data type",
        ),
        (
            "UPDATE {t} SET a = set_add(a, <<'x'>>) WHERE pk = '{k}' AND sk = 'b'",
            "An operand in the update expression has an incorrect data type",
        ),
        (
            "UPDATE {t} SET a.b = 1 WHERE pk = '{k}' AND sk = 'b'",
            "The document path provided in the update expression is invalid for update",
        ),
        (
            "UPDATE {t} SET m[0] = 1 WHERE pk = '{k}' AND sk = 'b'",
            "The document path provided in the update expression is invalid for update",
        ),
        ("UPDATE {t} SET g = 1 WHERE pk = '{k}' AND sk = 'b'", INDEX_KEY_TYPE),
        ("INSERT INTO {t} VALUE {{'pk':'{n}','sk':'b','g':1}}", INDEX_KEY_TYPE),
        # Both come ahead of a path into a key attribute among the other
        # conditions (measured 2026-11-05).
        (
            "UPDATE {t} SET pk = 'z' WHERE pk = '{k}' AND sk = 'b' AND NOT pk.c = 1",
            _key_update("pk"),
        ),
        ("UPDATE {t} SET g = 1 WHERE pk = '{k}' AND sk = 'b' AND NOT pk.c = 1", INDEX_KEY_TYPE),
        (
            "UPDATE {t} SET gs = 1 WHERE pk = '{k}' AND sk = 'b' AND NOT pk.c = 1",
            INDEX_KEY_TYPE.replace("Index Key g ", "Index Key gs "),
        ),
        (
            "UPDATE {t} SET g = 1, pk = 'z' WHERE pk = '{k}' AND sk = 'b' AND NOT pk.c = 1",
            _key_update("pk"),
        ),
        # An index key given the value of another attribute is checked on
        # the written item, after the path into a key attribute.
        (
            "UPDATE {t} SET g = a WHERE pk = '{k}' AND sk = 'b' AND NOT pk.c = 1",
            "Key attributes must be scalars; list random access '[]' and map lookup '.' are not "
            "allowed: Key: pk",
        ),
    ],
    ids=[
        "set-partition-key",
        "remove-sort-key",
        "operand-type",
        "set-add-to-a-number",
        "path-into-a-number",
        "index-into-a-map",
        "set-index-key-type",
        "insert-index-key-type",
        "set-partition-key-ahead-of-a-path-into-a-key",
        "set-index-key-type-ahead-of-a-path-into-a-key",
        "set-index-sort-key-type-ahead-of-a-path-into-a-key",
        "set-partition-key-ahead-of-an-index-key-type",
        "index-key-from-another-attribute-after-a-path-into-a-key",
    ],
)
def test_an_error_of_the_item_operation_names_the_table(
    dynamodb_client, gtable, statement, message
):
    pk = put(dynamodb_client, gtable, "tn")
    resp = _batch(
        dynamodb_client, [{"Statement": _sql(statement, gtable, k="tn", n="tn-new")}]
    )
    assert resp["Responses"] == [_validation(message, gtable)]
    assert _gget(dynamodb_client, gtable, pk) == item(pk)


def test_an_error_of_reading_the_statement_names_no_table(dynamodb_client, gtable):
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": _seed_read(gtable)},
            {"Statement": _sql("SELECT * FROM {t} WHERE pk = ? AND sk = 'b'", gtable)},
        ],
    )
    assert resp["Responses"] == [_seed_answer(gtable), _validation(PARAMETERS)]


# --- projections ------------------------------------------------------------
# A batch read flattens a projected path to its leaf, and refuses an
# identifier named twice and two paths that overlap as the statement's own
# error (measured 2026-11-05).


def test_a_projected_read_keys_each_path_on_its_leaf(dynamodb_client, gtable):
    resp = _batch(
        dynamodb_client,
        [{"Statement": _sql("SELECT a, m.k FROM {t} WHERE pk = '{s}' AND sk = 'b'", gtable,
                            s="seed")}],
    )
    assert resp["Responses"] == [
        {"TableName": gtable, "Item": {"a": {"N": "1"}, "k": {"N": "1"}}}
    ]


@pytest.mark.parametrize(
    "projection, message",
    [
        ("a, a", "Duplicate identifiers in select clause: a"),
        ("m.k, k", "Duplicate identifiers in select clause: k"),
        (
            "m.k, m",
            "Two document paths overlap with each other; must remove or rewrite one of these "
            "paths; path one: [m], path two: [m, k]",
        ),
    ],
    ids=["repeated", "repeated-leaf", "overlapping"],
)
def test_a_projection_refusal_is_the_statement_error(
    dynamodb_client, gtable, projection, message
):
    resp = _batch(
        dynamodb_client,
        [{"Statement": _sql(f"SELECT {projection} FROM {{t}} WHERE pk = '{{s}}' AND sk = 'b'",
                            gtable, s="seed")}],
    )
    assert resp["Responses"] == [_validation(message)]


# --- ConsumedCapacity with an index and with failed writes --------------------
# A batch sums the units of its statements per table, with the units of each
# index at INDEXES. A write that fails at its item is charged the units of the
# item stored under its key (one when there is none), counted in its table's
# CapacityUnits alone; a table on which every write failed reports nothing
# (measured 2026-11-05).

BIG = {"big": {"S": "x" * 3000}}


def _capacity(client, table, statements, rcc, **names):
    resp = _batch(
        client,
        [{"Statement": _sql(s, table, **names)} for s in statements],
        ReturnConsumedCapacity=rcc,
    )
    return resp.get("ConsumedCapacity")


def _arm(table, units, table_units=None, gsi_units=None):
    arm = {"TableName": table, "CapacityUnits": units}
    if table_units is not None:
        arm["Table"] = {"CapacityUnits": table_units}
    if gsi_units is not None:
        arm["GlobalSecondaryIndexes"] = {"gsi": {"CapacityUnits": gsi_units}}
    return arm


UPDATE_A = "UPDATE {t} SET a = 9 WHERE pk = '{ok}' AND sk = 'b'"
UPDATE_MISSING = "UPDATE {t} SET a = 9 WHERE pk = '{missing}' AND sk = 'b'"


def test_an_index_key_change_reports_the_index_units(dynamodb_client, gtable):
    put(dynamodb_client, gtable, "cb17")
    assert _capacity(
        dynamodb_client, gtable, ["UPDATE {t} SET g = 'x' WHERE pk = '{k}' AND sk = 'b'"],
        "INDEXES", k="cb17",
    ) == [_arm(gtable, 3.0, 1.0, 2.0)]


@pytest.mark.parametrize(
    "statements, rcc, expected",
    [
        ([UPDATE_A, UPDATE_MISSING], "TOTAL", (3.0, None, None)),
        ([UPDATE_A, UPDATE_MISSING], "INDEXES", (3.0, 1.0, 1.0)),
        ([UPDATE_MISSING, UPDATE_A], "INDEXES", (3.0, 1.0, 1.0)),
        ([UPDATE_A, UPDATE_MISSING,
          "UPDATE {t} SET a = 9 WHERE pk = '{missing2}' AND sk = 'b'"], "TOTAL", (4.0, None, None)),
        ([UPDATE_A, "UPDATE {t} SET a = 9 WHERE pk = '{ok2}' AND sk = 'b'"], "TOTAL",
         (4.0, None, None)),
        ([UPDATE_A, "UPDATE {t} SET a = 9 WHERE pk = '{big}' AND sk = 'b' AND a = 99"], "INDEXES",
         (5.0, 1.0, 1.0)),
        ([UPDATE_A, "INSERT INTO {t} VALUE {{'pk':'{big}','sk':'b','a':1}}"], "TOTAL",
         (5.0, None, None)),
        ([UPDATE_A, "DELETE FROM {t} WHERE pk = '{ok2}' AND sk = 'b' AND a = 99"], "TOTAL",
         (3.0, None, None)),
        ([UPDATE_A, "UPDATE {t} SET b = m + 1 WHERE pk = '{ok2}' AND sk = 'b'"], "TOTAL",
         (3.0, None, None)),
        ([UPDATE_A, "UPDATE {t} SET a.b = 1 WHERE pk = '{ok2}' AND sk = 'b'"], "TOTAL",
         (3.0, None, None)),
        ([UPDATE_A, "UPDATE {t} SET pk = 'z' WHERE pk = '{ok2}' AND sk = 'b'"], "TOTAL",
         (2.0, None, None)),
        ([UPDATE_A, "UPDATE {t} SET g = 1 WHERE pk = '{ok2}' AND sk = 'b'"], "TOTAL",
         (2.0, None, None)),
        (["DELETE FROM {t} WHERE pk = '{ok}' AND sk = 'b'",
          "DELETE FROM {t} WHERE pk = '{missing}' AND sk = 'b'"], "INDEXES", (3.0, 2.0, 1.0)),
        (["INSERT INTO {t} VALUE {{'pk':'{missing}','sk':'b','a':1,'g':'q','gs':'1'}}",
          "INSERT INTO {t} VALUE {{'pk':'{missing2}','sk':'b','a':1}}"], "INDEXES",
         (3.0, 2.0, 1.0)),
    ],
    ids=[
        "failed-condition-total",
        "failed-condition-indexes",
        "failure-first",
        "two-failures",
        "two-successes",
        "failed-condition-on-a-3kb-item",
        "duplicate-over-a-3kb-item",
        "failed-delete-condition",
        "operand-type",
        "document-path",
        "key-update-is-not-charged",
        "index-key-type-is-not-charged",
        "deletes",
        "inserts",
    ],
)
def test_a_failed_write_is_charged_on_its_table(dynamodb_client, gtable, statements, rcc, expected):
    put(dynamodb_client, gtable, "cap-ok")
    put(dynamodb_client, gtable, "cap-ok2")
    put(dynamodb_client, gtable, "cap-big", **BIG)
    names = {
        "ok": "cap-ok",
        "ok2": "cap-ok2",
        "big": "cap-big",
        "missing": f"cap-missing-{uuid.uuid4().hex[:6]}",
        "missing2": f"cap-missing-{uuid.uuid4().hex[:6]}",
    }
    units, table_units, gsi_units = expected
    assert _capacity(dynamodb_client, gtable, statements, rcc, **names) == [
        _arm(gtable, units, table_units, gsi_units)
    ]


def test_failed_writes_alone_report_no_capacity(dynamodb_client, gtable):
    assert _capacity(
        dynamodb_client,
        gtable,
        [UPDATE_MISSING, "UPDATE {t} SET a = 9 WHERE pk = '{missing2}' AND sk = 'b'"],
        "INDEXES",
        missing=f"cap-missing-{uuid.uuid4().hex[:6]}",
        missing2=f"cap-missing-{uuid.uuid4().hex[:6]}",
    ) is None


# --- a statement with a call an update cannot make -----------------------------
# A SET value that calls a condition function or a function the service does
# not know is the statement's own error, without the table, and the other
# statements run. The statement counts as neither a read nor a write, nor in
# the check for two statements on one item (measured 2026-11-06).

CALL_NOT_ALLOWED = "The function is not allowed in an update expression; function: "
INSERT_ONE = "INSERT INTO {t} VALUE {{'pk':'{one}','sk':'b'}}"
INSERT_TWO = "INSERT INTO {t} VALUE {{'pk':'{two}','sk':'b'}}"


def _unrecognized_at(statement, name):
    return f"Unrecognized function: {name} at 1:{statement.index(name + '(') + 1}:{len(name)}"


@pytest.mark.parametrize(
    "value, message",
    [
        ("size(m)", CALL_NOT_ALLOWED + "size"),
        ("attribute_exists(m)", CALL_NOT_ALLOWED + "attribute_exists"),
        ("begins_with(m, 'x')", CALL_NOT_ALLOWED + "begins_with"),
        ("upper(m)", None),
        (
            "if_not_exists(m)",
            "Incorrect number of operands for operator or function; operator or function: "
            "if_not_exists, number of operands: 1",
        ),
    ],
)
def test_a_call_an_update_cannot_make_fails_only_its_statement(
    dynamodb_client, gtable, value, message
):
    names = {n: f"bc-{n}-{uuid.uuid4().hex[:6]}" for n in ("k", "one", "two")}
    put(dynamodb_client, gtable, names["k"])
    update = _sql(f"UPDATE {{t}} SET a = {value} WHERE pk = '{{k}}' AND sk = 'b'", gtable, **names)
    statements = [_sql(INSERT_ONE, gtable, **names), update, _sql(INSERT_TWO, gtable, **names)]
    resp = _batch(dynamodb_client, [{"Statement": s} for s in statements])
    expected = message or _unrecognized_at(update, "upper")
    assert resp["Responses"] == [
        {"TableName": gtable},
        _validation(expected),
        {"TableName": gtable},
    ]
    for name in ("one", "two"):
        assert _gget(dynamodb_client, gtable, key(names[name])) is not None
    assert _gget(dynamodb_client, gtable, key(names["k"])) == item(key(names["k"]))


@pytest.mark.parametrize("value", ["size(m)", "upper(m)"])
def test_a_call_an_update_cannot_make_counts_as_no_write(dynamodb_client, gtable, value):
    name = f"bcw-{uuid.uuid4().hex[:6]}"
    put(dynamodb_client, gtable, name)
    update = _sql(f"UPDATE {{t}} SET a = {value} WHERE pk = '{{k}}' AND sk = 'b'", gtable, k=name)
    other = _sql("UPDATE {t} SET x = 1 WHERE pk = '{k}' AND sk = 'b'", gtable, k=name)
    read = {"Statement": _seed_read(gtable), "ConsistentRead": True}
    resp = _batch(dynamodb_client, [read, {"Statement": update}])
    assert resp["Responses"][0] == _seed_answer(gtable)
    assert resp["Responses"][1]["Error"]["Code"] == "ValidationError"
    resp = _batch(dynamodb_client, [{"Statement": update}, {"Statement": other}])
    assert resp["Responses"][1] == {"TableName": gtable}
    assert _gget(dynamodb_client, gtable, key(name))["x"] == {"N": "1"}


def test_grouped_arithmetic_runs_in_a_batch(dynamodb_client, gtable):
    name = f"bga-{uuid.uuid4().hex[:6]}"
    put(dynamodb_client, gtable, name)
    two = f"bga2-{uuid.uuid4().hex[:6]}"
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": _sql("UPDATE {t} SET a = (a + 1) + 1 WHERE pk = '{k}' AND sk = 'b'", gtable, k=name)},
            {"Statement": _sql(INSERT_TWO, gtable, two=two)},
        ],
    )
    assert resp["Responses"] == [{"TableName": gtable}, {"TableName": gtable}]
    assert _gget(dynamodb_client, gtable, key(name))["a"] == {"N": "3"}
