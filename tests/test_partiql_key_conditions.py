# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the key conditions of a PartiQL WHERE clause.

The service reads a WHERE clause as an OR of disjuncts (an OR with a key
attribute on either side, and an IN on a key attribute, split it; AND
distributes over the split) and takes the disjuncts in order. A disjunct
reads by key when it holds one distinct equality of the partition key with a
value and no other predicate on the partition key, and on the sort key either
nothing or one distinct key condition and nothing else. The first disjunct
that is a scan ends the reading. What follows from that, pinned here:

  * a key value of the wrong type is refused only inside a disjunct read by
    key before the first scan, the sort key value before the partition key
    value, and before the overlap refusal;
  * a value on the left is the mirrored comparison, on every operation;
  * an UPDATE or DELETE naming a key attribute with two values is refused,
    and an EXISTS check doing so fails; a failed EXISTS check returns no
    item for ALL_OLD;
  * a begins_with key condition takes a prefix of the sort key's type;
  * the filter of a read by key may not compare a key attribute with another
    path or read a path into one (refused at once, ahead of the key type and
    overlap refusals, beside other disjuncts and when another makes a scan),
    and a NOT over an AND or an OR stays in that filter, while an IN with a
    path in its list makes a scan;
  * a path into a key attribute is refused wherever it is in the WHERE of a
    table read, in a scan too;
  * a read of one disjunct has its key value sizes checked, and a batch
    member whose key value is too long names its table;
  * a projected call through a missing index is refused before the index is
    looked up.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-02).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

KEY_TYPE = "Key attribute's data type should match its data type in table's schema: Key {}"
OVERLAP = "Overlapping conditions with range keys are not supported in where clause"
FILTER_KEY = (
    "Filter Expression can only contain non-primary key attributes: Primary key attribute: {}"
)
SCALAR_KEY = (
    "Key attributes must be scalars; list random access '[]' and map lookup '.' are not "
    "allowed: Key {}"
)
MULTIPLE = (
    "Multiple conditions on same key {}. Only single item Update/Insert/Delete are supported"
)
HASH_SIZE = (
    "One or more parameter values were invalid: Size of hashkey has exceeded the maximum "
    "size limit of2048 bytes"
)
RANGE_SIZE = (
    "One or more parameter values were invalid: Aggregated size of all range keys has "
    "exceeded the size limit of 1024 bytes"
)
BEGINS_WITH_NUMBER = (
    "Incorrect operand type for operator or function; operator or function: begins_with, "
    "operand type: N"
)
CANCELLED = "Transaction cancelled, please refer cancellation reasons for specific reasons"
KEY_SCHEMA_MISMATCH = {
    "Code": "ValidationError",
    "Message": "The provided key element does not match the schema",
}


def _keyed_table(client, sort_type: str):
    return scoped_table(
        client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": sort_type},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    )


@pytest.fixture(scope="module")
def htable(dynamodb_client):
    """A table keyed on a string pk, holding the item pk = 'a'."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "a"}, "a": {"N": "1"}})
        yield name


@pytest.fixture(scope="module")
def ctable(dynamodb_client):
    """A table keyed on string pk and sk, holding the item a/x."""
    with _keyed_table(dynamodb_client, "S") as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "a"}, "sk": {"S": "x"}, "a": {"S": "x"}},
        )
        yield name


@pytest.fixture(scope="module")
def ntable(dynamodb_client):
    """A table keyed on a string pk and a number sk, holding the item a/1."""
    with _keyed_table(dynamodb_client, "N") as name:
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "a"}, "sk": {"N": "1"}})
        yield name


@pytest.fixture(scope="module")
def btable(dynamodb_client):
    """A table keyed on a string pk and a binary sk, holding the item a/x."""
    with _keyed_table(dynamodb_client, "B") as name:
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "a"}, "sk": {"B": b"x"}})
        yield name


@pytest.fixture(scope="module")
def gtable(dynamodb_client):
    """A table with an index keyed on a number g and a string gs."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "N"},
            {"AttributeName": "gs", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gix",
                "KeySchema": [
                    {"AttributeName": "g", "KeyType": "HASH"},
                    {"AttributeName": "gs", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


def _message(client, statement, **kwargs):
    with pytest.raises(ClientError) as e:
        client.execute_statement(Statement=statement, **kwargs)
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def _items(client, statement, **kwargs):
    return client.execute_statement(Statement=statement, **kwargs)["Items"]


def _sort_keys(client, table, condition):
    items = _items(client, f'SELECT * FROM "{table}" WHERE {condition}')
    return sorted(i["sk"]["S"] for i in items)


def _batch_entries(client, statement):
    return client.batch_execute_statement(Statements=[{"Statement": statement}])["Responses"]


# ---- which disjuncts have their key values checked ---------------------------


@pytest.mark.parametrize(
    ("condition", "rows"),
    [
        ("sk = 1", []),
        ("sk = 1 AND a = 1", []),
        ("sk IN [1, 2]", []),
        ("sk > 1", []),
        ("sk BETWEEN 1 AND 2", []),
        ("1 = sk", []),
        ("pk = 'a' OR sk = 1", ["x"]),
        ("sk = 1 OR pk = 1", []),
        ("a = 1 OR pk = 1", []),
        ("pk = 'a' OR a = 1 OR pk = 1", ["x"]),
        ("pk = 'a' AND sk = 'x' OR sk = 1", ["x"]),
        ("pk = 'a' AND a = 1 OR sk = 1", []),
        ("pk = 'a' AND (pk = 1 OR a = 1)", []),
        ("pk = 'a' AND sk = 1 AND pk = 1", []),
        ("pk = 1 AND pk = 'a' AND sk = 1", []),
        ("NOT (pk = 1 AND sk = 1)", ["x"]),
    ],
)
def test_a_value_outside_a_disjunct_read_by_key_is_not_checked(
    dynamodb_client, ctable, condition, rows
):
    assert _sort_keys(dynamodb_client, ctable, condition) == rows


@pytest.mark.parametrize(
    "condition",
    [
        "1 = pk",
        "1 = pk AND a = 1",
        "1 = pk OR a = 1",
        "1 = pk OR pk = 'a'",
        "pk = 'a' OR (a = 1 AND pk = 1)",
        "(a = 1 AND pk = 1) OR pk = 'a'",
        "pk = 'a' AND a = 1 OR pk = 1",
        "pk = 'a' OR pk = 1 OR a = 1",
    ],
)
def test_a_partition_key_value_of_the_wrong_type_in_a_keyed_disjunct(
    dynamodb_client, htable, condition
):
    statement = f'SELECT * FROM "{htable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == KEY_TYPE.format("pk")


@pytest.mark.parametrize(
    ("condition", "key"),
    [
        ("(pk = 1 AND sk = 'x') OR (pk = 'b' AND sk = 1)", "pk"),
        ("(pk = 'b' AND sk = 1) OR (pk = 1 AND sk = 'x')", "sk"),
        ("1 = pk AND sk = 'x'", "pk"),
        ("pk = 'a' AND sk = 'x' OR pk = 1", "pk"),
        ("pk = 'a' AND 1 = sk", "sk"),
        ("1 = sk AND pk = 'a'", "sk"),
        ("pk = 'a' AND 1 < sk", "sk"),
        ("'a' = pk AND 1 = sk", "sk"),
        ("pk IN [1, 'b'] AND sk = 1", "sk"),
        ("pk IN ['b', 1] AND sk = 1", "sk"),
        ("pk = 'a' AND pk = 'a' AND sk = 1", "sk"),
        ("pk = 'a' AND sk = 1 AND sk = 1", "sk"),
        ("pk = 'a' AND sk = 1 AND sk = a", "sk"),
        ("pk = 'a' AND (sk = 1 OR sk = 'y')", "sk"),
        ("pk = 'a' AND (sk = 'x' OR sk = 1)", "sk"),
        # Key values are checked before the overlap refusal.
        ("pk = 'a' AND (sk = 1 OR a = 1)", "sk"),
        ("pk = 'a' AND (a = 1 OR sk = 1)", "sk"),
    ],
)
def test_a_key_value_of_the_wrong_type_in_a_keyed_disjunct(
    dynamodb_client, ctable, condition, key
):
    statement = f'SELECT * FROM "{ctable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == KEY_TYPE.format(key)


def test_overlapping_disjuncts_of_the_right_type_are_refused(dynamodb_client, ctable):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'a' AND (sk = 'x' OR a = 1)"
    assert _message(dynamodb_client, statement) == OVERLAP


@pytest.mark.parametrize(
    "template",
    [
        "SELECT * FROM \"{t}\" WHERE pk = 1 AND sk = 1",
        "SELECT * FROM \"{t}\" WHERE sk = 1 AND pk = 1",
        "SELECT * FROM \"{t}\" WHERE pk = 1 AND sk > 1",
        "SELECT * FROM \"{t}\" WHERE pk = 1 AND sk BETWEEN 1 AND 2",
        "SELECT * FROM \"{t}\" WHERE pk = 1 AND sk IN [1]",
        "SELECT * FROM \"{t}\" WHERE a = 1 AND pk = 1 AND sk = 1",
        "SELECT * FROM \"{t}\" WHERE 1 = pk AND 1 = sk",
        "UPDATE \"{t}\" SET q = 1 WHERE pk = 1 AND sk = 1",
        "UPDATE \"{t}\" SET q = 1 WHERE 1 = pk AND sk = 1",
        "DELETE FROM \"{t}\" WHERE sk = 1 AND pk = 1",
        "DELETE FROM \"{t}\" WHERE 'a' = pk AND 1 = sk",
    ],
)
def test_the_sort_key_value_is_checked_before_the_partition_key_value(
    dynamodb_client, ctable, template
):
    assert _message(dynamodb_client, template.format(t=ctable)) == KEY_TYPE.format("sk")


def test_parameters_of_the_wrong_type_name_the_sort_key(dynamodb_client, ctable):
    statement = f'SELECT * FROM "{ctable}" WHERE pk = ? AND sk = ?'
    params = [{"N": "1"}, {"N": "1"}]
    assert _message(dynamodb_client, statement, Parameters=params) == KEY_TYPE.format("sk")


def test_a_batch_read_names_the_sort_key(dynamodb_client, ctable):
    statement = f'SELECT * FROM "{ctable}" WHERE pk = 1 AND sk = 1'
    assert _batch_entries(dynamodb_client, statement) == [
        {"Error": {"Code": "ValidationError", "Message": KEY_TYPE.format("sk")}}
    ]


# ---- a sort key with more than one condition ---------------------------------


@pytest.mark.parametrize(
    "condition",
    [
        "pk = 'a' AND sk = 'x' AND sk = 1",
        "pk = 'a' AND sk < 1 AND sk > 0",
        "pk = 'a' AND sk > 'a' AND sk < 1",
        "pk = 'a' AND sk > 'a' AND sk = 1",
        "pk = 'a' AND begins_with(sk, 'x') AND sk = 1",
        "pk = 'a' AND sk = 1 AND NOT sk = 'y'",
        "pk = 'a' AND sk = 1 AND contains(sk, 'x')",
        "pk = 'a' AND sk = 1 AND attribute_exists(sk)",
        "pk = 'a' AND sk = 1 AND sk <> 'y'",
        "pk = 'a' AND sk = 1 AND sk IS NOT MISSING",
        "pk = 'a' AND (sk = 'x' OR a = 1) AND sk = 1",
    ],
)
def test_a_second_condition_on_the_sort_key_leaves_its_values_unchecked(
    dynamodb_client, ctable, condition
):
    assert _sort_keys(dynamodb_client, ctable, condition) == []


def test_two_begins_with_on_a_number_sort_key_run(dynamodb_client, ntable):
    statement = (
        f"SELECT * FROM \"{ntable}\" WHERE pk = 'a' AND begins_with(sk, 'x') "
        "AND begins_with(sk, 'y')"
    )
    assert _items(dynamodb_client, statement) == []


@pytest.mark.parametrize("condition", ["pk = 1 AND pk > 'a'", "pk = 1 AND NOT pk = 'a'"])
def test_a_second_condition_on_the_partition_key_leaves_its_value_unchecked(
    dynamodb_client, htable, condition
):
    assert _items(dynamodb_client, f'SELECT * FROM "{htable}" WHERE {condition}') == []


# ---- begins_with on the sort key ---------------------------------------------


@pytest.mark.parametrize(
    ("table", "condition"),
    [
        ("ntable", "pk = 'a' AND begins_with(sk, 'x')"),
        ("ntable", "pk = 1 AND begins_with(sk, 'x')"),
        ("ntable", "begins_with(sk, 'x') AND pk = 1"),
        ("ntable", "pk = 'a' AND begins_with(sk, 'x') AND a = 1"),
        ("ntable", "pk = 'a' AND (begins_with(sk, 'x') OR a = 1)"),
        ("ntable", "pk = 'a' AND begins_with(sk, 'x') OR pk = 'b'"),
        ("btable", "pk = 'a' AND begins_with(sk, 'x')"),
    ],
)
def test_a_string_prefix_on_a_number_or_binary_sort_key(
    dynamodb_client, request, table, condition
):
    name = request.getfixturevalue(table)
    statement = f'SELECT * FROM "{name}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == KEY_TYPE.format("sk")


@pytest.mark.parametrize(
    ("table", "prefix", "message"),
    [
        ("ntable", {"S": "x"}, KEY_TYPE.format("sk")),
        ("ntable", {"B": b"x"}, KEY_TYPE.format("sk")),
        ("btable", {"S": "x"}, KEY_TYPE.format("sk")),
        ("ntable", {"N": "1"}, BEGINS_WITH_NUMBER),
        ("ctable", {"N": "1"}, BEGINS_WITH_NUMBER),
    ],
    ids=["number-string", "number-binary", "binary-string", "number-number", "string-number"],
)
def test_a_begins_with_parameter_of_another_type(
    dynamodb_client, request, table, prefix, message
):
    name = request.getfixturevalue(table)
    statement = f"SELECT * FROM \"{name}\" WHERE pk = 'a' AND begins_with(sk, ?)"
    assert _message(dynamodb_client, statement, Parameters=[prefix]) == message


def test_a_binary_prefix_on_a_binary_sort_key_reads(dynamodb_client, btable):
    statement = f"SELECT * FROM \"{btable}\" WHERE pk = 'a' AND begins_with(sk, ?)"
    items = _items(dynamodb_client, statement, Parameters=[{"B": b"x"}])
    assert items == [{"pk": {"S": "a"}, "sk": {"B": b"x"}}]


# ---- index keys --------------------------------------------------------------


def test_an_index_sort_key_alone_is_not_checked(dynamodb_client, gtable):
    assert _items(dynamodb_client, f'SELECT * FROM "{gtable}"."gix" WHERE gs = 1') == []


@pytest.mark.parametrize(
    ("condition", "key"), [("'x' = g", "g"), ("g = 'x'", "g"), ("g = 1 AND gs = 1", "gs")]
)
def test_an_index_key_value_of_the_wrong_type(dynamodb_client, gtable, condition, key):
    statement = f'SELECT * FROM "{gtable}"."gix" WHERE {condition}'
    assert _message(dynamodb_client, statement) == KEY_TYPE.format(key)


# ---- a value on the left -----------------------------------------------------


def test_a_value_on_the_left_reads_by_key(dynamodb_client, htable, ctable):
    assert _items(dynamodb_client, f"SELECT * FROM \"{htable}\" WHERE 'a' = pk") == [
        {"pk": {"S": "a"}, "a": {"N": "1"}}
    ]
    for statement in [
        f"SELECT * FROM \"{htable}\" WHERE 'a' = pk",
        f"SELECT * FROM \"{ctable}\" WHERE 'a' = pk AND 'x' = sk",
    ]:
        batch = _batch_entries(dynamodb_client, statement)
        assert [entry["Item"]["pk"] for entry in batch] == [{"S": "a"}]
        txn = dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
        assert [entry["Item"]["pk"] for entry in txn["Responses"]] == [{"S": "a"}]


def test_a_value_on_the_left_updates_by_key(dynamodb_client, htable, ctable):
    dynamodb_client.put_item(TableName=htable, Item={"pk": {"S": "w1"}})
    statement = f"UPDATE \"{htable}\" SET q = 1 WHERE 'w1' = pk RETURNING ALL NEW *"
    assert _items(dynamodb_client, statement) == [{"pk": {"S": "w1"}, "q": {"N": "1"}}]
    dynamodb_client.put_item(TableName=htable, Item={"pk": {"S": "w2"}})
    dynamodb_client.execute_transaction(
        TransactStatements=[{"Statement": f"UPDATE \"{htable}\" SET q = 2 WHERE 'w2' = pk"}]
    )
    item = dynamodb_client.get_item(
        TableName=htable, Key={"pk": {"S": "w2"}}, ConsistentRead=True
    )["Item"]
    assert item == {"pk": {"S": "w2"}, "q": {"N": "2"}}
    statement = f"UPDATE \"{ctable}\" SET q = 1 WHERE 'a' = pk AND 'x' = sk"
    assert _items(dynamodb_client, statement) == []


@pytest.mark.parametrize(
    "template",
    ["UPDATE \"{t}\" SET q = 1 WHERE 1 = pk", "DELETE FROM \"{t}\" WHERE 1 = pk"],
    ids=["update", "delete"],
)
def test_a_write_value_on_the_left_of_the_wrong_type(dynamodb_client, htable, template):
    assert _message(dynamodb_client, template.format(t=htable)) == KEY_TYPE.format("pk")


@pytest.mark.parametrize(
    "template",
    [
        "UPDATE \"{t}\" SET q = 1 WHERE pk = 'a' AND 1 = pk",
        "DELETE FROM \"{t}\" WHERE pk = 'a' AND 1 = pk",
    ],
    ids=["update", "delete"],
)
def test_a_write_with_two_values_for_a_key(dynamodb_client, htable, template):
    assert _message(dynamodb_client, template.format(t=htable)) == MULTIPLE.format("pk")


def test_a_batch_read_with_a_value_of_the_wrong_type_on_the_left(dynamodb_client, htable):
    statement = f'SELECT * FROM "{htable}" WHERE 1 = pk'
    assert _batch_entries(dynamodb_client, statement) == [
        {"Error": {"Code": "ValidationError", "Message": KEY_TYPE.format("pk")}}
    ]


def test_a_transaction_read_with_a_value_of_the_wrong_type_on_the_left(dynamodb_client, htable):
    statement = f'SELECT * FROM "{htable}" WHERE 1 = pk'
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    response = e.value.response
    assert response["Error"] == {
        "Code": "TransactionCanceledException",
        "Message": f"{CANCELLED} [ValidationError]",
    }
    assert response["CancellationReasons"] == [KEY_SCHEMA_MISMATCH]


# ---- key attributes in the filter of a read by key ---------------------------


@pytest.mark.parametrize(
    ("table", "condition", "message"),
    [
        ("ctable", "pk = 'a' AND sk = a", FILTER_KEY.format("sk")),
        ("ctable", "pk = 'a' AND a = sk", FILTER_KEY.format("sk")),
        ("ctable", "pk = 'a' AND sk = a AND a = 'x'", FILTER_KEY.format("sk")),
        ("ctable", "pk = 'a' AND sk = 'x' AND sk = a", FILTER_KEY.format("sk")),
        ("ctable", "pk = 'a' AND a = 'x' AND sk = a", FILTER_KEY.format("sk")),
        ("ctable", "pk = 'a' AND sk = pk", FILTER_KEY.format("sk")),
        ("ctable", "pk = 'a' AND pk = a", FILTER_KEY.format("pk")),
        ("ctable", "pk = 'a' AND sk = 'x' AND pk = a", FILTER_KEY.format("pk")),
        ("htable", "pk = 'a' AND pk = a", FILTER_KEY.format("pk")),
        ("htable", "pk = 'a' AND a = pk", FILTER_KEY.format("pk")),
        ("ctable", "pk = 'a' AND sk.x = 1", SCALAR_KEY.format("sk")),
        ("ctable", "pk = 'a' AND sk[0] = 1", SCALAR_KEY.format("sk")),
        ("htable", "pk = 'a' AND pk.x = 1", SCALAR_KEY.format("pk")),
        # Under a NOT over an AND or an OR, a path into a key attribute gets
        # the filter refusal; naming both keys, it names the sort key.
        ("ctable", "pk = 'a' AND NOT (sk.q = 1 AND n = 1)", FILTER_KEY.format("sk")),
        ("ctable", "pk = 'a' AND NOT (sk.q = 1 AND pk = b)", FILTER_KEY.format("sk")),
        ("ctable", "pk = 'a' AND NOT (pk = b AND sk.q = 1)", FILTER_KEY.format("sk")),
    ],
)
def test_a_keyed_filter_naming_a_key_attribute_is_refused(
    dynamodb_client, request, table, condition, message
):
    name = request.getfixturevalue(table)
    assert _message(dynamodb_client, f'SELECT * FROM "{name}" WHERE {condition}') == message


@pytest.mark.parametrize(
    ("condition", "rows"),
    [
        ("sk = a", ["x"]),
        ("pk = a", []),
        ("pk = 'a' AND size(sk) = 1", ["x"]),
        ("pk = 'a' AND attribute_exists(sk)", ["x"]),
        ("pk = 'a' AND sk IS NOT MISSING", ["x"]),
        ("pk = 'a' AND contains(sk, 'x')", ["x"]),
        ("pk = 'a' AND sk <> 'y'", ["x"]),
        ("pk = 'a' AND sk IN [a]", ["x"]),
    ],
)
def test_a_scan_filter_may_name_a_key_attribute(dynamodb_client, ctable, condition, rows):
    assert _sort_keys(dynamodb_client, ctable, condition) == rows


def test_overlapping_disjuncts_are_refused_before_the_filter(dynamodb_client, ctable):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'a' AND (sk = a OR a = 'x')"
    assert _message(dynamodb_client, statement) == OVERLAP


@pytest.mark.parametrize(
    "condition",
    [
        "(pk = 'a' AND sk.x = 1) OR pk = 'b'",
        # Ahead of the overlap refusal.
        "(pk = 'a' AND sk.x = 1) OR (pk = 'a' AND sk = 'y')",
        "(pk = 'a' AND sk = 'y') OR (pk = 'a' AND sk.x = 1)",
        # Ahead of a key value of the wrong type in another disjunct.
        "(pk = 'a' AND sk.x = 1) OR pk = 1",
        "pk = 1 OR (pk = 'a' AND sk.x = 1)",
        # When a later disjunct makes the statement a scan.
        "(pk = 'a' AND sk.x = 1) OR x = 1",
    ],
    ids=[
        "beside-a-partition",
        "overlap",
        "overlap-reversed",
        "key-type",
        "key-type-reversed",
        "scan",
    ],
)
def test_a_path_into_a_key_attribute_in_one_disjunct_is_refused_at_once(
    dynamodb_client, ctable, condition
):
    statement = f'SELECT * FROM "{ctable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == SCALAR_KEY.format("sk")


@pytest.mark.parametrize(
    "condition",
    ["sk.x = 1", "x = 1 OR (pk = 'a' AND sk.x = 1)"],
    ids=["alone", "after-a-scan"],
)
def test_a_path_into_a_key_attribute_is_refused_anywhere_in_the_where(
    dynamodb_client, ctable, condition
):
    statement = f'SELECT * FROM "{ctable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == SCALAR_KEY.format("sk")


def test_a_not_over_a_compound_condition_keeps_its_disjunct_read_by_key(
    dynamodb_client, ctable
):
    statement = (
        f"SELECT * FROM \"{ctable}\" WHERE pk = 'a' AND NOT (sk = 'x' AND a = 1) "
        "OR pk = 'a' AND sk = 'y'"
    )
    assert _message(dynamodb_client, statement) == OVERLAP


@pytest.mark.parametrize(
    ("written", "condition", "extra"),
    [
        ("w3", "pk = 'a' AND pk = 'b' AND a = 1", {}),
        ("w4", "pk = 'a' AND pk = 'b'", {}),
        # Item 'a' exists, and the reason still holds no item.
        (
            "w5",
            "pk = 'a' AND pk = 'b' AND a = 1",
            {"ReturnValuesOnConditionCheckFailure": "ALL_OLD"},
        ),
    ],
    ids=["further-condition", "no-further-condition", "all-old"],
)
def test_an_exists_check_with_two_values_for_a_key_fails(
    dynamodb_client, htable, written, condition, extra
):
    dynamodb_client.put_item(TableName=htable, Item={"pk": {"S": written}})
    check = dict({"Statement": f'EXISTS(SELECT * FROM "{htable}" WHERE {condition})'}, **extra)
    update = f"UPDATE \"{htable}\" SET q = 3 WHERE pk = '{written}'"
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[check, {"Statement": update}])
    response = e.value.response
    assert response["Error"] == {
        "Code": "TransactionCanceledException",
        "Message": f"{CANCELLED} [ConditionalCheckFailed, None]",
    }
    assert response["CancellationReasons"] == [
        {"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"},
        {"Code": "None"},
    ]
    item = dynamodb_client.get_item(
        TableName=htable, Key={"pk": {"S": written}}, ConsistentRead=True
    )["Item"]
    assert item == {"pk": {"S": written}}


@pytest.mark.parametrize(
    ("written", "condition"),
    [("w6", "pk = 'a' AND a = 2"), ("w7", "pk = 'a' AND pk = 'a' AND a = 2")],
    ids=["one-value", "one-value-twice"],
)
def test_a_failed_exists_check_returns_no_item_for_all_old(
    dynamodb_client, htable, written, condition
):
    dynamodb_client.put_item(TableName=htable, Item={"pk": {"S": written}})
    check = {
        "Statement": f'EXISTS(SELECT * FROM "{htable}" WHERE {condition})',
        "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
    }
    update = f"UPDATE \"{htable}\" SET q = 3 WHERE pk = '{written}'"
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[check, {"Statement": update}])
    response = e.value.response
    assert response["Error"] == {
        "Code": "TransactionCanceledException",
        "Message": f"{CANCELLED} [ConditionalCheckFailed, None]",
    }
    # Item 'a' exists, and the reason holds no item.
    assert response["CancellationReasons"] == [
        {"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"},
        {"Code": "None"},
    ]
    item = dynamodb_client.get_item(
        TableName=htable, Key={"pk": {"S": written}}, ConsistentRead=True
    )["Item"]
    assert item == {"pk": {"S": written}}


# ---- key value sizes ---------------------------------------------------------

BIG_HASH = "a" * 2049
BIG_RANGE = "a" * 1025


@pytest.mark.parametrize(
    "condition",
    [f"pk = '{BIG_HASH}'", f"pk = '{chr(0xE9) * 1025}'"],
    ids=["ascii", "two-byte"],
)
def test_a_partition_key_value_past_2048_bytes(dynamodb_client, htable, condition):
    statement = f'SELECT * FROM "{htable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == HASH_SIZE


def test_a_partition_key_parameter_past_2048_bytes(dynamodb_client, htable):
    statement = f'SELECT * FROM "{htable}" WHERE pk = ?'
    assert _message(dynamodb_client, statement, Parameters=[{"S": BIG_HASH}]) == HASH_SIZE


@pytest.mark.parametrize(
    ("condition", "message"),
    [
        (f"pk = 'a' AND sk = '{BIG_RANGE}'", RANGE_SIZE),
        (f"pk = 'a' AND sk > '{BIG_RANGE}'", RANGE_SIZE),
        (f"pk = 'a' AND sk BETWEEN 'a' AND '{BIG_RANGE}'", RANGE_SIZE),
        (f"pk = 'a' AND begins_with(sk, '{BIG_RANGE}')", RANGE_SIZE),
        (f"pk = '{BIG_HASH}' AND sk = '{BIG_RANGE}'", HASH_SIZE),
        (f"pk = '{BIG_HASH}'", HASH_SIZE),
    ],
    ids=["equal", "greater", "between", "begins-with", "both", "partition"],
)
def test_a_sort_key_value_past_1024_bytes(dynamodb_client, ctable, condition, message):
    statement = f'SELECT * FROM "{ctable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == message


@pytest.mark.parametrize(
    ("table", "condition"),
    [
        ("htable", f"pk = '{'a' * 2048}'"),
        ("ctable", f"pk = 'a' AND sk = '{'a' * 1024}'"),
        ("ctable", f"sk = '{BIG_RANGE}'"),
    ],
    ids=["hash-limit", "range-limit", "scan"],
)
def test_key_values_at_the_limit_or_outside_a_key_read_run(
    dynamodb_client, request, table, condition
):
    name = request.getfixturevalue(table)
    assert _items(dynamodb_client, f'SELECT * FROM "{name}" WHERE {condition}') == []


def test_a_long_value_in_a_scan_condition_on_the_partition_key_runs(dynamodb_client, htable):
    items = _items(dynamodb_client, f"SELECT * FROM \"{htable}\" WHERE pk > '{BIG_HASH}'")
    assert all(i["pk"]["S"] > BIG_HASH for i in items)


@pytest.mark.parametrize(
    "condition",
    [f"pk IN ['a', '{BIG_HASH}']", f"pk = 'a' OR pk = '{BIG_HASH}'"],
    ids=["in", "or"],
)
def test_several_disjuncts_return_the_other_items_before_the_size_check(
    dynamodb_client, htable, condition
):
    response = dynamodb_client.execute_statement(
        Statement=f'SELECT * FROM "{htable}" WHERE {condition}'
    )
    assert [i["pk"] for i in response["Items"]] == [{"S": "a"}]
    assert "NextToken" in response


@pytest.mark.parametrize(
    "template",
    [
        "SELECT * FROM \"{t}\" WHERE pk = '{v}'",
        "UPDATE \"{t}\" SET q = 1 WHERE pk = '{v}'",
        "DELETE FROM \"{t}\" WHERE pk = '{v}'",
        "INSERT INTO \"{t}\" VALUE {{'pk': '{v}'}}",
    ],
    ids=["select", "update", "delete", "insert"],
)
def test_a_batch_key_value_past_its_size_limit_names_the_table(
    dynamodb_client, htable, template
):
    statement = template.format(t=htable, v=BIG_HASH)
    assert _batch_entries(dynamodb_client, statement) == [
        {"Error": {"Code": "ValidationError", "Message": HASH_SIZE}, "TableName": htable}
    ]


# ---- a projected call through a missing index --------------------------------


@pytest.mark.parametrize(
    ("statement", "position"),
    [
        ("SELECT COUNT(*) FROM \"{t}\".\"nope\"", "1:8:5"),
        ("SELECT COUNT(*) FROM \"{t}\".\"nope\" WHERE pk = 'a'", "1:8:5"),
        ("SELECT COUNT(*) FROM \"{t}\".\"nope\" WHERE pk = 1e126", "1:8:5"),
        ("SELECT a, COUNT(*) FROM \"{t}\".\"nope\"", "1:11:5"),
        ("SELECT size(a) FROM \"{t}\".\"nope\"", "1:8:4"),
        ("SELECT COUNT(a) FROM \"{t}\".\"nope\"", "1:8:5"),
        ("SELECT count(*) FROM \"{t}\".\"nope\"", "1:8:5"),
        ("SELECT\nCOUNT(*) FROM \"{t}\".\"nope\"", "2:1:5"),
    ],
)
def test_a_projected_call_is_refused_before_the_index_lookup(
    dynamodb_client, htable, statement, position
):
    message = _message(dynamodb_client, statement.format(t=htable))
    assert message == f"Unexpected path component at {position}"
