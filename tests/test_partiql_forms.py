# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""PartiQL statement forms and functions over the wire.

Covers set_add and set_delete in an UPDATE SET, the FROM-first UPDATE
(``FROM "t" WHERE ... SET ... REMOVE ...``), a condition function compared with
a boolean, and ``EXISTS(SELECT ...)`` as an ExecuteTransaction member.

Dual-target: identical assertions run against real DynamoDB and against an
ExtendDB server (selected by EXTENDDB_TEST_ENDPOINT, see conftest). Every
expected response and error Code+Message is the live service's.

Two tables: a hash-key table pk(S), and a composite pk(S)/sk(S) table for the
partial-key cases. Each test uses its own partition key.
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import wait_for_active

FULL_KEY = "Where clause does not contain a mandatory equality on all key attributes"
WRONG_TYPE = "An operand in the update expression has an incorrect data type"
NOT_WELL_FORMED = "Statement wasn't well formed, can't be processed: "


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


def _create(client, name, key_schema, attribute_definitions):
    client.create_table(
        TableName=name,
        AttributeDefinitions=attribute_definitions,
        KeySchema=key_schema,
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(client, name)


def _drop(client, name):
    try:
        client.delete_table(TableName=name)
    except client.exceptions.ResourceNotFoundException:
        pass


@pytest.fixture(scope="module")
def htable(dynamodb_client):
    """A hash-key table pk(S)."""
    name = f"extenddb-pqforms-h-{uuid.uuid4().hex[:12]}"
    _create(
        dynamodb_client,
        name,
        [{"AttributeName": "pk", "KeyType": "HASH"}],
        [{"AttributeName": "pk", "AttributeType": "S"}],
    )
    try:
        yield name
    finally:
        _drop(dynamodb_client, name)


@pytest.fixture(scope="module")
def ctable(dynamodb_client):
    """A composite pk(S)/sk(S) table."""
    name = f"extenddb-pqforms-c-{uuid.uuid4().hex[:12]}"
    _create(
        dynamodb_client,
        name,
        [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
    )
    try:
        yield name
    finally:
        _drop(dynamodb_client, name)


def _exec(client, statement, parameters=None, **kwargs):
    if parameters is not None:
        kwargs["Parameters"] = parameters
    return client.execute_statement(Statement=statement, **kwargs)


def _error(excinfo):
    err = excinfo.value.response["Error"]
    return err["Code"], err["Message"]


def _refusal(client, statement, parameters=None):
    with pytest.raises(ClientError) as excinfo:
        _exec(client, statement, parameters)
    return _error(excinfo)


def _txn_refusal(client, statements):
    with pytest.raises(ClientError) as excinfo:
        client.execute_transaction(TransactStatements=statements)
    return excinfo.value.response


def _get(client, table, pk):
    return client.get_item(TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True).get(
        "Item"
    )


def _seed_sets(client, table, pk):
    client.put_item(
        TableName=table,
        Item={
            "pk": {"S": pk},
            "tags": {"SS": ["red", "green", "blue"]},
            "sizes": {"NS": ["10", "20", "30"]},
            "blobs": {"BS": [b"\x0a", b"\x0b"]},
            "label": {"S": "plain"},
        },
    )


def _seed_plain(client, table, pk):
    client.put_item(
        TableName=table,
        Item={"pk": {"S": pk}, "n": {"N": "7"}, "w": {"S": "word"}},
    )


def _at(statement, token, start=0, length=None):
    """`1:<column>:<length>` of the first `token` at or after `start`."""
    index = statement.index(token, start)
    return f"1:{index + 1}:{len(token) if length is None else length}"


def _numbers(values):
    return sorted(float(v) for v in values)


# --- set_add --------------------------------------------------------------


def test_set_add_adds_members_to_a_string_set(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-ss")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_add(tags, <<'amber', 'white'>>) WHERE pk = 'sa-ss'",
    )
    tags = _get(dynamodb_client, htable, "sa-ss")["tags"]["SS"]
    assert sorted(tags) == ["amber", "blue", "green", "red", "white"]


def test_set_add_adds_members_to_a_number_set(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-ns")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET sizes = set_add(sizes, <<40, 10>>) WHERE pk = 'sa-ns'",
    )
    sizes = _get(dynamodb_client, htable, "sa-ns")["sizes"]["NS"]
    assert _numbers(sizes) == [10, 20, 30, 40]


def test_set_add_takes_a_binary_set_parameter(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-bs")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET blobs = set_add(blobs, ?) WHERE pk = 'sa-bs'",
        [{"BS": [b"\x0b", b"\x0c"]}],
    )
    blobs = _get(dynamodb_client, htable, "sa-bs")["blobs"]["BS"]
    assert sorted(blobs) == [b"\x0a", b"\x0b", b"\x0c"]


def test_set_add_of_a_present_member_changes_nothing(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-same")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_add(tags, <<'red'>>) WHERE pk = 'sa-same'",
    )
    assert sorted(_get(dynamodb_client, htable, "sa-same")["tags"]["SS"]) == [
        "blue",
        "green",
        "red",
    ]


def test_set_add_to_a_missing_attribute_creates_the_set(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-new")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET fresh = set_add(fresh, <<5>>) WHERE pk = 'sa-new'",
    )
    assert _get(dynamodb_client, htable, "sa-new")["fresh"] == {"NS": ["5"]}


def test_set_add_name_is_case_insensitive(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-case")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = Set_Add(tags, <<'teal'>>) WHERE pk = 'sa-case'",
    )
    assert "teal" in _get(dynamodb_client, htable, "sa-case")["tags"]["SS"]


def test_set_add_in_two_set_clauses(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-two")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_add(tags, <<'teal'>>) "
        f"SET sizes = set_add(sizes, <<50>>) WHERE pk = 'sa-two'",
    )
    item = _get(dynamodb_client, htable, "sa-two")
    assert "teal" in item["tags"]["SS"]
    assert _numbers(item["sizes"]["NS"]) == [10, 20, 30, 50]


def test_set_add_beside_a_set_and_a_remove(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-mixed")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_add(tags, <<'teal'>>), note = 'n' "
        f"REMOVE label WHERE pk = 'sa-mixed'",
    )
    item = _get(dynamodb_client, htable, "sa-mixed")
    assert "teal" in item["tags"]["SS"]
    assert item["note"] == {"S": "n"}
    assert "label" not in item


def test_set_add_returning_all_new(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-ret")
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_add(tags, <<'teal'>>) WHERE pk = 'sa-ret' "
        f"RETURNING ALL NEW *",
    )
    assert len(resp["Items"]) == 1
    assert sorted(resp["Items"][0]["tags"]["SS"]) == ["blue", "green", "red", "teal"]


def test_set_add_on_a_missing_item_fails_its_condition(dynamodb_client, htable):
    code, message = _refusal(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_add(tags, <<'teal'>>) WHERE pk = 'sa-absent'",
    )
    assert (code, message) == ("ConditionalCheckFailedException", "The conditional request failed")
    assert _get(dynamodb_client, htable, "sa-absent") is None


def test_set_add_in_execute_transaction(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-txn")
    resp = dynamodb_client.execute_transaction(
        TransactStatements=[
            {
                "Statement": f"UPDATE \"{htable}\" SET tags = set_add(tags, <<'teal'>>) "
                f"WHERE pk = 'sa-txn'"
            }
        ]
    )
    assert resp["Responses"] == []
    assert "teal" in _get(dynamodb_client, htable, "sa-txn")["tags"]["SS"]


def test_set_add_in_batch_execute_statement(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-batch")
    resp = dynamodb_client.batch_execute_statement(
        Statements=[
            {
                "Statement": f"UPDATE \"{htable}\" SET tags = set_add(tags, <<'teal'>>) "
                f"WHERE pk = 'sa-batch'"
            }
        ]
    )
    assert resp["Responses"] == [{"TableName": htable}]
    assert "teal" in _get(dynamodb_client, htable, "sa-batch")["tags"]["SS"]


def test_set_add_of_a_number_to_a_string_set_is_refused(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-mismatch")
    code, message = _refusal(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_add(tags, <<1>>) WHERE pk = 'sa-mismatch'",
    )
    assert (code, message) == ("ValidationException", WRONG_TYPE)


def test_set_add_to_an_attribute_that_is_not_a_set_is_refused(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-scalar")
    code, message = _refusal(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET label = set_add(label, <<'x'>>) WHERE pk = 'sa-scalar'",
    )
    assert (code, message) == ("ValidationException", WRONG_TYPE)
    assert _get(dynamodb_client, htable, "sa-scalar")["label"] == {"S": "plain"}


def test_set_add_of_a_string_parameter_is_refused(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-sparam")
    statement = f"UPDATE \"{htable}\" SET tags = set_add(tags, ?) WHERE pk = 'sa-sparam'"
    code, message = _refusal(dynamodb_client, statement, [{"S": "teal"}])
    assert code == "ValidationException"
    assert message == (
        "The second argument to SET_ADD must be a value with type SET at "
        + _at(statement, "tags", statement.index("set_add("))
    )


def test_set_add_of_a_number_parameter_to_a_missing_attribute_is_refused(
    dynamodb_client, htable
):
    _seed_sets(dynamodb_client, htable, "sa-nparam")
    statement = f"UPDATE \"{htable}\" SET fresh = set_add(fresh, ?) WHERE pk = 'sa-nparam'"
    code, message = _refusal(dynamodb_client, statement, [{"N": "5"}])
    assert code == "ValidationException"
    assert message == (
        "The second argument to SET_ADD must be a value with type SET at "
        + _at(statement, "fresh", statement.index("set_add("))
    )
    assert "fresh" not in _get(dynamodb_client, htable, "sa-nparam")


def test_set_add_of_an_attribute_is_refused(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sa-path")
    statement = f"UPDATE \"{htable}\" SET tags = set_add(tags, label) WHERE pk = 'sa-path'"
    code, message = _refusal(dynamodb_client, statement)
    assert code == "ValidationException"
    assert message == (
        "The second argument to SET_ADD must be a value with type SET at "
        + _at(statement, "tags", statement.index("set_add("))
    )


def test_set_add_with_no_arguments_is_refused(dynamodb_client, htable):
    statement = f"UPDATE \"{htable}\" SET tags = set_add() WHERE pk = 'sa-noargs'"
    code, message = _refusal(dynamodb_client, statement)
    assert code == "ValidationException"
    assert message == "SET_ADD must have exactly two arguments at " + _at(statement, "set_add")


# --- set_delete -----------------------------------------------------------


def test_set_delete_removes_members(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sd-some")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_delete(tags, <<'red', 'blue'>>) WHERE pk = 'sd-some'",
    )
    assert _get(dynamodb_client, htable, "sd-some")["tags"] == {"SS": ["green"]}


def test_set_delete_removes_a_number(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sd-ns")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET sizes = set_delete(sizes, <<20>>) WHERE pk = 'sd-ns'",
    )
    assert _numbers(_get(dynamodb_client, htable, "sd-ns")["sizes"]["NS"]) == [10, 30]


def test_set_delete_of_every_member_removes_the_attribute(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sd-all")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_delete(tags, <<'red', 'green', 'blue'>>) "
        f"WHERE pk = 'sd-all'",
    )
    assert "tags" not in _get(dynamodb_client, htable, "sd-all")


def test_set_delete_of_an_absent_member_changes_nothing(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sd-absent")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_delete(tags, <<'black'>>) WHERE pk = 'sd-absent'",
    )
    assert sorted(_get(dynamodb_client, htable, "sd-absent")["tags"]["SS"]) == [
        "blue",
        "green",
        "red",
    ]


def test_set_delete_from_a_missing_attribute_leaves_it_missing(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sd-missing")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET fresh = set_delete(fresh, <<'x'>>) WHERE pk = 'sd-missing'",
    )
    assert "fresh" not in _get(dynamodb_client, htable, "sd-missing")


def test_set_delete_of_a_number_from_a_string_set_is_refused(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sd-mismatch")
    code, message = _refusal(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET tags = set_delete(tags, <<1>>) WHERE pk = 'sd-mismatch'",
    )
    assert (code, message) == ("ValidationException", WRONG_TYPE)


def test_set_delete_of_a_string_parameter_is_refused(dynamodb_client, htable):
    _seed_sets(dynamodb_client, htable, "sd-sparam")
    statement = f"UPDATE \"{htable}\" SET tags = set_delete(tags, ?) WHERE pk = 'sd-sparam'"
    code, message = _refusal(dynamodb_client, statement, [{"S": "red"}])
    assert code == "ValidationException"
    assert message == (
        "The second argument to SET_DELETE must be a value with type SET at "
        + _at(statement, "tags", statement.index("set_delete("))
    )
    assert sorted(_get(dynamodb_client, htable, "sd-sparam")["tags"]["SS"]) == [
        "blue",
        "green",
        "red",
    ]


# --- FROM-first UPDATE ----------------------------------------------------


def test_from_first_update_sets_an_attribute(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-set")
    resp = _exec(dynamodb_client, f"FROM \"{htable}\" WHERE pk = 'ff-set' SET extra = 'e'")
    assert resp["Items"] == []
    assert _get(dynamodb_client, htable, "ff-set") == {
        "pk": {"S": "ff-set"},
        "n": {"N": "7"},
        "w": {"S": "word"},
        "extra": {"S": "e"},
    }


def test_from_first_update_removes_an_attribute(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-remove")
    _exec(dynamodb_client, f"FROM \"{htable}\" WHERE pk = 'ff-remove' REMOVE w")
    assert _get(dynamodb_client, htable, "ff-remove") == {
        "pk": {"S": "ff-remove"},
        "n": {"N": "7"},
    }


def test_from_first_update_set_then_remove(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-both")
    _exec(dynamodb_client, f"FROM \"{htable}\" WHERE pk = 'ff-both' SET n = n + 1 REMOVE w")
    assert _get(dynamodb_client, htable, "ff-both") == {
        "pk": {"S": "ff-both"},
        "n": {"N": "8"},
    }


def test_from_first_update_remove_then_set(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-rs")
    _exec(dynamodb_client, f"FROM \"{htable}\" WHERE pk = 'ff-rs' REMOVE w SET extra = 1")
    assert _get(dynamodb_client, htable, "ff-rs") == {
        "pk": {"S": "ff-rs"},
        "n": {"N": "7"},
        "extra": {"N": "1"},
    }


def test_update_remove_then_set(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "up-rs")
    _exec(dynamodb_client, f"UPDATE \"{htable}\" REMOVE w SET extra = 1 WHERE pk = 'up-rs'")
    assert _get(dynamodb_client, htable, "up-rs") == {
        "pk": {"S": "up-rs"},
        "n": {"N": "7"},
        "extra": {"N": "1"},
    }


def test_a_comma_after_a_remove_path_is_a_leftover_token(dynamodb_client, htable):
    item = {"pk": {"S": "rm-comma"}, "m": {"M": {"a": {"N": "0"}}}, "w": {"S": "word"}}
    dynamodb_client.put_item(TableName=htable, Item=item)
    code, message = _refusal(
        dynamodb_client, f"UPDATE \"{htable}\" REMOVE m.a, m WHERE pk = 'rm-comma'"
    )
    assert (code, message) == (
        "ValidationException",
        NOT_WELL_FORMED + "Unexpected token after expression",
    )
    assert _get(dynamodb_client, htable, "rm-comma") == item


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE \"{t}\" SET n = 1 REMOVE w, extra WHERE pk = '{pk}'",
        "UPDATE \"{t}\" REMOVE w, extra SET n = 1 WHERE pk = '{pk}'",
        "FROM \"{t}\" WHERE pk = '{pk}' REMOVE w, extra",
    ],
)
def test_a_remove_takes_one_path(dynamodb_client, htable, statement):
    pk = f"rm-one-{uuid.uuid4().hex[:8]}"
    _seed_plain(dynamodb_client, htable, pk)
    code, message = _refusal(dynamodb_client, statement.format(t=htable, pk=pk))
    assert (code, message) == (
        "ValidationException",
        NOT_WELL_FORMED + "Unexpected token after expression",
    )
    assert _get(dynamodb_client, htable, pk) == {
        "pk": {"S": pk},
        "n": {"N": "7"},
        "w": {"S": "word"},
    }


def test_two_remove_clauses_remove_two_paths(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "rm-two")
    _exec(dynamodb_client, f"UPDATE \"{htable}\" REMOVE w REMOVE n WHERE pk = 'rm-two'")
    assert _get(dynamodb_client, htable, "rm-two") == {"pk": {"S": "rm-two"}}


def test_from_first_update_returning_all_new(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-ret")
    resp = _exec(
        dynamodb_client,
        f"FROM \"{htable}\" WHERE pk = 'ff-ret' SET extra = 2 RETURNING ALL NEW *",
    )
    assert resp["Items"] == [
        {"pk": {"S": "ff-ret"}, "n": {"N": "7"}, "w": {"S": "word"}, "extra": {"N": "2"}}
    ]


def test_from_first_update_keywords_are_case_insensitive(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-lower")
    _exec(dynamodb_client, f"from \"{htable}\" where pk = 'ff-lower' set extra = 3")
    assert _get(dynamodb_client, htable, "ff-lower")["extra"] == {"N": "3"}


def test_from_first_update_takes_parameters(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-params")
    _exec(
        dynamodb_client,
        f"FROM \"{htable}\" WHERE pk = ? SET extra = ?",
        [{"S": "ff-params"}, {"S": "via-param"}],
    )
    assert _get(dynamodb_client, htable, "ff-params")["extra"] == {"S": "via-param"}


def test_from_first_update_condition_that_holds(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-cond-ok")
    _exec(dynamodb_client, f"FROM \"{htable}\" WHERE pk = 'ff-cond-ok' AND n = 7 SET extra = 4")
    assert _get(dynamodb_client, htable, "ff-cond-ok")["extra"] == {"N": "4"}


def test_from_first_update_condition_that_fails(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-cond-no")
    code, message = _refusal(
        dynamodb_client, f"FROM \"{htable}\" WHERE pk = 'ff-cond-no' AND n = 8 SET extra = 4"
    )
    assert (code, message) == ("ConditionalCheckFailedException", "The conditional request failed")
    assert "extra" not in _get(dynamodb_client, htable, "ff-cond-no")


def test_from_first_update_of_a_missing_item_fails_its_condition(dynamodb_client, htable):
    code, message = _refusal(
        dynamodb_client, f"FROM \"{htable}\" WHERE pk = 'ff-absent' SET extra = 1"
    )
    assert (code, message) == ("ConditionalCheckFailedException", "The conditional request failed")
    assert _get(dynamodb_client, htable, "ff-absent") is None


def test_from_first_update_in_execute_transaction(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-txn")
    resp = dynamodb_client.execute_transaction(
        TransactStatements=[
            {"Statement": f"FROM \"{htable}\" WHERE pk = 'ff-txn' SET extra = 5"}
        ]
    )
    assert resp["Responses"] == []
    assert _get(dynamodb_client, htable, "ff-txn")["extra"] == {"N": "5"}


def test_from_first_update_in_batch_execute_statement(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ff-batch")
    resp = dynamodb_client.batch_execute_statement(
        Statements=[{"Statement": f"FROM \"{htable}\" WHERE pk = 'ff-batch' SET extra = 6"}]
    )
    assert resp["Responses"] == [{"TableName": htable}]
    assert _get(dynamodb_client, htable, "ff-batch")["extra"] == {"N": "6"}


@pytest.mark.parametrize("clause", ["SET extra = 1", "REMOVE w"])
def test_from_first_update_without_where_is_refused(dynamodb_client, htable, clause):
    code, message = _refusal(dynamodb_client, f"FROM \"{htable}\" {clause}")
    assert (code, message) == ("ValidationException", FULL_KEY)


def test_from_first_update_without_where_on_a_missing_table(dynamodb_client, htable):
    code, message = _refusal(dynamodb_client, f"FROM \"{htable}-absent\" SET extra = 1")
    assert (code, message) == ("ResourceNotFoundException", "Requested resource not found")


def test_from_first_update_with_a_non_key_where_is_refused(dynamodb_client, htable):
    code, message = _refusal(dynamodb_client, f"FROM \"{htable}\" WHERE n = 7 SET extra = 1")
    assert (code, message) == ("ValidationException", FULL_KEY)


def test_from_first_update_with_a_partial_key_is_refused(dynamodb_client, ctable):
    code, message = _refusal(dynamodb_client, f"FROM \"{ctable}\" WHERE pk = 'ff-p' SET extra = 1")
    assert (code, message) == ("ValidationException", FULL_KEY)


def test_from_first_update_on_a_missing_table(dynamodb_client, htable):
    code, message = _refusal(
        dynamodb_client, f"FROM \"{htable}-absent\" WHERE pk = 'ff-x' SET extra = 1"
    )
    assert (code, message) == ("ResourceNotFoundException", "Requested resource not found")


def test_from_first_update_without_a_clause_is_refused(dynamodb_client, htable):
    code, message = _refusal(dynamodb_client, f"FROM \"{htable}\" WHERE pk = 'ff-none'")
    assert (code, message) == ("ValidationException", NOT_WELL_FORMED + "Expected data manipulation")


# --- a condition function compared with a boolean -------------------------


@pytest.mark.parametrize(
    ("condition", "matches"),
    [
        ("attribute_exists(w) = true", True),
        ("attribute_exists(absent) = true", False),
        ("attribute_exists(w) = false", False),
        ("attribute_exists(absent) = false", True),
        ("attribute_exists(w) <> true", False),
        ("attribute_exists(w) <> false", True),
        ("attribute_not_exists(absent) = true", True),
        ("begins_with(w, 'wo') = true", True),
        ("begins_with(w, 'xx') = true", False),
    ],
)
def test_condition_function_compared_with_a_boolean(dynamodb_client, htable, condition, matches):
    _seed_plain(dynamodb_client, htable, "cb-item")
    resp = _exec(
        dynamodb_client,
        f"SELECT pk FROM \"{htable}\" WHERE pk = 'cb-item' AND {condition}",
        ConsistentRead=True,
    )
    assert resp["Items"] == ([{"pk": {"S": "cb-item"}}] if matches else [])


def test_update_condition_compared_with_true(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "cb-upd")
    _exec(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET extra = 1 WHERE pk = 'cb-upd' AND attribute_exists(w) = true",
    )
    assert _get(dynamodb_client, htable, "cb-upd")["extra"] == {"N": "1"}
    code, message = _refusal(
        dynamodb_client,
        f"UPDATE \"{htable}\" SET extra = 2 WHERE pk = 'cb-upd' "
        f"AND attribute_exists(absent) = true",
    )
    assert (code, message) == ("ConditionalCheckFailedException", "The conditional request failed")


# --- function operands in a WHERE and an INSERT ---------------------------


def test_attribute_not_exists_with_a_string_is_refused(dynamodb_client, htable):
    code, message = _refusal(
        dynamodb_client,
        f"SELECT * FROM \"{htable}\" WHERE pk = 'fo-x' AND attribute_not_exists('w')",
    )
    assert (code, message) == (
        "ValidationException",
        "Operator or function requires a document path; "
        "operator or function: attribute_not_exists",
    )


def test_an_unknown_function_on_the_right_of_a_comparison_is_refused(dynamodb_client, htable):
    statement = f"SELECT * FROM \"{htable}\" WHERE pk = 'fo-x' AND w = upper(w)"
    code, message = _refusal(dynamodb_client, statement)
    assert (code, message) == (
        "ValidationException",
        "Unrecognized function: upper at " + _at(statement, "upper"),
    )


def test_list_append_as_an_insert_value_is_refused(dynamodb_client, htable):
    statement = f"INSERT INTO \"{htable}\" VALUE {{'pk': 'fo-ins', 'l': list_append([1], [2])}}"
    code, message = _refusal(dynamodb_client, statement)
    assert (code, message) == (
        "ValidationException",
        "Unsupported data type: list_append under key root at " + _at(statement, "list_append"),
    )
    assert _get(dynamodb_client, htable, "fo-ins") is None


def test_a_name_as_an_insert_value_is_refused_naming_itself(dynamodb_client, htable):
    statement = f"INSERT INTO \"{htable}\" VALUE {{'pk': pk}}"
    code, message = _refusal(dynamodb_client, statement)
    assert (code, message) == (
        "ValidationException",
        "Unsupported data type: pk under key root at " + _at(statement, "pk}", length=2),
    )


# --- EXISTS(SELECT ...) in ExecuteTransaction -----------------------------

EXISTS_REFUSAL = "EXISTS() must contain a single item read with additional condition"


def test_exists_without_a_further_condition_is_refused(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ex-bare")
    response = _txn_refusal(
        dynamodb_client,
        [
            {"Statement": f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-bare')"},
            {"Statement": f"UPDATE \"{htable}\" SET extra = 1 WHERE pk = 'ex-bare'"},
        ],
    )
    assert response["Error"]["Code"] == "ValidationException"
    assert (
        response["Error"]["Message"] == f"Validation failed in TransactStatements[0]: {EXISTS_REFUSAL}"
    )
    assert "extra" not in _get(dynamodb_client, htable, "ex-bare")


def test_exists_refusal_names_its_position(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ex-pos")
    response = _txn_refusal(
        dynamodb_client,
        [
            {"Statement": f"UPDATE \"{htable}\" SET extra = 1 WHERE pk = 'ex-pos'"},
            {"Statement": f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-other')"},
        ],
    )
    assert response["Error"]["Code"] == "ValidationException"
    assert (
        response["Error"]["Message"] == f"Validation failed in TransactStatements[1]: {EXISTS_REFUSAL}"
    )


def test_exists_with_a_partial_key_is_refused(dynamodb_client, ctable):
    response = _txn_refusal(
        dynamodb_client,
        [
            {"Statement": f"EXISTS(SELECT * FROM \"{ctable}\" WHERE pk = 'ex-p' AND n = 7)"},
            {"Statement": f"UPDATE \"{ctable}\" SET extra = 1 WHERE pk = 'ex-p' AND sk = 's'"},
        ],
    )
    assert response["Error"]["Code"] == "ValidationException"
    assert (
        response["Error"]["Message"] == f"Validation failed in TransactStatements[0]: {EXISTS_REFUSAL}"
    )


def test_exists_that_holds_lets_the_transaction_write(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ex-ok")
    _seed_plain(dynamodb_client, htable, "ex-ok-target")
    resp = dynamodb_client.execute_transaction(
        TransactStatements=[
            {"Statement": f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-ok' AND n = 7)"},
            {"Statement": f"UPDATE \"{htable}\" SET extra = 1 WHERE pk = 'ex-ok-target'"},
        ]
    )
    assert resp["Responses"] == []
    assert _get(dynamodb_client, htable, "ex-ok-target")["extra"] == {"N": "1"}


def test_exists_that_fails_cancels_the_transaction(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ex-no")
    _seed_plain(dynamodb_client, htable, "ex-no-target")
    response = _txn_refusal(
        dynamodb_client,
        [
            {"Statement": f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-no' AND n = 8)"},
            {"Statement": f"UPDATE \"{htable}\" SET extra = 1 WHERE pk = 'ex-no-target'"},
        ],
    )
    assert response["Error"]["Code"] == "TransactionCanceledException"
    assert response["Error"]["Message"] == (
        "Transaction cancelled, please refer cancellation reasons for specific reasons "
        "[ConditionalCheckFailed, None]"
    )
    assert [r["Code"] for r in response["CancellationReasons"]] == [
        "ConditionalCheckFailed",
        "None",
    ]
    assert "extra" not in _get(dynamodb_client, htable, "ex-no-target")


def test_exists_on_a_missing_item_whose_condition_holds_lets_the_write_run(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ex-gone-target")
    dynamodb_client.execute_transaction(
        TransactStatements=[
            {
                "Statement": f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-gone' "
                f"AND w IS MISSING)"
            },
            {"Statement": f"UPDATE \"{htable}\" SET extra = 1 WHERE pk = 'ex-gone-target'"},
        ]
    )
    assert _get(dynamodb_client, htable, "ex-gone-target")["extra"] == {"N": "1"}


def test_exists_alone_is_a_transaction(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ex-alone")
    resp = dynamodb_client.execute_transaction(
        TransactStatements=[
            {"Statement": f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-alone' AND n = 7)"}
        ]
    )
    assert resp["Responses"] == []


def test_exists_beside_a_select_mixes_reads_and_writes(dynamodb_client, htable):
    _seed_plain(dynamodb_client, htable, "ex-mix")
    response = _txn_refusal(
        dynamodb_client,
        [
            {"Statement": f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-mix' AND n = 7)"},
            {"Statement": f"SELECT * FROM \"{htable}\" WHERE pk = 'ex-mix'"},
        ],
    )
    assert response["Error"]["Code"] == "ValidationException"
    assert response["Error"]["Message"] == (
        "ExecuteTransaction API does not support both read and write operations in the same "
        "request."
    )


def test_exists_in_execute_statement_is_refused(dynamodb_client, htable):
    code, message = _refusal(
        dynamodb_client, f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-single' AND n = 7)"
    )
    assert (code, message) == (
        "ValidationException",
        "EXISTS can only be used in ExecuteTransaction write requests.",
    )


def test_exists_in_batch_execute_statement_is_refused(dynamodb_client, htable):
    resp = dynamodb_client.batch_execute_statement(
        Statements=[
            {"Statement": f"EXISTS(SELECT * FROM \"{htable}\" WHERE pk = 'ex-batch' AND n = 7)"}
        ]
    )
    assert resp["Responses"] == [
        {
            "Error": {
                "Code": "ValidationError",
                "Message": "EXISTS can only be used in ExecuteTransaction write requests.",
            }
        }
    ]
