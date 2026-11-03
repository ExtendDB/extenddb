# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the constraints the request model puts on the
fields of a PartiQL request: the length of each statement, the number of
statements, the client request token, NextToken, Limit, Parameters, the two
enum fields, and the members a request must carry.

The service checks these before it reads a statement, and reports every one
that fails in a single ValidationException: "1 validation error detected:
<part>" for one, "N validation errors detected: <part>; <part>" for more, in
the order it walks the request, at most ten parts. A statement list past its
limit is echoed with every field of every statement, or in some answers with
each statement's class name and a hash.

Runs against real DynamoDB (EXTENDDB_TEST_ENDPOINT unset, AWS credentials) and
against an extenddb server (EXTENDDB_TEST_ENDPOINT set) with identical
assertions. Every expected message was measured on the service (2026-11-02).
The client skips its own parameter checks, which would refuse some of these
requests before they are sent.
"""

from __future__ import annotations

import re

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

MAX_LENGTH = 8192
TOO_LONG = f"Member must have length less than or equal to {MAX_LENGTH}"
TOO_SHORT = "Member must have length greater than or equal to 1"
AT_LEAST_ONE = "Member must have value greater than or equal to 1"
ON_FAILURE_SET = "Member must satisfy enum value set: [ALL_OLD, NONE]"
CAPACITY_SET = "Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
TOKEN_TOO_LONG = "Member must have length less than or equal to 36"

# The members of a parameter's value class, as the service echoes them.
VALUE_FIELDS = (
    "s", "n", "b", "iNT", "dECIMAL", "dOUBLE", "sS", "nS", "bS", "iNTSET",
    "dECIMALSET", "dOUBLESET", "m", "l", "nULL", "bOOL",
)
BUFFER = "java.nio.HeapByteBuffer[pos=0 lim=1 cap=1]"

# The service echoes a statement list past its limit in one of two forms.
# Most answers write every field of every request object; the others write
# each object as its class name and a hash, for example
# "com.amazonaws.dynamodb.v20120810.BatchStatementRequest@8e2ac1ba" (8 of 40
# batch answers and 14 of 40 transaction answers, measured 2026-11-02).
# ExtendDB always writes the fields; a test accepts either form.
HASHED_CLASS = {
    "BatchStatementRequest": "com.amazonaws.dynamodb.v20120810.BatchStatementRequest",
    "ParameterizedStatement": "com.amazonaws.dynamodb.v20120810.ParameterizedStatement",
}


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """A table keyed on a string pk, holding the item pk = 'a'."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "a"}, "a": {"N": "1"}})
        yield name


@pytest.fixture(scope="module")
def client(dynamodb_client_no_validation):
    return dynamodb_client_no_validation


def _refusal(call, **kwargs) -> str:
    with pytest.raises(ClientError) as e:
        call(**kwargs)
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def _part(value: str, path: str, constraint: str) -> str:
    return f"Value '{value}' at '{path}' failed to satisfy constraint: {constraint}"


def _null(path: str) -> str:
    return f"Value null at '{path}' failed to satisfy constraint: Member must not be null"


class _Expected:
    """A refusal that holds an echoed statement list: equal to the text with
    every field written out, or to the same text with the list in its class
    and hash form."""

    def __init__(self, exact: str, pattern: str):
        self.exact, self.pattern = exact, pattern

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, str):
            return NotImplemented
        return other == self.exact or re.fullmatch(self.pattern, other) is not None

    def __repr__(self) -> str:
        return repr(self.exact)


def _list_part(kind: str, members: list[str], path: str, constraint: str) -> tuple[str, str]:
    """The part for a statement list past its limit, as its exact text and as
    a pattern for the class and hash form."""
    exact = _part(f"[{', '.join(members)}]", path, constraint)
    one = re.escape(HASHED_CLASS[kind]) + "@[0-9a-f]{1,8}"
    hashed = r"\[" + ", ".join([one] * len(members)) + r"\]"
    tail = f"' at '{path}' failed to satisfy constraint: {constraint}"
    return exact, re.escape("Value '") + hashed + re.escape(tail)


def _errors(*parts):
    head = (
        "1 validation error detected: "
        if len(parts) == 1
        else f"{len(parts)} validation errors detected: "
    )
    exact = head + "; ".join(p if isinstance(p, str) else p[0] for p in parts)
    if all(isinstance(p, str) for p in parts):
        return exact
    pattern = re.escape(head) + "; ".join(
        re.escape(p) if isinstance(p, str) else p[1] for p in parts
    )
    return _Expected(exact, pattern)


def _select(table: str) -> str:
    return f"SELECT * FROM \"{table}\" WHERE pk = 'a'"


def _long(table: str) -> str:
    """A statement one character past the limit."""
    head = _select(table)
    return head + " " * (MAX_LENGTH + 1 - len(head))


def _value(**fields: str) -> str:
    """A parameter as echoed, the given members set and the rest null."""
    shown = ", ".join(f"{name}={fields.get(name, 'null')}" for name in VALUE_FIELDS)
    return f"AttributeValue({shown})"


def _batch_member(statement: str, parameters="null", consistent_read="null", on_failure="null"):
    return (
        f"BatchStatementRequest(statement={statement}, parameters={parameters}, "
        f"consistentRead={consistent_read}, returnValuesOnConditionCheckFailure={on_failure})"
    )


def _transaction_member(statement: str, parameters="null", on_failure="null"):
    return (
        f"ParameterizedStatement(statement={statement}, parameters={parameters}, "
        f"returnValuesOnConditionCheckFailure={on_failure})"
    )


# --- one statement too long or too short ------------------------------------


def test_a_long_statement_in_a_batch_fails_the_request(client, table):
    long = _long(table)
    message = _refusal(client.batch_execute_statement, Statements=[{"Statement": long}])
    assert message == _errors(_part(long, "statements.1.member.statement", TOO_LONG))


def test_a_long_statement_in_a_transaction_fails_the_request(client, table):
    long = _long(table)
    message = _refusal(client.execute_transaction, TransactStatements=[{"Statement": long}])
    assert message == _errors(_part(long, "transactStatements.1.member.statement", TOO_LONG))


def test_an_empty_statement_in_a_batch_fails_the_request(client, table):
    statements = [{"Statement": ""}, {"Statement": _select(table)}]
    message = _refusal(client.batch_execute_statement, Statements=statements)
    assert message == _errors(_part("", "statements.1.member.statement", TOO_SHORT))


def test_an_empty_statement_in_a_transaction_fails_the_request(client, table):
    statements = [{"Statement": _select(table)}, {"Statement": ""}]
    message = _refusal(client.execute_transaction, TransactStatements=statements)
    assert message == _errors(_part("", "transactStatements.2.member.statement", TOO_SHORT))


def test_an_empty_statement_is_too_short(client):
    message = _refusal(client.execute_statement, Statement="")
    assert message == _errors(_part("", "statement", TOO_SHORT))


def test_the_length_counts_code_points(client, table):
    head = f"SELECT * FROM \"{table}\" WHERE pk = 'a' AND a = '"
    emoji = "\U0001F600"
    at_limit = head + emoji * (MAX_LENGTH - len(head) - 1) + "'"
    assert client.execute_statement(Statement=at_limit)["Items"] == []
    over = head + emoji * (MAX_LENGTH - len(head)) + "'"
    assert _refusal(client.execute_statement, Statement=over) == _errors(
        _part(over, "statement", TOO_LONG)
    )


# --- several failures in one request -----------------------------------------


def test_each_long_statement_in_a_batch_is_reported(client, table):
    long = _long(table)
    statements = [{"Statement": long}, {"Statement": long}]
    assert _refusal(client.batch_execute_statement, Statements=statements) == _errors(
        _part(long, "statements.1.member.statement", TOO_LONG),
        _part(long, "statements.2.member.statement", TOO_LONG),
    )


def test_an_empty_and_a_long_statement_in_a_batch(client, table):
    long = _long(table)
    statements = [{"Statement": ""}, {"Statement": long}]
    assert _refusal(client.batch_execute_statement, Statements=statements) == _errors(
        _part("", "statements.1.member.statement", TOO_SHORT),
        _part(long, "statements.2.member.statement", TOO_LONG),
    )


def test_a_batch_statement_before_its_parameters(client, table):
    long = _long(table)
    statements = [{"Statement": long, "Parameters": []}]
    assert _refusal(client.batch_execute_statement, Statements=statements) == _errors(
        _part(long, "statements.1.member.statement", TOO_LONG),
        _part("[]", "statements.1.member.parameters", TOO_SHORT),
    )


def test_a_transaction_statement_with_empty_parameters(client, table):
    statements = [{"Statement": _select(table), "Parameters": []}]
    assert _refusal(client.execute_transaction, TransactStatements=statements) == _errors(
        _part("[]", "transactStatements.1.member.parameters", TOO_SHORT),
    )


def test_each_long_statement_in_a_transaction_is_reported(client, table):
    long = _long(table)
    statements = [{"Statement": long}, {"Statement": long}]
    assert _refusal(client.execute_transaction, TransactStatements=statements) == _errors(
        _part(long, "transactStatements.1.member.statement", TOO_LONG),
        _part(long, "transactStatements.2.member.statement", TOO_LONG),
    )


def test_a_long_token_comes_before_the_statements(client, table):
    long, token = _long(table), "t" * 37
    message = _refusal(
        client.execute_transaction, TransactStatements=[{"Statement": long}], ClientRequestToken=token
    )
    assert message == _errors(
        _part(token, "clientRequestToken", "Member must have length less than or equal to 36"),
        _part(long, "transactStatements.1.member.statement", TOO_LONG),
    )


def test_execute_statement_reports_the_statement_before_its_parameters(client, table):
    long = _long(table)
    assert _refusal(client.execute_statement, Statement=long, Parameters=[]) == _errors(
        _part(long, "statement", TOO_LONG),
        _part("[]", "parameters", TOO_SHORT),
    )


def test_execute_statement_reports_the_limit_before_the_statement(client, table):
    long = _long(table)
    assert _refusal(client.execute_statement, Statement=long, Limit=0) == _errors(
        _part("0", "limit", "Member must have value greater than or equal to 1"),
        _part(long, "statement", TOO_LONG),
    )


def test_empty_parameters_are_refused(client, table):
    assert _refusal(client.execute_statement, Statement=_select(table), Parameters=[]) == (
        _errors(_part("[]", "parameters", TOO_SHORT))
    )


# --- a statement list past its limit -----------------------------------------


def test_a_batch_past_25_statements_is_echoed_before_a_long_statement(client, table):
    plain, long = _select(table), _long(table)
    statements = [{"Statement": plain}] * 25 + [{"Statement": long}]
    members = [_batch_member(plain)] * 25 + [_batch_member(long)]
    assert _refusal(client.batch_execute_statement, Statements=statements) == _errors(
        _list_part("BatchStatementRequest", members, "statements", "Member must have length less than or equal to 25"),
        _part(long, "statements.26.member.statement", TOO_LONG),
    )


def test_a_transaction_past_100_statements_is_echoed_before_a_long_statement(client, table):
    plain, long = _select(table), _long(table)
    statements = [{"Statement": plain}] * 100 + [{"Statement": long}]
    members = [_transaction_member(plain)] * 100 + [_transaction_member(long)]
    assert _refusal(client.execute_transaction, TransactStatements=statements) == _errors(
        _list_part("ParameterizedStatement", members, "transactStatements", "Member must have length less than or equal to 100"),
        _part(long, "transactStatements.101.member.statement", TOO_LONG),
    )


def test_a_long_token_comes_before_the_statement_count(client, table):
    plain, token = _select(table), "t" * 37
    message = _refusal(
        client.execute_transaction,
        TransactStatements=[{"Statement": plain}] * 101,
        ClientRequestToken=token,
    )
    members = [_transaction_member(plain)] * 101
    assert message == _errors(
        _part(token, "clientRequestToken", "Member must have length less than or equal to 36"),
        _list_part("ParameterizedStatement", members, "transactStatements", "Member must have length less than or equal to 100"),
    )


PARAMETERS = [
    {"S": "a"}, {"N": "1"}, {"B": b"x"}, {"BOOL": True}, {"NULL": True},
    {"L": [{"S": "x"}]}, {"M": {"k": {"S": "v"}}}, {"SS": ["a", "b"]},
    {"NS": ["1"]}, {"BS": [b"x"]},
]
ECHOED_PARAMETERS = [
    _value(s="a"), _value(n="1"), _value(b=BUFFER), _value(bOOL="true"), _value(nULL="true"),
    _value(l=f"[{_value(s='x')}]"), _value(m=f"{{k={_value(s='v')}}}"), _value(sS="[a, b]"),
    _value(nS="[1]"), _value(bS=f"[{BUFFER}]"),
]


def test_a_batch_past_its_limit_echoes_every_field_of_a_statement(client, table):
    plain = _select(table)
    rich = f'SELECT * FROM "{table}" WHERE pk = ?'
    first = {
        "Statement": rich,
        "Parameters": PARAMETERS,
        "ConsistentRead": True,
        "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
    }
    message = _refusal(
        client.batch_execute_statement, Statements=[first] + [{"Statement": plain}] * 25
    )
    members = (
        [_batch_member(rich, f"[{', '.join(ECHOED_PARAMETERS)}]", "true", "ALL_OLD")]
        + [_batch_member(plain)] * 25
    )
    assert message == _errors(
        _list_part("BatchStatementRequest", members, "statements", "Member must have length less than or equal to 25")
    )


def test_a_batch_past_its_limit_echoes_consistent_read_false(client, table):
    plain = _select(table)
    statements = [{"Statement": plain, "ConsistentRead": False}] * 26
    members = [_batch_member(plain, consistent_read="false")] * 26
    assert _refusal(client.batch_execute_statement, Statements=statements) == _errors(
        _list_part("BatchStatementRequest", members, "statements", "Member must have length less than or equal to 25")
    )


def test_a_transaction_past_its_limit_echoes_every_field_of_a_statement(client, table):
    plain = _select(table)
    rich = f'SELECT * FROM "{table}" WHERE pk = ?'
    first = {
        "Statement": rich,
        "Parameters": PARAMETERS[:2],
        "ReturnValuesOnConditionCheckFailure": "NONE",
    }
    message = _refusal(
        client.execute_transaction, TransactStatements=[first] + [{"Statement": plain}] * 100
    )
    members = (
        [_transaction_member(rich, f"[{', '.join(ECHOED_PARAMETERS[:2])}]", "NONE")]
        + [_transaction_member(plain)] * 100
    )
    assert message == _errors(
        _list_part("ParameterizedStatement", members, "transactStatements", "Member must have length less than or equal to 100")
    )


# --- every field of a request in one refusal ---------------------------------


@pytest.mark.parametrize(
    "token, constraint",
    [("", TOO_SHORT), ("n" * 32769, "Member must have length less than or equal to 32768")],
    ids=["empty token", "long token"],
)
def test_execute_statement_reports_every_field_in_order(client, table, token, constraint):
    long = _long(table)
    message = _refusal(
        client.execute_statement,
        Statement=long,
        Parameters=[],
        Limit=0,
        NextToken=token,
        ReturnConsumedCapacity="X",
        ReturnValuesOnConditionCheckFailure="X",
        ConsistentRead=True,
    )
    assert message == _errors(
        _part("X", "returnValuesOnConditionCheckFailure", ON_FAILURE_SET),
        _part(token, "nextToken", constraint),
        _part("0", "limit", AT_LEAST_ONE),
        _part(long, "statement", TOO_LONG),
        _part("X", "returnConsumedCapacity", CAPACITY_SET),
        _part("[]", "parameters", TOO_SHORT),
    )


def test_a_batch_reports_its_capacity_then_its_count_then_each_member(client, table):
    plain, long = _select(table), _long(table)
    first = {
        "Statement": long,
        "Parameters": [],
        "ConsistentRead": True,
        "ReturnValuesOnConditionCheckFailure": "X",
    }
    message = _refusal(
        client.batch_execute_statement,
        Statements=[first] + [{"Statement": plain}] * 25,
        ReturnConsumedCapacity="X",
    )
    members = [_batch_member(long, "[]", "true", "X")] + [_batch_member(plain)] * 25
    assert message == _errors(
        _part("X", "returnConsumedCapacity", CAPACITY_SET),
        _list_part("BatchStatementRequest", members, "statements", "Member must have length less than or equal to 25"),
        _part("X", "statements.1.member.returnValuesOnConditionCheckFailure", ON_FAILURE_SET),
        _part(long, "statements.1.member.statement", TOO_LONG),
        _part("[]", "statements.1.member.parameters", TOO_SHORT),
    )


def test_a_transaction_reports_its_capacity_after_its_members(client, table):
    plain, long, token = _select(table), _long(table), "t" * 37
    first = {"Statement": long, "Parameters": [], "ReturnValuesOnConditionCheckFailure": "X"}
    message = _refusal(
        client.execute_transaction,
        TransactStatements=[first] + [{"Statement": plain}] * 100,
        ClientRequestToken=token,
        ReturnConsumedCapacity="X",
    )
    members = [_transaction_member(long, "[]", "X")] + [_transaction_member(plain)] * 100
    assert message == _errors(
        _part(token, "clientRequestToken", TOKEN_TOO_LONG),
        _list_part("ParameterizedStatement", members, "transactStatements", "Member must have length less than or equal to 100"),
        _part("X", "transactStatements.1.member.returnValuesOnConditionCheckFailure", ON_FAILURE_SET),
        _part(long, "transactStatements.1.member.statement", TOO_LONG),
        _part("[]", "transactStatements.1.member.parameters", TOO_SHORT),
        _part("X", "returnConsumedCapacity", CAPACITY_SET),
    )


def test_a_refusal_lists_the_first_ten_failed_constraints(client):
    statements = [{"Statement": "", "Parameters": []}] * 25
    message = _refusal(client.batch_execute_statement, Statements=statements)
    parts = []
    for i in range(1, 6):
        parts.append(_part("", f"statements.{i}.member.statement", TOO_SHORT))
        parts.append(_part("[]", f"statements.{i}.member.parameters", TOO_SHORT))
    assert message == _errors(*parts)
    assert message.startswith("10 validation errors detected: ")


# --- a member the request does not carry ------------------------------------


def test_execute_statement_without_a_statement(client):
    assert _refusal(client.execute_statement) == _errors(_null("statement"))


def test_a_missing_statement_joins_the_other_parts(client):
    assert _refusal(client.execute_statement, Limit=0, Parameters=[]) == _errors(
        _part("0", "limit", AT_LEAST_ONE),
        _null("statement"),
        _part("[]", "parameters", TOO_SHORT),
    )


def test_a_batch_without_statements(client):
    assert _refusal(client.batch_execute_statement) == _errors(_null("statements"))
    assert _refusal(client.batch_execute_statement, ReturnConsumedCapacity="X") == _errors(
        _part("X", "returnConsumedCapacity", CAPACITY_SET),
        _null("statements"),
    )


def test_a_batch_member_without_a_statement(client):
    message = _refusal(client.batch_execute_statement, Statements=[{"Parameters": [{"S": "a"}]}])
    assert message == _errors(_null("statements.1.member.statement"))
    message = _refusal(client.batch_execute_statement, Statements=[{"Parameters": []}])
    assert message == _errors(
        _null("statements.1.member.statement"),
        _part("[]", "statements.1.member.parameters", TOO_SHORT),
    )


def test_a_transaction_without_statements(client):
    assert _refusal(client.execute_transaction) == _errors(_null("transactStatements"))
    token = "t" * 37
    assert _refusal(client.execute_transaction, ClientRequestToken=token) == _errors(
        _part(token, "clientRequestToken", TOKEN_TOO_LONG),
        _null("transactStatements"),
    )


def test_a_transaction_member_without_a_statement(client):
    message = _refusal(
        client.execute_transaction, TransactStatements=[{"Parameters": [{"S": "a"}]}]
    )
    assert message == _errors(_null("transactStatements.1.member.statement"))


# --- the enum fields ---------------------------------------------------------


def test_an_unknown_return_values_on_failure_is_refused(client, table):
    plain = _select(table)
    message = _refusal(
        client.execute_statement, Statement=plain, ReturnValuesOnConditionCheckFailure="X"
    )
    assert message == _errors(_part("X", "returnValuesOnConditionCheckFailure", ON_FAILURE_SET))
    member = {"Statement": plain, "ReturnValuesOnConditionCheckFailure": "X"}
    message = _refusal(client.batch_execute_statement, Statements=[member])
    assert message == _errors(
        _part("X", "statements.1.member.returnValuesOnConditionCheckFailure", ON_FAILURE_SET)
    )
    message = _refusal(client.execute_transaction, TransactStatements=[member])
    assert message == _errors(
        _part("X", "transactStatements.1.member.returnValuesOnConditionCheckFailure", ON_FAILURE_SET)
    )


def test_an_unknown_consumed_capacity_fails_the_request(client, table):
    plain = _select(table)
    expected = _errors(_part("X", "returnConsumedCapacity", CAPACITY_SET))
    assert _refusal(
        client.execute_statement, Statement=plain, ReturnConsumedCapacity="X"
    ) == expected
    assert _refusal(
        client.batch_execute_statement, Statements=[{"Statement": plain}], ReturnConsumedCapacity="X"
    ) == expected
    assert _refusal(
        client.execute_transaction,
        TransactStatements=[{"Statement": plain}],
        ReturnConsumedCapacity="X",
    ) == expected


# --- the two tokens ----------------------------------------------------------


def test_an_empty_next_token_is_too_short(client, table):
    message = _refusal(client.execute_statement, Statement=_select(table), NextToken="")
    assert message == _errors(_part("", "nextToken", TOO_SHORT))


def test_a_next_token_past_32768_characters_is_too_long(client, table):
    token = "n" * 32769
    message = _refusal(client.execute_statement, Statement=_select(table), NextToken=token)
    assert message == _errors(
        _part(token, "nextToken", "Member must have length less than or equal to 32768")
    )


@pytest.mark.parametrize(
    "token",
    ["n" * 32768, "x", "a b!", "bm90LWEtdG9rZW4="],
    ids=["32768 characters", "one character", "a space and a bang", "base64 text"],
)
def test_a_next_token_that_does_not_decode_is_invalid(client, table, token):
    message = _refusal(client.execute_statement, Statement=_select(table), NextToken=token)
    assert message == "Invalid NextToken"


def test_an_empty_client_request_token_is_too_short(client, table):
    message = _refusal(
        client.execute_transaction,
        TransactStatements=[{"Statement": _select(table)}],
        ClientRequestToken="",
    )
    assert message == _errors(_part("", "clientRequestToken", TOO_SHORT))


# --- the order of a map's members in the echo ---------------------------------
#
# The service prints a map's members in the order of the buckets a Java
# HashMap sorts them into. These maps have no two keys in one bucket, so the
# order does not depend on the order the request lists them in.


def _map(*names: str) -> dict:
    return {"M": {name: {"S": name} for name in names}}


def _echoed_map(*names: str) -> str:
    members = ", ".join(f"{name}={_value(s=name)}" for name in names)
    return f"[{_value(m='{' + members + '}')}]"


@pytest.mark.parametrize(
    "sent, printed",
    [(("c", "a", "p"), ("p", "a", "c")),
     (("zeta", "alpha", "mid", "beta"), ("zeta", "mid", "beta", "alpha")),
     (("m0", "m1", "m2"), ("m1", "m2", "m0")),
     (("k0", "k1", "k2", "k3", "k4", "k5"), ("k3", "k4", "k5", "k0", "k1", "k2"))],
    ids=["three keys", "four keys", "three keys in four buckets", "six keys in eight buckets"],
)
def test_a_batch_past_its_limit_echoes_a_map_in_the_service_order(client, table, sent, printed):
    plain = _select(table)
    first = {"Statement": plain, "Parameters": [_map(*sent)]}
    message = _refusal(
        client.batch_execute_statement, Statements=[first] + [{"Statement": plain}] * 25
    )
    members = [_batch_member(plain, _echoed_map(*printed))] + [_batch_member(plain)] * 25
    assert message == _errors(
        _list_part("BatchStatementRequest", members, "statements", "Member must have length less than or equal to 25")
    )


def test_a_transaction_past_its_limit_echoes_a_map_in_the_service_order(client, table):
    plain = _select(table)
    first = {"Statement": plain, "Parameters": [_map("c", "a", "p")]}
    message = _refusal(
        client.execute_transaction, TransactStatements=[first] + [{"Statement": plain}] * 100
    )
    members = (
        [_transaction_member(plain, _echoed_map("p", "a", "c"))] + [_transaction_member(plain)] * 100
    )
    assert message == _errors(
        _list_part("ParameterizedStatement", members, "transactStatements", "Member must have length less than or equal to 100")
    )
