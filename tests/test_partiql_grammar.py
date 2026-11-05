# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for how PartiQL statements are read: comments,
number text, IS tests, the syntax error a statement gets where it breaks off,
table aliases, reserved words in paths, OR branches that overlap on the sort
key, statement ends, unquoted and empty names, the table name rule, operators
a condition refuses, and bare expressions.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table, wait_for_active

WF = "Statement wasn't well formed, can't be processed: "
ALIASING = "Aliasing is not supported"
OVERLAP = "Overlapping conditions with range keys are not supported in where clause"


@pytest.fixture(scope="module")
def stable(dynamodb_client):
    """A table keyed on a string pk."""
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def ntable(dynamodb_client):
    """A table keyed on a number pk."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[{"AttributeName": "pk", "AttributeType": "N"}],
        key_schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def ctable(dynamodb_client):
    """A table keyed on a string pk and a string sk."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        yield name


def _select(client, statement, **kwargs):
    return client.execute_statement(Statement=statement, ConsistentRead=True, **kwargs).get(
        "Items", []
    )


def _err(client, statement, **kwargs):
    with pytest.raises(ClientError) as e:
        client.execute_statement(Statement=statement, **kwargs)
    err = e.value.response["Error"]
    return err["Code"], err["Message"]


def _message(client, statement, **kwargs):
    code, message = _err(client, statement, **kwargs)
    assert code == "ValidationException", message
    return message


# ---- comments ----------------------------------------------------------------


@pytest.mark.parametrize(
    "template",
    [
        "SELECT * FROM \"{t}\" WHERE pk = 'cm-a' -- a note",
        "SELECT * FROM \"{t}\" -- a note\nWHERE pk = 'cm-a'",
        "-- a note\nSELECT * FROM \"{t}\" WHERE pk = 'cm-a'",
        "SELECT * FROM \"{t}\" /* a note */ WHERE pk = 'cm-a'",
        "SELECT * FROM \"{t}\" WHERE pk = 'cm-a' /* a note */",
        "SELECT * FROM \"{t}\" /* a\nlonger note */ WHERE pk = 'cm-a'",
    ],
)
def test_comments_read_as_whitespace(dynamodb_client, stable, template):
    dynamodb_client.put_item(TableName=stable, Item={"pk": {"S": "cm-a"}})
    dynamodb_client.put_item(TableName=stable, Item={"pk": {"S": "cm-b"}})
    items = _select(dynamodb_client, template.format(t=stable))
    assert [i["pk"]["S"] for i in items] == ["cm-a"]


@pytest.mark.parametrize(
    "tail",
    [
        "# note",
        "/* note",
        "'unterminated",
        '"unterminated',
        "& 1",
        "~ 1",
        "\\ 1",
    ],
)
def test_text_the_lexer_cannot_read_has_nothing_after_the_prefix(dynamodb_client, stable, tail):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'x' AND a = 1 {tail}"
    assert _message(dynamodb_client, statement) == WF


# ---- numbers -----------------------------------------------------------------


def test_numbers_written_from_the_decimal_point(dynamodb_client, ntable):
    for n in ("7.3", "-7.3"):
        dynamodb_client.put_item(TableName=ntable, Item={"pk": {"N": n}})
    assert [i["pk"]["N"] for i in _select(dynamodb_client, f'SELECT * FROM "{ntable}" WHERE pk = .73e1')] == ["7.3"]
    assert [i["pk"]["N"] for i in _select(dynamodb_client, f'SELECT * FROM "{ntable}" WHERE pk = -.73e1')] == ["-7.3"]
    dynamodb_client.execute_statement(Statement=f"INSERT INTO \"{ntable}\" VALUE {{'pk': .7325e1}}")
    got = dynamodb_client.get_item(TableName=ntable, Key={"pk": {"N": "7.325"}}, ConsistentRead=True)
    assert "Item" in got


def test_a_sign_before_a_number_belongs_to_the_number(dynamodb_client, ntable):
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{ntable}\" VALUE {{'pk': -41, 'm': {{'x': -2}}, 'l': [-3, + 4]}}"
    )
    item = dynamodb_client.get_item(TableName=ntable, Key={"pk": {"N": "-41"}}, ConsistentRead=True)["Item"]
    assert item["m"] == {"M": {"x": {"N": "-2"}}}
    assert item["l"] == {"L": [{"N": "-3"}, {"N": "4"}]}
    dynamodb_client.execute_statement(Statement=f'UPDATE "{ntable}" SET a = - 5 WHERE pk = -41')
    items = _select(dynamodb_client, f'SELECT a FROM "{ntable}" WHERE pk = - 41 AND a = -5')
    assert items == [{"a": {"N": "-5"}}]


# ---- IS ----------------------------------------------------------------------


@pytest.fixture(scope="module")
def typed_item(dynamodb_client, stable):
    dynamodb_client.put_item(
        TableName=stable,
        Item={
            "pk": {"S": "is-item"},
            "nul": {"NULL": True},
            "s": {"S": "str"},
            "n": {"N": "1"},
            "b": {"B": b"\x01"},
            "flag": {"BOOL": True},
            "m": {"M": {"x": {"S": "y"}}},
            "l": {"L": [{"S": "y"}]},
        },
    )
    return "is-item"


@pytest.mark.parametrize(
    ("condition", "matches"),
    [
        ("nul IS NULL", True),
        ("s IS NULL", False),
        ("absent IS NULL", False),
        ("s IS NOT NULL", True),
        ("nul IS NOT NULL", False),
        ("absent IS NOT NULL", True),
        ("flag IS BOOL", True),
        ("flag IS BOOLEAN", True),
        ("s IS STRING", True),
        ("s IS BOOL", False),
        ("s IS NOT BOOL", True),
        ("n IS NUMERIC", True),
        ("b IS BLOB", True),
        ("m IS STRUCT", True),
        ("m IS TUPLE", True),
        ("l IS LIST", True),
        ("absent IS MISSING", True),
        ("s IS NOT MISSING", True),
    ],
)
def test_is_filters(dynamodb_client, stable, typed_item, condition, matches):
    items = _select(dynamodb_client, f"SELECT pk FROM \"{stable}\" WHERE pk = '{typed_item}' AND {condition}")
    assert items == ([{"pk": {"S": typed_item}}] if matches else [])


@pytest.mark.parametrize(
    ("type_name", "service_name"),
    [
        ("INT4", "Integer4Type"),
        ("INT", "IntegerType"),
        ("SMALLINT", "SmallintType"),
        ("BIGINT", "Integer8Type"),
        ("FLOAT", "FloatType"),
        ("DECIMAL", "DecimalType"),
        ("TIMESTAMP", "TimestampType"),
        ("VARCHAR", "CharacterVaryingType"),
        ("SYMBOL", "SymbolType"),
        ("BAG", "BagType"),
    ],
)
def test_an_is_filter_on_a_type_with_no_filter(dynamodb_client, stable, type_name, service_name):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'x' AND a IS {type_name}"
    # The refusal carries the position of the IS test's left side.
    column = statement.index(" a IS ") + 2
    assert _message(dynamodb_client, statement) == (
        f"Unsupported data type for IS operator: {service_name}. Supported data types are "
        f"[MISSING, NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] at 1:{column}:1"
    )


def test_an_is_filter_reads_the_length_after_the_type(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'x' AND sv IS VARCHAR(10)"
    column = statement.index(" sv IS ") + 2
    assert _message(dynamodb_client, statement) == (
        "Unsupported data type for IS operator: CharacterVaryingType. Supported data types are "
        f"[MISSING, NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] at 1:{column}:2"
    )


def test_an_is_test_on_a_parameter(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'x' AND ? IS NULL"
    assert _message(dynamodb_client, statement, Parameters=[{"S": "v"}]) == (
        "The left hand side argument to IS must be a Variable Reference, Literal, or Path"
    )


def test_an_is_test_on_a_literal_runs(dynamodb_client, stable):
    dynamodb_client.put_item(TableName=stable, Item={"pk": {"S": "is-lit"}})
    items = _select(dynamodb_client, f"SELECT pk FROM \"{stable}\" WHERE pk = 'is-lit' AND 'a' IS STRING")
    assert items == [{"pk": {"S": "is-lit"}}]


def test_an_is_test_as_a_set_value(dynamodb_client, stable):
    statement = f"UPDATE \"{stable}\" SET a = b IS NULL WHERE pk = 'x'"
    assert _message(dynamodb_client, statement) == "IS operator is not supported in update expressions"


@pytest.mark.parametrize(
    "type_name",
    [
        "MISSING", "NULL", "BOOL", "BOOLEAN", "SMALLINT", "INT2", "INTEGER2", "INT4",
        "INTEGER4", "INT8", "INTEGER8", "BIGINT", "INT", "INTEGER", "FLOAT", "REAL",
        "DOUBLE PRECISION", "DECIMAL", "DEC", "NUMERIC", "TIMESTAMP", "DATE", "TIME",
        "CHARACTER", "CHAR", "CHARACTER VARYING", "VARCHAR", "STRING", "SYMBOL", "CLOB",
        "BLOB", "STRUCT", "TUPLE", "LIST", "SEXP", "BAG", "ANY",
    ],
)
def test_a_bare_is_type_test_is_refused_by_name(dynamodb_client, type_name):
    assert _message(dynamodb_client, f"x IS {type_name}") == "Unsupported operation: IsType"


@pytest.mark.parametrize("statement", ["x IS FOO", "x IS DOUBLE", "x IS VARYING"])
def test_is_with_a_name_that_is_not_a_type(dynamodb_client, statement):
    assert _message(dynamodb_client, statement) == f"{WF}Expected type name"


# ---- where a statement breaks off --------------------------------------------


def test_an_empty_where_in_a_transaction_and_a_batch(dynamodb_client, stable):
    statement = f'SELECT * FROM "{stable}" WHERE'
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    assert e.value.response["Error"]["Code"] == "ValidationException"
    assert e.value.response["Error"]["Message"] == (
        f"Validation failed in TransactStatements[0]: {WF}Unexpected term"
    )
    resp = dynamodb_client.batch_execute_statement(Statements=[{"Statement": statement}])
    assert resp["Responses"] == [{"Error": {"Code": "ValidationError", "Message": f"{WF}Unexpected term"}}]


def test_an_alias_in_a_transaction_carries_the_statement_position(dynamodb_client, stable):
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(
            TransactStatements=[
                {"Statement": f"SELECT * FROM \"{stable}\" WHERE pk = 'a'"},
                {"Statement": f"SELECT * FROM \"{stable}\" t WHERE pk = 'b'"},
            ]
        )
    assert e.value.response["Error"]["Message"] == f"Validation failed in TransactStatements[1]: {ALIASING}"


@pytest.mark.parametrize(
    "condition",
    ["pk = 'a' AND", "pk = 'a' OR", "pk =", "pk = 'a' AND n <", "pk = 'a' AND n = 1 +", "pk = 'a' AND n IN", "pk = 'a' AND n BETWEEN"],
)
def test_an_infix_operator_with_nothing_after_it(dynamodb_client, stable, condition):
    statement = f'SELECT * FROM "{stable}" WHERE {condition}'
    assert _message(dynamodb_client, statement) == f"{WF}Missing right-hand side expression of infix operator"


@pytest.mark.parametrize(
    ("condition", "reason"),
    [
        ("ORDER BY pk", "Unexpected keyword"),
        ("pk = 'a' AND value = 1", "Unexpected keyword"),
        ("IS NULL", "Unexpected operator"),
        ("pk = LIKE", "Unexpected operator"),
        ("= 1", "Unexpected operator"),
        ("pk = 'a' AND , 1", "Unexpected term"),
        ("pk = 'a' AND asc = 1", "Unexpected term"),
        ("pk = 'a' AND;", "Unexpected term"),
    ],
)
def test_a_term_that_cannot_start(dynamodb_client, stable, condition, reason):
    assert _message(dynamodb_client, f'SELECT * FROM "{stable}" WHERE {condition}') == f"{WF}{reason}"


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM",
        "DELETE FROM WHERE pk = 'a'",
        "UPDATE SET a = 1 WHERE pk = 'a'",
        "UPDATE WHERE pk = 'a'",
        "INSERT INTO VALUE {'pk': 'a'}",
        "INSERT INTO",
    ],
)
def test_a_write_with_no_table(dynamodb_client, statement):
    assert _message(dynamodb_client, statement) == f"{WF}Expected identifier for simple path"


@pytest.mark.parametrize(
    ("statement", "reason"),
    [
        ("SELECT * FROM", "Unexpected term"),
        ("SELECT * FROM;", "Unexpected term"),
        ("SELECT * FROM WHERE pk = 'a'", "Unexpected keyword"),
    ],
)
def test_a_select_with_no_table(dynamodb_client, statement, reason):
    assert _message(dynamodb_client, statement) == f"{WF}{reason}"


# ---- aliases -----------------------------------------------------------------


@pytest.mark.parametrize(
    "template",
    [
        "SELECT * FROM \"{t}\" t WHERE pk = 'al'",
        "SELECT * FROM \"{t}\" AS t WHERE pk = 'al'",
        "SELECT * FROM \"{t}\" \"t\" WHERE pk = 'al'",
        "SELECT pk AS k FROM \"{t}\" WHERE pk = 'al'",
        "SELECT pk k FROM \"{t}\" WHERE pk = 'al'",
        "UPDATE \"{t}\" t SET a = 3 WHERE pk = 'al'",
        "UPDATE \"{t}\" t SET a = 3",
        "DELETE FROM \"{t}\" t WHERE pk = 'al'",
        "DELETE FROM \"{t}\" AS t WHERE pk = 'al'",
    ],
)
def test_an_alias_is_refused_and_nothing_runs(dynamodb_client, stable, template):
    dynamodb_client.put_item(TableName=stable, Item={"pk": {"S": "al"}})
    assert _message(dynamodb_client, template.format(t=stable)) == ALIASING
    item = dynamodb_client.get_item(TableName=stable, Key={"pk": {"S": "al"}}, ConsistentRead=True)
    assert item["Item"] == {"pk": {"S": "al"}}


def test_an_alias_on_a_table_that_does_not_exist(dynamodb_client, stable):
    assert _message(dynamodb_client, f"SELECT * FROM \"{stable}-absent\" t WHERE pk = 'x'") == ALIASING


def test_a_syntax_error_is_reported_ahead_of_an_alias(dynamodb_client, stable):
    assert _message(dynamodb_client, f'SELECT * FROM "{stable}" t WHERE') == f"{WF}Unexpected term"


def test_an_alias_on_an_insert_is_a_leftover_token(dynamodb_client, stable):
    statement = f"INSERT INTO \"{stable}\" t VALUE {{'pk': 'al-ins'}}"
    assert _message(dynamodb_client, statement) == f"{WF}Unexpected token after expression"
    item = dynamodb_client.get_item(TableName=stable, Key={"pk": {"S": "al-ins"}}, ConsistentRead=True)
    assert "Item" not in item


# ---- reserved words in paths -------------------------------------------------

# Every reserved word the parser knows, read as an attribute name after a dot.
RESERVED = """
absolute action add all allocate alter any are assertion authorization avg bag begin
bigint bit bit_length blob bool boolean can_cast can_lossless_cast cascade cascaded
case cast catalog char char_length character character_length check clob close
coalesce collate collation column commit conflict connect connection constraint
constraints continue convert corresponding count create cross current current_date
current_time current_timestamp current_user cursor date date_add date_diff deallocate
dec decimal declare default deferrable deferred delete describe descriptor diagnostics
disconnect distinct do domain double drop else end escape exception exec execute
exists external extract fetch float foreign found from full get global go goto grant
group having identity immediate index indicator initially inner input insensitive
insert int int2 int4 int8 integer integer2 integer4 integer8 interval into isolation
join key language left let level limit list local lower match max min modified module
names national natural nchar new next no nothing nullif numeric octet_length of offset
old on only open option order outer output overlaps pad partial pivot position
precision prepare preserve primary prior privileges procedure public read real
references relative remove restrict returning revoke right rollback rows schema scroll
section select session session_user set sexp size smallint some space sql sqlcode
sqlerror sqlstate string struct substring sum symbol system_user table temporary then
time timestamp to transaction translate translation trim tuple unique unknown unpivot
update upper usage user using value values varchar varying view when whenever where
with work write zone
and or not is in like between union except intersect
as at by asc desc for nulls first last
true false null missing
""".split()


@pytest.mark.parametrize("word", RESERVED)
def test_a_reserved_word_after_a_dot(dynamodb_client, stable, word):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'x' AND attribute_exists(m.{word})"
    assert _message(dynamodb_client, statement) == f"{WF}Invalid path dot component"


@pytest.mark.parametrize("word", ["name", "type", "status", "data", "year", "month", "hour", "item"])
def test_an_ordinary_word_after_a_dot_is_a_name(dynamodb_client, stable, word):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'x' AND attribute_exists(m.{word})"
    assert _select(dynamodb_client, statement) == []


def test_a_quoted_reserved_word_after_a_dot_is_a_name(dynamodb_client, stable):
    dynamodb_client.put_item(TableName=stable, Item={"pk": {"S": "rw"}, "m": {"M": {"inner": {"S": "v"}}}})
    items = _select(dynamodb_client, f"SELECT pk FROM \"{stable}\" WHERE pk = 'rw' AND attribute_exists(m.\"inner\")")
    assert items == [{"pk": {"S": "rw"}}]


@pytest.mark.parametrize(
    ("template", "reason"),
    [
        # SELECT VALUE reads an expression, here a FROM-led form with no
        # data manipulation after it.
        ("SELECT value FROM \"{t}\" WHERE pk = 'x'", "Expected data manipulation"),
        ("UPDATE \"{t}\" SET value = 1 WHERE pk = 'x'", "Expected identifier for simple path"),
        ("SELECT * FROM \"{t}\" WHERE pk = 'x' AND attribute_exists(value)", "Unexpected keyword"),
    ],
)
def test_a_reserved_word_where_an_attribute_name_starts(dynamodb_client, stable, template, reason):
    assert _message(dynamodb_client, template.format(t=stable)) == f"{WF}{reason}"


# ---- empty FROM names --------------------------------------------------------


def test_an_empty_table_name_is_refused_before_the_table_is_looked_up(dynamodb_client):
    assert _message(dynamodb_client, "SELECT * FROM \"\" WHERE pk = 'x'") == "Path component cannot be an empty string"


# ---- OR branches that overlap on the sort key --------------------------------


@pytest.fixture(scope="module")
def overlap_items(dynamodb_client, ctable):
    for sk in ("1", "2", "3"):
        dynamodb_client.put_item(TableName=ctable, Item={"pk": {"S": "ov"}, "sk": {"S": sk}})
    dynamodb_client.put_item(TableName=ctable, Item={"pk": {"S": "ov2"}, "sk": {"S": "1"}})
    return "ov"


@pytest.mark.parametrize(
    "condition",
    [
        "pk = 'ov' AND x = '1' OR pk = 'ov' AND sk = '2'",
        "pk = 'ov' AND x <> '1' OR pk = 'ov' AND sk = '2'",
        "pk = 'ov' OR pk = 'ov' AND sk = '2'",
        "pk = 'ov' AND sk <= '2' OR pk = 'ov' AND sk >= '2'",
        "pk = 'ov' AND sk BETWEEN '1' AND '2' OR pk = 'ov' AND sk = '2'",
        "pk = 'ov' AND sk > '1' OR pk = 'ov' AND sk = '3'",
        "pk = 'ov' AND begins_with(sk, '2') OR pk = 'ov' AND sk = '2'",
        "pk = 'ov' AND (sk = '1' OR x = '2')",
        "pk IN ['ov', 'ov']",
    ],
)
def test_overlapping_branches_are_refused(dynamodb_client, ctable, overlap_items, condition):
    assert _message(dynamodb_client, f'SELECT * FROM "{ctable}" WHERE {condition}') == OVERLAP


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        ("pk = 'ov' AND sk < '2' OR pk = 'ov' AND sk > '2'", [("ov", "1"), ("ov", "3")]),
        ("pk = 'ov' AND sk = '1' OR pk = 'ov' AND sk = '3'", [("ov", "1"), ("ov", "3")]),
        ("pk = 'ov' AND sk IN ['1', '3']", [("ov", "1"), ("ov", "3")]),
        ("pk = 'ov' AND sk = '1' OR pk = 'ov2' AND sk = '1'", [("ov", "1"), ("ov2", "1")]),
        ("pk = 'ov' OR pk = 'ov2'", [("ov", "1"), ("ov", "2"), ("ov", "3"), ("ov2", "1")]),
        ("pk = 'ov' AND sk = '1' OR x = '2'", [("ov", "1")]),
        ("pk = 'ov' AND begins_with(sk, '1') OR pk = 'ov' AND sk = '2'", [("ov", "1"), ("ov", "2")]),
    ],
)
def test_branches_that_do_not_overlap_run(dynamodb_client, ctable, overlap_items, condition, expected):
    items = _select(dynamodb_client, f'SELECT * FROM "{ctable}" WHERE {condition}')
    assert sorted((i["pk"]["S"], i["sk"]["S"]) for i in items) == expected


# ---- statement ends, unquoted names, INSERT forms ----------------------------


@pytest.fixture(scope="module")
def word_table(dynamodb_client):
    """A table whose name is a plain word, for unquoted references."""
    name = f"extenddb_test_{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    try:
        yield name
    finally:
        dynamodb_client.delete_table(TableName=name)


def test_unquoted_table_names(dynamodb_client, word_table):
    t = word_table
    dynamodb_client.execute_statement(Statement=f"INSERT INTO {t} VALUE {{'pk': 'uq'}}")
    assert _select(dynamodb_client, f"SELECT pk FROM {t} WHERE pk = 'uq'") == [{"pk": {"S": "uq"}}]
    dynamodb_client.execute_statement(Statement=f"UPDATE {t} SET a = 1 WHERE pk = 'uq'")
    assert _select(dynamodb_client, f"SELECT a FROM {t} WHERE pk = 'uq'") == [{"a": {"N": "1"}}]
    dynamodb_client.execute_statement(Statement=f"DELETE FROM {t} WHERE pk = 'uq'")
    assert _select(dynamodb_client, f"SELECT pk FROM {t} WHERE pk = 'uq'") == []
    assert _err(dynamodb_client, f"SELECT * FROM {t}_absent WHERE pk = 'uq'") == (
        "ResourceNotFoundException",
        "Requested resource not found",
    )


def test_a_statement_may_end_with_a_semicolon(dynamodb_client, stable):
    dynamodb_client.put_item(TableName=stable, Item={"pk": {"S": "semi"}})
    assert _select(dynamodb_client, f"SELECT pk FROM \"{stable}\" WHERE pk = 'semi';") == [
        {"pk": {"S": "semi"}}
    ]
    dynamodb_client.execute_statement(Statement=f"UPDATE \"{stable}\" SET a = 2 WHERE pk = 'semi';")
    assert _message(dynamodb_client, "x IS INT4;") == "Unsupported operation: IsType"


@pytest.mark.parametrize("tail", ["{'pk': 'x'}", "t", "'x'"])
def test_an_insert_without_value_is_refused_as_many_items(dynamodb_client, stable, tail):
    assert _message(dynamodb_client, f"INSERT INTO \"{stable}\" {tail}") == (
        "Unsupported operation: Inserting multiple items in a single statement is not "
        "supported, use \"INSERT INTO tableName VALUE item\" instead"
    )


def test_an_insert_with_nothing_after_the_table(dynamodb_client, stable):
    assert _message(dynamodb_client, f"INSERT INTO \"{stable}\"") == f"{WF}Unexpected term"


# ---- aliases, ranges and operators -------------------------------------------


@pytest.mark.parametrize(
    "template",
    ["SELECT pk AS FROM \"{t}\" WHERE pk = 'p'", "SELECT * FROM \"{t}\" AS WHERE pk = 'p'"],
)
def test_as_with_no_name_after_it(dynamodb_client, stable, template):
    assert _message(dynamodb_client, template.format(t=stable)) == f"{WF}Expected identifier for AS-alias"


@pytest.mark.parametrize("word", ["AT", "BY"])
def test_at_and_by_after_a_table_are_aliases(dynamodb_client, stable, word):
    statement = f"SELECT * FROM \"{stable}\" {word} x WHERE pk = 'p'"
    assert _message(dynamodb_client, statement) == ALIASING


def test_an_alias_is_refused_ahead_of_an_empty_index_name(dynamodb_client, stable):
    assert _message(dynamodb_client, f"SELECT * FROM \"{stable}\".\"\" t WHERE pk = 'p'") == ALIASING


@pytest.mark.parametrize(
    ("tail", "reason"),
    [("n BETWEEN 1 AND", "Expected expression after AND"), ("n BETWEEN 1", "Expected AND keyword")],
)
def test_a_between_that_breaks_off(dynamodb_client, stable, tail, reason):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' AND {tail}"
    assert _message(dynamodb_client, statement) == f"{WF}{reason}"


def test_like_with_nothing_after_it(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' AND s LIKE"
    assert _message(dynamodb_client, statement) == f"{WF}Missing right-hand side expression of infix operator"


@pytest.mark.parametrize(
    ("condition", "operator"),
    [
        ("pk = 'p' AND n = -n", "-"),
        ("pk = 'p' AND n = +n", "+"),
        ("pk = 1-2", "-"),
        ("pk = 1+2", "+"),
        ("pk = 'p' AND n % 2 = 1", "%"),
        ("pk = 'p' AND n / 2 = 1", "/"),
        ("pk = 'p' AND s || 'x' = 'y'", "||"),
    ],
)
def test_a_condition_takes_no_arithmetic(dynamodb_client, stable, condition, operator):
    assert _message(dynamodb_client, f"SELECT * FROM \"{stable}\" WHERE {condition}") == (
        f"Unsupported operator in Condition Expression. Operator: {operator}"
    )


def test_a_sign_before_a_path_in_a_set(dynamodb_client, stable):
    assert _message(dynamodb_client, f"UPDATE \"{stable}\" SET a = -b WHERE pk = 'p'") == (
        "Incorrect number of operands for operator or function; operator or function: -, "
        "number of operands: 1"
    )


# ---- words and characters ----------------------------------------------------


def test_at_and_dollar_words_are_names(dynamodb_client, stable):
    dynamodb_client.put_item(TableName=stable, Item={"pk": {"S": "at"}, "n": {"N": "1"}})
    assert _select(dynamodb_client, f"SELECT pk FROM \"{stable}\" WHERE pk = 'at' AND @n = 1") == [
        {"pk": {"S": "at"}}
    ]
    assert _select(dynamodb_client, f"SELECT pk FROM \"{stable}\" WHERE pk = 'at' AND $x = 1") == []


def test_a_word_with_a_letter_outside_ascii_does_not_lex(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' AND café = 1"
    assert _message(dynamodb_client, statement) == WF


def test_a_backtick_literal_is_refused_by_its_ion_type(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' AND s = `1`"
    column = statement.index("`") + 1
    assert _message(dynamodb_client, statement) == (
        f"Ion Literals are not supported: INT under key root at 1:{column}:3"
    )


# ---- bare expressions ----------------------------------------------------------

CHECK = "EXISTS can only be used in ExecuteTransaction write requests."


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        ("x", "Unsupported operation: Id"),
        ("1", "Unsupported operation: Lit"),
        ("x = 1", CHECK),
        ("NOT x", CHECK),
        ("x IS NOT INT4", CHECK),
        ("x IS VARCHAR(10)", "Unsupported operation: IsType"),
        ("x IS TIME WITH TIME ZONE", "Unsupported operation: IsType"),
        ("'a' IS STRING", "Unsupported operation: IsType"),
    ],
)
def test_a_bare_expression_is_refused_by_what_it_is(dynamodb_client, statement, message):
    assert _message(dynamodb_client, statement) == message


def test_a_bare_is_test_on_a_parameter(dynamodb_client):
    assert _message(dynamodb_client, "? IS NULL", Parameters=[{"S": "v"}]) == "Unsupported operation: IsType"


# ---- empty names and the table name rule -------------------------------------

EMPTY = "Path component cannot be an empty string"
NAME_RULE = (
    "Table name should be within the length [3, 255] and only contain 'a-z', 'A-Z', '0-9', "
    "'-', '_', '.'."
)


@pytest.mark.parametrize(
    "statement",
    ["UPDATE \"\" SET a = 1 WHERE pk = 'p'", "DELETE FROM \"\" WHERE pk = 'p'"],
)
def test_an_empty_write_target(dynamodb_client, statement):
    assert _message(dynamodb_client, statement) == EMPTY


@pytest.mark.parametrize("condition", ["\"\" = 1", "m.\"\" = 1"])
def test_an_empty_name_in_a_condition(dynamodb_client, stable, condition):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' AND {condition}"
    assert _message(dynamodb_client, statement) == EMPTY


@pytest.mark.parametrize("name", ["", "a", "ab", "bad!name", "x" * 256])
@pytest.mark.parametrize(
    "template",
    [
        "SELECT * FROM \"{n}\" WHERE pk = 'p'",
        "UPDATE \"{n}\" SET a = 1 WHERE pk = 'p'",
        "DELETE FROM \"{n}\" WHERE pk = 'p'",
        "INSERT INTO \"{n}\" VALUE {{'pk': 'p'}}",
    ],
    ids=["select", "update", "delete", "insert"],
)
def test_a_table_name_outside_the_name_rule(dynamodb_client, name, template):
    if name == "" and not template.startswith("INSERT"):
        pytest.skip("an empty FROM path component has its own refusal")
    assert _message(dynamodb_client, template.format(n=name)) == NAME_RULE


@pytest.mark.parametrize("index", ["i", "ix", "bad!name", "x" * 256])
@pytest.mark.parametrize("verb", ["select", "update", "delete"])
def test_an_index_name_outside_the_name_rule(dynamodb_client, stable, index, verb):
    q = f"\"{stable}\".\"{index}\""
    statement = {
        "select": f"SELECT * FROM {q} WHERE pk = 'p'",
        "update": f"UPDATE {q} SET a = 1 WHERE pk = 'p'",
        "delete": f"DELETE FROM {q} WHERE pk = 'p'",
    }[verb]
    assert _message(dynamodb_client, statement) == NAME_RULE


@pytest.mark.parametrize("index", ["idx", "x" * 255])
def test_an_index_name_inside_the_name_rule(dynamodb_client, stable, index):
    q = f"\"{stable}\".\"{index}\""
    assert _message(dynamodb_client, f"SELECT * FROM {q} WHERE pk = 'p'") == (
        "The table does not have the specified index"
    )
    for statement in [f"UPDATE {q} SET a = 1 WHERE pk = 'p'", f"DELETE FROM {q} WHERE pk = 'p'"]:
        assert _message(dynamodb_client, statement) == "This operation is not supported on an index"


# ---- number text, FROM path steps, projections --------------------------------


@pytest.mark.parametrize(
    ("number", "reason"),
    [
        ("1.2.3", "Unexpected token after expression"),
        ("1e5e5", "Unexpected token after expression"),
        ("1_000", "Unexpected token after expression"),
        ("1..2", ""),
        ("1e+", ""),
        ("5.e", ""),
        ("1.2.", "Invalid path dot component"),
        ("1.2..3", "Invalid path dot component"),
        ("..5", "Unexpected term"),
    ],
)
def test_number_text_in_a_condition(dynamodb_client, stable, number, reason):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = {number}"
    assert _message(dynamodb_client, statement) == f"{WF}{reason}"


def test_a_path_from_a_number_is_refused_at_the_number(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 1.2.e3"
    column = statement.index("1.2") + 1
    assert _message(dynamodb_client, statement) == f"Unexpected path component at 1:{column}:3"


@pytest.mark.parametrize("number", ["1.e3", "1.", "00.5", "1e05", "1E5"])
def test_number_text_the_service_reads(dynamodb_client, ntable, number):
    assert _select(dynamodb_client, f"SELECT * FROM \"{ntable}\" WHERE pk = {number}") == []


@pytest.mark.parametrize("tail", [".index", ". WHERE pk = 'p'"])
def test_a_from_path_step_that_is_not_a_name(dynamodb_client, stable, tail):
    assert _message(dynamodb_client, f"SELECT * FROM \"{stable}\"{tail}") == f"{WF}Invalid path dot component"


def test_a_projected_path_with_two_steps_runs(dynamodb_client, stable):
    dynamodb_client.put_item(
        TableName=stable,
        Item={"pk": {"S": "deep"}, "m": {"M": {"a": {"M": {"b": {"S": "c"}}}}}},
    )
    items = _select(dynamodb_client, f"SELECT m.a.b FROM \"{stable}\" WHERE pk = 'deep'")
    assert items == [{"b": {"S": "c"}}]


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("\"{t}\".\"idx\"", "This operation is not supported on an index"),
        ("\"{t}\".\"i\"", NAME_RULE),
        ("\"{t}\".\"a\".\"b\"", "A path may contain at most 2 components in the FROM clause"),
        ("\"{t}\".\"\"", EMPTY),
        ("\"{t}\" t", ALIASING),
        ("\"{t}\" AS t", ALIASING),
    ],
)
def test_a_from_first_update_target(dynamodb_client, stable, target, message):
    statement = f"FROM {target.format(t=stable)} WHERE pk = 'p' SET c = 1"
    assert _message(dynamodb_client, statement) == message


@pytest.mark.parametrize("function", ["set_add", "set_delete"])
def test_a_bag_member_followed_by_a_second_number(dynamodb_client, stable, function):
    statement = f"UPDATE \"{stable}\" SET ss = {function}(ss, <<1.2.3>>) WHERE pk = 'p'"
    assert _message(dynamodb_client, statement) == f"{WF}Expected RIGHT_DOUBLE_ANGLE_BRACKET"


def test_an_update_without_where(dynamodb_client, stable):
    statement = f"UPDATE \"{stable}\" SET a = 1"
    assert _message(dynamodb_client, statement) == (
        "Where clause does not contain a mandatory equality on all key attributes"
    )
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    assert e.value.response["Error"]["Code"] == "TransactionCanceledException"
    assert e.value.response["Error"]["Message"] == (
        "Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]"
    )


# ---- shapes read by the same rules as the measured ones ---------------------


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        ("x.y", "Unsupported operation: Path"),
        ("size(x)", CHECK),
        ("attribute_exists(x)", CHECK),
        ("foo(x)", CHECK),
        ("{'a': 1}", "Unsupported operation: Struct"),
        ("[1]", "Unsupported operation: List"),
        ("1 + 2", CHECK),
        ("x AND y = 1", CHECK),
        ("x BETWEEN 1 AND 2", CHECK),
    ],
)
def test_a_bare_expression_of_another_kind(dynamodb_client, statement, message):
    assert _message(dynamodb_client, statement) == message


def test_a_bare_parameter(dynamodb_client):
    assert _message(dynamodb_client, "?", Parameters=[{"S": "v"}]) == "Unsupported operation: Parameter"


@pytest.mark.parametrize(
    ("literal", "ion_type"),
    [("\"a\"", "STRING"), ("1.5", "DECIMAL"), ("1e0", "FLOAT"), ("{a: 1}", "STRUCT"), ("[1]", "LIST")],
)
def test_a_backtick_literal_of_another_ion_type(dynamodb_client, stable, literal, ion_type):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' AND s = `{literal}`"
    column = statement.index("`") + 1
    assert _message(dynamodb_client, statement) == (
        f"Ion Literals are not supported: {ion_type} under key root at 1:{column}:{len(literal) + 2}"
    )


def test_a_condition_takes_no_multiplication(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 'p' AND n * 2 = 1"
    assert _message(dynamodb_client, statement) == (
        "Unsupported operator in Condition Expression. Operator: *"
    )


@pytest.mark.parametrize(("literal", "length"), [("1", 1), ("'a'", 3)])
def test_a_projected_number_or_string(dynamodb_client, stable, literal, length):
    statement = f"SELECT {literal} FROM \"{stable}\" WHERE pk = 'p'"
    assert _message(dynamodb_client, statement) == f"Unexpected path component at 1:8:{length}"


def test_a_plus_sign_before_a_path_in_a_set(dynamodb_client, stable):
    assert _message(dynamodb_client, f"UPDATE \"{stable}\" SET a = +b WHERE pk = 'p'") == (
        "Incorrect number of operands for operator or function; operator or function: +, "
        "number of operands: 1"
    )


def test_an_update_without_where_on_a_missing_table(dynamodb_client, stable):
    assert _err(dynamodb_client, f"UPDATE \"{stable}-absent\" SET a = 1") == (
        "ResourceNotFoundException",
        "Requested resource not found",
    )


# ---- key values of the wrong type --------------------------------------------

KEY_TYPE = "Key attribute's data type should match its data type in table's schema: Key {}"


@pytest.fixture(scope="module")
def sntable(dynamodb_client):
    """A table keyed on a string pk and a number sk."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        yield name


@pytest.mark.parametrize(
    "template",
    [
        "SELECT * FROM \"{t}\" WHERE pk = {v}",
        "UPDATE \"{t}\" SET a = 1 WHERE pk = {v}",
        "DELETE FROM \"{t}\" WHERE pk = {v}",
    ],
    ids=["select", "update", "delete"],
)
def test_a_number_against_a_string_key(dynamodb_client, stable, template):
    assert _message(dynamodb_client, template.format(t=stable, v="1")) == KEY_TYPE.format("pk")


@pytest.mark.parametrize(
    "template",
    [
        "SELECT * FROM \"{t}\" WHERE pk = {v}",
        "UPDATE \"{t}\" SET a = 1 WHERE pk = {v}",
        "DELETE FROM \"{t}\" WHERE pk = {v}",
    ],
    ids=["select", "update", "delete"],
)
def test_a_string_against_a_number_key(dynamodb_client, ntable, template):
    assert _message(dynamodb_client, template.format(t=ntable, v="'a'")) == KEY_TYPE.format("pk")


def test_a_parameter_of_the_wrong_type_for_a_key(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = ?"
    assert _message(dynamodb_client, statement, Parameters=[{"N": "1"}]) == KEY_TYPE.format("pk")


@pytest.mark.parametrize(
    "template",
    [
        "SELECT * FROM \"{t}\" WHERE pk = 'a' AND sk = 'b'",
        "UPDATE \"{t}\" SET a = 1 WHERE pk = 'a' AND sk = 'b'",
        "DELETE FROM \"{t}\" WHERE pk = 'a' AND sk = 'b'",
    ],
    ids=["select", "update", "delete"],
)
def test_a_string_against_a_number_sort_key(dynamodb_client, sntable, template):
    assert _message(dynamodb_client, template.format(t=sntable)) == KEY_TYPE.format("sk")


def test_a_wrong_type_on_a_key_in_an_or_branch_is_refused(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = 1 OR a = 'no-such-value'"
    assert _message(dynamodb_client, statement) == KEY_TYPE.format("pk")


# ---- parameters inside literals ----------------------------------------------


@pytest.mark.parametrize(
    ("template", "path"),
    [
        ("INSERT INTO \"{t}\" VALUE {{'pk':'m1','w':{{'k':?}}}}", "root.k"),
        ("INSERT INTO \"{t}\" VALUE {{'pk':'m2','w':[?]}}", "root[0]"),
        ("INSERT INTO \"{t}\" VALUE {{'pk':'m3','w':[1,?]}}", "root[1]"),
        ("INSERT INTO \"{t}\" VALUE {{'pk':'m4','w':{{'a':{{'b':?}}}}}}", "root.a.b"),
        ("INSERT INTO \"{t}\" VALUE {{'pk':'m7','w':{{'k':?, 'j':?}}}}", "root.k"),
        ("UPDATE \"{t}\" SET w = {{'k':?}} WHERE pk = 'p'", "root.k"),
        ("UPDATE \"{t}\" SET w = [?] WHERE pk = 'p'", "root[0]"),
        ("SELECT * FROM \"{t}\" WHERE pk = 'p' AND w = {{'k':?}}", "root.k"),
        ("SELECT * FROM \"{t}\" WHERE pk = 'p' AND w = [?]", "root[0]"),
    ],
)
def test_a_parameter_inside_a_literal(dynamodb_client, stable, template, path):
    statement = template.format(t=stable)
    column = statement.index("?") + 1
    params = [{"S": "v"}] * statement.count("?")
    assert _message(dynamodb_client, statement, Parameters=params) == (
        f"Unsupported data type: Parameter under key {path} at 1:{column}:1"
    )


def test_a_parameter_inside_a_bag(dynamodb_client, stable):
    statement = f"INSERT INTO \"{stable}\" VALUE {{'pk':'m5','w':<<?>>}}"
    column = statement.index("?") + 1
    assert _message(dynamodb_client, statement, Parameters=[{"S": "v"}]) == (
        "Unsupported data type in Bag. DynamoDB only supports either numbers or strings in bags "
        f"at 1:{column}:1"
    )


def test_a_parameter_as_a_whole_value(dynamodb_client, stable):
    statement = f"INSERT INTO \"{stable}\" VALUE {{'pk':'m6','w':?}}"
    dynamodb_client.execute_statement(Statement=statement, Parameters=[{"S": "v"}])
    item = dynamodb_client.get_item(TableName=stable, Key={"pk": {"S": "m6"}}, ConsistentRead=True)
    assert item["Item"]["w"] == {"S": "v"}


# ---- items for the path and parenthesis tests ----------------------------------

OVERFLOW = (
    "Number overflow. Attempting to store a number with magnitude larger than supported "
    "range under {}"
)


@pytest.fixture(scope="module")
def pair(dynamodb_client, ctable):
    """The partition key of two items P/b and P/c in a partition of their own,
    each with a = 1, b = 2, x = 0 and m = {k: 1, 'my key': 1}."""
    pk = f"pp-{uuid.uuid4().hex[:8]}"
    for sk in ("b", "c"):
        dynamodb_client.put_item(
            TableName=ctable,
            Item={
                "pk": {"S": pk},
                "sk": {"S": sk},
                "a": {"N": "1"},
                "b": {"N": "2"},
                "x": {"N": "0"},
                "m": {"M": {"k": {"N": "1"}, "my key": {"N": "1"}}},
            },
        )
    return pk


def _sorts(client, statement, **kwargs):
    return sorted(i["sk"]["S"] for i in _select(client, statement, **kwargs))


# ---- a string or a name in brackets ---------------------------------------------
#
# Measured 2026-11-05. A string, a quoted name or a name in brackets after a
# path is a map member, as after a dot: m['k'], m["k"] and m[k] are m.k, in a
# condition, a SET target, a REMOVE, a SET value and the select list. A
# number is a list index as before. Anything else in brackets parses and is
# refused once the statement parses; a reserved word there is a syntax
# error, and so is a bracket left open.


@pytest.mark.parametrize(
    ("condition", "sorts"),
    [
        ("m['k'] = 1", ["b", "c"]),
        ("m['my key'] = 1", ["b", "c"]),
        ("m[k] = 1", ["b", "c"]),
        ('m["k"] = 1', ["b", "c"]),
        ("m[ 'k' ] = 1", ["b", "c"]),
        ("m['k'] = 1 AND m.k = 1", ["b", "c"]),
        ("m['k'].z IS MISSING", ["b", "c"]),
        ("m[k].j IS MISSING", ["b", "c"]),
        ("m['0'] = 1", []),
        ("m['a.b'] = 1", []),
        ("m.k = 1", ["b", "c"]),
    ],
)
def test_a_string_or_a_name_in_brackets_is_a_map_member(dynamodb_client, ctable, pair, condition, sorts):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = '{pair}' AND {condition}"
    assert _sorts(dynamodb_client, statement) == sorts


def test_a_member_in_brackets_in_the_select_list(dynamodb_client, ctable, pair):
    for path in ("m['k']", "m[k]"):
        statement = f"SELECT {path} FROM \"{ctable}\" WHERE pk = '{pair}' AND sk = 'b'"
        assert _select(dynamodb_client, statement) == [{"k": {"N": "1"}}]


@pytest.mark.parametrize(
    ("clause", "m", "x"),
    [
        ("SET m['k'] = 5", {"k": {"N": "5"}, "my key": {"N": "1"}}, {"N": "0"}),
        ("SET m['new key'] = 7", {"k": {"N": "1"}, "my key": {"N": "1"}, "new key": {"N": "7"}}, {"N": "0"}),
        ("REMOVE m['k']", {"my key": {"N": "1"}}, {"N": "0"}),
        ("SET x = m['my key']", {"k": {"N": "1"}, "my key": {"N": "1"}}, {"N": "1"}),
    ],
)
def test_a_member_in_brackets_in_an_update(dynamodb_client, ctable, clause, m, x):
    pk = f"pb-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(
        TableName=ctable,
        Item={
            "pk": {"S": pk},
            "sk": {"S": "b"},
            "x": {"N": "0"},
            "m": {"M": {"k": {"N": "1"}, "my key": {"N": "1"}}},
        },
    )
    dynamodb_client.execute_statement(
        Statement=f"UPDATE \"{ctable}\" {clause} WHERE pk = '{pk}' AND sk = 'b'"
    )
    item = dynamodb_client.get_item(
        TableName=ctable, Key={"pk": {"S": pk}, "sk": {"S": "b"}}, ConsistentRead=True
    )["Item"]
    assert item["m"] == {"M": m}
    assert item["x"] == x


def test_a_member_in_brackets_and_after_a_dot_is_one_path(dynamodb_client, ctable):
    statement = f"UPDATE \"{ctable}\" SET m['k'] = 5, m.k = 6 WHERE pk = 'pb' AND sk = 'b'"
    assert _message(dynamodb_client, statement) == (
        "Two document paths overlap with each other; must remove or rewrite one of these paths; "
        "path one: [m, k], path two: [m, k]"
    )


@pytest.mark.parametrize(
    ("brackets", "refusal", "at"),
    [
        ("['']", "Path component cannot be an empty string", None),
        ('[""]', "Path component cannot be an empty string", None),
        ("[?]", "Unexpected path component", "?"),
        ("[1.5]", "Unexpected path component", "1.5"),
        ("[1e0]", "Unexpected path component", "1e0"),
        ("[true]", "Unexpected path component", "true"),
        ("[MISSING]", "Unexpected path component", "MISSING"),
        ("[-1]", "List index is not within the allowable range; index: [-1]", "1"),
        ("[- 1]", "List index is not within the allowable range; index: [-1]", "1"),
        ("[-1.5]", "Unexpected path component", "1.5"),
        ("[k.j]", "Unexpected path component", "k"),
        ("[k[0]]", "Unexpected path component", "k"),
        ('["k".x]', "Unexpected path component", '"k"'),
        ("['k'.x]", "Unexpected path component", "'k'"),
        ("[select]", WF + "Unexpected term", None),
        ("[from]", WF + "Unexpected term", None),
        ("[desc]", WF + "Unexpected term", None),
        ("[and]", WF + "Unexpected operator", None),
        ("['k' 'j']", WF + "Expected RIGHT_BRACKET", None),
        ("[k j]", WF + "Expected RIGHT_BRACKET", None),
    ],
)
def test_other_contents_of_brackets_are_refused(dynamodb_client, ctable, brackets, refusal, at):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'pb' AND m{brackets} = 1"
    kwargs = {"Parameters": [{"S": "k"}]} if "?" in statement else {}
    expected = refusal
    if at is not None:
        column = statement.index(at, statement.index("m[") + 2) + 1
        expected = f"{refusal} at 1:{column}:{len(at)}"
    assert _message(dynamodb_client, statement, **kwargs) == expected


@pytest.mark.parametrize("tail", ["m['k'", "m[k", "m[0", "m[-1", "m[1.5", "m[?", "m[k.j"])
def test_a_bracket_left_open_is_a_syntax_error(dynamodb_client, ctable, tail):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'pb' AND {tail}"
    kwargs = {"Parameters": [{"S": "k"}]} if "?" in statement else {}
    assert _message(dynamodb_client, statement, **kwargs) == WF + "Expected RIGHT_BRACKET"


@pytest.mark.parametrize(
    ("condition", "first"),
    [
        ("a = 1e126 AND m[''] = 1", "overflow"),
        ("m[''] = 1 AND a = 1e126", "empty"),
        ("a = 1e126 AND m[k.j] = 1", "overflow"),
        ("m[k.j] = 1e126", "path"),
    ],
)
def test_a_refusal_in_brackets_keeps_its_place_in_the_condition(dynamodb_client, ctable, condition, first):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'pb' AND {condition}"
    if first == "overflow":
        expected = OVERFLOW.format("root")
    elif first == "empty":
        expected = "Path component cannot be an empty string"
    else:
        expected = f"Unexpected path component at 1:{statement.index('k.j') + 1}:1"
    assert _message(dynamodb_client, statement) == expected


def test_a_path_into_a_key_in_brackets_is_refused(dynamodb_client, ctable):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'pb' AND sk['x'] = 'b'"
    assert _message(dynamodb_client, statement) == (
        "Key attributes must be scalars; list random access '[]' and map lookup '.' are not "
        "allowed: Key sk"
    )


# ---- one value or path in parentheses ------------------------------------------
#
# Measured 2026-11-05. Parentheses around one value or path only group it, in
# a condition, a SET value, a call's argument and an INSERT value; a comma
# inside them makes a list. A sign before a number in parentheses belongs to
# the number. An operand alone in parentheses where a condition belongs is
# refused as the operand alone is, also inside a group and after a NOT.


@pytest.mark.parametrize(
    ("condition", "sorts"),
    [
        ("pk = ('{p}')", ["b", "c"]),
        ("(pk) = '{p}'", ["b", "c"]),
        ("((pk)) = (('{p}'))", ["b", "c"]),
        ("pk = '{p}' AND (a) = 1", ["b", "c"]),
        ("pk = '{p}' AND (a) = (1)", ["b", "c"]),
        ("pk = '{p}' AND ('x') = 'x'", ["b", "c"]),
        ("pk = '{p}' AND a = -(-1)", ["b", "c"]),
        ("pk = '{p}' AND x = (-0)", ["b", "c"]),
        ("pk = '{p}' AND (a) BETWEEN 0 AND 2", ["b", "c"]),
        ("pk = '{p}' AND (m) IS NOT MISSING", ["b", "c"]),
        ("pk = '{p}' AND (a) IS MISSING", []),
        ("pk = '{p}' AND (a) IN [1]", ["b", "c"]),
        ("pk = '{p}' AND (a) NOT IN [2]", ["b", "c"]),
        ("pk = '{p}' AND NOT (a) = 2", ["b", "c"]),
        ("pk = '{p}' AND (a) = 1 = true", ["b", "c"]),
        ("pk = '{p}' AND (a) > (b)", []),
        ("pk = '{p}' AND begins_with((sk), 'b')", ["b"]),
        ("pk = '{p}' AND size((m)) > 1", ["b", "c"]),
        ("pk = '{p}' AND attribute_exists((a))", ["b", "c"]),
        ("pk = '{p}' AND (m).k = 1", ["b", "c"]),
        ("pk = '{p}' AND (m)['k'] = 1", ["b", "c"]),
        ("(pk) = '{p}' AND (sk) > 'b'", ["c"]),
        ("(pk = '{p}') AND (a) = 1", ["b", "c"]),
    ],
)
def test_parentheses_around_one_value_or_path_group_it(dynamodb_client, ctable, pair, condition, sorts):
    statement = f'SELECT * FROM "{ctable}" WHERE ' + condition.format(p=pair)
    assert _sorts(dynamodb_client, statement) == sorts


def test_a_parameter_in_parentheses_and_a_key_in_parentheses_read_by_key(dynamodb_client, ctable, pair):
    statement = f'SELECT * FROM "{ctable}" WHERE pk = (?)'
    assert _sorts(dynamodb_client, statement, Parameters=[{"S": pair}]) == ["b", "c"]
    statement = f"SELECT * FROM \"{ctable}\" WHERE (pk) = '{pair}' ORDER BY sk DESC"
    assert [i["sk"]["S"] for i in _select(dynamodb_client, statement)] == ["c", "b"]


@pytest.mark.parametrize(
    ("assignment", "stored"),
    [
        ("x = -(5)", {"N": "-5"}),
        ("x = - (5)", {"N": "-5"}),
        ("x = -(-(5))", {"N": "5"}),
        ("x = (5)", {"N": "5"}),
        ("x = (a + 1)", {"N": "2"}),
        ("x = (a) + 1", {"N": "2"}),
        ("x = (m).k", {"N": "1"}),
        ("x = ('a', 'b')", {"L": [{"S": "a"}, {"S": "b"}]}),
    ],
)
def test_a_set_value_in_parentheses(dynamodb_client, ctable, assignment, stored):
    pk = f"pp-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(
        TableName=ctable,
        Item={"pk": {"S": pk}, "sk": {"S": "b"}, "a": {"N": "1"}, "m": {"M": {"k": {"N": "1"}}}},
    )
    dynamodb_client.execute_statement(
        Statement=f"UPDATE \"{ctable}\" SET {assignment} WHERE pk = '{pk}' AND sk = 'b'"
    )
    item = dynamodb_client.get_item(
        TableName=ctable, Key={"pk": {"S": pk}, "sk": {"S": "b"}}, ConsistentRead=True
    )["Item"]
    assert item["x"] == stored


def test_an_insert_value_in_parentheses(dynamodb_client, ctable):
    pk = f"pp-{uuid.uuid4().hex[:8]}"
    dynamodb_client.execute_statement(
        Statement=(
            f"INSERT INTO \"{ctable}\" VALUE {{'pk': ('{pk}'), 'sk': 'b', 'x': (5), "
            "'l': [(5)], 'p': (1, 2)}"
        )
    )
    item = dynamodb_client.get_item(
        TableName=ctable, Key={"pk": {"S": pk}, "sk": {"S": "b"}}, ConsistentRead=True
    )["Item"]
    assert item["x"] == {"N": "5"}
    assert item["l"] == {"L": [{"N": "5"}]}
    assert item["p"] == {"L": [{"N": "1"}, {"N": "2"}]}


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        ("UPDATE {t} SET x = -(a) WHERE pk = 'pp' AND sk = 'b'", "minus"),
        ("UPDATE {t} SET x = -(?) WHERE pk = 'pp' AND sk = 'b'", "minus"),
        ("SELECT * FROM {t} WHERE (a)", "WHERE clause must contain an expression that resolves to a boolean"),
        ("SELECT * FROM {t} WHERE ((a))", "WHERE clause must contain an expression that resolves to a boolean"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (a)", "The arguments to AND/OR must be Expressions"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (a) OR pk = 'b'", "The arguments to AND/OR must be Expressions"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND ((a) AND b = 2)", "The arguments to AND/OR must be Expressions"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (a AND b = 2)", "The arguments to AND/OR must be Expressions"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (b = 2 AND a)", "The arguments to AND/OR must be Expressions"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND NOT (a)", "The argument to NOT must be a Type or Expression"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND NOT a", "The argument to NOT must be a Type or Expression"),
        ("SELECT * FROM {t} WHERE (NOT a)", "The argument to NOT must be a Type or Expression"),
        ("SELECT * FROM {t} WHERE NOT ((a))", "The argument to NOT must be a Type or Expression"),
        ("SELECT * FROM {t} WHERE NOT (a) AND pk = 'pp'", "The argument to NOT must be a Type or Expression"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (a) AND b = 1e126", "The arguments to AND/OR must be Expressions"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (a + 1) = 2", "Unsupported operator in Condition Expression. Operator: +"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (a) LIKE 'x'", "Unsupported operator in Condition Expression. Operator: like"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (a) = 1 IS NULL", "The left hand side argument to IS must be a Variable Reference, Literal, or Path"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (sk.x) = 1", "Key attributes must be scalars; list random access '[]' and map lookup '.' are not allowed: Key sk"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND a = -(1e126)", OVERFLOW.format("root")),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (1e126) = a", OVERFLOW.format("root")),
        ("SELECT * FROM {t} WHERE pk = ('pp'", WF + "Expected RIGHT_PAREN"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND a = (1", WF + "Expected RIGHT_PAREN"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND a = (1))", WF + "Unexpected token after expression"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND a = ()", WF + "Unexpected term"),
    ],
)
def test_parentheses_around_one_operand_keep_its_refusals(dynamodb_client, ctable, statement, message):
    statement = statement.format(t=f'"{ctable}"')
    if message == "minus":
        message = (
            "Incorrect number of operands for operator or function; operator or function: -, "
            "number of operands: 1"
        )
    kwargs = {"Parameters": [{"N": "5"}]} if "?" in statement else {}
    assert _message(dynamodb_client, statement, **kwargs) == message


def test_an_is_test_on_an_operand_in_parentheses_names_the_operand(dynamodb_client, ctable):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'pp' AND (a) IS INT4"
    column = statement.index("(a)") + 2
    assert _message(dynamodb_client, statement) == (
        "Unsupported data type for IS operator: Integer4Type. Supported data types are [MISSING, "
        f"NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] at 1:{column}:1"
    )


# Measured 2026-11-05: an operand followed by a token that does not go on with
# it ends there, and the part of the statement around the operand reads that
# token. A syntax error there comes ahead of the operand's own refusal.
@pytest.mark.parametrize(
    ("statement", "message"),
    [
        ("SELECT * FROM {t} WHERE a b", WF + "Unexpected token after expression"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND a b", WF + "Unexpected token after expression"),
        (
            "DELETE FROM {t} WHERE pk = 'pp' AND sk = 'b' AND a b",
            WF + "Unexpected token after expression",
        ),
        ("SELECT * FROM {t} WHERE (a b)", WF + "Expected RIGHT_PAREN"),
        ("SELECT * FROM {t} WHERE pk = 'pp' AND (a b)", WF + "Expected RIGHT_PAREN"),
        ("SELECT * FROM {t} WHERE (pk = 'pp' AND a LIMIT 1)", WF + "Expected RIGHT_PAREN"),
        (
            "SELECT * FROM {t} WHERE a ORDER BY pk",
            "WHERE clause must contain an expression that resolves to a boolean",
        ),
    ],
)
def test_a_token_after_an_operand_ends_it(dynamodb_client, ctable, statement, message):
    assert _message(dynamodb_client, statement.format(t=f'"{ctable}"')) == message


def test_a_limit_after_an_operand_is_refused_for_the_clause(dynamodb_client, ctable):
    statement = f"SELECT * FROM \"{ctable}\" WHERE a LIMIT 1"
    # The refusal names the clause's value, the last character.
    assert _message(dynamodb_client, statement) == (
        f"Unsupported clause: LIMIT at 1:{len(statement)}:1"
    )


# ---- negative-zero decimals ----------------------------------------------------
#
# Measured 2026-11-05. A zero written with a decimal point or an exponent and a
# minus sign before it, anywhere in the run of signs and parentheses before it,
# is refused at the number, naming where it sits in its value. Among the other
# refusals of a value it comes in statement order with the number rules and a
# parameter inside a literal, after the condition's, and ahead of a sign
# before a SET value that is not a number and of the table lookup. An integer
# zero and a parameter are stored as 0.

NEGATIVE_ZERO = "Negative zero decimal is not supported under key {} at 1:{}:{}"


def _negative_zero(statement, number, path, start=0):
    column = statement.index(number, start) + 1
    return NEGATIVE_ZERO.format(path, column, len(number))


@pytest.mark.parametrize(
    ("value", "number", "path"),
    [
        ("-0.0", "0.0", "root"),
        ("- 0.0", "0.0", "root"),
        ("- -0.0", "0.0", "root"),
        ("-+0.0", "0.0", "root"),
        ("+-0.0", "0.0", "root"),
        ("-0e0", "0e0", "root"),
        ("-0E-3", "0E-3", "root"),
        ("-.0", ".0", "root"),
        ("-0.", "0.", "root"),
        ("-00.0", "00.0", "root"),
        ("-0.0e0", "0.0e0", "root"),
        ("[1, -0.0]", "0.0", "root[1]"),
        ("{'y': -0.0}", "0.0", "root.y"),
        ("<<-0.0, 1>>", "0.0", "root"),
    ],
)
def test_a_negative_zero_decimal_in_an_insert_is_refused(dynamodb_client, stable, value, number, path):
    pk = f"nz-{uuid.uuid4().hex[:8]}"
    statement = f"INSERT INTO \"{stable}\" VALUE {{'pk': '{pk}', 'x': {value}}}"
    start = statement.index("'x'")
    assert _message(dynamodb_client, statement) == _negative_zero(statement, number, path, start)
    got = dynamodb_client.get_item(TableName=stable, Key={"pk": {"S": pk}}, ConsistentRead=True)
    assert "Item" not in got


@pytest.mark.parametrize(
    ("template", "number", "path"),
    [
        ("UPDATE {t} SET x = -0e0 WHERE pk = '{k}'", "0e0", "root"),
        ("UPDATE {t} SET x = -(0.0) WHERE pk = '{k}'", "0.0", "root"),
        ("UPDATE {t} SET x = (-0.0) WHERE pk = '{k}'", "0.0", "root"),
        ("UPDATE {t} SET x = -(+0.0) WHERE pk = '{k}'", "0.0", "root"),
        ("UPDATE {t} SET x = {{'y': -0.0}} WHERE pk = '{k}'", "0.0", "root.y"),
        ("UPDATE {t} SET x = set_add(x, <<-0.0>>) WHERE pk = '{k}'", "0.0", "root"),
        ("SELECT * FROM {t} WHERE pk = '{k}' AND x = -0.0", "0.0", "root"),
        ("SELECT * FROM {t} WHERE pk = '{k}' AND x = - -0.0", "0.0", "root"),
        ("SELECT * FROM {t} WHERE pk = '{k}' AND x IN [-0.0]", "0.0", "root"),
        ("SELECT * FROM {t} WHERE pk = '{k}' AND x = [-0.0]", "0.0", "root[0]"),
        ("SELECT * FROM {t}-absent WHERE pk = '{k}' AND x = -0.0", "0.0", "root"),
    ],
    ids=[
        "set-exponent",
        "set-sign-before-parentheses",
        "set-sign-in-parentheses",
        "set-minus-before-a-plus",
        "set-map-member",
        "set-add-bag",
        "where",
        "where-two-minus-signs",
        "where-in",
        "where-list",
        "ahead-of-the-table-lookup",
    ],
)
def test_a_negative_zero_decimal_in_a_set_or_a_condition_is_refused(
    dynamodb_client, stable, template, number, path
):
    pk = f"nz-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(TableName=stable, Item={"pk": {"S": pk}, "x": {"N": "0"}})
    statement = template.format(t=f'"{stable}"', k=pk).replace('"-absent', '-absent"')
    start = statement.index("x =") if "x =" in statement else statement.index("x IN")
    assert _message(dynamodb_client, statement) == _negative_zero(statement, number, path, start)
    item = dynamodb_client.get_item(TableName=stable, Key={"pk": {"S": pk}}, ConsistentRead=True)
    assert item["Item"]["x"] == {"N": "0"}


def test_an_integer_zero_and_a_negative_zero_parameter_are_zero(dynamodb_client, stable):
    first, second = (f"nz-{uuid.uuid4().hex[:8]}" for _ in range(2))
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{stable}\" VALUE {{'pk': '{first}', 'x': -0, 'y': +0.0}}"
    )
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{stable}\" VALUE {{'pk': '{second}', 'x': ?}}",
        Parameters=[{"N": "-0.0"}],
    )
    for pk in (first, second):
        item = dynamodb_client.get_item(TableName=stable, Key={"pk": {"S": pk}}, ConsistentRead=True)
        assert item["Item"]["x"] == {"N": "0"}
    statement = f"SELECT x FROM \"{stable}\" WHERE pk = '{second}' AND x = ?"
    assert _select(dynamodb_client, statement, Parameters=[{"N": "-0.0"}]) == [{"x": {"N": "0"}}]


@pytest.mark.parametrize(
    ("value", "first"),
    [
        ("'x': 1e126, 'y': -0.0", "overflow"),
        ("'x': -0.0, 'y': 1e126", "zero"),
        ("'x': {'b': -0.0, 'a': 1e126}", "zero"),
        ("'x': [-0.0, ?]", "zero"),
        ("'x': [?, -0.0]", "parameter"),
    ],
)
def test_a_negative_zero_in_an_insert_keeps_statement_order(dynamodb_client, stable, value, first):
    statement = f"INSERT INTO \"{stable}\" VALUE {{'pk': 'nz-order', {value}}}"
    parameters = [{"N": "1"}] if "?" in statement else None
    kwargs = {"Parameters": parameters} if parameters else {}
    if first == "overflow":
        expected = OVERFLOW.format("root")
    elif first == "zero":
        path = "root.b" if "'b'" in value else ("root[0]" if "[" in value else "root")
        expected = _negative_zero(statement, "0.0", path, statement.index("-0.0"))
    else:
        column = statement.index("?") + 1
        expected = f"Unsupported data type: Parameter under key root[0] at 1:{column}:1"
    assert _message(dynamodb_client, statement, **kwargs) == expected


@pytest.mark.parametrize(
    ("assignment", "condition", "first"),
    [
        ("x = -0.0, y = 1e126", "", "zero"),
        ("x = 1e126, y = -0.0", "", "overflow"),
        ("x = +b, y = -0.0", "", "zero"),
        ("x = -0.0", " AND a = 1e126", "overflow"),
    ],
    ids=["zero-first", "overflow-first", "ahead-of-a-sign", "after-the-condition"],
)
def test_a_negative_zero_in_a_set_keeps_its_place(dynamodb_client, stable, assignment, condition, first):
    statement = f"UPDATE \"{stable}\" SET {assignment} WHERE pk = 'nz-order'{condition}"
    if first == "zero":
        expected = _negative_zero(statement, "0.0", "root", statement.index("-0.0"))
    else:
        expected = OVERFLOW.format("root")
    assert _message(dynamodb_client, statement) == expected


def test_a_negative_zero_in_a_batch_and_a_transaction(dynamodb_client, stable):
    statement = f"INSERT INTO \"{stable}\" VALUE {{'pk': 'nz-batch', 'x': -0.0}}"
    expected = _negative_zero(statement, "0.0", "root", statement.index("'x'"))
    responses = dynamodb_client.batch_execute_statement(Statements=[{"Statement": statement}])[
        "Responses"
    ]
    assert responses == [{"Error": {"Code": "ValidationError", "Message": expected}}]
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    assert e.value.response["Error"]["Message"] == (
        f"Validation failed in TransactStatements[0]: {expected}"
    )
