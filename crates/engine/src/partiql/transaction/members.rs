// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A write transaction's members: their TransactWriteItems form and their cancellation reasons.

use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{InsertStatement, SelectStatement, UpdateStatement, WhereExpr};
use extenddb_core::types::{AttributeValue, CancellationReason, Item, TableKeyInfo};
use extenddb_core::validation::validate_item_keys;

use super::not_found_reason;
use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::partiql::{SingleRequest, common, write};

/// Translate one PartiQL write statement (at position `index`) into its
/// `TransactWriteItems` member, returning the member and whether it is an
/// `INSERT`.
pub(super) async fn build_write_member(
    ctx: &OperationContext,
    index: usize,
    stmt: &Statement,
    req: &SingleRequest,
) -> Result<(Value, bool), DynamoDbError> {
    match stmt {
        Statement::Exists(select) => Ok((exists_member(ctx, index, select, req).await?, false)),
        Statement::Insert(insert) => {
            let parts = write::build_insert_parts(ctx, insert, &req.parameters).await?;
            let mut put = json!({
                "TableName": parts.table,
                "Item": parts.item,
                "ConditionExpression": parts.condition,
                "ExpressionAttributeNames": Value::Object(parts.names),
            });
            attach_rvoccf(&mut put, req);
            Ok((json!({ "Put": put }), true))
        }
        Statement::Update(update) => {
            let parts = write::build_update_parts(ctx, update, &req.parameters).await?;
            let mut upd = json!({
                "TableName": parts.table,
                "Key": Value::Object(parts.key),
                "UpdateExpression": parts.update_expression,
                "ConditionExpression": parts.condition,
            });
            if !parts.names.is_empty() {
                upd["ExpressionAttributeNames"] = Value::Object(parts.names);
            }
            if !parts.values.is_empty() {
                upd["ExpressionAttributeValues"] = Value::Object(parts.values);
            }
            attach_rvoccf(&mut upd, req);
            Ok((json!({ "Update": upd }), false))
        }
        Statement::Delete(delete) => {
            let parts = write::build_delete_parts(ctx, delete, &req.parameters).await?;
            let mut del = json!({
                "TableName": parts.table,
                "Key": Value::Object(parts.key),
            });
            if let Some(condition) = parts.condition {
                del["ConditionExpression"] = json!(condition);
            }
            if !parts.names.is_empty() {
                del["ExpressionAttributeNames"] = Value::Object(parts.names);
            }
            if !parts.values.is_empty() {
                del["ExpressionAttributeValues"] = Value::Object(parts.values);
            }
            attach_rvoccf(&mut del, req);
            Ok((json!({ "Delete": del }), false))
        }
        Statement::Select(_) => Err(DynamoDbError::InternalServerError(
            "SELECT routed to the transaction write path".to_owned(),
        )),
    }
}

/// The `ConditionCheck` member of an `EXISTS` statement at position
/// `index`.
///
/// # Errors
///
/// The refusal of a check through an index, and of one that is not a read
/// of one item with a further condition, naming the position.
async fn exists_member(
    ctx: &OperationContext,
    index: usize,
    select: &SelectStatement,
    req: &SingleRequest,
) -> Result<Value, DynamoDbError> {
    if select.from.index.is_some() {
        return Err(exists_index_refusal(index));
    }
    let parts = write::build_exists_parts(ctx, index, select, &req.parameters)
        .await?
        .ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "Validation failed in TransactStatements[{index}]: EXISTS() must \
                 contain a single item read with additional condition"
            ))
        })?;
    let mut check = json!({
        "TableName": parts.table,
        "Key": Value::Object(parts.key),
        "ConditionExpression": parts.condition,
        "ExpressionAttributeNames": Value::Object(parts.names),
    });
    if !parts.values.is_empty() {
        check["ExpressionAttributeValues"] = Value::Object(parts.values);
    }
    // A failed check returns no item for `ALL_OLD` (measured
    // 2026-11-02 with one key value and with two: the reason holds
    // no item though the item exists).
    Ok(json!({ "ConditionCheck": check }))
}

/// The refusal of a condition check through an index, at position `index`
/// (measured 2026-11-02 on an index the table does not have).
fn exists_index_refusal(index: usize) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "Validation failed in TransactStatements[{index}]: Operations on indices are \
         not supported in transactions"
    ))
}

/// Refuse an `EXISTS` member at position `index` whose form the service
/// refuses (see [`exists_member`]), before the members are authorized: a
/// caller that may not read the table gets the refusal of the form
/// (measured 2026-11-03). A check through an index is refused ahead of a
/// missing table (measured 2026-11-05 with a table and an index that do not
/// exist); any other member on a missing table is left to the checks made
/// after authorization.
///
/// # Errors
///
/// The refusal of the member's form.
pub(super) async fn check_exists_form(
    ctx: &OperationContext,
    index: usize,
    stmt: &Statement,
    req: &SingleRequest,
) -> Result<(), DynamoDbError> {
    let Statement::Exists(select) = stmt else {
        return Ok(());
    };
    if select.from.index.is_some() {
        return Err(exists_index_refusal(index));
    }
    if ctx.table_key_info(&select.from.table).await.is_err() {
        return Ok(());
    }
    exists_member(ctx, index, select, req).await.map(|_| ())
}

/// Carry a statement's per-statement `ReturnValuesOnConditionCheckFailure` onto
/// its transact member, so a conditional cancel returns the pre-image item.
fn attach_rvoccf(member: &mut Value, req: &SingleRequest) {
    if let Some(rvoccf) = &req.return_values_on_condition_check_failure {
        member["ReturnValuesOnConditionCheckFailure"] = json!(rvoccf);
    }
}

/// Refuse a member whose statement has more `?` placeholders than the request
/// gives it parameters, naming its position (measured 2026-11-02). Extra
/// parameters are ignored, as for a single statement.
pub(super) fn check_parameter_count(
    stmt: &Statement,
    supplied: usize,
    index: usize,
) -> Result<(), DynamoDbError> {
    if supplied < stmt.parameter_count() {
        return Err(DynamoDbError::ValidationException(format!(
            "Validation failed in TransactStatements[{index}]: Number of parameters in request \
             and statement don't match."
        )));
    }
    Ok(())
}

/// The reason a write member cancels the request with when it cannot be
/// translated against its table, or `None` when it can.
///
/// A missing table is `ResourceNotFound`. A WHERE that does not give the full
/// key by equality, or an `UPDATE` key value of the wrong type, is a
/// `ValidationError` saying the key does not match the schema, and so is a
/// WHERE that tests a path into a key attribute against values (measured
/// 2026-11-05 on UPDATE and DELETE); a path into a key attribute among the
/// other conditions of an `UPDATE`, a `DELETE` or an `EXISTS` is a
/// `ValidationError` with its refusal (see [`condition_key_reason`]); a `SET` of a
/// key attribute and an `INSERT` without one are `ValidationError`s naming
/// the attribute (measured 2026-11-02); a secondary index key given a value
/// of the wrong type is a `ValidationError` with the `UpdateItem` or
/// `PutItem` refusal (see [`index_key_reason`]). A `DELETE` key value of the
/// wrong type is treated like an `UPDATE` one, a `REMOVE` of a key attribute
/// like a `SET`, and an `INSERT` key value of the wrong type gets the message
/// a `TransactWriteItems` `Put` gives for it.
///
/// # Errors
///
/// Returns the refusal of a multi-attribute key, which fails the request.
pub(super) async fn member_reason(
    ctx: &OperationContext,
    stmt: &Statement,
    req: &SingleRequest,
) -> Result<Option<CancellationReason>, DynamoDbError> {
    let key_info = match ctx.table_key_info(common::statement_table(stmt)).await {
        Ok(info) => info,
        Err(e) => {
            return match storage_err_to_dynamo(e) {
                DynamoDbError::ResourceNotFoundException(_) => Ok(Some(not_found_reason())),
                other => Err(other),
            };
        }
    };
    common::ensure_single_attribute_keys(&key_info.key_schema)?;
    let params = &req.parameters;
    let condition_key =
        |where_clause: Option<&WhereExpr>| condition_key_reason(where_clause, &key_info, params);
    match stmt {
        Statement::Insert(insert) => match insert_key_reason(insert, &key_info, params)? {
            Some(reason) => Ok(Some(reason)),
            None => Ok(index_key_reason(ctx, stmt, &key_info, params).await),
        },
        Statement::Update(update) => {
            if let Some(reason) = key_reason(stmt, &key_info, params)?
                .or_else(|| key_update_reason(update, &key_info))
            {
                return Ok(Some(reason));
            }
            Ok(match index_key_reason(ctx, stmt, &key_info, params).await {
                Some(reason) => Some(reason),
                None => condition_key(update.where_clause.as_ref()),
            })
        }
        Statement::Delete(delete) => Ok(key_reason(stmt, &key_info, params)?
            .or_else(|| condition_key(Some(&delete.where_clause)))),
        Statement::Exists(select) => Ok(condition_key(select.where_clause.as_ref())),
        Statement::Select(_) => Ok(None),
    }
}

/// The reason an `INSERT` or an `UPDATE` member reports when its request
/// gives a secondary index key a value of the wrong type: the `INSERT`'s
/// item, or a value the `UPDATE` sets. Each member that does so has the
/// reason beside the reasons of the other members, and an `UPDATE` has it
/// after a SET of a key attribute and ahead of a path into a key attribute
/// among its other conditions (measured 2026-11-05). A member whose request
/// cannot be built has none here: its refusal comes when it is built.
async fn index_key_reason(
    ctx: &OperationContext,
    stmt: &Statement,
    key_info: &TableKeyInfo,
    params: &[AttributeValue],
) -> Option<CancellationReason> {
    let refusal = match stmt {
        Statement::Insert(insert) => {
            let parts = write::build_insert_parts(ctx, insert, params).await.ok()?;
            write::insert_refusal(&parts, key_info).ok()?
        }
        Statement::Update(update) => {
            let parts = write::build_update_parts(ctx, update, params).await.ok()?;
            write::update_index_key_refusal(&ctx.limits, &parts, key_info).ok()?
        }
        Statement::Delete(_) | Statement::Exists(_) | Statement::Select(_) => None,
    };
    refusal.map(|error| CancellationReason::validation_error(error.message()))
}

/// The reason a member reports when its WHERE pins its item and reads a
/// path into a key attribute among its other conditions (see
/// `common::condition_key_refusal`), after the reasons of its key and of a
/// SET of a key attribute (measured 2026-11-05 on UPDATE, DELETE and
/// EXISTS).
fn condition_key_reason(
    where_clause: Option<&WhereExpr>,
    key_info: &TableKeyInfo,
    params: &[AttributeValue],
) -> Option<CancellationReason> {
    let (Some(pk), sk) = common::key_names(&key_info.key_schema) else {
        return None;
    };
    common::condition_key_refusal(where_clause?, pk, sk, params)
        .map(CancellationReason::validation_error)
}

/// The reason an `UPDATE` or `DELETE` reports when its WHERE does not give the
/// full primary key by equality, or gives a key value whose type is not the
/// one the table defines.
fn key_reason(
    stmt: &Statement,
    key_info: &TableKeyInfo,
    params: &[AttributeValue],
) -> Result<Option<CancellationReason>, DynamoDbError> {
    let (Some(pk), sk) = common::key_names(&key_info.key_schema) else {
        return Err(DynamoDbError::InternalServerError(
            "table has no partition key".to_owned(),
        ));
    };
    let matches = common::write_key(stmt, pk, sk, params)?.is_some_and(|key| {
        key.iter()
            .all(|(name, value)| common::wire_key_value_matches(key_info, name, value))
    });
    if matches {
        Ok(None)
    } else {
        Ok(Some(CancellationReason::validation_error(
            common::KEY_SCHEMA_MISMATCH,
        )))
    }
}

/// The reason an `UPDATE` reports when it sets or removes a key attribute,
/// naming the first one in statement order.
fn key_update_reason(
    update: &UpdateStatement,
    key_info: &TableKeyInfo,
) -> Option<CancellationReason> {
    update
        .sets
        .iter()
        .map(|set| &set.path)
        .chain(&update.removes)
        .find_map(|path| {
            key_info
                .key_schema
                .iter()
                .find(|key| key.attribute_name == path.root)
        })
        .map(|key| {
            CancellationReason::validation_error(format!(
                "One or more parameter values were invalid: Cannot update attribute {}. \
                 This attribute is part of the key",
                key.attribute_name
            ))
        })
}

/// The reason an `INSERT` reports when its item leaves out a key attribute or
/// gives one a value of the wrong type, worded as a `TransactWriteItems` `Put`
/// of that item words it rather than as `ExecuteStatement` does.
fn insert_key_reason(
    insert: &InsertStatement,
    key_info: &TableKeyInfo,
    params: &[AttributeValue],
) -> Result<Option<CancellationReason>, DynamoDbError> {
    let mut keys = Item::new();
    for (name, operand) in common::insert_values(insert) {
        if key_info
            .key_schema
            .iter()
            .any(|key| key.attribute_name == name)
        {
            keys.insert(name.to_owned(), write::resolve_av(operand, params)?);
        }
    }
    Ok(
        validate_item_keys(&keys, &key_info.key_schema, &key_info.attribute_definitions)
            .err()
            .map(|e| CancellationReason::validation_error(e.message())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::partiql::parse;
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    /// A pk(S)/sk(S) table.
    fn composite_table() -> TableKeyInfo {
        let key = |name: &str, key_type| KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        };
        let string = |name: &str| AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: ScalarAttributeType::S,
        };
        TableKeyInfo {
            table_name: "t".to_owned(),
            key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
            attribute_definitions: vec![string("pk"), string("sk")],
            ..TableKeyInfo::default()
        }
    }

    /// The reason message a write member reports on the composite table.
    fn member_message(sql: &str) -> Option<String> {
        let table = composite_table();
        let stmt = parse(sql).unwrap();
        let reason = match &stmt {
            Statement::Insert(insert) => insert_key_reason(insert, &table, &[]).unwrap(),
            Statement::Update(update) => key_reason(&stmt, &table, &[])
                .unwrap()
                .or_else(|| key_update_reason(update, &table)),
            Statement::Delete(_) => key_reason(&stmt, &table, &[]).unwrap(),
            Statement::Select(_) | Statement::Exists(_) => unreachable!("not a write"),
        };
        reason.map(|r| {
            assert_eq!(r.code, "ValidationError", "{sql}");
            r.message.unwrap()
        })
    }

    #[test]
    fn a_member_without_the_full_key_does_not_match_the_schema() {
        for sql in [
            "UPDATE \"t\" SET a = 1 WHERE pk = 'k'",
            "UPDATE \"t\" SET a = 1 WHERE a = 2",
            "DELETE FROM \"t\" WHERE pk = 'k'",
            "UPDATE \"t\" SET a = 1 WHERE pk = 'k' AND sk = 1",
            "DELETE FROM \"t\" WHERE pk = 'k' AND sk = 1",
        ] {
            assert_eq!(
                member_message(sql).as_deref(),
                Some("The provided key element does not match the schema"),
                "{sql}"
            );
        }
    }

    #[test]
    fn a_member_with_the_full_key_translates() {
        for sql in [
            "UPDATE \"t\" SET a = 1 WHERE pk = 'k' AND sk = 's'",
            "UPDATE \"t\" SET a = 1 WHERE pk = 'k' AND sk = 's' AND a = 2",
            "DELETE FROM \"t\" WHERE sk = 's' AND pk = 'k'",
            "INSERT INTO \"t\" VALUE {'pk': 'k', 'sk': 's', 'a': 1}",
        ] {
            assert_eq!(member_message(sql), None, "{sql}");
        }
    }

    #[test]
    fn an_update_of_a_key_attribute_names_it() {
        for (sql, name) in [
            (
                "UPDATE \"t\" SET pk = 'z' WHERE pk = 'k' AND sk = 's'",
                "pk",
            ),
            ("UPDATE \"t\" REMOVE sk WHERE pk = 'k' AND sk = 's'", "sk"),
            (
                "UPDATE \"t\" SET a = 1, sk = 'z' WHERE pk = 'k' AND sk = 's'",
                "sk",
            ),
        ] {
            assert_eq!(
                member_message(sql),
                Some(format!(
                    "One or more parameter values were invalid: Cannot update attribute \
                     {name}. This attribute is part of the key"
                )),
                "{sql}"
            );
        }
    }

    #[test]
    fn an_insert_key_problem_takes_the_put_message() {
        for (sql, message) in [
            (
                "INSERT INTO \"t\" VALUE {'pk': 'k'}",
                "Missing the key sk in the item",
            ),
            (
                "INSERT INTO \"t\" VALUE {'sk': 's'}",
                "Missing the key pk in the item",
            ),
            (
                "INSERT INTO \"t\" VALUE {'pk': 'k', 'sk': 1}",
                "Type mismatch for key sk expected: S actual: N",
            ),
        ] {
            assert_eq!(
                member_message(sql),
                Some(format!(
                    "One or more parameter values were invalid: {message}"
                )),
                "{sql}"
            );
        }
    }

    #[test]
    fn too_few_parameters_name_the_member() {
        let stmt = parse("UPDATE \"t\" SET a = ? WHERE pk = ? AND sk = 's'").unwrap();
        assert_eq!(
            check_parameter_count(&stmt, 1, 2)
                .err()
                .map(|e| e.message().to_owned())
                .as_deref(),
            Some(
                "Validation failed in TransactStatements[2]: Number of parameters in request \
                 and statement don't match."
            )
        );
        assert!(check_parameter_count(&stmt, 2, 2).is_ok());
        assert!(check_parameter_count(&stmt, 3, 2).is_ok());
        let quoted = parse("UPDATE \"t\" SET a = '?' WHERE pk = 'k'").unwrap();
        assert!(check_parameter_count(&quoted, 0, 0).is_ok());
    }
}
