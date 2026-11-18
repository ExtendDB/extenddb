// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The echo of a statement list past its limit: the service prints its
//! request objects with every field (measured 2026-11-02).

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Map, Value};

/// The longest echo of a statement list, in bytes. Every echo measured is
/// shorter. A list can echo to about 13 times its size on the wire (a
/// `{"NULL": true}` parameter prints in 186 characters), so the echo stops
/// here rather than render a 16 MB request into hundreds of MB. The
/// service's text for an echo this long is not measured.
pub(super) const MAX_ECHO_BYTES: usize = 1 << 20;

/// The text of an echo, which stops growing at [`MAX_ECHO_BYTES`].
pub(super) struct Echo(String);

impl Echo {
    fn new() -> Self {
        Self(String::new())
    }

    /// Whether the echo has reached [`MAX_ECHO_BYTES`], so nothing more is
    /// written.
    fn full(&self) -> bool {
        self.0.len() >= MAX_ECHO_BYTES
    }

    /// Append `text`, or as much of it as fits, cut at a character boundary.
    fn push(&mut self, text: &str) {
        let room = MAX_ECHO_BYTES.saturating_sub(self.0.len());
        if text.len() <= room {
            self.0.push_str(text);
            return;
        }
        let mut end = room;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        self.0.push_str(&text[..end]);
    }
}

/// A statement list as the service echoes it, each member written by
/// `member`, at most [`MAX_ECHO_BYTES`] of it.
pub(super) fn echo_list(list: Option<&Value>, member: fn(&mut Echo, &Value)) -> String {
    let mut echo = Echo::new();
    write_list(&mut echo, list, member);
    echo.0
}

/// A `BatchExecuteStatement` member as the service echoes it (measured
/// 2026-11-02): a field it does not carry prints as `null`.
pub(super) fn write_batch_member(echo: &mut Echo, member: &Value) {
    echo.push("BatchStatementRequest(statement=");
    write_text(echo, member.get("Statement"));
    echo.push(", parameters=");
    write_list(echo, member.get("Parameters"), write_attribute_value);
    echo.push(", consistentRead=");
    write_flag(echo, member.get("ConsistentRead"));
    echo.push(", returnValuesOnConditionCheckFailure=");
    write_text(echo, member.get("ReturnValuesOnConditionCheckFailure"));
    echo.push(")");
}

/// An `ExecuteTransaction` member as the service echoes it (measured
/// 2026-11-02).
pub(super) fn write_transaction_member(echo: &mut Echo, member: &Value) {
    echo.push("ParameterizedStatement(statement=");
    write_text(echo, member.get("Statement"));
    echo.push(", parameters=");
    write_list(echo, member.get("Parameters"), write_attribute_value);
    echo.push(", returnValuesOnConditionCheckFailure=");
    write_text(echo, member.get("ReturnValuesOnConditionCheckFailure"));
    echo.push(")");
}

/// A parameter as the service echoes it (measured 2026-11-02): every member
/// of its value class, the ones the wire form cannot carry always `null`.
fn write_attribute_value(echo: &mut Echo, value: &Value) {
    let field = |name: &str| value.get(name);
    echo.push("AttributeValue(s=");
    write_text(echo, field("S"));
    echo.push(", n=");
    write_text(echo, field("N"));
    echo.push(", b=");
    write_binary(echo, field("B"));
    echo.push(", iNT=null, dECIMAL=null, dOUBLE=null, sS=");
    write_list(echo, field("SS"), |echo, v| write_text(echo, Some(v)));
    echo.push(", nS=");
    write_list(echo, field("NS"), |echo, v| write_text(echo, Some(v)));
    echo.push(", bS=");
    write_list(echo, field("BS"), |echo, v| write_binary(echo, Some(v)));
    echo.push(", iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=");
    write_map(echo, field("M"));
    echo.push(", l=");
    write_list(echo, field("L"), write_attribute_value);
    echo.push(", nULL=");
    write_flag(echo, field("NULL"));
    echo.push(", bOOL=");
    write_flag(echo, field("BOOL"));
    echo.push(")");
}

/// A string as it was sent, without quotes.
fn write_text(echo: &mut Echo, value: Option<&Value>) {
    match value {
        Some(Value::String(text)) => echo.push(text),
        _ => echo.push("null"),
    }
}

/// A boolean as `true` or `false`.
fn write_flag(echo: &mut Echo, value: Option<&Value>) {
    match value {
        Some(Value::Bool(true)) => echo.push("true"),
        Some(Value::Bool(false)) => echo.push("false"),
        _ => echo.push("null"),
    }
}

/// A binary value as the buffer holding its decoded bytes. A value that is
/// not base64 is not a binary the request can carry, and prints as `null`.
fn write_binary(echo: &mut Echo, value: Option<&Value>) {
    match value
        .and_then(Value::as_str)
        .map(|text| BASE64.decode(text))
    {
        Some(Ok(bytes)) => echo.push(&format!(
            "java.nio.HeapByteBuffer[pos=0 lim={0} cap={0}]",
            bytes.len()
        )),
        _ => echo.push("null"),
    }
}

/// A list as `[a, b]`. Writing stops at the first member past
/// [`MAX_ECHO_BYTES`].
fn write_list(echo: &mut Echo, value: Option<&Value>, write: impl Fn(&mut Echo, &Value)) {
    let Some(Value::Array(items)) = value else {
        echo.push("null");
        return;
    };
    echo.push("[");
    for (index, item) in items.iter().enumerate() {
        if echo.full() {
            return;
        }
        if index > 0 {
            echo.push(", ");
        }
        write(echo, item);
    }
    echo.push("]");
}

/// A map as `{name=value, ...}`, its members in the order the service prints
/// them (see [`print_order`]). Writing stops at the first member past
/// [`MAX_ECHO_BYTES`].
fn write_map(echo: &mut Echo, value: Option<&Value>) {
    let Some(Value::Object(members)) = value else {
        echo.push("null");
        return;
    };
    echo.push("{");
    for (index, (name, member)) in print_order(members).into_iter().enumerate() {
        if echo.full() {
            return;
        }
        if index > 0 {
            echo.push(", ");
        }
        echo.push(name);
        echo.push("=");
        write_attribute_value(echo, member);
    }
    echo.push("}");
}

/// The members of a map in the order the service prints them (measured
/// 2026-11-02 on maps of 2, 3, 4, 6, 12, 13, 24 and 48 members): the
/// iteration order of a Java `HashMap` filled from the request's map. Its
/// table has the smallest power of two of buckets at or above `size / 0.75`
/// rounded up. A key
/// sits in the bucket its Java `String.hashCode` picks once the hash's high
/// 16 bits are folded into its low 16 by exclusive or. The buckets print in
/// ascending order, and the keys of one bucket in the order the request sends
/// them.
///
/// Here the keys of one bucket come in the order the parsed request holds
/// them: the request's when the build includes the MongoDB storage crate,
/// whose JSON dependency makes every JSON map keep it (any build of the whole
/// workspace), and name order otherwise. In name order, two keys that share
/// a bucket print sorted by name, which differs from the service when the
/// request sends them in another order.
fn print_order(members: &Map<String, Value>) -> Vec<(&String, &Value)> {
    // `size / 0.75` rounded up, in whole numbers.
    let buckets = (members.len() * 4).div_ceil(3).next_power_of_two();
    let mut ordered: Vec<(usize, (&String, &Value))> = members
        .iter()
        .map(|member| (bucket(member.0, buckets), member))
        .collect();
    // The sort is stable, so the keys of one bucket keep their order.
    ordered.sort_by_key(|(bucket, _)| *bucket);
    ordered.into_iter().map(|(_, member)| member).collect()
}

/// The bucket of `key` in a Java `HashMap` table of `buckets` buckets.
fn bucket(key: &str, buckets: usize) -> usize {
    // Java's `String.hashCode`: 31 times the hash plus each UTF-16 unit,
    // wrapping at 32 bits.
    let hash = key.encode_utf16().fold(0_u32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(u32::from(unit))
    });
    (hash ^ (hash >> 16)) as usize & (buckets - 1)
}

#[cfg(test)]
mod tests;
