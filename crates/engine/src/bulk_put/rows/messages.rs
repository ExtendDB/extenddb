// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The report messages the row parser emits and the bounds behind them,
//! each measured on the service.
/// The report message for a decoded attribute value with an unknown type
/// tag (measured), and for the parse-level causes with no message of their
/// own: a value with no type tag, a line over the byte cap (ExtendDB's own
/// limit), a condition field of the wrong JSON type. A parse-level error
/// abandons the remainder of the file, so every parse-level message says
/// so. Text that does not parse as JSON carries [`CORRUPT_STREAM_MESSAGE`]
/// instead, and a value with more than one tag carries
/// [`REPEATED_MEMBER_MESSAGE`].
pub(crate) const PARSE_ABANDON_MESSAGE: &str = "Expected DynamoDB type to be one of [S, N, B, BOOL, NULL, L, M, SS, NS, BS]. Remainder of the file will not be processed.";

/// The same text without the abandon suffix: an unknown type tag inside an
/// `ExpressionAttributeValues` entry fails the row alone, so the entry's
/// message carries the bare text (measured on the service).
/// [`PARSE_ABANDON_MESSAGE`] is this text plus the suffix, pinned by a
/// test.
pub(crate) const TYPE_TAG_MESSAGE: &str =
    "Expected DynamoDB type to be one of [S, N, B, BOOL, NULL, L, M, SS, NS, BS]";

/// The report message for a binary value that is not base64 (measured on
/// the service). The value did not decode, so the file is abandoned, with
/// this message rather than the type-tag one.
pub(crate) const BINARY_NOT_BASE64_MESSAGE: &str = "Expected a binary value in base64-encoded format. Remainder of the file will not be processed.";

/// The report message for a line whose bytes are not valid UTF-8 (measured
/// on the service). The file is abandoned at it.
pub(crate) const INVALID_UTF8_MESSAGE: &str = "Expected a Unicode sequence with UTF-8 binary encoding. Remainder of the file will not be processed.";

/// The report message for bytes the tokenizer cannot read as JSON: a line
/// that is not JSON text (plain words, a truncated object, invalid bytes
/// between tokens), a compressed input stream that is corrupt or ends
/// before its trailer, or gzip bytes read as text (all measured on the
/// service). The file is abandoned at it.
pub(crate) const CORRUPT_STREAM_MESSAGE: &str =
    "Unexpected token. Remainder of the file will not be processed.";

/// The report message for a file that is not a compressed object when
/// `InputCompressionType` is `GZIP` (measured on the service). The file counts
/// one processed and failed row and the operation ends `FAILED`.
pub(crate) const NOT_COMPRESSED_MESSAGE: &str = "The S3 object could not be decompressed";

/// The report message for an input object overwritten after the operation
/// listed it (measured on the service). The file counts one processed and
/// failed row and the operation ends `FAILED`.
pub(crate) const OVERWRITTEN_MESSAGE: &str =
    "The S3 object could not be imported because it was overwritten";

/// The report message for a line whose object has no `Item` (measured).
pub(crate) const ITEM_MISSING_MESSAGE: &str =
    "Expected 'Item' top level container. Remainder of the file will not be processed.";

/// The report message for a line whose object carries a top-level key other
/// than the four the record shape allows (measured on the service with an
/// extra key beside `Item` and with `Item` misspelled; the check runs
/// before the missing-`Item` one). The file is
/// abandoned at it.
pub(crate) const UNKNOWN_FIELD_MESSAGE: &str = "Unexpected top-level field. Expected one of [Item, ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues]. Remainder of the file will not be processed.";

/// The report message for an attribute value nested deeper than
/// [`MAX_PARSE_NESTING_DEPTH`] levels (measured on the service at 64 and 65
/// levels). The file is abandoned at it. Between the item nesting limit and
/// this bound the row fails alone with the short message the runner writes.
pub(crate) const NESTING_ABANDON_MESSAGE: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit. Remainder of the file will not be processed.";

/// Levels of map or list nesting in one attribute value at or beyond which
/// the file is abandoned (measured on the service: 63 levels is an item-level
/// fault, 64 abandons).
pub(super) const MAX_PARSE_NESTING_DEPTH: usize = 64;

/// [`MAX_PARSE_NESTING_DEPTH`] as a bracket depth of the raw line: the
/// record object and the attribute map open two brackets, every map or list
/// level opens two more (`{"M":{` or `{"L":[`), and the leaf typed value
/// opens one. A line at or beyond this depth is abandoned before parsing.
pub(super) const MAX_PARSE_BRACKET_DEPTH: usize = 2 * MAX_PARSE_NESTING_DEPTH + 3;

/// The four top-level keys a record may carry.
pub(super) const RECORD_KEYS: [&str; 4] = [
    "Item",
    "ConditionExpression",
    "ExpressionAttributeNames",
    "ExpressionAttributeValues",
];

/// The report message for an `Item` that is not a JSON object (measured).
pub(crate) const ITEM_NOT_OBJECT_MESSAGE: &str =
    "Expected character {. Remainder of the file will not be processed.";

/// The report message for an `N` value inside `Item` whose text is not a
/// number (measured on the service on a sort key). The value did not
/// decode, so the file is abandoned at the row with no item index. The same
/// text inside `ExpressionAttributeValues` fails the row alone.
pub(crate) const INVALID_NUMERIC_MESSAGE: &str =
    "Expected a valid numeric. Remainder of the file will not be processed.";

/// The report message for a member given twice where one was expected: a
/// record carrying a top-level member twice, even with equal values, and a
/// typed value carrying more than one tag, whether the tag is repeated or
/// the tags differ (measured on the service). The
/// file is abandoned at the row.
pub(crate) const REPEATED_MEMBER_MESSAGE: &str =
    "Expected character }. Remainder of the file will not be processed.";

/// The report message for a set tag whose value is not a JSON array
/// (measured on the service on `SS`). The file is abandoned at the
/// row.
pub(crate) const EXPECTED_ARRAY_MESSAGE: &str =
    "Expected character [. Remainder of the file will not be processed.";

/// The `PutItem` message for `ExpressionAttributeNames` supplied with no
/// expression to reference them. It is the message
/// `expression::validate_expression_param_usage` produces for the names
/// case, which carries no suffix.
pub(crate) const NAMES_WITHOUT_EXPRESSION_MESSAGE: &str =
    "ExpressionAttributeNames can only be specified when using expressions";

/// The `PutItem` message for `ExpressionAttributeValues` supplied with no
/// expression to reference them. It is the message
/// `expression::validate_expression_param_usage` produces for the values
/// case with `PutItem`'s single value-capable parameter,
/// `ConditionExpression`.
pub(crate) const VALUES_WITHOUT_EXPRESSION_MESSAGE: &str = "ExpressionAttributeValues can only be specified when using expressions: ConditionExpression is null";

/// The record message for an empty `ExpressionAttributeNames` map supplied
/// with a `ConditionExpression` (measured on the service). The row
/// fails alone and the file continues. It is the text the single-item
/// deserializer produces for the same map.
pub(crate) const NAMES_EMPTY_MESSAGE: &str = "ExpressionAttributeNames must not be empty";

/// The record message for an empty `ExpressionAttributeValues` map supplied
/// with a `ConditionExpression` (measured on the service).
pub(crate) const VALUES_EMPTY_MESSAGE: &str = "ExpressionAttributeValues must not be empty";

/// The report message for an `ExpressionAttributeValues` map over
/// [`MAX_EXPRESSION_VALUES_BYTES`] (measured on the service, and
/// ). The file is abandoned at the row, which
/// counts one failed row and carries no item index; the rows before it are
/// written. The row measured sat under the line cap, so the line-length
/// check does not reach this case.
pub(crate) const VALUES_TOO_LARGE_MESSAGE: &str =
    "ExpressionAttributeValues exceeds max size. Remainder of the file will not be processed.";

/// Serialized bytes of an `ExpressionAttributeValues` map at or below which
/// the map is not an abandon. Measured on the service: a map whose compact JSON
/// text is 2,097,152 bytes is not abandoned and one of 2,097,153
/// bytes is; the bound is on the whole map (two
/// values of 1,100,000 bytes abandon together) and counts each value's JSON
/// text, so a binary value counts its base64 text and a nested
/// value counts its map.
pub(crate) const MAX_EXPRESSION_VALUES_BYTES: usize = 2 * 1024 * 1024;

/// The report message for a row whose `ExpressionAttributeNames` and
/// `ExpressionAttributeValues` together pass [`MAX_EXPRESSION_MAPS_BYTES`]
/// while the values map alone is within its abandon bound (measured on the
/// service: a values map of exactly 2,097,152 bytes beside the names map
/// `{"#v": "v"}`). An item-level fault: the row fails with its
/// item index and the file goes on.
pub(crate) const MAPS_TOO_LARGE_MESSAGE: &str =
    "Combined size of ExpressionAttributeNames and ExpressionAttributeValues exceeds max size";

/// The bound the two maps are measured against together: the values map's
/// compact JSON text plus, for the names map, the bytes of each placeholder
/// and attribute name without JSON punctuation. Measured on the service: beside
/// `{"#v": "v"}` (3 such bytes) a values map of 2,097,152 bytes fails the
/// row with [`MAPS_TOO_LARGE_MESSAGE`] while 2,097,148 does not
/// reach this class; beside `{"#vvvvvvvvv": "v"}` (11 bytes)
/// 2,097,140 does not reach it. The compact JSON text of the names map
/// (10 and 18 bytes) would put both of those over the bound, so the names
/// map counts its raw bytes.
pub(crate) const MAX_EXPRESSION_MAPS_BYTES: usize = 2 * 1024 * 1024;

/// The report message for a row whose `ConditionExpression` text plus the
/// content bytes of its `ExpressionAttributeValues` reach
/// [`MAX_EXPRESSION_SIZE_BYTES`] while the two bounds above hold: the
/// single-item `PutItem` expression-size text without its parameter prefix
/// or byte count (measured on the service). An item-level fault: the row
/// fails with its item index and the file goes on.
pub(crate) const EXPRESSION_SIZE_MESSAGE: &str =
    "Expression size has exceeded the maximum allowed size;";

/// Bytes of the condition expression text plus the content of its values
/// at which the row fails with [`EXPRESSION_SIZE_MESSAGE`]. Measured
/// by bisection with the 35-byte condition
/// `attribute_not_exists(pk) OR #v = :v`: a value of 1,048,540 bytes
/// (1,048,575 together) is evaluated; a value of 1,048,541 bytes
/// (1,048,576 together) fails the row. Twelve sizes from 4,000 to
/// 2,097,133 bytes fit the rule.
pub(crate) const MAX_EXPRESSION_SIZE_BYTES: usize = 1024 * 1024;

/// The report message for an item whose attribute name or string value
/// contains U+0000. The hosted service stores the character; ExtendDB's
/// PostgreSQL backend cannot (`jsonb` refuses the byte, SQLSTATE 22021)
/// and the single-item write path answers it with an internal error
/// today. The bulk parser refuses the row ahead of the write so the fault
/// names the row and the load continues; the message is ExtendDB's own.
pub(crate) const NUL_IN_STRING_MESSAGE: &str = "One or more parameter values were invalid: Attribute names and string values may not contain U+0000";

/// The prefix of every `AttributeValue` deserializer message that names a
/// value rule a decoded value failed (an empty set, a duplicate member, a
/// `NULL` that is not `true`); `PutItem` answers these `ValidationException`
/// with the same text. Any other deserializer message is a shape fault.
pub(super) const VALUE_RULE_PREFIX: &str = "One or more parameter values were invalid:";
