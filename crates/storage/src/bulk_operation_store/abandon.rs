// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How the runner's abandon of an input file is recorded.

use serde_json::Value;

/// Why the runner stopped reading an input file before its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbandonKind {
    /// A line, or a compressed stream's header, the runner could not use.
    Content,
    /// The file was not a compressed object under the requested codec. The
    /// operation ends `FAILED` once the other files are read.
    NotCompressed,
    /// The object at the key was replaced after the operation listed it.
    /// The operation ends `FAILED` once the other files are read.
    Overwritten,
}

impl AbandonKind {
    /// The stored vocabulary for this kind, shared by every backend.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::NotCompressed => "not_compressed",
            Self::Overwritten => "overwritten",
        }
    }

    /// Parse the stored vocabulary back; `None` for anything else.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "content" => Some(Self::Content),
            "not_compressed" => Some(Self::NotCompressed),
            "overwritten" => Some(Self::Overwritten),
            _ => None,
        }
    }
}

/// An abandoned input file as its `input_files` entry keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileAbandon {
    /// Row index of the abandon. The file's processed range ends before it.
    pub row: i64,
    pub kind: AbandonKind,
    /// Bytes the file contributed to `ProcessedInputBytes`; `None` on an
    /// entry written before the figure was recorded.
    pub bytes: Option<i64>,
}

impl FileAbandon {
    /// The stored form, the `abandon` member of an `input_files` element,
    /// shared by every backend.
    #[must_use]
    pub fn to_json(self) -> Value {
        let mut stored = serde_json::json!({ "row": self.row, "kind": self.kind.as_str() });
        stored["bytes"] = self.bytes.map_or(Value::Null, Value::from);
        stored
    }

    /// Parse the stored form back; `None` for anything else.
    #[must_use]
    pub fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            row: value.get("row")?.as_i64()?,
            kind: AbandonKind::parse(value.get("kind")?.as_str()?)?,
            bytes: value.get("bytes").and_then(Value::as_i64),
        })
    }
}

/// What one checkpoint records about an input file the runner abandoned.
#[derive(Clone, Copy, Debug)]
pub struct AbandonedFile {
    /// Index into the operation's `input_files`.
    pub index: usize,
    /// Row index of the abandon.
    pub row: u64,
    pub kind: AbandonKind,
    /// Bytes the file contributed to `ProcessedInputBytes`.
    pub bytes: u64,
}
