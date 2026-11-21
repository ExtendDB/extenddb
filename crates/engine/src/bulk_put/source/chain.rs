// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The decode chain every input store reads an object through: the
//! enumerated size as the read ceiling, the counting and hashing reader, the
//! codec's magic check and decoder, and the batched line reader that serves
//! capped lines off the blocking pool, with the JSON prefix classifier that
//! decides the fault class of an invalid byte. A store opens the object and
//! hands its raw bytes to [`line_reader`].

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::codec::{CountingReader, ZstdFrames};
use super::*;

/// Lines pulled per blocking hop by the line reader.
const LINE_BATCH: usize = 64;

/// Byte bound for one batch of buffered lines: the fill stops once the
/// buffered lines reach this many bytes, whatever the line count, so a
/// batch of near-cap lines cannot hold hundreds of megabytes per open
/// file. Serving order and fault delivery are unchanged; the next fill
/// starts earlier.
const LINE_BATCH_BYTES: usize = 8 * 1024 * 1024;

/// Build the line reader over the raw bytes of one opened input object.
/// The reader yields lines from at most `size` bytes, the enumerated
/// compressed size; bytes past it read as end of file. Under `Gzip` or
/// `Zstd` the stream is decompressed while reading, every member or frame
/// of a concatenated stream included. Blocking: it peeks the codec's
/// magic bytes, so callers run it on the blocking pool.
pub(crate) fn line_reader(
    raw: Box<dyn Read + Send>,
    size: u64,
    compression: InputCodec,
) -> Result<ChainLineReader, SourceError> {
    // The enumerated size is the read ceiling: bytes appended after
    // enumeration are never read, and hitting the ceiling reads as end
    // of file.
    let limited = raw.take(size);
    let count = Arc::new(AtomicU64::new(0));
    let hasher = Arc::new(Mutex::new(<md5::Md5 as md5::Digest>::new()));
    let counting = CountingReader {
        inner: limited,
        count: Arc::clone(&count),
        hasher: Arc::clone(&hasher),
    };
    let inner: Box<dyn BufRead + Send> = if compression.decodes() {
        // A file that does not start with the codec's magic is not a
        // compressed object at all (its own fault class); a stream that
        // fails after the header is corrupt. Peek the magic through the
        // counting reader so the position stays right.
        let magic = compression.magic();
        let mut counting = BufReader::new(counting);
        let head = counting
            .fill_buf()
            .map_err(|e| StoreFault::of_io(&e).map_or(SourceError::Io(e), SourceError::Store))?;
        if head.len() < magic.len() || &head[..magic.len()] != magic {
            return Err(SourceError::NotCompressed);
        }
        match compression {
            // `MultiGzDecoder` reads every member of a concatenated gzip
            // stream (RFC 1952); a single-member decoder would silently
            // stop at the first trailer.
            InputCodec::Gzip => {
                Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(counting)))
            }
            // Every frame of a concatenated zstd stream is read (RFC
            // 8878); a truncated or corrupt frame surfaces as a read
            // error, the same contract as the gzip path. A header that
            // does not parse is corrupt data: the magic was already
            // checked, so this is not the `NotCompressed` class.
            InputCodec::Zstd => {
                let decoder = ZstdFrames::new(counting).map_err(|error| {
                    if error.kind() == std::io::ErrorKind::InvalidData {
                        SourceError::DecodeFailed
                    } else {
                        SourceError::Io(error)
                    }
                })?;
                Box::new(BufReader::new(decoder))
            }
            InputCodec::None => unreachable!("decodes() is false for None"),
        }
    } else {
        Box::new(BufReader::new(counting))
    };
    Ok(ChainLineReader {
        inner: Some(inner),
        filling: None,
        buffered: VecDeque::new(),
        line_ends: VecDeque::new(),
        last_line_end: 0,
        decoded_offset: 0,
        fault: None,
        compressed_bytes: count,
        hasher,
        at_end: false,
        codec: compression,
    })
}

/// Whether a JSON tokenizer that has consumed `prefix` (valid UTF-8 up to
/// the first invalid byte) is inside a string literal of a well-formed
/// single-value JSON prefix. The tokenizer walks the prefix as the hosted
/// one does, left to right: a bareword, a second top-level value, or any
/// structural error before the invalid byte is an unexpected token and wins
/// over the encoding fault (measured on the service:
/// `name,"value` then an invalid byte, and a complete object followed by a
/// stray quote then an invalid byte, both answer "Unexpected token.").
fn inside_string_literal(prefix: &[u8]) -> bool {
    #[derive(Clone, Copy, PartialEq)]
    enum Frame {
        Object,
        Array,
    }
    #[derive(Clone, Copy, PartialEq)]
    enum Expect {
        Value,
        KeyOrEnd,
        Key,
        Colon,
        CommaOrEnd,
        Done,
    }
    let mut stack: Vec<Frame> = Vec::new();
    let mut expect = Expect::Value;
    let mut i = 0;
    let n = prefix.len();

    // Consume a string body starting after the opening quote. Returns None
    // when the prefix ends inside the string.
    fn skip_string(prefix: &[u8], mut i: usize) -> Option<usize> {
        while i < prefix.len() {
            match prefix[i] {
                b'\\' => i += 2,
                b'"' => return Some(i + 1),
                _ => i += 1,
            }
        }
        None
    }

    // After a value closes, what the enclosing frame expects next.
    fn after_value(stack: &[Frame]) -> Expect {
        match stack.last() {
            None => Expect::Done,
            Some(_) => Expect::CommaOrEnd,
        }
    }

    while i < n {
        let b = prefix[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        match expect {
            Expect::Done => return false,
            Expect::Value => match b {
                b'"' => match skip_string(prefix, i + 1) {
                    Some(next) => {
                        i = next;
                        expect = after_value(&stack);
                    }
                    None => return true,
                },
                b'{' => {
                    stack.push(Frame::Object);
                    expect = Expect::KeyOrEnd;
                    i += 1;
                }
                b'[' => {
                    stack.push(Frame::Array);
                    i += 1;
                    if i < n && prefix[i] == b']' {
                        stack.pop();
                        expect = after_value(&stack);
                        i += 1;
                    } else {
                        expect = Expect::Value;
                    }
                }
                b'-' | b'0'..=b'9' => {
                    while i < n
                        && matches!(prefix[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                    {
                        i += 1;
                    }
                    expect = after_value(&stack);
                }
                b't' | b'f' | b'n' => {
                    let word: &[u8] = match b {
                        b't' => b"true",
                        b'f' => b"false",
                        _ => b"null",
                    };
                    let avail = (n - i).min(word.len());
                    if prefix[i..i + avail] != word[..avail] {
                        return false;
                    }
                    i += avail;
                    if avail < word.len() {
                        // The prefix ends inside the literal; the invalid
                        // byte follows a partial bareword, outside a string.
                        return false;
                    }
                    expect = after_value(&stack);
                }
                _ => return false,
            },
            Expect::KeyOrEnd | Expect::Key => match b {
                b'"' => match skip_string(prefix, i + 1) {
                    Some(next) => {
                        i = next;
                        expect = Expect::Colon;
                    }
                    None => return true,
                },
                b'}' if expect == Expect::KeyOrEnd => {
                    if stack.pop() != Some(Frame::Object) {
                        return false;
                    }
                    expect = after_value(&stack);
                    i += 1;
                }
                _ => return false,
            },
            Expect::Colon => {
                if b != b':' {
                    return false;
                }
                expect = Expect::Value;
                i += 1;
            }
            Expect::CommaOrEnd => match (b, stack.last()) {
                (b',', Some(Frame::Object)) => {
                    expect = Expect::Key;
                    i += 1;
                }
                (b',', Some(Frame::Array)) => {
                    expect = Expect::Value;
                    i += 1;
                }
                (b'}', Some(Frame::Object)) | (b']', Some(Frame::Array)) => {
                    stack.pop();
                    expect = after_value(&stack);
                    i += 1;
                }
                _ => return false,
            },
        }
    }
    // The prefix ended between tokens: the invalid byte is not inside a
    // string literal.
    false
}

/// What one blocking fill ended with.
enum Fill {
    /// The batch filled; the file has more.
    More,
    /// End of file.
    Eof,
    /// A fault, delivered to the caller after the buffered lines.
    Fault(SourceError),
}

/// What one blocking fill hands back: the chain, the lines it read with
/// their end offsets, how it ended, and the decoded offset after them.
type Filled = (Box<dyn BufRead + Send>, Vec<(String, u64)>, Fill, u64);

/// Line reader over one input object's decode chain. One `spawn_blocking`
/// hop pulls up to [`LINE_BATCH`] lines, or [`LINE_BATCH_BYTES`] bytes of them,
/// whichever comes first, into a buffer and `next_line` serves from it, so
/// file reads and decompression never run on the async runtime, the
/// blocking pool is not visited once per line, and one batch is bounded in
/// memory. After end of file,
/// `LineTooLong`, or an I/O error the inner reader is dropped and, once the
/// fault is delivered, every later call answers `Ok(None)`: both fault
/// classes abandon the file, so nothing may read past the fault point.
///
/// The chain moves into the blocking fill while it runs. The fill's handle
/// stays in the reader, so a `next_line` dropped before the fill returns
/// leaves the fill running and the next call awaits the same fill and
/// serves its lines; dropping the call never loses the chain.
pub(crate) struct ChainLineReader {
    pub(super) inner: Option<Box<dyn BufRead + Send>>,
    /// The blocking fill in progress, which holds the chain until it
    /// returns.
    filling: Option<tokio::task::JoinHandle<Filled>>,
    buffered: VecDeque<String>,
    /// The end offset of each buffered line, in the same order: the file
    /// offset for a plain object, the compressed bytes consumed when a
    /// decoder sits in the chain.
    line_ends: VecDeque<u64>,
    /// The end offset of the last line handed to the caller, which the
    /// runner reads through `last_line_end`.
    last_line_end: u64,
    /// Raw bytes of the decoded stream consumed by the lines read so far,
    /// terminators included. For a plain object this is the file offset;
    /// for a decoded one the compressed counter is used instead.
    decoded_offset: u64,
    fault: Option<SourceError>,
    compressed_bytes: Arc<AtomicU64>,
    /// The running digest the counting reader feeds; see `CountingReader`.
    hasher: Arc<Mutex<md5::Md5>>,
    /// Set when a fill reached end of file, so the digest covers the whole
    /// stream. A fault never sets it.
    at_end: bool,
    /// The codec `inner` decodes, which decides how a read error is
    /// classified (see `read_capped_line`).
    codec: InputCodec,
}

impl ChainLineReader {
    /// The decoded stream, for a caller that counts its lines rather than
    /// reading them; `None` once the reader reached its end or a fault.
    pub(crate) fn into_inner(mut self) -> Option<Box<dyn BufRead + Send>> {
        self.inner.take()
    }

    /// Start one blocking fill of up to [`LINE_BATCH`] lines, or
    /// [`LINE_BATCH_BYTES`] bytes of them, over `inner`.
    fn spawn_fill(
        &self,
        mut inner: Box<dyn BufRead + Send>,
        max_line_bytes: usize,
    ) -> tokio::task::JoinHandle<Filled> {
        let codec = self.codec;
        let decoded_start = self.decoded_offset;
        let compressed = Arc::clone(&self.compressed_bytes);
        tokio::task::spawn_blocking(move || {
            let mut lines: Vec<(String, u64)> = Vec::with_capacity(LINE_BATCH);
            let mut batch_bytes = 0usize;
            let mut fill = Fill::More;
            let mut decoded = decoded_start;
            while lines.len() < LINE_BATCH && batch_bytes < LINE_BATCH_BYTES {
                match read_capped_line(&mut inner, max_line_bytes, codec) {
                    Ok(Some((line, raw))) => {
                        batch_bytes = batch_bytes.saturating_add(line.len());
                        decoded = decoded.saturating_add(raw);
                        // A plain object's line ends at its own file
                        // offset; under a decoder the stored object's
                        // offsets are the compressed bytes the chain has
                        // pulled, which is what the progress counter
                        // reports.
                        let end = if codec.decodes() {
                            compressed.load(Ordering::Relaxed)
                        } else {
                            decoded
                        };
                        lines.push((line, end));
                    }
                    Ok(None) => {
                        fill = Fill::Eof;
                        break;
                    }
                    Err(fault) => {
                        fill = Fill::Fault(fault);
                        break;
                    }
                }
            }
            (inner, lines, fill, decoded)
        })
    }
}

impl LineReader for ChainLineReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if let Some(line) = self.buffered.pop_front() {
                if let Some(end) = self.line_ends.pop_front() {
                    self.last_line_end = end;
                }
                return Ok(Some(line));
            }
            if let Some(fault) = self.fault.take() {
                return Err(fault);
            }
            if self.filling.is_none() {
                let Some(inner) = self.inner.take() else {
                    return Ok(None);
                };
                self.filling = Some(self.spawn_fill(inner, max_line_bytes));
            }
            // The handle stays in the reader while it is awaited, so a call
            // dropped here leaves the fill for the next call to await.
            let joined = match self.filling.as_mut() {
                Some(filling) => filling.await,
                None => return Ok(None),
            };
            self.filling = None;
            let (inner, lines, fill, decoded_offset) =
                joined.map_err(|e| SourceError::Io(std::io::Error::other(e)))?;
            match fill {
                Fill::More => self.inner = Some(inner),
                Fill::Eof => self.at_end = true,
                Fill::Fault(fault) => self.fault = Some(fault),
            }
            self.decoded_offset = decoded_offset;
            let (lines, ends): (VecDeque<String>, VecDeque<u64>) = lines.into_iter().unzip();
            self.buffered = lines;
            self.line_ends = ends;
            match self.buffered.pop_front() {
                Some(line) => {
                    if let Some(end) = self.line_ends.pop_front() {
                        self.last_line_end = end;
                    }
                    Ok(Some(line))
                }
                None => match self.fault.take() {
                    Some(fault) => Err(fault),
                    None => Ok(None),
                },
            }
        })
    }

    fn bytes_consumed(&self) -> u64 {
        self.compressed_bytes.load(Ordering::Relaxed)
    }

    fn last_line_end(&self) -> u64 {
        self.last_line_end
    }

    fn etag(&self) -> Option<String> {
        if !self.at_end {
            return None;
        }
        let hasher = self.hasher.lock().ok()?;
        Some(format!("{:x}", md5::Digest::finalize(hasher.clone())))
    }
}

/// Read one line of at most `max_line_bytes` bytes, excluding the
/// terminator, with the raw bytes the read consumed for it (the line, its
/// terminator and a stripped carriage return included). `Ok(None)` at end
/// of file. The accumulation buffer never
/// grows past the cap: the moment the bytes seen for the current line
/// exceed it, the read stops with `LineTooLong` and consumes nothing
/// further, which the caller treats as a parse-level abandon of the file.
fn read_capped_line(
    reader: &mut impl BufRead,
    max_line_bytes: usize,
    codec: InputCodec,
) -> Result<Option<(String, u64)>, SourceError> {
    let mut line: Vec<u8> = Vec::new();
    let mut raw: u64 = 0;
    loop {
        let (consumed, done) = {
            // A decoder reports a corrupt or truncated stream as
            // InvalidData or UnexpectedEof on the read; a plain file read
            // never produces those kinds, so they classify the compressed
            // stream rather than the filesystem. The class depends on the
            // codec: a gzip stream decodes up to the damage, so the hosted
            // service reports the cut as an unreadable line; a zstd frame
            // that fails is reported as a failed decompression.
            let available = reader.fill_buf().map_err(|e| {
                // A fault the store rendered (an object store read that
                // failed mid-object) keeps its code and message under
                // every codec.
                if let Some(fault) = StoreFault::of_io(&e) {
                    return SourceError::Store(fault);
                }
                let decoder_fault = matches!(
                    e.kind(),
                    std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
                );
                match codec {
                    InputCodec::Gzip if decoder_fault => SourceError::Corrupt,
                    InputCodec::Zstd if decoder_fault => SourceError::DecodeFailed,
                    _ => SourceError::Io(e),
                }
            })?;
            if available.is_empty() {
                if line.is_empty() {
                    return Ok(None);
                }
                (0, true)
            } else if let Some(newline) = available.iter().position(|&b| b == b'\n') {
                if line.len() + newline > max_line_bytes {
                    return Err(SourceError::LineTooLong);
                }
                line.extend_from_slice(&available[..newline]);
                (newline + 1, true)
            } else {
                // No terminator in the buffer, so all of it belongs to the
                // current line: the line is already too long once the
                // running total passes the cap.
                if line.len() + available.len() > max_line_bytes {
                    return Err(SourceError::LineTooLong);
                }
                line.extend_from_slice(available);
                (available.len(), false)
            }
        };
        if consumed > 0 {
            reader.consume(consumed);
            raw = raw.saturating_add(consumed as u64);
        }
        if done {
            break;
        }
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    // JSON text is UTF-8, so a line that is not valid UTF-8 is not
    // well-formed JSON. It is reported as its own fault, which the runner
    // treats as parse-level (the class the spec gives malformed content);
    // decoding it lossily would write the customer's data altered, with
    // no counter, record or log line to say so. Which fault depends on
    // where the first invalid byte sits (measured on the service): inside a
    // string literal it is the UTF-8 fault, anywhere else the line is an
    // unexpected token.
    match String::from_utf8(line) {
        Ok(s) => Ok(Some((s, raw))),
        Err(e) => {
            let valid_up_to = e.utf8_error().valid_up_to();
            if inside_string_literal(&e.as_bytes()[..valid_up_to]) {
                Err(SourceError::InvalidUtf8)
            } else {
                Err(SourceError::Corrupt)
            }
        }
    }
}
