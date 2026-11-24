// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `next_line` dropped while its blocking fill runs: the next call on
//! the same reader serves the lines that fill read, never an early end of
//! file.

use std::io::Read;
use std::sync::mpsc;
use std::time::Duration;

use super::*;

/// Raw bytes that are held back until the gate opens, as an object store
/// read that waits on its answer.
struct Gated {
    gate: mpsc::Receiver<()>,
    started: mpsc::Sender<()>,
    open: bool,
    data: std::io::Cursor<Vec<u8>>,
}

impl Read for Gated {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.open {
            let _ = self.started.send(());
            let _ = self.gate.recv();
            self.open = true;
        }
        self.data.read(buf)
    }
}

#[tokio::test]
async fn a_line_read_dropped_during_its_fill_hands_the_lines_to_the_next_call() {
    let data = b"line-1\nline-2\nline-3\n".to_vec();
    let size = u64::try_from(data.len()).unwrap();
    let (open_gate, gate) = mpsc::channel();
    let (started, fill_started) = mpsc::channel();
    let raw = Gated {
        gate,
        started,
        open: false,
        data: std::io::Cursor::new(data),
    };
    let mut reader = line_reader(Box::new(raw), size, InputCodec::None).unwrap();

    let first = tokio::time::timeout(Duration::from_millis(100), reader.next_line(1024)).await;
    assert!(first.is_err(), "the call answered while its fill was held");
    fill_started
        .recv_timeout(Duration::from_secs(5))
        .expect("the fill reached the held read");
    open_gate.send(()).unwrap();

    let mut lines = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), reader.next_line(1024))
            .await
            .expect("the next call answers")
            .expect("no fault");
        match next {
            Some(line) => lines.push(line),
            None => break,
        }
    }
    assert_eq!(lines, ["line-1", "line-2", "line-3"]);
    assert!(reader.etag().is_some(), "the reader reached its end");
}
