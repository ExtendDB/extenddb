// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A scripted HTTP listener on 127.0.0.1 for the tests that need a real
//! connection under the SDK's own HTTP client: an answer that never comes,
//! a body that stops after its head, a body far longer than the range
//! asked for. Each scripted answer serves one connection, and every
//! answer closes its connection, so each request the client sends arrives
//! on a connection of its own; once the script is spent the listener is
//! closed and a further connection is refused.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{Credentials, Region};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::super::S3Clients;
use super::super::client::behavior_version;
use super::super::retry::RetryPolicy;

/// What the listener does with one connection, after reading the request
/// head.
pub(super) enum Answer {
    /// Send the head and the body, then close the connection; a body
    /// shorter than the head's `content-length` is a body cut mid-stream.
    Send { head: String, body: Vec<u8> },
    /// Read the request and close the connection without an answer.
    Close,
    /// Send the head and the start of the body, then send nothing more
    /// while holding the connection open.
    Stall { head: String, body: Vec<u8> },
    /// Read the request and never answer.
    Hold,
    /// Send the head, then write body bytes until `total` or until the
    /// client stops reading; the bytes written are counted.
    Flood { head: String, total: u64 },
}

/// A response head with `connection: close` and the given headers.
pub(super) fn head(status: &str, headers: &[(&str, &str)]) -> String {
    let mut head = format!("HTTP/1.1 {status}\r\nconnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    head
}

/// The listener and what it saw.
pub(super) struct Stub {
    pub(super) url: String,
    heads: Arc<Mutex<Vec<String>>>,
    flooded: Arc<AtomicU64>,
    released: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Stub {
    pub(super) async fn start(answers: Vec<Answer>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let heads = Arc::new(Mutex::new(Vec::new()));
        let flooded = Arc::new(AtomicU64::new(0));
        let released = Arc::new(AtomicU64::new(0));
        let task = {
            let heads = Arc::clone(&heads);
            let flooded = Arc::clone(&flooded);
            let released = Arc::clone(&released);
            tokio::spawn(async move {
                for answer in answers {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let seen = Seen {
                        heads: Arc::clone(&heads),
                        flooded: Arc::clone(&flooded),
                        released: Arc::clone(&released),
                    };
                    tokio::spawn(serve(stream, answer, seen));
                }
            })
        };
        Self {
            url,
            heads,
            flooded,
            released,
            task,
        }
    }

    /// The request heads received, in arrival order.
    pub(super) fn heads(&self) -> Vec<String> {
        self.heads.lock().unwrap().clone()
    }

    /// The value of header `name` in request `index`, names compared
    /// without case.
    pub(super) fn header(&self, index: usize, name: &str) -> Option<String> {
        let heads = self.heads();
        heads.get(index)?.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name)
                .then(|| value.trim().to_owned())
        })
    }

    /// Body bytes a `Flood` answer managed to write.
    pub(super) fn flooded(&self) -> u64 {
        self.flooded.load(Ordering::SeqCst)
    }

    /// `Hold` connections the client has closed.
    pub(super) fn released(&self) -> u64 {
        self.released.load(Ordering::SeqCst)
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// What the connections of one listener record.
struct Seen {
    heads: Arc<Mutex<Vec<String>>>,
    flooded: Arc<AtomicU64>,
    released: Arc<AtomicU64>,
}

async fn serve(mut stream: TcpStream, answer: Answer, seen: Seen) {
    let mut request = Vec::new();
    let mut buf = [0u8; 4096];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => request.extend_from_slice(&buf[..n]),
        }
    }
    seen.heads
        .lock()
        .unwrap()
        .push(String::from_utf8_lossy(&request).into_owned());
    let flooded = seen.flooded;
    match answer {
        Answer::Send { head, body } => {
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        }
        Answer::Close => {
            let _ = stream.shutdown().await;
        }
        Answer::Stall { head, body } => {
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.flush().await;
            hold(stream).await;
        }
        Answer::Hold => {
            hold(stream).await;
            seen.released.fetch_add(1, Ordering::SeqCst);
        }
        Answer::Flood { head, total } => {
            if stream.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            let chunk = vec![b'x'; 64 * 1024];
            let mut written = 0u64;
            while written < total {
                let n = usize::try_from(total - written)
                    .unwrap_or(usize::MAX)
                    .min(chunk.len());
                if stream.write_all(&chunk[..n]).await.is_err() {
                    break;
                }
                written += n as u64;
                flooded.store(written, Ordering::SeqCst);
            }
        }
    }
}

/// Keep the connection open until the client closes it.
async fn hold(mut stream: TcpStream) {
    let mut buf = [0u8; 1024];
    while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
}

/// A store's clients against `url` as an S3-compatible endpoint, on the
/// SDK's own HTTP client, path-style, no retries by the SDK or by the
/// store, with `timeouts`.
pub(super) fn stub_clients(url: &str, timeouts: TimeoutConfig) -> S3Clients {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(behavior_version())
        .region(Region::new("us-east-1"))
        .endpoint_url(url)
        .force_path_style(true)
        .credentials_provider(Credentials::new("AKIDTEST", "secret", None, None, "test"))
        .retry_config(RetryConfig::disabled())
        .timeout_config(timeouts)
        .build();
    S3Clients::new(config, false, true).with_retry(RetryPolicy::NONE)
}

/// Short timeouts for a test against the listener.
pub(super) fn short_timeouts() -> TimeoutConfig {
    TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(2))
        .read_timeout(Duration::from_secs(2))
        .build()
}
