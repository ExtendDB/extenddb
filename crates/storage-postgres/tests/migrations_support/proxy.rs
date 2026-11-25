// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A TCP proxy to the test server that can stop forwarding while it keeps
//! both connections open, what a client sees when the database host is
//! paused or vanishes without closing the connection, and can close both
//! connections, what the database sees when the client process dies. It
//! can also hold each chunk from the client for a delay before passing it
//! to the server, so what a client sends last (a close's Terminate) reaches
//! the server after the client has moved on.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use sqlx::postgres::PgConnectOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::Scratch;

pub struct Proxy {
    pub port: u16,
    /// While set, nothing is forwarded and both connections stay open.
    pub paused: Arc<AtomicBool>,
    /// Once set, every connection is closed on both sides within 50 ms.
    pub cut: Arc<AtomicBool>,
    /// Milliseconds each chunk from the client waits before it is passed to
    /// the server; 0 passes it at once.
    pub to_server_delay_ms: Arc<AtomicU64>,
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    paused: Arc<AtomicBool>,
    cut: Arc<AtomicBool>,
    delay_ms: Option<Arc<AtomicU64>>,
) {
    let mut buf = vec![0_u8; 16 * 1024];
    loop {
        // Returning drops both halves; once both pumps of a connection
        // return, its two sockets are closed.
        if cut.load(Ordering::SeqCst) {
            return;
        }
        if paused.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        // A read that times out is dropped before it completes, which loses
        // nothing on a TcpStream, so the pause is noticed within 50 ms.
        match tokio::time::timeout(Duration::from_millis(50), from.read(&mut buf)).await {
            Err(_) => {}
            Ok(Ok(0) | Err(_)) => return,
            Ok(Ok(n)) => {
                let delay = delay_ms.as_ref().map_or(0, |d| d.load(Ordering::SeqCst));
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                if to.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        }
    }
}

impl Proxy {
    /// Listen on a loopback port and forward each connection to `upstream`.
    pub async fn start(upstream: (String, u16)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let paused = Arc::new(AtomicBool::new(false));
        let cut = Arc::new(AtomicBool::new(false));
        let to_server_delay_ms = Arc::new(AtomicU64::new(0));
        let (pause_flag, cut_flag) = (Arc::clone(&paused), Arc::clone(&cut));
        let delay = Arc::clone(&to_server_delay_ms);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect((upstream.0.as_str(), upstream.1)).await else {
                    continue;
                };
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                let directions = [
                    (client_read, server_write, Some(Arc::clone(&delay))),
                    (server_read, client_write, None),
                ];
                for (from, to, delay_ms) in directions {
                    tokio::spawn(pump(
                        from,
                        to,
                        Arc::clone(&pause_flag),
                        Arc::clone(&cut_flag),
                        delay_ms,
                    ));
                }
            }
        });
        Self {
            port,
            paused,
            cut,
            to_server_delay_ms,
        }
    }

    /// The scratch catalog's options, through this proxy.
    pub fn options(&self, s: &Scratch) -> PgConnectOptions {
        s.options().host("127.0.0.1").port(self.port)
    }
}
