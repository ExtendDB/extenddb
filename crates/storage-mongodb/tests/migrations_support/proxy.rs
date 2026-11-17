// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A TCP proxy in front of the test server that passes no bytes while it is
//! paused, as a primary that hangs does. A connection that carried bytes
//! during the silence stays silent after it: what it held is never
//! delivered, and it stays open, as a connection to a primary that never
//! answers what was sent to it. Connections that carried nothing during the
//! silence, and connections opened after it, pass bytes again once it is
//! resumed, as connections to the primary that answers do.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub struct PausingProxy {
    pub address: SocketAddr,
    paused: watch::Sender<bool>,
    accepting: JoinHandle<()>,
}

impl PausingProxy {
    /// A proxy on a port of 127.0.0.1 in front of the first host of `uri`.
    ///
    /// # Panics
    ///
    /// When the URI names no host or the port cannot be bound.
    pub async fn in_front_of(uri: &str) -> Self {
        let upstream = uri
            .strip_prefix("mongodb://")
            .and_then(|rest| rest.split(['/', '?', ',']).next())
            .map(|host| host.rsplit_once('@').map_or(host, |(_, h)| h).to_owned())
            .expect("a host in the URI");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (paused, watching) = watch::channel(false);
        let accepting = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(&upstream).await else {
                    continue;
                };
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                let silent = Arc::new(AtomicBool::new(false));
                tokio::spawn(copy_unless_silent(
                    client_read,
                    server_write,
                    watching.clone(),
                    silent.clone(),
                ));
                tokio::spawn(copy_unless_silent(
                    server_read,
                    client_write,
                    watching.clone(),
                    silent,
                ));
            }
        });
        Self {
            address,
            paused,
            accepting,
        }
    }

    /// The connection string of the proxy, for a direct connection. A
    /// connection the driver opens during the silence never finishes its
    /// handshake, and the driver opens at most two at a time by default, so
    /// the string lets it open more.
    #[must_use]
    pub fn uri(&self) -> String {
        format!(
            "mongodb://{}/?directConnection=true&maxConnecting=20",
            self.address
        )
    }

    pub fn pause(&self) {
        self.paused.send_replace(true);
    }

    pub fn resume(&self) {
        self.paused.send_replace(false);
    }
}

impl Drop for PausingProxy {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

/// Copy `from` to `to`. Bytes read while the proxy is paused make the
/// connection `silent`, both directions: from then on it passes nothing, and
/// its halves are kept so the connection stays open.
async fn copy_unless_silent(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    paused: watch::Receiver<bool>,
    silent: Arc<AtomicBool>,
) {
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if *paused.borrow() {
            silent.store(true, Ordering::SeqCst);
        }
        if silent.load(Ordering::SeqCst) {
            let _open = (from, to);
            std::future::pending::<()>().await;
            return;
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}
