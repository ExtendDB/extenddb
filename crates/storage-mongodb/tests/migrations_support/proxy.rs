// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A TCP proxy in front of the test server that stops passing bytes while it
//! is paused and passes them again once it is resumed, as a primary that
//! hangs and then answers again does. Bytes that arrive while it is paused
//! are held, not dropped, and connections stay open.

use std::net::SocketAddr;

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
                tokio::spawn(copy_unless_paused(
                    client_read,
                    server_write,
                    watching.clone(),
                ));
                tokio::spawn(copy_unless_paused(
                    server_read,
                    client_write,
                    watching.clone(),
                ));
            }
        });
        Self {
            address,
            paused,
            accepting,
        }
    }

    /// The connection string of the proxy, for a direct connection.
    #[must_use]
    pub fn uri(&self) -> String {
        format!("mongodb://{}/?directConnection=true", self.address)
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

/// Copy `from` to `to`, holding what is read while the proxy is paused
/// until it is resumed.
async fn copy_unless_paused(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    mut paused: watch::Receiver<bool>,
) {
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if paused.wait_for(|paused| !*paused).await.is_err() {
            return;
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}
