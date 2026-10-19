//! The connections between replicas: replica i dials every replica j > i at wss://dosa-j.<domain>/mesh (and is
//! dialled by every lower one), so each pair has one WebSocket, used both ways. Both ends say Hello first; then
//! every message is a coord::Msg as JSON text. A ping every PING keeps Cloudflare from closing it (it closes one
//! silent for 100 s), and one that hears nothing for SILENT is closed. A dial that fails is tried again, 1 s after
//! the first failure up to 30 s apart.

use crate::coord::Node;
use crate::raft::Msg;
use crate::replica_url;
use axum::extract::ws::{Message as AxMsg, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as TMsg;

const PING: Duration = Duration::from_secs(20);
const SILENT: Duration = Duration::from_secs(60);

/// A frame, whichever WebSocket library carried it.
pub enum Frame {
    Text(String),
    Ping,
    Close,
    Other,
}

/// The mesh key, when one is set (DOSA_MESH_KEY): a connection without it is refused.
fn mesh_key() -> Option<String> {
    std::env::var("DOSA_MESH_KEY").ok().filter(|k| !k.is_empty())
}

/// GET /mesh: another replica connects.
pub async fn accept(ws: WebSocketUpgrade, State(node): State<Arc<Node>>, Query(q): Query<HashMap<String, String>>) -> Response {
    let ok = match mesh_key() {
        Some(k) => q.get("key") == Some(&k),
        None => true,
    };
    ws.on_upgrade(move |socket: WebSocket| async move {
        if !ok {
            return;
        }
        let (sink, stream) = socket.split();
        let stream = stream.map(|m| {
            m.map(|m| match m {
                AxMsg::Text(t) => Frame::Text(t),
                AxMsg::Close(_) => Frame::Close,
                _ => Frame::Other,
            })
        });
        let sink = sink.with(|f: Frame| async move {
            Ok::<_, axum::Error>(match f {
                Frame::Text(t) => AxMsg::Text(t),
                Frame::Ping => AxMsg::Ping(Vec::new()),
                _ => AxMsg::Close(None),
            })
        });
        link(node, stream, Box::pin(sink), None).await;
    })
}

/// Dials replica j for good: connected, then again whenever the connection ends.
pub async fn dial(node: Arc<Node>, j: u32) {
    let base = replica_url(j).replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
    let url = match mesh_key() {
        Some(k) => format!("{base}/mesh?from={}&key={k}", node.me),
        None => format!("{base}/mesh?from={}", node.me),
    };
    let mut wait = Duration::from_secs(1);
    loop {
        match tokio::time::timeout(Duration::from_secs(15), tokio_tungstenite::connect_async(url.as_str())).await {
            Ok(Ok((ws, _))) => {
                wait = Duration::from_secs(1);
                let (sink, stream) = ws.split();
                let stream = stream.map(|m| {
                    m.map(|m| match m {
                        TMsg::Text(t) => Frame::Text(t.to_string()),
                        TMsg::Close(_) => Frame::Close,
                        _ => Frame::Other,
                    })
                });
                let sink = sink.with(|f: Frame| async move {
                    Ok::<_, tokio_tungstenite::tungstenite::Error>(match f {
                        Frame::Text(t) => TMsg::text(t),
                        Frame::Ping => TMsg::Ping(Default::default()),
                        _ => TMsg::Close(None),
                    })
                });
                link(node.clone(), stream, Box::pin(sink), Some(j)).await;
            }
            _ => {}
        }
        let jitter = Duration::from_millis(crate::coord::random_u64() % 1000);
        tokio::time::sleep(wait + jitter).await;
        wait = (wait * 2).min(Duration::from_secs(30));
    }
}

/// One connection: Hello both ways, then the peer's messages to the node and the node's to the peer, until it ends.
async fn link<S, E1, K, E2>(node: Arc<Node>, mut stream: S, mut sink: std::pin::Pin<Box<K>>, dialled: Option<u32>)
where
    S: Stream<Item = Result<Frame, E1>> + Unpin,
    K: Sink<Frame, Error = E2> + ?Sized,
{
    let hello = serde_json::to_string(&node.hello()).unwrap();
    if sink.send(Frame::Text(hello)).await.is_err() {
        return;
    }
    // Their Hello first.
    let (id, boot, first) = loop {
        match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(Frame::Text(t)))) => match serde_json::from_str::<Msg>(&t) {
                Ok(m @ Msg::Hello { .. }) => {
                    let Msg::Hello { id, ref boot, .. } = m else { unreachable!() };
                    let boot = boot.clone();
                    break (id, boot, m);
                }
                _ => return,
            },
            Ok(Some(Ok(Frame::Other))) => continue,
            _ => return,
        }
    };
    if id == node.me || dialled.map(|j| j != id).unwrap_or(false) || id == 0 || id > node.n + 16 {
        return; // (itself, or an address that now answers as another replica)
    }
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let serial = node.register(id, boot, tx);
    node.on_msg(id, first);
    let writer = async {
        let mut ping = tokio::time::interval(PING);
        ping.tick().await;
        loop {
            tokio::select! {
                m = rx.recv() => match m {
                    Some(t) => if sink.send(Frame::Text(t)).await.is_err() { break },
                    None => break, // (replaced by a newer connection)
                },
                _ = ping.tick() => if sink.send(Frame::Ping).await.is_err() { break },
            }
        }
        let _ = sink.send(Frame::Close).await;
    };
    let reader = async {
        loop {
            match tokio::time::timeout(SILENT, stream.next()).await {
                Ok(Some(Ok(Frame::Text(t)))) => match serde_json::from_str::<Msg>(&t) {
                    Ok(m) => node.on_msg(id, m),
                    Err(e) => println!("mesh: replica {id} sent something unreadable: {e}"),
                },
                Ok(Some(Ok(Frame::Other))) | Ok(Some(Ok(Frame::Ping))) => {}
                _ => break,
            }
        }
    };
    tokio::select! {
        _ = writer => {}
        _ = reader => {}
    }
    node.unregister(id, serial);
}
