//! Protocol behaviour against a minimal in-process WebSocket server on 127.0.0.1.
//! The same scenarios as packages/js/test/fake.test.js and packages/python/tests.

#![allow(clippy::result_large_err)] // the handshake callback signature is tungstenite's

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sol_feed::{Error, Feed, ReconnectInfo};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// What the n-th connection (1-based) gets.
enum Conn {
    /// Send these messages, then keep the socket open until the client goes away.
    Hold(Vec<Value>),
    /// Send these messages, then close with this code and reason.
    Close(Vec<Value>, u16, &'static str),
    /// Refuse the handshake with this HTTP status and body.
    Reject(u16, &'static str),
}

#[derive(Clone, Debug)]
struct Seen {
    path: String,
    auth: Option<String>,
}

struct Fake {
    url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Fake {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

async fn fake(script: impl Fn(usize) -> Conn + Send + Sync + 'static) -> Fake {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let script = Arc::new(script);
    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            let seen = seen2.clone();
            let script = script.clone();
            tokio::spawn(async move {
                let mut action = None;
                let cb = |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                    let mut seen = seen.lock().unwrap();
                    seen.push(Seen {
                        path: req.uri().to_string(),
                        auth: req.headers().get("authorization").map(|v| v.to_str().unwrap().to_string()),
                    });
                    let conn = script(seen.len());
                    if let Conn::Reject(status, body) = conn {
                        let mut r = ErrorResponse::new(Some(body.to_string()));
                        *r.status_mut() = StatusCode::from_u16(status).unwrap();
                        return Err(r);
                    }
                    action = Some(conn);
                    Ok(resp)
                };
                let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(tcp, cb).await else {
                    return;
                };
                match action {
                    Some(Conn::Hold(msgs)) => {
                        for m in msgs {
                            let _ = ws.send(WsMessage::text(m.to_string())).await;
                        }
                        while let Some(Ok(_)) = ws.next().await {}
                    }
                    Some(Conn::Close(msgs, code, reason)) => {
                        for m in msgs {
                            let _ = ws.send(WsMessage::text(m.to_string())).await;
                        }
                        let frame = CloseFrame {
                            code: CloseCode::from(code),
                            reason: reason.into(),
                        };
                        let _ = ws.close(Some(frame)).await;
                        while let Some(Ok(_)) = ws.next().await {}
                    }
                    _ => {}
                }
            });
        }
    });
    Fake { url, seen }
}

fn seq(m: &Result<sol_feed::Message, Error>) -> u64 {
    m.as_ref().expect("a message, not an error").seq().expect("a seq")
}

async fn next(s: &mut (impl futures_util::Stream<Item = Result<sol_feed::Message, Error>> + Unpin)) -> Result<sol_feed::Message, Error> {
    tokio::time::timeout(Duration::from_secs(10), s.next())
        .await
        .expect("timed out")
        .expect("stream ended")
}

#[tokio::test]
async fn sends_the_key_only_in_the_authorization_header() {
    let srv = fake(|_| Conn::Hold(vec![json!({"kind":"hello","seq":0,"epoch":1}), json!({"kind":"swap","seq":1})])).await;
    let mut feed = Feed::new("secret-key").url(&srv.url).kinds(["swap", "launch"]).venues(["pump"]);
    {
        let mut s = feed.stream();
        assert_eq!(seq(&next(&mut s).await), 1);
    }
    let seen = srv.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].auth.as_deref(), Some("Bearer secret-key"));
    assert_eq!(seen[0].path, "/ws?kinds=swap,launch&venues=pump");
    assert!(!seen[0].path.contains("secret-key"));
}

#[tokio::test]
async fn hello_and_heartbeat_are_not_yielded_and_a_message_exposes_its_fields() {
    let srv = fake(|_| {
        Conn::Hold(vec![
            json!({"kind":"hello","seq":0,"epoch":1}),
            json!({"kind":"heartbeat","seq":0}),
            json!({"v":1,"seq":1,"kind":"swap","venue":"pump","base_amount":"5"}),
        ])
    })
    .await;
    let mut feed = Feed::new("k").url(&srv.url);
    let mut s = feed.stream();
    let m = next(&mut s).await.unwrap();
    assert_eq!(m.kind(), "swap");
    assert_eq!(m.seq(), Some(1));
    assert_eq!(m["venue"], "pump");
    assert_eq!(m.value()["base_amount"], "5");
}

#[tokio::test]
async fn resumes_after_a_disconnect_from_the_last_event_never_a_heartbeat() {
    let srv = fake(|n| match n {
        1 => Conn::Close(
            vec![
                json!({"kind":"hello","seq":0,"epoch":7}),
                json!({"kind":"swap","seq":1}),
                json!({"kind":"swap","seq":2}),
                json!({"kind":"heartbeat","seq":50}),
            ],
            1000,
            "",
        ),
        _ => Conn::Hold(vec![json!({"kind":"hello","seq":0,"epoch":7}), json!({"kind":"swap","seq":3})]),
    })
    .await;
    let infos: Arc<Mutex<Vec<ReconnectInfo>>> = Arc::default();
    let infos2 = infos.clone();
    let mut feed = Feed::new("k")
        .url(&srv.url)
        .kinds(["swap"])
        .on_reconnect(move |i| infos2.lock().unwrap().push(i.clone()));
    {
        let mut s = feed.stream();
        let got = [seq(&next(&mut s).await), seq(&next(&mut s).await), seq(&next(&mut s).await)];
        assert_eq!(got, [1, 2, 3]);
    }
    assert_eq!((feed.epoch(), feed.last_seq()), (Some(7), 3));
    let seen = srv.seen();
    assert_eq!(seen[0].path, "/ws?kinds=swap");
    assert_eq!(seen[1].path, "/ws?kinds=swap&since=7:2");
    let infos = infos.lock().unwrap();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].attempt, 1);
    assert!(
        infos[0].delay >= Duration::from_secs(1) && infos[0].delay < Duration::from_secs(2),
        "{:?}",
        infos[0].delay
    );
}

#[tokio::test]
async fn a_new_epoch_in_hello_restarts_the_sequence() {
    let srv = fake(|n| match n {
        1 => Conn::Close(
            vec![json!({"kind":"hello","seq":0,"epoch":7}), json!({"kind":"swap","seq":40})],
            1001,
            "going away",
        ),
        _ => Conn::Hold(vec![json!({"kind":"hello","seq":0,"epoch":8}), json!({"kind":"swap","seq":1})]),
    })
    .await;
    let mut feed = Feed::new("k").url(&srv.url);
    {
        let mut s = feed.stream();
        assert_eq!(seq(&next(&mut s).await), 40);
        assert_eq!(seq(&next(&mut s).await), 1);
    }
    assert_eq!((feed.epoch(), feed.last_seq()), (Some(8), 1));
    assert_eq!(srv.seen()[1].path, "/ws?since=7:40");
}

#[tokio::test]
async fn an_epoch_changed_gap_moves_the_resume_point_to_the_new_epoch() {
    let srv = fake(|n| match n {
        1 => Conn::Hold(vec![
            json!({"kind":"hello","seq":0,"epoch":10}),
            json!({"kind":"swap","seq":5}),
            json!({"kind":"gap","seq":0,"missed":null,"reason":"epoch_changed","epoch":20}),
            json!({"kind":"swap","seq":1}),
            json!({"kind":"swap","seq":2}),
        ]),
        _ => Conn::Hold(vec![json!({"kind":"swap","seq":3})]),
    })
    .await;
    let mut feed = Feed::new("k").url(&srv.url);
    let mut seqs = Vec::new();
    {
        let mut s = feed.stream();
        loop {
            let m = next(&mut s).await.unwrap();
            seqs.push(m.seq().unwrap());
            if m.seq() == Some(2) {
                break;
            }
        }
    }
    assert_eq!(seqs, [5, 0, 1, 2]);
    assert_eq!((feed.epoch(), feed.last_seq()), (Some(20), 2));
    {
        // a fresh connection resumes from the new epoch
        let mut s = feed.stream();
        assert_eq!(seq(&next(&mut s).await), 3);
    }
    assert_eq!(srv.seen()[1].path, "/ws?since=20:2");
}

#[tokio::test]
async fn a_429_is_retried_with_backoff_and_reported_to_on_reconnect() {
    let srv = fake(|n| match n {
        1 => Conn::Reject(429, "connection limit for this key"),
        _ => Conn::Hold(vec![json!({"kind":"hello","seq":0,"epoch":3}), json!({"kind":"swap","seq":1})]),
    })
    .await;
    let infos: Arc<Mutex<Vec<ReconnectInfo>>> = Arc::default();
    let infos2 = infos.clone();
    let mut feed = Feed::new("k")
        .url(&srv.url)
        .on_reconnect(move |i| infos2.lock().unwrap().push(i.clone()));
    {
        let mut s = feed.stream();
        assert_eq!(seq(&next(&mut s).await), 1);
    }
    let infos = infos.lock().unwrap();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].attempt, 1);
    assert!(infos[0].reason.contains("429"), "{}", infos[0].reason);
    assert!(infos[0].reason.contains("connection limit"), "{}", infos[0].reason);
    assert_eq!(srv.seen().len(), 2);
}

#[tokio::test]
async fn a_503_is_retried() {
    let srv = fake(|n| match n {
        1 => Conn::Reject(503, "restarting"),
        _ => Conn::Hold(vec![json!({"kind":"hello","seq":0,"epoch":3}), json!({"kind":"swap","seq":9})]),
    })
    .await;
    let mut feed = Feed::new("k").url(&srv.url);
    let mut s = feed.stream();
    assert_eq!(seq(&next(&mut s).await), 9);
}

#[tokio::test]
async fn a_401_is_an_auth_error_with_the_server_reason_and_ends_the_stream() {
    let srv = fake(|_| Conn::Reject(401, "unknown key")).await;
    let mut feed = Feed::new("not-a-real-key").url(&srv.url);
    let mut s = feed.stream();
    match next(&mut s).await {
        Err(Error::Auth(msg)) => assert_eq!(msg, "unknown key"),
        other => panic!("expected Error::Auth, got {other:?}"),
    }
    assert!(s.next().await.is_none());
    drop(s);
    assert_eq!(srv.seen().len(), 1);
}

#[tokio::test]
async fn a_403_is_an_auth_error() {
    let srv = fake(|_| Conn::Reject(403, "")).await;
    let mut feed = Feed::new("k").url(&srv.url);
    let mut s = feed.stream();
    assert!(matches!(next(&mut s).await, Err(Error::Auth(m)) if m == "HTTP 403"));
}

#[tokio::test]
async fn a_400_is_a_feed_error_with_the_server_reason() {
    let srv = fake(|_| Conn::Reject(400, "unknown kind \"swapz\"\n")).await;
    let mut feed = Feed::new("k").url(&srv.url).kinds(["swapz"]);
    let mut s = feed.stream();
    match next(&mut s).await {
        Err(Error::Feed(msg)) => assert_eq!(msg, "unknown kind \"swapz\""),
        other => panic!("expected Error::Feed, got {other:?}"),
    }
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn close_4001_is_an_auth_error() {
    let srv = fake(|_| {
        Conn::Close(
            vec![json!({"kind":"hello","seq":0,"epoch":1}), json!({"kind":"swap","seq":1})],
            4001,
            "key revoked",
        )
    })
    .await;
    let mut feed = Feed::new("k").url(&srv.url);
    let mut s = feed.stream();
    assert_eq!(seq(&next(&mut s).await), 1);
    assert!(matches!(next(&mut s).await, Err(Error::Auth(m)) if m == "key revoked"));
    assert!(s.next().await.is_none());
    drop(s);
    assert_eq!(srv.seen().len(), 1);
}

#[tokio::test]
async fn close_1008_is_a_feed_error() {
    let srv = fake(|_| Conn::Close(vec![], 1008, "filter rejected")).await;
    let mut feed = Feed::new("k").url(&srv.url);
    let mut s = feed.stream();
    assert!(matches!(next(&mut s).await, Err(Error::Feed(m)) if m == "filter rejected"));
}

#[tokio::test]
async fn a_network_error_is_retried_and_reported() {
    // a port nothing listens on
    let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
    let infos: Arc<Mutex<Vec<ReconnectInfo>>> = Arc::default();
    let infos2 = infos.clone();
    let mut feed = Feed::new("k")
        .url(format!("ws://127.0.0.1:{port}/ws"))
        .on_reconnect(move |i| infos2.lock().unwrap().push(i.clone()));
    let mut s = feed.stream();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), s.next()).await.is_err(),
        "a network error must not end the stream"
    );
    let infos = infos.lock().unwrap();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].attempt, 1);
}

#[tokio::test]
async fn an_empty_key_is_refused_without_connecting() {
    let mut feed = Feed::new("");
    let mut s = feed.stream();
    assert!(matches!(next(&mut s).await, Err(Error::Auth(m)) if m == "no API key given"));
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn into_stream_is_owned_and_can_be_spawned() {
    let srv = fake(|_| Conn::Hold(vec![json!({"kind":"hello","seq":0,"epoch":1}), json!({"kind":"launch","seq":4})])).await;
    let feed = Feed::new("k").url(&srv.url);
    let handle = tokio::spawn(async move {
        let mut s = feed.into_stream();
        next(&mut s).await.unwrap().seq()
    });
    assert_eq!(handle.await.unwrap(), Some(4));
}

/// Hits production without a valid key: proves the rustls/ring TLS stack completes a real
/// handshake and that the server's 401 maps to Error::Auth. Run with `cargo test -- --ignored`.
#[tokio::test]
#[ignore = "needs network access to feed.lunarshadowlabs.com"]
async fn live_unknown_key_is_an_auth_error_over_tls() {
    let mut feed = Feed::new("not-a-real-key");
    let mut s = feed.stream();
    match tokio::time::timeout(Duration::from_secs(20), s.next()).await.expect("timed out") {
        Some(Err(Error::Auth(msg))) => println!("server said: {msg}"),
        other => panic!("expected Error::Auth, got {other:?}"),
    }
}
