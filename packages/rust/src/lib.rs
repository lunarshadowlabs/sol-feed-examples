//! Rust client for the [Lunar Shadow Labs](https://lunarshadowlabs.com) feed: decoded Solana DEX
//! swaps, launches, migrations, pool creations and liquidity changes over one WebSocket.
//! Market data only.
//!
//! ```no_run
//! use sol_feed::{Feed, StreamExt};
//!
//! # async fn run() -> Result<(), sol_feed::Error> {
//! let mut feed = Feed::new(std::env::var("LSL_KEY").unwrap()).kinds(["swap"]);
//! let mut stream = feed.stream();
//! while let Some(m) = stream.next().await {
//!     let m = m?;
//!     println!("{} {} {}", m["venue"], m["base_mint"], m["price"]);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The stream reconnects with jittered exponential backoff and resumes where it left off
//! (`?since=<epoch>:<seq>`). It yields every event and every `gap` message (the only indication
//! that data was missed); `hello` and `heartbeat` are handled internally. It ends after yielding an
//! [`Error`], which only happens for problems retrying cannot fix: a refused key ([`Error::Auth`]),
//! a refused request ([`Error::Feed`]) or an undecodable message ([`Error::Decode`]).
//!
//! Message reference: <https://lunarshadowlabs.com/docs/>

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::borrow::BorrowMut;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

pub use futures_util::stream::BoxStream;
/// Re-exported so `stream.next().await` works without a direct `futures-util` dependency.
pub use futures_util::StreamExt;

use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message as WsMessage};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

/// The production feed endpoint.
pub const DEFAULT_URL: &str = "wss://feed.lunarshadowlabs.com/ws";

/// The crate version, sent nowhere; useful for logging.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The server heartbeats every few seconds: this much silence means a dead link.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// A problem retrying cannot fix. The stream yields it once and then ends.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The API key was refused (unknown, expired or revoked): HTTP 401/403 on connect, or close
    /// code 4001 on a live connection. The text is the server's reason. Your key is on
    /// <https://lunarshadowlabs.com/account/>; it is not your Whop license key.
    Auth(String),
    /// The server refused the request (HTTP 400 or close code 1008, for example an unknown filter
    /// name), or the URL is invalid. The text is the server's own reason.
    Feed(String),
    /// A message from the server was not valid JSON.
    Decode(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Auth(m) => write!(f, "API key refused: {m}"),
            Error::Feed(m) => write!(f, "request refused: {m}"),
            Error::Decode(e) => write!(f, "undecodable message: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Decode(e) => Some(e),
            _ => None,
        }
    }
}

/// Passed to the [`Feed::on_reconnect`] callback before each reconnect.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReconnectInfo {
    /// 1 for the first retry after a working connection, counting up while retries fail.
    pub attempt: u32,
    /// How long the client waits before this attempt.
    pub delay: Duration,
    /// Why the previous connection ended or failed.
    pub reason: String,
}

/// One message from the feed: an event or a `gap`. Derefs to the raw [`serde_json::Value`], so
/// `m["venue"]` works. Amounts are strings of raw integer units; see [`ui_amount`].
#[derive(Clone, Debug, PartialEq)]
pub struct Message(Value);

impl Message {
    /// The `kind` field (`swap`, `launch`, `curve_complete`, `migration`, `pool_create`,
    /// `liquidity`, `mint_update`, `gap`, ...), or `""` if absent.
    pub fn kind(&self) -> &str {
        self.0.get("kind").and_then(Value::as_str).unwrap_or("")
    }

    /// The `seq` field: the message's position in the server's stream.
    pub fn seq(&self) -> Option<u64> {
        self.0.get("seq").and_then(Value::as_u64)
    }

    /// True for a `gap` message: data was missed (`missed` holds the seq range when known).
    pub fn is_gap(&self) -> bool {
        self.kind() == "gap"
    }

    /// The raw JSON.
    pub fn value(&self) -> &Value {
        &self.0
    }

    /// The raw JSON, by value.
    pub fn into_value(self) -> Value {
        self.0
    }
}

impl std::ops::Deref for Message {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.0
    }
}

impl From<Message> for Value {
    fn from(m: Message) -> Value {
        m.0
    }
}

/// Scale a raw amount string by its `*_decimals` field as an `f64` (may lose precision; see
/// [`ui_amount_exact`]). `None` if the amount is not an unsigned integer.
pub fn ui_amount(amount: &str, decimals: u8) -> Option<f64> {
    ui_amount_exact(amount, decimals)?.parse().ok()
}

/// Scale a raw amount string by its `*_decimals` field, exactly, as a decimal string
/// (`("915407038388", 6)` gives `"915407.038388"`). `None` if the amount is not an unsigned integer.
pub fn ui_amount_exact(amount: &str, decimals: u8) -> Option<String> {
    if amount.is_empty() || !amount.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let trimmed = amount.trim_start_matches('0');
    let d = decimals as usize;
    let digits = format!("{trimmed:0>width$}", width = d + 1);
    if d == 0 {
        return Some(digits);
    }
    let (int, frac) = digits.split_at(digits.len() - d);
    let frac = frac.trim_end_matches('0');
    Some(if frac.is_empty() {
        int.to_string()
    } else {
        format!("{int}.{frac}")
    })
}

type Callback = Arc<dyn Fn(&ReconnectInfo) + Send + Sync>;

/// One subscription to the feed. Configure it with the builder methods, then read messages from
/// [`Feed::stream`] (borrowing) or [`Feed::into_stream`] (owned, `'static`, for `tokio::spawn`).
///
/// `kinds`, `venues` and `watch` are server-side filters (names in the docs). With no filter at
/// all the server applies the filter saved on your account page. An empty list is a real filter
/// (match nothing), not "no filter".
///
/// The resume point (`epoch`, `last_seq`) lives in the `Feed`, so dropping a stream and calling
/// [`Feed::stream`] again picks up right after the last message yielded.
#[derive(Clone)]
pub struct Feed {
    key: String,
    url: String,
    kinds: Option<Vec<String>>,
    venues: Option<Vec<String>>,
    watch: Option<Vec<String>>,
    max_backoff: Duration,
    on_reconnect: Option<Callback>,
    epoch: Option<u64>,
    last_seq: u64,
}

impl fmt::Debug for Feed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Feed")
            .field("key", &"<redacted>")
            .field("url", &self.url)
            .field("kinds", &self.kinds)
            .field("venues", &self.venues)
            .field("watch", &self.watch)
            .field("max_backoff", &self.max_backoff)
            .field("epoch", &self.epoch)
            .field("last_seq", &self.last_seq)
            .finish_non_exhaustive()
    }
}

fn list<I: IntoIterator<Item = S>, S: Into<String>>(v: I) -> Option<Vec<String>> {
    Some(v.into_iter().map(Into::into).collect())
}

impl Feed {
    /// A feed for this API key (sent only in the `Authorization: Bearer` header, never the URL).
    pub fn new(key: impl Into<String>) -> Self {
        Feed {
            key: key.into(),
            url: DEFAULT_URL.to_string(),
            kinds: None,
            venues: None,
            watch: None,
            max_backoff: Duration::from_secs(30),
            on_reconnect: None,
            epoch: None,
            last_seq: 0,
        }
    }

    /// The endpoint (default [`DEFAULT_URL`]). An existing query string is kept and extended.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// Only these event kinds, for example `["swap", "launch"]`.
    pub fn kinds<I: IntoIterator<Item = S>, S: Into<String>>(mut self, kinds: I) -> Self {
        self.kinds = list(kinds);
        self
    }

    /// Only these venues, for example `["pump", "pumpswap"]`.
    pub fn venues<I: IntoIterator<Item = S>, S: Into<String>>(mut self, venues: I) -> Self {
        self.venues = list(venues);
        self
    }

    /// Only events touching these mints, pools or wallets (a wallet matches every event it is the
    /// actor of).
    pub fn watch<I: IntoIterator<Item = S>, S: Into<String>>(mut self, watch: I) -> Self {
        self.watch = list(watch);
        self
    }

    /// The longest wait between reconnect attempts (default 30 s). The wait starts at 1 s, doubles
    /// up to this cap, and gets 0 to 1 s of random jitter on top.
    pub fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Called before each reconnect; useful for logging.
    pub fn on_reconnect(mut self, f: impl Fn(&ReconnectInfo) + Send + Sync + 'static) -> Self {
        self.on_reconnect = Some(Arc::new(f));
        self
    }

    /// Start from a resume point saved earlier (`epoch()` and `last_seq()` of a previous run).
    /// The server sends what it still buffers after `last_seq`, or a `gap` if it cannot.
    pub fn resume_from(mut self, epoch: u64, last_seq: u64) -> Self {
        self.epoch = Some(epoch);
        self.last_seq = last_seq;
        self
    }

    /// The server epoch of the stream being followed (from `hello`), once connected.
    pub fn epoch(&self) -> Option<u64> {
        self.epoch
    }

    /// The `seq` of the last event or gap yielded: the resume point.
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// The connect URL: filters and the resume point, never the key.
    pub fn connect_url(&self) -> String {
        let mut q: Vec<String> = Vec::new();
        for (name, v) in [("kinds", &self.kinds), ("venues", &self.venues), ("watch", &self.watch)] {
            if let Some(v) = v {
                let csv: Vec<String> = v.iter().map(|s| encode_component(s)).collect();
                q.push(format!("{name}={}", csv.join(",")));
            }
        }
        if let Some(epoch) = self.epoch {
            q.push(format!("since={epoch}:{}", self.last_seq));
        }
        if q.is_empty() {
            return self.url.clone();
        }
        let sep = if self.url.contains('?') { '&' } else { '?' };
        format!("{}{sep}{}", self.url, q.join("&"))
    }

    /// The messages, borrowing the feed. Drop the stream and call this again to resume.
    pub fn stream(&mut self) -> BoxStream<'_, Result<Message, Error>> {
        events(self)
    }

    /// The messages, owning the feed: a `'static` stream that can move into `tokio::spawn`.
    pub fn into_stream(self) -> BoxStream<'static, Result<Message, Error>> {
        events(self)
    }

    /// Track the resume point for one decoded frame. `Some` means yield it.
    fn observe(&mut self, m: Value) -> Option<Message> {
        let kind = m.get("kind").and_then(Value::as_str).unwrap_or("");
        if kind == "hello" {
            let epoch = m.get("epoch").and_then(Value::as_u64);
            if epoch.is_some() && epoch != self.epoch {
                // first connect, or the server restarted
                self.epoch = epoch;
                self.last_seq = 0;
            }
            return None;
        }
        if kind == "heartbeat" {
            return None; // never resume from a heartbeat's seq
        }
        if let Some(seq) = m.get("seq").and_then(Value::as_u64) {
            self.last_seq = seq;
        }
        if kind == "gap" && m.get("reason").and_then(Value::as_str) == Some("epoch_changed") {
            // the server restarted under a live connection: no new hello comes, and resuming
            // against the old epoch would replay events already yielded
            if let Some(epoch) = m.get("epoch").and_then(Value::as_u64) {
                self.epoch = Some(epoch);
            }
        }
        Some(Message(m))
    }
}

/// Percent-encode like JavaScript's `encodeURIComponent`, so a value can never smuggle in `&`,
/// `=` or a `,` that would split it.
fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

enum Ended {
    Fatal(Error),
    Retry(String),
}

struct State<F> {
    feed: F,
    ws: Option<Ws>,
    delay: Duration,
    attempt: u32,
    done: bool,
}

fn events<'a, F: BorrowMut<Feed> + Send + 'a>(feed: F) -> BoxStream<'a, Result<Message, Error>> {
    let st = State {
        feed,
        ws: None,
        delay: Duration::from_secs(1),
        attempt: 0,
        done: false,
    };
    futures_util::stream::unfold(st, |mut st| async move {
        if st.done {
            return None;
        }
        match next_message(&mut st).await {
            Ok(m) => Some((Ok(m), st)),
            Err(e) => {
                st.done = true;
                st.ws = None;
                Some((Err(e), st))
            }
        }
    })
    .boxed()
}

async fn next_message<F: BorrowMut<Feed>>(st: &mut State<F>) -> Result<Message, Error> {
    loop {
        let ws = match st.ws.as_mut() {
            Some(ws) => ws,
            None => match connect(st.feed.borrow()).await {
                Ok(ws) => {
                    st.delay = Duration::from_secs(1);
                    st.attempt = 0;
                    st.ws.insert(ws)
                }
                Err(Ended::Fatal(e)) => return Err(e),
                Err(Ended::Retry(reason)) => {
                    backoff(st, reason).await;
                    continue;
                }
            },
        };
        let frame = match tokio::time::timeout(IDLE_TIMEOUT, ws.next()).await {
            Ok(frame) => frame,
            Err(_) => Some(Err(tungstenite::Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("no data for {}s", IDLE_TIMEOUT.as_secs()),
            )))),
        };
        let reason = match frame {
            Some(Ok(frame @ (WsMessage::Text(_) | WsMessage::Binary(_)))) => {
                let v: Value = serde_json::from_slice(&frame.into_data()).map_err(Error::Decode)?;
                match st.feed.borrow_mut().observe(v) {
                    Some(m) => return Ok(m),
                    None => continue,
                }
            }
            Some(Ok(WsMessage::Close(Some(frame)))) => {
                let (code, reason) = (u16::from(frame.code), frame.reason.to_string());
                if code == 4001 {
                    return Err(Error::Auth(or(reason, "key revoked")));
                }
                if frame.code == CloseCode::Policy {
                    return Err(Error::Feed(or(reason, "policy violation")));
                }
                format!("closed {code} {reason}").trim_end().to_string()
            }
            Some(Ok(WsMessage::Close(None))) | None => "closed by server".to_string(),
            Some(Ok(_)) => continue, // ping, pong (answered by tungstenite)
            Some(Err(e)) => e.to_string(),
        };
        st.ws = None;
        backoff(st, reason).await;
    }
}

fn or(s: String, default: &str) -> String {
    if s.is_empty() {
        default.to_string()
    } else {
        s
    }
}

async fn backoff<F: BorrowMut<Feed>>(st: &mut State<F>, reason: String) {
    st.attempt += 1;
    let wait = st.delay + Duration::from_secs_f64(jitter());
    let feed = st.feed.borrow();
    if let Some(cb) = &feed.on_reconnect {
        cb(&ReconnectInfo {
            attempt: st.attempt,
            delay: wait,
            reason,
        });
    }
    let cap = feed.max_backoff;
    tokio::time::sleep(wait).await;
    st.delay = (st.delay * 2).min(cap);
}

/// Uniform in [0, 1), from the std hasher's per-instance random keys (no rand dependency).
fn jitter() -> f64 {
    let mut h = RandomState::new().build_hasher();
    h.write_u64(0);
    (h.finish() >> 11) as f64 / (1u64 << 53) as f64
}

async fn connect(feed: &Feed) -> Result<Ws, Ended> {
    if feed.key.is_empty() {
        return Err(Ended::Fatal(Error::Auth("no API key given".into())));
    }
    let mut req = feed
        .connect_url()
        .into_client_request()
        .map_err(|e| Ended::Fatal(Error::Feed(format!("invalid url: {e}"))))?;
    let auth = HeaderValue::from_str(&format!("Bearer {}", feed.key))
        .map_err(|_| Ended::Fatal(Error::Auth("the key is not a valid HTTP header value".into())))?;
    req.headers_mut().insert("Authorization", auth);
    let connector = tls_connector().map_err(|e| Ended::Fatal(Error::Feed(format!("TLS setup failed: {e}"))))?;
    // no size limits, like the Python client: a large message must not become a reconnect loop
    let config = WebSocketConfig::default().max_message_size(None).max_frame_size(None);
    let fut = tokio_tungstenite::connect_async_tls_with_config(req, Some(config), false, Some(connector));
    match tokio::time::timeout(CONNECT_TIMEOUT, fut).await {
        Err(_) => Err(Ended::Retry(format!("connect timed out after {}s", CONNECT_TIMEOUT.as_secs()))),
        Ok(Ok((ws, _))) => Ok(ws),
        Ok(Err(e)) => Err(classify(e)),
    }
}

fn classify(e: tungstenite::Error) -> Ended {
    match e {
        tungstenite::Error::Http(resp) => {
            let status = resp.status().as_u16();
            let body = resp
                .body()
                .as_deref()
                .map(|b| String::from_utf8_lossy(b).trim().to_string())
                .unwrap_or_default();
            match status {
                401 | 403 => Ended::Fatal(Error::Auth(or(body, &format!("HTTP {status}")))),
                400 => Ended::Fatal(Error::Feed(or(body, "bad request"))),
                // 429 connection limit for this key, 503 server restarting: retry
                _ => Ended::Retry(format!("HTTP {status} {body}").trim_end().to_string()),
            }
        }
        tungstenite::Error::Url(e) => Ended::Fatal(Error::Feed(format!("invalid url: {e}"))),
        e => Ended::Retry(e.to_string()),
    }
}

/// rustls with an explicit ring provider and the Mozilla roots. Using an explicit provider means
/// this works whatever else in the program enabled rustls features, and never touches the
/// process-wide default provider.
fn tls_connector() -> Result<Connector, rustls::Error> {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Connector::Rustls(Arc::new(config)))
}

/// Compiles the README's examples as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_amount_exact_is_exact() {
        assert_eq!(ui_amount_exact("915407038388", 6).as_deref(), Some("915407.038388"));
        assert_eq!(ui_amount_exact("18446744073709551615", 9).as_deref(), Some("18446744073.709551615"));
        assert_eq!(ui_amount_exact("500000000", 9).as_deref(), Some("0.5"));
        assert_eq!(ui_amount_exact("5", 9).as_deref(), Some("0.000000005"));
        assert_eq!(ui_amount_exact("1000", 3).as_deref(), Some("1"));
        assert_eq!(ui_amount_exact("42", 0).as_deref(), Some("42"));
        assert_eq!(ui_amount_exact("0", 0).as_deref(), Some("0"));
        assert_eq!(ui_amount_exact("0", 6).as_deref(), Some("0"));
        assert_eq!(ui_amount_exact("007", 1).as_deref(), Some("0.7"));
        // wider than u64 is fine: it is string arithmetic
        assert_eq!(
            ui_amount_exact("123456789012345678901234567890", 20).as_deref(),
            Some("1234567890.1234567890123456789")
        );
        assert_eq!(ui_amount_exact("", 6), None);
        assert_eq!(ui_amount_exact("-1", 6), None);
        assert_eq!(ui_amount_exact("1.5", 6), None);
    }

    #[test]
    fn ui_amount_is_a_float() {
        assert_eq!(ui_amount("915407038388", 6), Some(915407.038388));
        assert_eq!(ui_amount("500000000", 9), Some(0.5));
        assert_eq!(ui_amount("x", 9), None);
    }

    #[test]
    fn the_url_carries_filters_and_the_resume_point_never_the_key() {
        let f = Feed::new("secret-key").url("wss://x/ws").kinds(["swap", "launch"]).venues(["pump"]);
        assert_eq!(f.connect_url(), "wss://x/ws?kinds=swap,launch&venues=pump");
        let f = f.resume_from(17, 42);
        assert_eq!(f.connect_url(), "wss://x/ws?kinds=swap,launch&venues=pump&since=17:42");
        assert!(!f.connect_url().contains("secret-key"));
        assert_eq!(Feed::new("k").url("wss://x/ws").connect_url(), "wss://x/ws");
        assert_eq!(Feed::new("k").connect_url(), DEFAULT_URL);
    }

    #[test]
    fn the_url_escapes_watch_keeps_an_empty_filter_and_extends_a_query() {
        assert_eq!(
            Feed::new("k").url("wss://x/ws").watch(["a&kinds=launch"]).connect_url(),
            "wss://x/ws?watch=a%26kinds%3Dlaunch"
        );
        assert_eq!(
            Feed::new("k").url("wss://x/ws").kinds(Vec::<String>::new()).connect_url(),
            "wss://x/ws?kinds="
        );
        assert_eq!(
            Feed::new("k").url("wss://x/ws?a=1").kinds(["swap"]).connect_url(),
            "wss://x/ws?a=1&kinds=swap"
        );
        assert_eq!(
            Feed::new("k").url("wss://x/ws").watch(["a,b", "c d"]).connect_url(),
            "wss://x/ws?watch=a%2Cb,c%20d"
        );
    }

    #[test]
    fn debug_never_prints_the_key() {
        assert!(!format!("{:?}", Feed::new("secret-key")).contains("secret-key"));
    }

    #[test]
    fn the_tls_connector_builds_with_the_ring_provider() {
        // Must not panic: rustls 0.23 panics at first use if no single provider can be picked.
        assert!(matches!(tls_connector(), Ok(Connector::Rustls(_))));
    }

    #[test]
    fn jitter_is_in_range() {
        for _ in 0..1000 {
            let j = jitter();
            assert!((0.0..1.0).contains(&j));
        }
    }
}
