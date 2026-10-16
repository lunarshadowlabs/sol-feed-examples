# sol-feed (Rust)

Rust client for the [Lunar Shadow Labs](https://lunarshadowlabs.com) feed: every trade, token
launch, bonding-curve completion, migration, pool creation and liquidity change on 12 Solana venues,
already decoded into one JSON shape, over one WebSocket. **Market data only.**

The client reconnects with jittered exponential backoff and resumes from the last message it gave
you. TLS is rustls with the ring provider and the Mozilla root store: no OpenSSL.

- Docs (every message type, filters, resume): <https://lunarshadowlabs.com/docs/>
- Get a key: subscribe at <https://lunarshadowlabs.com>, then copy it from your
  [account page](https://lunarshadowlabs.com/account/).

## Install

```sh
cargo add sol-feed
cargo add tokio --features macros,rt-multi-thread
```

Rust 1.80 or newer.

## Example

```rust,no_run
use sol_feed::{ui_amount, Feed, StreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut feed = Feed::new(std::env::var("LSL_KEY")?).kinds(["swap"]);
    let mut stream = feed.stream();
    while let Some(m) = stream.next().await {
        let m = m?; // an Err is final: a refused key or request
        if m.is_gap() {
            println!("gap: {} {}", m["reason"], m["missed"]);
            continue;
        }
        let size = match (m["base_amount"].as_str(), m["base_decimals"].as_u64()) {
            (Some(a), Some(d)) => ui_amount(a, d as u8).map_or("?".into(), |x| x.to_string()),
            _ => "?".into(),
        };
        println!("{} {} {size} {} @ {}", m["venue"], m["is_buy"], m["base_mint"], m["price"]);
    }
    Ok(())
}
```

Each item is a `Message`: it derefs to the raw `serde_json::Value` (so `m["venue"]` works), and has
`kind()`, `seq()`, `is_gap()`, `value()` and `into_value()`. Amounts are strings of raw integer
units: `ui_amount(amount, decimals)` scales one to an `f64`, and `ui_amount_exact` gives the exact
decimal string (`"915407.038388"`).

## Filters

Filters run on the server. Names are in the [docs](https://lunarshadowlabs.com/docs/).

```rust
use sol_feed::Feed;

let feed = Feed::new("<your API key>")
    .kinds(["swap", "launch"])           // event kinds
    .venues(["pump", "pumpswap"])        // venues
    .watch(["<mint, pool or wallet>"]);  // a wallet matches every event it is the actor of
```

With no filter at all the server applies the filter saved on your account page. An empty list is a
real filter that matches nothing. Other options: `.url(...)` (default
`wss://feed.lunarshadowlabs.com/ws`), `.max_backoff(Duration)` (default 30 s) and
`.on_reconnect(|info| ...)`, which gets `attempt`, `delay` and `reason` before each reconnect.

The key is sent only in the `Authorization: Bearer` header, never in the URL.

## Resume

The stream yields every event and every `gap` message; `hello` and `heartbeat` are handled for you.
After a disconnect it reconnects with `?since=<epoch>:<last seq>`, so the server sends whatever it
still buffers after the last message you received. A `gap` is the only indication that data was
missed.

The resume point lives in the `Feed`. Drop a stream and call `feed.stream()` again to carry on where
it stopped. To survive a process restart, save `feed.epoch()` and `feed.last_seq()` and start the
next run with `.resume_from(epoch, last_seq)`. Use `feed.into_stream()` for an owned `'static` stream
you can move into `tokio::spawn`.

## Errors

The stream ends after yielding an `Err`, which only happens for problems a retry cannot fix:

| error | when |
|---|---|
| `Error::Auth(reason)` | the key was refused: HTTP 401/403, or close code 4001 (key revoked) |
| `Error::Feed(reason)` | the request was refused: HTTP 400 or close code 1008 (for example an unknown filter name), or an invalid URL |
| `Error::Decode(e)` | a message was not valid JSON |

Everything else (HTTP 429 connection limit, 503 while the server restarts, network errors, 30 s of
silence) is retried: the wait starts at 1 s, doubles up to `max_backoff`, and gets 0 to 1 s of random
jitter on top.

## License

MIT
