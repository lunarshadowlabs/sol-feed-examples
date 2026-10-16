//! Stream decoded Solana DEX swaps from the Lunar Shadow Labs feed.
//!
//!     LSL_KEY=<your API key> cargo run --release
//!
//! The sol-feed client reconnects with backoff and resumes where it left off.

use sol_feed::{ui_amount, Feed, StreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("LSL_URL").unwrap_or_else(|_| sol_feed::DEFAULT_URL.into());
    let mut feed = Feed::new(std::env::var("LSL_KEY")?).url(url).kinds(["swap"]);
    let mut stream = feed.stream();
    while let Some(m) = stream.next().await {
        let m = m?; // an error here is final: a refused key or request
        if m.is_gap() {
            println!("gap: {} {}", m["reason"], m["missed"]); // the only indication that data was missed
            continue;
        }
        let side = if m["is_buy"] == true { "buy " } else { "sell" };
        let size = match (m["base_amount"].as_str(), m["base_decimals"].as_u64()) {
            (Some(a), Some(d)) => ui_amount(a, d as u8).map_or("?".into(), |x| x.to_string()),
            _ => "?".into(),
        };
        let (venue, mint) = (m["venue"].as_str().unwrap_or("?"), m["base_mint"].as_str().unwrap_or("?"));
        println!("{venue:<15} {side} {size} {mint} @ {}", m["price"]);
    }
    Ok(())
}
