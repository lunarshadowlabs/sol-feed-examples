//! Stream decoded Solana DEX swaps from the Lunar Shadow Labs feed.
//!
//!     LSL_KEY=<your API key> cargo run --release

use futures_util::StreamExt;
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

/// Amounts are strings of raw integer units; scale them with the matching `*_decimals` field.
fn ui(amount: &Value, decimals: &Value) -> Option<f64> {
    Some(amount.as_str()?.parse::<u64>().ok()? as f64 / 10f64.powi(decimals.as_i64()? as i32))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var("LSL_URL").unwrap_or_else(|_| "wss://feed.lunarshadowlabs.com/ws".into());
    let mut req = format!("{base}?kinds=swap").into_client_request()?;
    req.headers_mut().insert("Authorization", format!("Bearer {}", std::env::var("LSL_KEY")?).parse()?);
    let (mut ws, _) = connect_async(req).await?;

    while let Some(msg) = ws.next().await {
        let msg = msg?;
        if !msg.is_text() {
            continue;
        }
        let m: Value = serde_json::from_str(msg.to_text()?)?;
        match m["kind"].as_str() {
            Some("swap") => {
                let side = if m["is_buy"] == true { "buy " } else { "sell" };
                let size = ui(&m["base_amount"], &m["base_decimals"]).map_or("?".into(), |s| s.to_string());
                let (venue, mint) = (m["venue"].as_str().unwrap_or("?"), m["base_mint"].as_str().unwrap_or("?"));
                println!("{venue:<15} {side} {size} {mint} @ {}", m["price"]);
            }
            // the only indication that data was missed
            Some("gap") => println!("gap: {} {}", m["reason"], m["missed"]),
            _ => {}
        }
    }
    Ok(())
}
