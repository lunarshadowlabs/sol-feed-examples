// Stream decoded Solana DEX swaps from the Lunar Shadow Labs feed.
// Node 22+, no dependencies:
//   LSL_KEY=<your API key> node --experimental-strip-types stream_swaps.ts
// Built-in WebSockets can't set headers, so the key goes in the subprotocol list.
const url = (process.env.LSL_URL ?? "wss://feed.lunarshadowlabs.com/ws") + "?kinds=swap";
const ws = new WebSocket(url, ["solfeed.v1", `key.${process.env.LSL_KEY}`]);

// Amounts are strings of raw integer units; scale them with the matching *_decimals field.
const ui = (amount: string, decimals: number | null) =>
  decimals === null ? null : Number(BigInt(amount)) / 10 ** decimals;

ws.onmessage = (e: MessageEvent) => {
  const m = JSON.parse(String(e.data));
  if (m.kind === "swap") {
    const side = m.is_buy ? "buy " : "sell";
    console.log(`${m.venue.padEnd(15)} ${side} ${ui(m.base_amount, m.base_decimals)} ${m.base_mint} @ ${m.price}`);
  } else if (m.kind === "gap") {
    console.log("gap:", m.reason, m.missed); // the only indication that data was missed
  }
};
// A refused connection (bad key, connection limit) arrives as an error with no status code.
ws.onerror = () => console.error("connection failed: check LSL_KEY at lunarshadowlabs.com/account");
ws.onclose = (e: CloseEvent) => console.log(`closed ${e.code} ${e.reason}`);
