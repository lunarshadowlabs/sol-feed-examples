# sol-feed (JavaScript / TypeScript)

Client for the [Lunar Shadow Labs](https://lunarshadowlabs.com) feed: decoded Solana DEX swaps,
launches, migrations, pool creations and liquidity changes on 12 venues, over one WebSocket, in one
JSON shape. Market data only. No dependencies; Node 22+ (uses the built-in `WebSocket`).

```
npm install sol-feed
```

```ts
import { Feed, isGap, isSwap, uiAmount } from "sol-feed";

for await (const m of new Feed(process.env.LSL_KEY!, { kinds: ["swap"], venues: ["pump", "pumpswap"] })) {
  if (isGap(m)) console.log("gap:", m.reason, m.missed); // the only indication that data was missed
  else if (isSwap(m)) console.log(m.venue, m.is_buy ? "buy" : "sell", uiAmount(m.base_amount, m.base_decimals), m.base_mint, m.price);
}
```

`LSL_KEY` is the API key on your [account page](https://lunarshadowlabs.com/account/) (it is not your
Whop license key). Keep it server-side: anything shipped to a browser page is public.

## What the client does for you

- Authenticates through the WebSocket subprotocol list (`solfeed.v1`, `key.<key>`), the way the feed
  accepts keys from clients that can't set headers; the key never goes in the URL.
- Reconnects with exponential backoff (1 s → 30 s, with jitter) and resumes from the last message it
  gave you (`?since=<epoch>:<seq>`), so a short disconnect loses nothing still in the server's buffer.
  Anything that could not be recovered arrives as a `gap` message.
- Handles `hello` and `heartbeat` internally; you receive events and gaps only.
- Throws `AuthError` for an unknown, expired or revoked key (checked against the account endpoint
  when a connection is refused) and `FeedError` for an unknown kind or venue name, or when the server
  closes the stream over a bad request.

## Options

| option | meaning |
|---|---|
| `kinds` | `swap`, `launch`, `curve_complete`, `migration`, `pool_create`, `liquidity`, `mint_update` |
| `venues` | `pump`, `pumpswap`, `launchlab`, `meteora_dbc`, `raydium_v4`, `raydium_cpmm`, `raydium_clmm`, `pancake_v3`, `orca`, `meteora_dlmm`, `meteora_damm_v1`, `meteora_damm_v2`, and `token2022` (token-extension updates, the venue of every `mint_update`) |
| `watch` | mints, pools or wallets to follow (a wallet matches every event it is the actor of) |
| `url` | defaults to `wss://feed.lunarshadowlabs.com/ws` |
| `onReconnect` | `(info) => void` with `attempt`, `delayMs`, `reason`, called before each reconnect |

With no filter at all, the server applies the filter saved on your account page. Kind and venue
names are checked when the `Feed` is created (a `FeedError` for an unknown one). A `watch` address the
server rejects shows up as repeated reconnects instead, because a browser-style WebSocket does not
expose the HTTP status of a refused handshake.

If your code falls more than 10,000 messages behind, the client drops the connection and resumes from
the last message it gave you, so memory stays bounded; anything the server can no longer replay
arrives as a `gap`.

`uiAmount(amount, decimals)` scales a raw amount string exactly and returns a decimal string (amounts
are strings because they exceed `Number.MAX_SAFE_INTEGER`); wrap it in `Number()` if a float is fine.

The full message reference is at <https://lunarshadowlabs.com/docs/>.

## License

MIT
