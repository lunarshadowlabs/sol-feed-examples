# sol-feed (Python)

Python client for the [Lunar Shadow Labs](https://lunarshadowlabs.com) feed: decoded Solana DEX
swaps, launches, migrations, pool creations and liquidity changes on 12 venues, over one WebSocket,
in one JSON shape. Market data only.

```
pip install sol-feed
```

```python
import asyncio, os
from sol_feed import Feed, ui_amount

async def main():
    async for m in Feed(os.environ["LSL_KEY"], kinds=["swap"], venues=["pump", "pumpswap"]):
        if m["kind"] == "gap":
            print("gap:", m["reason"], m["missed"])   # the only indication that data was missed
            continue
        side = "buy" if m["is_buy"] else "sell"
        print(m["venue"], side, ui_amount(m["base_amount"], m["base_decimals"]), m["base_mint"], m["price"])

asyncio.run(main())
```

`LSL_KEY` is the API key on your [account page](https://lunarshadowlabs.com/account/) (it is not your
Whop license key).

## What the client does for you

- Sends the key as an `Authorization: Bearer` header (never in the URL).
- Reconnects with exponential backoff (1 s → 30 s, with jitter) and resumes from the last message it
  gave you (`?since=<epoch>:<seq>`), so a short disconnect loses nothing that is still in the server's
  buffer. Anything that could not be recovered arrives as a `gap` message.
- Handles `hello` and `heartbeat` internally; you receive events and gaps only.
- Raises `AuthError` for an unknown, expired or revoked key and `FeedError` for a request the server
  rejects (for example an unknown kind or venue name). Those are not retried.

## Options

| argument | meaning |
|---|---|
| `kinds` | `swap`, `launch`, `curve_complete`, `migration`, `pool_create`, `liquidity`, `mint_update` |
| `venues` | `pump`, `pumpswap`, `launchlab`, `meteora_dbc`, `raydium_v4`, `raydium_cpmm`, `raydium_clmm`, `pancake_v3`, `orca`, `meteora_dlmm`, `meteora_damm_v1`, `meteora_damm_v2`, and `token2022` (token-extension updates, the venue of every `mint_update`) |
| `watch` | mints, pools or wallets to follow (a wallet matches every event it is the actor of) |
| `url` | defaults to `wss://feed.lunarshadowlabs.com/ws` |
| `on_reconnect` | `callable(info)` with `attempt`, `delay`, `reason`, called before each reconnect |

With no filter at all, the server applies the filter saved on your account page.

Breaking out of `async for m in Feed(...)` closes the connection. If you keep the iterator from
`feed.events()` in a variable, call `await it.aclose()` when you are done, or the connection keeps
one of your key's connection slots until it is garbage-collected.

`ui_amount(amount, decimals)` scales a raw amount string to an exact `Decimal` (amounts are strings
because they exceed the integers a float can hold).

The full message reference is at <https://lunarshadowlabs.com/docs/>.

## License

MIT
