# Lunar Shadow Labs feed — examples and clients

Stream decoded Solana DEX swaps in about 20 lines of Python, TypeScript or Rust.

[Lunar Shadow Labs](https://lunarshadowlabs.com) publishes every trade, token launch, bonding-curve
completion, migration, pool creation and liquidity change on 12 Solana venues over one WebSocket,
already decoded into one JSON shape, with Token-2022 mint flags (transfer fees, freeze authority,
permanent delegate, …) on both sides of every trade. No IDLs, no account parsing, no per-venue code.
**Market data only.**

Venues: pump.fun, PumpSwap, Raydium LaunchLab (bonk.fun, StonkFun, …), Meteora DBC, Raydium AMM v4 /
CPMM / CLMM, PancakeSwap v3, Orca Whirlpool, Meteora DLMM / DAMM v1 / DAMM v2.

- Website: <https://lunarshadowlabs.com>
- Docs (message reference): <https://lunarshadowlabs.com/docs/>
- Guides: <https://lunarshadowlabs.com/guides/>
- Status: <https://lunarshadowlabs.com/status/>

## Get a key

Subscribe at [lunarshadowlabs.com](https://lunarshadowlabs.com), then copy your API key from your
[account page](https://lunarshadowlabs.com/account/). Every example reads it from `LSL_KEY`.

## Run an example

| language | file | run |
|---|---|---|
| Python 3.9+ | [`examples/python/stream_swaps.py`](examples/python/stream_swaps.py) | `pip install "websockets>=13"` then `LSL_KEY=… python stream_swaps.py` |
| TypeScript (Node 22+) | [`examples/typescript/stream_swaps.ts`](examples/typescript/stream_swaps.ts) | `LSL_KEY=… node --experimental-strip-types stream_swaps.ts` |
| Rust | [`examples/rust/`](examples/rust/src/main.rs) | `LSL_KEY=… cargo run --release` |

Output:

```
pumpswap        buy  915407.038388 FKB4cff48QyEyKDkPdmqV3AAUEZKZJPWnAGCEcxmpump @ 5.462051077086785e-07
raydium_cpmm    sell 42678.171662 CCVYpFakBd4kkwn7G4rvbpaiB5pGJwUpiNwNYrkC5ygQ @ 0.0005660472569285295
orca            buy  70.27478926 J3NKxxXZcnNiMjKw9hYb2K4LUxgwB6t1FtPtQVsv3KFr @ 0.003511809620473275
```

The examples are deliberately bare: one connection, no reconnect. For production use, the client
packages below reconnect with backoff and resume from where they left off.

## Client packages

| | install | source |
|---|---|---|
| Python | `pip install sol-feed` | [`packages/python`](packages/python) |
| JavaScript / TypeScript | `npm install sol-feed` | [`packages/js`](packages/js) |

```python
import asyncio, os
from sol_feed import Feed

async def main():
    async for m in Feed(os.environ["LSL_KEY"], kinds=["swap"]):
        print(m["venue"], m["base_mint"], m["price"])

asyncio.run(main())
```

## One swap, as it arrives

```json
{"v":1,"seq":1918,"kind":"swap","venue":"pumpswap",
 "keys":["FKB4cff48QyEyKDkPdmqV3AAUEZKZJPWnAGCEcxmpump","So11111111111111111111111111111111111111112","HWkc2hbojhAXGDnhK1GsMxVeGrpUuMxrV8BPRpx9yPUn","AYt71Hz9vsgtbds1tsYDfr9R9wcAp12TCpD9qhBB8pT3"],
 "stage":"executed","source":"geyser","commitment":"processed","slot":453800890,
 "sig":"5Zd41xN3vH8CeRYBnLfAqTQiyY3KpLroQZNkcstpDUgU5fMvj3gEgPxYM3nV8xvBM7MNndaqWhSj2D4TjQsbNwgo",
 "ix":7,"inner":6,"t_ingest_us":1791421675869622,"t_emit_us":1791421675869646,"via":null,
 "pool":"HWkc2hbojhAXGDnhK1GsMxVeGrpUuMxrV8BPRpx9yPUn","user":"AYt71Hz9vsgtbds1tsYDfr9R9wcAp12TCpD9qhBB8pT3",
 "base_mint":"FKB4cff48QyEyKDkPdmqV3AAUEZKZJPWnAGCEcxmpump","quote_mint":"So11111111111111111111111111111111111111112",
 "is_buy":true,"base_amount":"915407038388","quote_amount":"500000000","base_decimals":6,"quote_decimals":9,
 "price":0.0000005462051077086785,
 "fees":{"lp":"988143","protocol":"247036","coin_creator":"4693676","cashback":"0","buyback":"123518","holder_rewards":"0"},
 "partial":false,"derived":false,
 "base_flags":{"program":"token2022","decimals":6,"supply":"979645177870030","mint_authority":null,"freeze_authority":null,"metadata_pointer":"FKB4cff48QyEyKDkPdmqV3AAUEZKZJPWnAGCEcxmpump","extensions":[18,19]},
 "quote_flags":null,
 "x":{"pool_quote_amount_in":"500000000","pool_quote_amount_in_with_lp_fee":"495059288","user_quote_amount_in":"494071145","pool_base_token_reserves":"182354455676307","pool_quote_token_reserves":"80346014470","virtual_quote_reserves":"17581786571","ix_name":"buy_exact_quote_in"}}
```

A few things worth knowing (the [docs](https://lunarshadowlabs.com/docs/) have the rest):

- **Amounts are strings** of raw integer units; scale them with `base_decimals` / `quote_decimals`.
  `price` (quote per whole base token) is a float, and `null` when an input is unknown.
- On every data event `keys` is `[base, quote, pool, actor]`, so one filter works across every event
  kind (`gap`, `hello` and `heartbeat` carry an empty `keys`).
- **Filters run on the server**: `?kinds=swap,launch`, `?venues=pump,pumpswap`, `?watch=<mint, pool or wallet>,…`.
- **`gap` is the only indication that data was missed**, with the missed `seq` range when known.
  Reconnect with `?since=<epoch>:<last seq>` to get back anything still in the server's buffer.
- **The key never goes in the URL.** Send `Authorization: Bearer <key>`, or, from a client that can't
  set headers, the subprotocols `solfeed.v1` and `key.<key>`.
- Events are published at `processed` commitment, so a rare fork can drop a transaction.

## License

MIT — see [LICENSE](LICENSE).
