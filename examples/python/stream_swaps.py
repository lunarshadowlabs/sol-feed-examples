"""Stream decoded Solana DEX swaps from the Lunar Shadow Labs feed.

pip install "websockets>=13"
LSL_KEY=<your API key> python stream_swaps.py
"""
import asyncio, json, os
from websockets.asyncio.client import connect

URL = os.environ.get("LSL_URL", "wss://feed.lunarshadowlabs.com/ws") + "?kinds=swap"


def ui(amount, decimals):
    """Amounts are strings of raw integer units; scale them with the matching *_decimals field."""
    return int(amount) / 10**decimals if decimals is not None else None


async def main():
    headers = {"Authorization": f"Bearer {os.environ['LSL_KEY']}"}
    async with connect(URL, additional_headers=headers, compression=None, max_size=None) as ws:
        async for raw in ws:
            m = json.loads(raw)
            if m["kind"] == "swap":
                side = "buy " if m["is_buy"] else "sell"
                size = ui(m["base_amount"], m["base_decimals"])
                print(f"{m['venue']:<15} {side} {size} {m['base_mint']} @ {m['price']}")
            elif m["kind"] == "gap":  # the only indication that data was missed
                print("gap:", m["reason"], m.get("missed"))


asyncio.run(main())
