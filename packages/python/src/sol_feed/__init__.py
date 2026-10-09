"""Python client for the Lunar Shadow Labs Solana DEX data feed (https://lunarshadowlabs.com).

    import asyncio, os
    from sol_feed import Feed

    async def main():
        async for m in Feed(os.environ["LSL_KEY"], kinds=["swap"]):
            print(m["venue"], m["base_mint"], m["price"])

    asyncio.run(main())

The client reconnects with backoff and resumes where it left off (``?since=<epoch>:<seq>``). It
yields every event and every ``gap`` message (the only indication that data was missed); ``hello`` and
``heartbeat`` are handled internally. Message format: https://lunarshadowlabs.com/docs/
"""

from __future__ import annotations

import asyncio
import json
import random
from decimal import Decimal
from typing import Any, AsyncIterator, Callable, Dict, Iterable, Optional
from urllib.parse import urlencode

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

__version__ = "0.1.1"
__all__ = ["Feed", "FeedError", "AuthError", "ui_amount", "DEFAULT_URL"]

DEFAULT_URL = "wss://feed.lunarshadowlabs.com/ws"

Message = Dict[str, Any]


class FeedError(Exception):
    """The server refused the request in a way retrying cannot fix (for example an unknown filter
    name). The message is the server's own reason."""


class AuthError(FeedError):
    """The API key was refused (unknown, expired or revoked). Your key is on
    https://lunarshadowlabs.com/account/ — it is not your Whop license key."""


def ui_amount(amount: Optional[str], decimals: Optional[int]) -> Optional[Decimal]:
    """Scale a raw amount string by its ``*_decimals`` field, exactly. ``None`` if either is unknown."""
    if amount is None or decimals is None:
        return None
    return Decimal(int(amount)).scaleb(-decimals)


def _csv(v: Optional[Iterable[str]]) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        raise TypeError("pass a list of names, not a string")
    return ",".join(v)


class Feed:
    """One subscription to the feed. Iterate it (``async for``) to receive messages.

    ``kinds``, ``venues`` and ``watch`` are server-side filters (see the docs for the names).
    With no filter at all the server applies the filter saved on your account page.
    ``on_reconnect(info)`` is called before each reconnect with ``attempt``, ``delay`` (seconds)
    and ``reason`` — useful for logging.
    """

    def __init__(
        self,
        key: str,
        *,
        url: str = DEFAULT_URL,
        kinds: Optional[Iterable[str]] = None,
        venues: Optional[Iterable[str]] = None,
        watch: Optional[Iterable[str]] = None,
        max_backoff: float = 30.0,
        on_reconnect: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        if not key:
            raise AuthError("no API key given")
        self._key = key
        self._url = url
        # an empty list is sent as-is: it is a real filter (match nothing), not "no filter"
        self._query = {k: v for k, v in (("kinds", _csv(kinds)), ("venues", _csv(venues)), ("watch", _csv(watch))) if v is not None}
        self._max_backoff = max_backoff
        self._on_reconnect = on_reconnect
        self.epoch: Optional[int] = None
        """The server epoch of the stream being followed (from ``hello``)."""
        self.last_seq = 0
        """The ``seq`` of the last event or gap yielded — the resume point."""

    def _connect_url(self) -> str:
        q = dict(self._query)
        if self.epoch is not None:
            q["since"] = f"{self.epoch}:{self.last_seq}"
        # the key never goes in the URL: the server refuses it there
        sep = "&" if "?" in self._url else "?"
        return f"{self._url}{sep}{urlencode(q, safe=',:')}" if q else self._url

    def __aiter__(self) -> AsyncIterator[Message]:
        return self.events()

    async def events(self) -> AsyncIterator[Message]:
        headers = {"Authorization": f"Bearer {self._key}"}
        delay, attempt = 1.0, 0
        while True:
            reason = ""
            try:
                async with connect(self._connect_url(), additional_headers=headers, compression=None, max_size=None) as ws:
                    delay, attempt = 1.0, 0
                    async for raw in ws:
                        m = json.loads(raw)
                        kind = m["kind"]
                        if kind == "hello":
                            if m["epoch"] != self.epoch:  # first connect, or the server restarted
                                self.epoch, self.last_seq = m["epoch"], 0
                            continue
                        if kind == "heartbeat":
                            continue  # never resume from a heartbeat's seq
                        self.last_seq = m["seq"]
                        if kind == "gap" and m.get("reason") == "epoch_changed" and m.get("epoch"):
                            # the server restarted under a live connection: no new hello comes, and
                            # resuming against the old epoch would replay events already yielded
                            self.epoch = m["epoch"]
                        yield m
            except InvalidStatus as e:
                status = e.response.status_code
                body = (e.response.body or b"").decode("utf-8", "replace").strip()
                if status in (401, 403):
                    raise AuthError(body or f"HTTP {status}") from None
                if status == 400:
                    raise FeedError(body or "bad request") from None
                reason = f"HTTP {status} {body}".strip()  # 429 connection limit, 503 restarting: retry
            except ConnectionClosed as e:
                rcvd = e.rcvd
                if rcvd is not None and rcvd.code == 4001:
                    raise AuthError(rcvd.reason or "key revoked") from None
                if rcvd is not None and rcvd.code == 1008:
                    raise FeedError(rcvd.reason or "policy violation") from None
                reason = f"closed {rcvd.code} {rcvd.reason}" if rcvd else "connection lost"
            except (OSError, asyncio.TimeoutError, WebSocketException) as e:
                reason = f"{type(e).__name__}: {e}"
            else:
                reason = "closed by server"
            attempt += 1
            wait = delay + random.random()
            if self._on_reconnect:
                self._on_reconnect({"attempt": attempt, "delay": wait, "reason": reason})
            await asyncio.sleep(wait)
            delay = min(delay * 2, self._max_backoff)
