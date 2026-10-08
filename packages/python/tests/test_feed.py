"""Unit tests run anywhere. The live tests need a feed server and a key:
LSL_TEST_URL=ws://localhost:8080/ws LSL_TEST_KEY=... pytest"""
import asyncio
import json
import os
from decimal import Decimal

import pytest

from sol_feed import AuthError, Feed, FeedError, ui_amount

URL = os.environ.get("LSL_TEST_URL")
KEY = os.environ.get("LSL_TEST_KEY")
live = pytest.mark.skipif(not (URL and KEY), reason="LSL_TEST_URL / LSL_TEST_KEY not set")


def test_ui_amount_is_exact():
    assert ui_amount("915407038388", 6) == Decimal("915407.038388")
    assert ui_amount("18446744073709551615", 9) == Decimal("18446744073.709551615")
    assert ui_amount("0", 0) == 0
    assert ui_amount(None, 6) is None
    assert ui_amount("1", None) is None


def test_url_has_filters_and_resume_point_but_never_the_key():
    f = Feed("secret-key", url="wss://x/ws", kinds=["swap", "launch"], venues=["pump"])
    assert f._connect_url() == "wss://x/ws?kinds=swap,launch&venues=pump"
    f.epoch, f.last_seq = 17, 42
    assert f._connect_url() == "wss://x/ws?kinds=swap,launch&venues=pump&since=17:42"
    assert "secret-key" not in f._connect_url()
    assert Feed("k", url="wss://x/ws")._connect_url() == "wss://x/ws"


def test_url_escapes_watch_keeps_empty_filter_and_extends_query():
    assert Feed("k", url="wss://x/ws", watch=["a&kinds=launch"])._connect_url() == "wss://x/ws?watch=a%26kinds%3Dlaunch"
    assert Feed("k", url="wss://x/ws", kinds=[])._connect_url() == "wss://x/ws?kinds="
    assert Feed("k", url="wss://x/ws?a=1", kinds=["swap"])._connect_url() == "wss://x/ws?a=1&kinds=swap"


def test_a_string_filter_is_rejected():
    with pytest.raises(TypeError):
        Feed("k", kinds="swap")


async def take(feed, n):
    out = []
    async for m in feed:
        out.append(m)
        if len(out) == n:
            return out


@live
def test_live_swaps_arrive_with_the_documented_shape():
    feed = Feed(KEY, url=URL, kinds=["swap"])
    msgs = asyncio.run(asyncio.wait_for(take(feed, 20), 30))
    assert feed.epoch and feed.last_seq == msgs[-1]["seq"]
    for m in msgs:
        assert m["kind"] in ("swap", "gap")
        if m["kind"] == "swap":
            assert list(m)[:5] == ["v", "seq", "kind", "venue", "keys"]
            assert isinstance(m["base_amount"], str) and isinstance(m["is_buy"], bool)


@live
def test_live_unknown_key_raises_auth_error():
    with pytest.raises(AuthError):
        asyncio.run(asyncio.wait_for(take(Feed("not-a-real-key", url=URL), 1), 30))


@live
def test_live_unknown_kind_raises_feed_error_with_the_server_reason():
    with pytest.raises(FeedError, match="unknown kind"):
        asyncio.run(asyncio.wait_for(take(Feed(KEY, url=URL, kinds=["swapz"]), 1), 30))


def test_epoch_changed_gap_moves_the_resume_point_to_the_new_epoch():
    # An origin restart under a live relay connection sends gap{epoch_changed, epoch} and no new
    # hello; resuming against the old epoch would make the server replay its whole buffer.
    from websockets.asyncio.server import serve

    paths = []
    script = [
        {"kind": "hello", "seq": 0, "epoch": 10},
        {"kind": "swap", "seq": 5},
        {"kind": "gap", "seq": 0, "missed": None, "reason": "epoch_changed", "epoch": 20},
        {"kind": "swap", "seq": 1},
        {"kind": "swap", "seq": 2},
    ]

    async def handler(ws):
        paths.append(ws.request.path)
        if len(paths) == 1:
            for m in script:
                await ws.send(json.dumps(m))
        else:
            await ws.send(json.dumps({"kind": "swap", "seq": 3}))
        await asyncio.sleep(5)

    async def run():
        async with serve(handler, "127.0.0.1", 0) as srv:
            port = srv.sockets[0].getsockname()[1]
            feed = Feed("k", url=f"ws://127.0.0.1:{port}/ws")
            seqs = []
            async for m in feed:
                seqs.append(m["seq"])
                if m["seq"] == 2:
                    break
            assert (feed.epoch, feed.last_seq) == (20, 2)
            async for m in feed:  # a fresh connection resumes from the new epoch
                break
            return seqs

    assert asyncio.run(asyncio.wait_for(run(), 15)) == [5, 0, 1, 2]
    assert paths[1] == "/ws?since=20:2"
