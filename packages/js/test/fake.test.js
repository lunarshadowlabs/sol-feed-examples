// Protocol edge cases against a minimal in-process WebSocket server (text frames only).
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { Feed, FeedError } from "../dist/index.js";

function frame(text) {
  const p = Buffer.from(text);
  const head = p.length < 126 ? Buffer.from([0x81, p.length]) : Buffer.from([0x81, 126, p.length >> 8, p.length & 255]);
  return Buffer.concat([head, p]);
}

/** Serves `script(n)` (the n-th connection's messages) and records each request path. */
async function fakeServer(script) {
  const paths = [];
  const socks = [];
  const server = createServer();
  server.on("upgrade", (req, sock) => {
    paths.push(req.url);
    const accept = createHash("sha1").update(req.headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
    sock.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\nSec-WebSocket-Protocol: solfeed.v1\r\n\r\n`);
    sock.write(Buffer.concat(script(paths.length).map((m) => frame(JSON.stringify(m)))));
    sock.on("error", () => {});
    socks.push(sock);
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return { url: `ws://127.0.0.1:${server.address().port}/ws`, paths, close: () => {
    socks.forEach((s) => s.destroy());
    server.close();
  } };
}

test("an epoch_changed gap moves the resume point to the new epoch", async () => {
  const srv = await fakeServer((n) =>
    n === 1
      ? [
          { kind: "hello", seq: 0, epoch: 10 },
          { kind: "swap", seq: 5 },
          { kind: "gap", seq: 0, missed: null, reason: "epoch_changed", epoch: 20 },
          { kind: "swap", seq: 1 },
          { kind: "swap", seq: 2 },
        ]
      : [{ kind: "swap", seq: 3 }],
  );
  try {
    const feed = new Feed("k", { url: srv.url });
    const seqs = [];
    for await (const m of feed) {
      seqs.push(m.seq);
      if (m.seq === 2) break;
    }
    assert.deepEqual(seqs, [5, 0, 1, 2]);
    assert.deepEqual([feed.epoch, feed.lastSeq], [20, 2]);
    for await (const _ of feed) break; // a fresh connection resumes from the new epoch
    assert.equal(srv.paths[1], "/ws?since=20:2");
  } finally {
    srv.close();
  }
});

test("a consumer slower than the feed is resumed, not buffered without limit", async () => {
  const total = 10_050;
  const srv = await fakeServer((n) =>
    n === 1 ? [{ kind: "hello", seq: 0, epoch: 7 }, ...Array.from({ length: total }, (_, i) => ({ kind: "swap", seq: i + 1 }))] : [{ kind: "swap", seq: 10_002 }],
  );
  try {
    const feed = new Feed("k", { url: srv.url });
    let prev = 0;
    for await (const m of feed) {
      assert.equal(m.seq, prev + 1); // no duplicate, no silent hole
      prev = m.seq;
      if (prev === 1) await new Promise((r) => setTimeout(r, 1000)); // fall behind
      if (prev === 10_002) break;
    }
    assert.equal(srv.paths.length, 2);
    assert.equal(srv.paths[1], "/ws?since=7:10001");
  } finally {
    srv.close();
  }
});

test("unknown kind or venue names are refused before connecting", () => {
  assert.throws(() => new Feed("k", { kinds: ["swapz"] }), FeedError);
  assert.throws(() => new Feed("k", { venues: ["raydium"] }), FeedError);
  new Feed("k", { kinds: ["swap", "gap"], venues: ["token2022"] }); // every server name is accepted
});
