// Unit tests run anywhere. The live tests need a feed server and a key:
//   LSL_TEST_URL=ws://localhost:8080/ws LSL_TEST_KEY=... npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { AuthError, Feed, uiAmount } from "../dist/index.js";

const URL = process.env.LSL_TEST_URL;
const KEY = process.env.LSL_TEST_KEY;
const live = { skip: !(URL && KEY) && "LSL_TEST_URL / LSL_TEST_KEY not set", timeout: 30_000 };

test("uiAmount is exact", () => {
  assert.equal(uiAmount("915407038388", 6), "915407.038388");
  assert.equal(uiAmount("18446744073709551615", 9), "18446744073.709551615");
  assert.equal(uiAmount("500000000", 9), "0.5");
  assert.equal(uiAmount("5", 9), "0.000000005");
  assert.equal(uiAmount("1000", 3), "1");
  assert.equal(uiAmount("42", 0), "42");
  assert.equal(uiAmount(null, 6), null);
  assert.equal(uiAmount("1", null), null);
});

test("the URL carries filters and the resume point, never the key", () => {
  const f = new Feed("secret-key", { url: "wss://x/ws", kinds: ["swap", "launch"], venues: ["pump"] });
  assert.equal(f.connectUrl(), "wss://x/ws?kinds=swap,launch&venues=pump");
  f.epoch = 17;
  f.lastSeq = 42;
  assert.equal(f.connectUrl(), "wss://x/ws?kinds=swap,launch&venues=pump&since=17:42");
  assert.ok(!f.connectUrl().includes("secret-key"));
  assert.equal(new Feed("k", { url: "wss://x/ws" }).connectUrl(), "wss://x/ws");
});

test("connectUrl escapes watch values, keeps an empty filter and extends an existing query", () => {
  assert.equal(new Feed("k", { url: "wss://x/ws", watch: ["a&kinds=launch"] }).connectUrl(), "wss://x/ws?watch=a%26kinds%3Dlaunch");
  assert.equal(new Feed("k", { url: "wss://x/ws", kinds: [] }).connectUrl(), "wss://x/ws?kinds=");
  assert.equal(new Feed("k", { url: "wss://x/ws?a=1", kinds: ["swap"] }).connectUrl(), "wss://x/ws?a=1&kinds=swap");
});

test("live: swaps arrive with the documented shape", live, async () => {
  const feed = new Feed(KEY, { url: URL, kinds: ["swap"] });
  const got = [];
  for await (const m of feed) {
    got.push(m);
    if (got.length === 20) break;
  }
  assert.ok(feed.epoch);
  assert.equal(feed.lastSeq, got.at(-1).seq);
  for (const m of got) {
    assert.ok(m.kind === "swap" || m.kind === "gap");
    if (m.kind === "swap") {
      assert.deepEqual(Object.keys(m).slice(0, 5), ["v", "seq", "kind", "venue", "keys"]);
      assert.equal(typeof m.base_amount, "string");
      assert.equal(typeof m.is_buy, "boolean");
    }
  }
});

test("live: an unknown key throws AuthError with the server's reason", live, async () => {
  const feed = new Feed("not-a-real-key", { url: URL });
  await assert.rejects(async () => {
    for await (const _ of feed) break;
  }, (e) => e instanceof AuthError && /unknown key/.test(e.message));
});

test("live: resume picks up right after the last seq", live, async () => {
  const a = new Feed(KEY, { url: URL });
  for await (const _ of a) break;
  const b = new Feed(KEY, { url: URL });
  b.epoch = a.epoch;
  b.lastSeq = a.lastSeq - 100;
  for await (const m of b) {
    assert.equal(m.seq, a.lastSeq - 99);
    break;
  }
});
