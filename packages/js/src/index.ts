/**
 * Client for the Lunar Shadow Labs feed (https://lunarshadowlabs.com): decoded Solana DEX swaps,
 * launches, migrations, pool creations and liquidity changes over one WebSocket.
 *
 *   import { Feed } from "sol-feed";
 *   for await (const m of new Feed(process.env.LSL_KEY!, { kinds: ["swap"] })) console.log(m);
 *
 * Reconnects with backoff and resumes where it left off (`?since=<epoch>:<seq>`). Yields every event
 * and every `gap` (the only indication that data was missed); `hello` and `heartbeat` are handled here.
 * Message reference: https://lunarshadowlabs.com/docs/
 */

export const DEFAULT_URL = "wss://feed.lunarshadowlabs.com/ws";

/** The server refused the request in a way retrying cannot fix; the message is the server's reason. */
export class FeedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FeedError";
  }
}

/** The API key was refused (unknown, expired or revoked). Your key is on lunarshadowlabs.com/account. */
export class AuthError extends FeedError {
  constructor(message: string) {
    super(message);
    this.name = "AuthError";
  }
}

export const KINDS = ["swap", "launch", "curve_complete", "migration", "pool_create", "liquidity", "mint_update", "gap"] as const;
export type Kind = (typeof KINDS)[number];

export const VENUES = [
  "pump", "pumpswap", "launchlab", "meteora_dbc", "raydium_v4", "raydium_cpmm", "raydium_clmm",
  "pancake_v3", "orca", "meteora_dlmm", "meteora_damm_v1", "meteora_damm_v2", "token2022",
] as const;
export type Venue = (typeof VENUES)[number];

/** Messages held for a consumer that is slower than the feed before the client drops the socket and
 * resumes from its last yielded seq (the server then sends a gap if it can't fill the hole). */
const MAX_QUEUE = 10_000;

/** Fields every message starts with. Amounts are strings of raw integer units. */
export interface FeedMessage {
  v: number;
  seq: number;
  kind: Kind;
  venue: string | null;
  /** [base, quote, pool, actor]; treat it as a list — more entries may be added at the end. */
  keys: (string | null)[];
  [field: string]: unknown;
}

export interface Swap extends FeedMessage {
  kind: "swap";
  venue: string;
  slot: number;
  sig: string;
  pool: string | null;
  user: string;
  base_mint: string | null;
  quote_mint: string | null;
  is_buy: boolean;
  base_amount: string;
  quote_amount: string;
  base_decimals: number | null;
  quote_decimals: number | null;
  /** Quote per whole base token, or null when an input is unknown. */
  price: number | null;
  t_ingest_us: number;
  t_emit_us: number;
}

export interface Gap extends FeedMessage {
  kind: "gap";
  missed: [number, number] | null;
  reason: string;
}

export const isSwap = (m: FeedMessage): m is Swap => m.kind === "swap";
export const isGap = (m: FeedMessage): m is Gap => m.kind === "gap";

export interface ReconnectInfo {
  attempt: number;
  delayMs: number;
  reason: string;
}

export interface FeedOptions {
  url?: string;
  kinds?: Kind[];
  venues?: Venue[];
  /** Mints, pools or wallets to follow (a wallet matches every event it is the actor of). */
  watch?: string[];
  maxBackoffMs?: number;
  onReconnect?: (info: ReconnectInfo) => void;
}

/** Scale a raw amount string by its `*_decimals` field, exactly, as a decimal string. */
export function uiAmount(amount: string | null | undefined, decimals: number | null | undefined): string | null {
  if (amount == null || decimals == null) return null;
  const digits = BigInt(amount).toString().padStart(decimals + 1, "0");
  if (decimals === 0) return digits;
  const frac = digits.slice(-decimals).replace(/0+$/, "");
  return digits.slice(0, -decimals) + (frac ? "." + frac : "");
}

type End = { reason: string; fatal?: FeedError; opened: boolean };

export class Feed implements AsyncIterable<FeedMessage> {
  /** The server epoch of the stream being followed (from `hello`). */
  epoch: number | null = null;
  /** The `seq` of the last event or gap yielded: the resume point. */
  lastSeq = 0;
  private readonly key: string;
  private readonly opts: FeedOptions;

  constructor(key: string, opts: FeedOptions = {}) {
    if (!key) throw new AuthError("no API key given");
    // a browser-style WebSocket hides the server's 400 for an unknown name, so check names here
    for (const k of opts.kinds ?? []) if (!KINDS.includes(k)) throw new FeedError(`unknown kind "${k}"`);
    for (const v of opts.venues ?? []) if (!VENUES.includes(v)) throw new FeedError(`unknown venue "${v}"`);
    this.key = key;
    this.opts = opts;
  }

  /** The connect URL: filters and the resume point — never the key (the server refuses it there). */
  connectUrl(): string {
    const q: string[] = [];
    const { kinds, venues, watch } = this.opts;
    // an empty list is sent as-is: it is a real filter (match nothing), not "no filter"
    const csv = (v: readonly string[]) => v.map(encodeURIComponent).join(",");
    if (kinds) q.push(`kinds=${csv(kinds)}`);
    if (venues) q.push(`venues=${csv(venues)}`);
    if (watch) q.push(`watch=${csv(watch)}`);
    if (this.epoch !== null) q.push(`since=${this.epoch}:${this.lastSeq}`);
    const base = this.opts.url ?? DEFAULT_URL;
    return q.length ? `${base}${base.includes("?") ? "&" : "?"}${q.join("&")}` : base;
  }

  [Symbol.asyncIterator](): AsyncGenerator<FeedMessage> {
    return this.events();
  }

  async *events(): AsyncGenerator<FeedMessage> {
    let delay = 1000;
    let attempt = 0;
    for (;;) {
      const end = yield* this.session(() => {
        delay = 1000;
        attempt = 0;
      });
      if (end.fatal) throw end.fatal;
      let reason = end.reason;
      if (!end.opened) {
        // A browser-style WebSocket hides the HTTP status of a refused handshake; ask the account
        // endpoint whether the key itself is the problem, so a bad key fails fast.
        const probe = await this.probeKey();
        if (probe.refused) throw new AuthError(probe.refused);
        reason = probe.valid
          ? "handshake refused though the key is valid: connection limit for this key, a rejected filter, or the server restarting"
          : "server unreachable";
      }
      attempt += 1;
      const wait = delay + Math.random() * 1000;
      this.opts.onReconnect?.({ attempt, delayMs: wait, reason });
      await new Promise((r) => setTimeout(r, wait));
      delay = Math.min(delay * 2, this.opts.maxBackoffMs ?? 30_000);
    }
  }

  private async *session(onOpen: () => void): AsyncGenerator<FeedMessage, End> {
    const queue: FeedMessage[] = [];
    let end: End | null = null;
    let opened = false;
    let wake: (() => void) | null = null;
    const notify = () => {
      const w = wake;
      wake = null;
      w?.();
    };
    const finish = (e: End) => {
      if (!end) end = e;
      notify();
    };
    const ws = new WebSocket(this.connectUrl(), ["solfeed.v1", `key.${this.key}`]);
    ws.onopen = () => {
      opened = true;
      onOpen();
    };
    ws.onmessage = (e: MessageEvent) => {
      if (end) return;
      if (queue.length >= MAX_QUEUE) {
        finish({ reason: "consumer too slow: dropped the connection to resume", opened });
        ws.close(1000);
        return;
      }
      queue.push(JSON.parse(String(e.data)));
      notify();
    };
    ws.onclose = (e: CloseEvent) => {
      if (e.code === 4001) finish({ reason: "key revoked", fatal: new AuthError(e.reason || "key revoked"), opened });
      else if (e.code === 1008) finish({ reason: e.reason, fatal: new FeedError(e.reason || "policy violation"), opened });
      else finish({ reason: `closed ${e.code} ${e.reason}`.trim(), opened });
    };
    // Node fires no close event after a refused handshake; after an open socket's error, close follows.
    ws.onerror = () => {
      if (!opened) finish({ reason: "connection failed", opened });
      else setTimeout(() => finish({ reason: "connection error", opened }), 1000);
    };
    try {
      for (;;) {
        while (queue.length) {
          const m = queue.shift()!;
          if (m.kind === ("hello" as string)) {
            const epoch = m.epoch as number;
            if (epoch !== this.epoch) {
              // first connect, or the server restarted
              this.epoch = epoch;
              this.lastSeq = 0;
            }
            continue;
          }
          if (m.kind === ("heartbeat" as string)) continue; // never resume from a heartbeat's seq
          this.lastSeq = m.seq;
          if (m.kind === "gap" && m.reason === "epoch_changed" && typeof m.epoch === "number") {
            // the server restarted under a live connection: no new hello comes, and resuming
            // against the old epoch would replay events already yielded
            this.epoch = m.epoch;
          }
          yield m;
        }
        if (end) return end;
        await new Promise<void>((r) => (wake = r));
      }
    } finally {
      if (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN) ws.close(1000);
    }
  }

  /** Whether the account endpoint accepts this key, or its reason for refusing it (401/403). */
  private async probeKey(): Promise<{ valid: boolean; refused?: string }> {
    try {
      const u = new URL(this.opts.url ?? DEFAULT_URL);
      u.protocol = u.protocol === "ws:" ? "http:" : "https:";
      u.pathname = u.pathname.replace(/\/ws\/?$/, "/account");
      u.search = "";
      const r = await fetch(u, { headers: { Authorization: `Bearer ${this.key}` }, signal: AbortSignal.timeout(10_000) });
      if (r.status === 401 || r.status === 403) return { valid: false, refused: (await r.text()).trim() || `HTTP ${r.status}` };
      return { valid: r.ok };
    } catch {
      // unreachable or blocked (e.g. a browser's CORS rules): keep retrying
      return { valid: false };
    }
  }
}
