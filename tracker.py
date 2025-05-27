#!/usr/bin/env python3
"""
BTC-USDT futures order-book tracker – Railway edition
—————————————————————————————————————————————————————————
• REST snapshot  ➜ Redis
• WebSocket diffs ➜ Redis
• Minute snapshots archived   • Band/Δ/ratio printer
• Structured logging & back-off

Environment variables expected (Railway injects these automatically
after you add its Redis plug-in):

    REDISHOST        e.g. 'monorail.proxy.rlwy.net'
    REDISPORT        e.g. '10263'
    REDISPASSWORD    long random string

Optional overrides you can set in Railway → Variables (or export locally):

    SYMBOL           default BTCUSDT
    UPDATE_SPEED     default 100ms   (valid: 100ms / 10ms)
    REST_LIMIT       default 1000
    TRIM_DEPTH_PCT   default 30      (% depth discarded by archiver)

Run locally (example):
    export REDISHOST=localhost
    export REDISPORT=6379
    python tracker.py
"""

import os, sys, time, json, asyncio, random, logging
from typing import Dict, List, Tuple

import requests, websockets, redis

# ────────────────────── Runtime configuration ────────────────────────────
SYMBOL           = os.getenv("SYMBOL", "BTCUSDT").upper()
REST_LIMIT       = int(os.getenv("REST_LIMIT", 1000))
UPDATE_SPEED     = os.getenv("UPDATE_SPEED", "100ms")
REFRESH_INTERVAL = 60.0                        # seconds between REST refreshes
ARCHIVE_TTL      = 24 * 3600                   # seconds to keep snapshots
TRIM_DEPTH_PCT   = float(os.getenv("TRIM_DEPTH_PCT", 30.0))

REDIS_HOST       = os.getenv("REDISHOST", "localhost")
REDIS_PORT       = int(os.getenv("REDISPORT", 6379))
REDIS_PWD        = os.getenv("REDISPASSWORD")  # can be None for local Redis

BASE_URL          = "https://fapi.binance.com"
SNAPSHOT_ENDPOINT = "/fapi/v1/depth"
WS_URL            = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@depth@{UPDATE_SPEED}"

BIDS_ZSET  = f"orderbook:{SYMBOL}:bids"
ASKS_ZSET  = f"orderbook:{SYMBOL}:asks"
QTY_HASH   = f"orderbook:{SYMBOL}:qty"
DIFFS_LIST = f"orderbook:{SYMBOL}:diffs"
MAX_DIFFS  = 10_000                            # keep last 10 k raw diffs

# ────────────────────── Logging ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("tracker")

# ────────────────────── Redis client ──────────────────────────────────────
r = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PWD,
    decode_responses=True,
)
try:
    r.ping()
except redis.RedisError as e:
    log.critical("Redis connection failed: %s", e)
    sys.exit(1)

# ────────────────────── Static band definitions ───────────────────────────
PRINT_INTERVAL = 3.0                          # seconds between console prints
BANDS: List[Tuple[float, float]] = [
    (0, 1),
    (1, 2.5),
    (2.5, 5),
    (5, 10),
    (10, 25),
]

# ────────────────────── In-memory state ───────────────────────────────────
bids: Dict[float, float] = {}   # price ➜ qty
asks: Dict[float, float] = {}
last_update_id: int = 0

# ────────────────────── Helpers ───────────────────────────────────────────
def now_minute() -> int:
    """Unix timestamp truncated to minute."""
    return int(time.time()) // 60

def backoff(attempt: int, base: float = 5.0, cap: float = 60.0) -> float:
    """Binary exponential back-off with jitter."""
    return min(cap, base * 2 ** attempt) * (0.5 + random.random() / 2)

# ────────────────────── REST snapshot loader ──────────────────────────────
def fetch_snapshot() -> int:
    url = BASE_URL + SNAPSHOT_ENDPOINT
    params = {"symbol": SYMBOL, "limit": REST_LIMIT}
    attempt = 0
    while True:
        try:
            data = requests.get(url, params=params, timeout=7).json()
            sid = data["lastUpdateId"]

            # fresh start – clear both Redis and local dicts
            r.delete(BIDS_ZSET, ASKS_ZSET, QTY_HASH)
            bids.clear(); asks.clear()

            pipe = r.pipeline()
            # bids
            for p_str, q_str in data["bids"]:
                qty = float(q_str)
                if qty:
                    price = float(p_str)
                    bids[price] = qty
                    pipe.zadd(BIDS_ZSET, {p_str: price})
                    pipe.hset(QTY_HASH, p_str, q_str)
            # asks
            for p_str, q_str in data["asks"]:
                qty = float(q_str)
                if qty:
                    price = float(p_str)
                    asks[price] = qty
                    pipe.zadd(ASKS_ZSET, {p_str: price})
                    pipe.hset(QTY_HASH, p_str, q_str)
            pipe.execute()

            log.info("[REST] snapshot id=%s bids=%d asks=%d", sid, len(bids), len(asks))
            return sid
        except Exception as e:
            delay = backoff(attempt)
            log.warning("[REST] %s – retry in %.1fs", e, delay)
            time.sleep(delay)
            attempt += 1

# ────────────────────── Diff application helpers ──────────────────────────
def _update_level(book: Dict[float, float], zset: str,
                  price: float, qty: float, p_str: str, q_str: str):
    """Insert/update/remove one price level in both Redis & local dict."""
    if qty == 0:
        book.pop(price, None)
        r.zrem(zset, p_str)
        r.hdel(QTY_HASH, p_str)
    else:
        book[price] = qty
        r.zadd(zset, {p_str: price})
        r.hset(QTY_HASH, p_str, q_str)

def apply_diff_to_state(diff: dict):
    """Apply one diff message to Redis + RAM + raw diff list."""
    # store raw diff
    r.rpush(DIFFS_LIST, json.dumps(diff))
    r.ltrim(DIFFS_LIST, -MAX_DIFFS, -1)

    # apply bids/asks
    for p_str, q_str in diff.get("b", []):
        price, qty = float(p_str), float(q_str)
        _update_level(bids, BIDS_ZSET, price, qty, p_str, q_str)
    for p_str, q_str in diff.get("a", []):
        price, qty = float(p_str), float(q_str)
        _update_level(asks, ASKS_ZSET, price, qty, p_str, q_str)

# ────────────────────── Convenience Redis queries ─────────────────────────
def best_bid_ask() -> Tuple[Tuple[float, float], Tuple[float, float]]:
    """Return ((bid_price, bid_qty), (ask_price, ask_qty))."""
    bid_p, _ = r.zrevrange(BIDS_ZSET, 0, 0, withscores=True)[0]
    ask_p, _ = r.zrange(ASKS_ZSET, 0, 0, withscores=True)[0]
    bid_q = float(r.hget(QTY_HASH, bid_p) or 0)
    ask_q = float(r.hget(QTY_HASH, ask_p) or 0)
    return (float(bid_p), bid_q), (float(ask_p), ask_q)

# ────────────────────── Volume-band maths ─────────────────────────────────
def band_metrics() -> List[Tuple[str, float, float, float, float]]:
    if not bids or not asks:
        return []
    (best_bid, _), (best_ask, _) = best_bid_ask()
    rows = []
    for lo, hi in BANDS:
        bid_low  = best_bid * (1 - hi / 100)
        bid_high = best_bid * (1 - lo / 100)
        ask_low  = best_ask * (1 + lo / 100)
        ask_high = best_ask * (1 + hi / 100)

        bid_vol = sum(q for p, q in bids.items() if bid_low <= p < bid_high)
        ask_vol = sum(q for p, q in asks.items() if ask_low <= p < ask_high)

        delta = bid_vol - ask_vol
        total = bid_vol + ask_vol
        ratio = (delta / total * 100) if total else 0.0
        rows.append((f"{lo:g}-{hi:g}%", bid_vol, ask_vol, delta, ratio))
    return rows

async def printer():
    """Pretty console print every PRINT_INTERVAL seconds."""
    while True:
        os.system("cls" if os.name == "nt" else "clear")
        (bb_p, bb_q), (ba_p, ba_q) = best_bid_ask()
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {SYMBOL}")
        print(f" Best Bid {bb_p:,.1f}  Qty {bb_q:,.3f}")
        print(f" Best Ask {ba_p:,.1f}  Qty {ba_q:,.3f}")
        print("-" * 60)
        print(f"{'Band':<9} {'BidVol':>10} {'AskVol':>10} {'Δ':>10} {'Ratio%':>8}")
        for lbl, b, a, d, r_ in band_metrics():
            print(f"{lbl:<9} {b:10.3f} {a:10.3f} {d:10.3f} {r_:8.2f}")
        await asyncio.sleep(PRINT_INTERVAL)

# ────────────────────── Snapshot archiver task ────────────────────────────
async def snapshot_archiver():
    while True:
        # wait until next full minute
        nxt = (now_minute() + 1) * 60
        await asyncio.sleep(max(0, nxt - time.time()))

        key = f"snapshot:{SYMBOL}:{now_minute()}"
        payload = {"id": last_update_id, "bids": bids, "asks": asks}
        r.setex(key, ARCHIVE_TTL, json.dumps(payload, separators=(",", ":")))
        log.info("[ARCHIVE] stored %s (levels=%d+%d)",
                 key, len(bids), len(asks))

        # optional depth trimming outside ±TRIM_DEPTH_PCT
        if TRIM_DEPTH_PCT:
            (best_bid, _), (best_ask, _) = best_bid_ask()
            bid_floor   = best_bid * (1 - TRIM_DEPTH_PCT / 100)
            ask_ceiling = best_ask * (1 + TRIM_DEPTH_PCT / 100)
            r.zremrangebyscore(BIDS_ZSET, "-inf", bid_floor)
            r.zremrangebyscore(ASKS_ZSET, ask_ceiling, "+inf")

# ────────────────────── WebSocket helpers ─────────────────────────────────
async def open_ws():
    attempt = 0
    while True:
        try:
            ws = await websockets.connect(
                WS_URL,
                ping_interval=15,
                ping_timeout=15,
                max_queue=None,
                compression=None,
            )
            log.info("[WS] connected")
            return ws
        except Exception as e:
            delay = backoff(attempt)
            log.warning("[WS] %s – reconnect in %.1fs", e, delay)
            await asyncio.sleep(delay)
            attempt += 1

async def ws_handler():
    global last_update_id
    last_update_id = fetch_snapshot()

    ws = await open_ws()
    synced = False
    last_refresh = time.time()

    while True:
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=30)
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            log.warning("[WS] timeout/closed – reconnect")
            ws = await open_ws()
            synced = False
            continue

        diff = json.loads(msg)
        U, u = diff["U"], diff["u"]

        # 1️⃣ discard stale messages
        if u <= last_update_id:
            continue

        # 2️⃣ first bridge after REST snapshot
        if not synced:
            if U <= last_update_id + 1 <= u:
                apply_diff_to_state(diff)
                last_update_id = u
                synced = True
                log.info("[SYNC] bridged diff id=%d", u)
            continue

        now = time.time()
        # 3️⃣ periodic hard refresh
        if now - last_refresh >= REFRESH_INTERVAL:
            last_update_id = fetch_snapshot()
            synced = False
            last_refresh = now
            continue

        # 4️⃣ gap detected – resync
        if U > last_update_id + 1:
            log.warning("[GAP] %d → %d – resync", last_update_id, U)
            last_update_id = fetch_snapshot()
            synced = False
            continue

        # 5️⃣ normal in-order diff
        apply_diff_to_state(diff)
        last_update_id = u

# ────────────────────── Main entry ────────────────────────────────────────
async def main():
    await asyncio.gather(
        ws_handler(),
        snapshot_archiver(),
        printer(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Exited.")
