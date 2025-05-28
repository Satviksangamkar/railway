#!/usr/bin/env python3
"""
BTC-USDT futures order-book tracker – production version

• REST snapshot → Redis       • WebSocket diffs → Redis
• Minute snapshots archived   • Band/Δ/ratio printer
• Best bid/ask queried from Redis
• Structured logging, back-off, latency stats
"""

import asyncio, json, time, sys, random, logging, os
from typing import Dict, List, Tuple

import requests, websockets, redis

# ───────────────────────────── CONFIG ──────────────────────────────────────
SYMBOL            = "BTCUSDT"
REST_LIMIT        = 1000
UPDATE_SPEED      = "100ms"
REFRESH_INTERVAL  = 60.0                 # seconds between full REST refreshes
ARCHIVE_TTL       = 24 * 3600            # keep snapshots 24 h in Redis
TRIM_DEPTH_PCT    = 30.0                 # trim levels >30 % away; 0 = never

REDIS_HOST        = "redis"
REDIS_PORT        = 6379
REDIS_PWD         = "sgP7uvhkNQvn9bV57hRQiHQSkU2MU46A"

BASE_URL          = "https://fapi.binance.com"
SNAPSHOT_ENDPOINT = "/fapi/v1/depth"
WS_URL            = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@depth@{UPDATE_SPEED}"

BIDS_ZSET    = f"orderbook:{SYMBOL}:bids"
ASKS_ZSET    = f"orderbook:{SYMBOL}:asks"
QTY_HASH     = f"orderbook:{SYMBOL}:qty"
DIFFS_LIST   = f"orderbook:{SYMBOL}:diffs"

MAX_UPDATES  = 10_000           # retain last N diffs in Redis list

# ──────────────────────────── LOGGING ──────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tracker")

# ──────────────────────────── REDIS ────────────────────────────────────────
r = redis.StrictRedis(
    host=REDIS_HOST, port=REDIS_PORT,
    password=REDIS_PWD, decode_responses=True
)
try:
    r.ping()
except redis.RedisError as e:
    log.critical("Redis connection failed: %s", e)
    sys.exit(1)

# ──────────────────────────── BANDS ────────────────────────────────────────
PRINT_INTERVAL = 3.0  # seconds
BANDS: List[Tuple[float, float]] = [
    (0, 1),
    (1, 2.5),
    (2.5, 5),
    (5, 10),
    (10, 25),
]

# ───────────────────────── GLOBAL STATE ────────────────────────────────────
bids: Dict[float, float] = {}   # price → qty
asks: Dict[float, float] = {}
last_update_id: int = 0

# ──────────────────────── HELPER FUNCTIONS ────────────────────────────────
def clear_screen() -> None:
    print("\033[2J\033[H", end="")

def now_minute() -> int:
    return int(time.time()) // 60

def backoff(attempt: int, base: float = 5.0, cap: float = 60.0) -> float:
    return min(cap, base * 2 ** attempt) * (0.5 + random.random() / 2)

# ───────────────────────── SNAPSHOT FETCH ─────────────────────────────────
def fetch_snapshot() -> int:
    url = BASE_URL + SNAPSHOT_ENDPOINT
    attempt = 0
    while True:
        try:
            resp = requests.get(
                url, params={"symbol": SYMBOL, "limit": REST_LIMIT}, timeout=7
            )
            resp.raise_for_status()
            data = resp.json()
            sid = data["lastUpdateId"]

            r.delete(BIDS_ZSET, ASKS_ZSET, QTY_HASH)
            bids.clear(); asks.clear()

            pipe = r.pipeline()
            for p_str, q_str in data["bids"]:
                qty = float(q_str)
                if qty:
                    price = float(p_str)
                    bids[price] = qty
                    pipe.zadd(BIDS_ZSET, {p_str: price})
                    pipe.hset(QTY_HASH, p_str, q_str)
            for p_str, q_str in data["asks"]:
                qty = float(q_str)
                if qty:
                    price = float(p_str)
                    asks[price] = qty
                    pipe.zadd(ASKS_ZSET, {p_str: price})
                    pipe.hset(QTY_HASH, p_str, q_str)
            pipe.execute()

            log.info("[REST] snapshot id=%s bids=%d asks=%d",
                     sid, len(bids), len(asks))
            return sid
        except Exception as e:
            delay = backoff(attempt)
            log.warning("[REST] error %s – retrying in %.1fs", e, delay)
            time.sleep(delay)
            attempt += 1

# ───────────────────────── APPLY DIFF ─────────────────────────────────────
def _update_level(book: Dict[float, float], zset: str,
                  price: float, qty: float, price_str: str, qty_str: str):
    if qty == 0:
        book.pop(price, None)
        r.zrem(zset, price_str)
        r.hdel(QTY_HASH, price_str)
    else:
        book[price] = qty
        r.zadd(zset, {price_str: price})
        r.hset(QTY_HASH, price_str, qty_str)

def apply_diff_to_state(diff: dict):
    u = diff["u"]

    r.rpush(DIFFS_LIST, json.dumps(diff))
    r.ltrim(DIFFS_LIST, -MAX_UPDATES, -1)

    t0 = time.perf_counter()
    for p_str, q_str in diff.get("b", []):
        price, qty = float(p_str), float(q_str)
        _update_level(bids, BIDS_ZSET, price, qty, p_str, q_str)
    for p_str, q_str in diff.get("a", []):
        price, qty = float(p_str), float(q_str)
        _update_level(asks, ASKS_ZSET, price, qty, p_str, q_str)
    latency = (time.perf_counter() - t0) * 1000
    log.debug("[DIFF] applied id=%d levels=%d latency=%.2f ms",
              u, len(diff.get("b", [])) + len(diff.get("a", [])), latency)

# ───────────────────────── REDIS HELPERS ──────────────────────────────────
def best_bid_ask() -> Tuple[Tuple[float, float], Tuple[float, float]]:
    bid_p, _ = r.zrevrange(BIDS_ZSET, 0, 0, withscores=True)[0]
    ask_p, _ = r.zrange(ASKS_ZSET, 0, 0, withscores=True)[0]
    bid_q = float(r.hget(QTY_HASH, bid_p) or 0)
    ask_q = float(r.hget(QTY_HASH, ask_p) or 0)
    return (float(bid_p), bid_q), (float(ask_p), ask_q)

# ───────────────────────── BAND METRICS ───────────────────────────────────
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
    while True:
        clear_screen()
        (bb_p, bb_q), (ba_p, ba_q) = best_bid_ask()
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {SYMBOL}")
        print(f" Best Bid {bb_p:,.1f}  Qty {bb_q:,.3f}")
        print(f" Best Ask {ba_p:,.1f}  Qty {ba_q:,.3f}")
        print("-" * 60)
        print(f"{'Band':<9} {'BidVol':>10} {'AskVol':>10} {'Δ':>10} {'Ratio%':>8}")
        for lbl, b, a, d, r_ in band_metrics():
            print(f"{lbl:<9} {b:10.3f} {a:10.3f} {d:10.3f} {r_:8.2f}")
        await asyncio.sleep(PRINT_INTERVAL)

# ─────────────────────── SNAPSHOT ARCHIVER ────────────────────────────────
async def snapshot_archiver():
    while True:
        nxt = (now_minute() + 1) * 60
        await asyncio.sleep(max(0, nxt - time.time()))
        key = f"snapshot:{SYMBOL}:{now_minute()}"
        payload = {
            "id": last_update_id,
            "bids": bids,
            "asks": asks,
        }
        r.setex(key, ARCHIVE_TTL, json.dumps(payload, separators=(",", ":")))
        log.info("[ARCHIVE] stored %s (levels=%d+%d)",
                 key, len(bids), len(asks))

        if TRIM_DEPTH_PCT:
            (best_bid, _), (best_ask, _) = best_bid_ask()
            bid_floor = best_bid * (1 - TRIM_DEPTH_PCT / 100)
            ask_ceiling = best_ask * (1 + TRIM_DEPTH_PCT / 100)
            r.zremrangebyscore(BIDS_ZSET, "-inf", bid_floor)
            r.zremrangebyscore(ASKS_ZSET, ask_ceiling, "+inf")

# ───────────────────────── WEBSOCKET HANDLER ──────────────────────────────
async def open_ws():
    attempt = 0
    while True:
        try:
            ws = await websockets.connect(
                WS_URL,
                ping_interval=20, ping_timeout=20,
                max_queue=None,
                compression=None
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

    while True:                                     # outer resync loop
        last_update_id = fetch_snapshot()
        ws = await open_ws()
        synced = False
        last_refresh = time.time()

        try:
            while True:                             # recv loop
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                except (asyncio.TimeoutError,
                        websockets.exceptions.ConnectionClosed):
                    log.warning("[WS] timed-out/closed – reconnecting")
                    break                           # resync outer loop

                diff = json.loads(msg)
                U, u = diff["U"], diff["u"]
                log.debug("[WS] recv U=%d u=%d", U, u)

                if u <= last_update_id:
                    continue                        # old diff

                if not synced:
                    if U <= last_update_id + 1 <= u:
                        apply_diff_to_state(diff)
                        last_update_id = u
                        synced = True
                        log.info("[SYNC] initial bridge at id=%d", u)
                    continue

                now = time.time()
                if now - last_refresh >= REFRESH_INTERVAL:
                    log.info("[REST] periodic refresh")
                    break                           # resync outer loop

                if U > last_update_id + 1:
                    log.warning("[GAP] %d → %d – resync", last_update_id, U)
                    break                           # resync outer loop

                apply_diff_to_state(diff)
                last_update_id = u

        finally:
            await ws.close()

# ──────────────────────────── MAIN ────────────────────────────────────────
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
