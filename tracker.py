#!/usr/bin/env python3
"""
BTC-USDT futures order-book tracker – production version

• REST snapshot → Redis  (startup or gap)
• WebSocket diffs → Redis (continuous @100 ms)
• Dashboard updates every 1 second, clearing previous output
"""

import json, time, threading, logging, sys
import requests, redis, websocket

# ────────────────── Configuration ─────────────────────────────────────────
SYMBOL       = "BTCUSDT"
DEPTH_LIMIT  = 5000
REST_URL     = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL}&limit={DEPTH_LIMIT}"
# Fastest Binance depth stream is 100 ms
WS_URL       = f"wss://stream.binance.com:9443/stream?streams={SYMBOL.lower()}@depth@100ms"

REDIS_HOST   = "redis"
REDIS_PORT   = 6379
REDIS_PWD    = "sgP7uvhkNQvn9bV57hRQiHQSkU2MU46A"

DISPLAY_INT  = 0.0001   # refresh dashboard every 1 second

# Percentage bands for volume metrics
BANDS = [
    (0, 1), (1, 2.5), (2.5, 5),
    (5, 10), (10, 25)
]

# Only emit screen-clear codes if running in a real terminal
CLEAR = "" if not sys.stdout.isatty() else "\033[H\033[J"

# ────────────────── Redis keys ────────────────────────────────────────────
BIDS_SET        = f"{SYMBOL}:orderbook:bids"
ASKS_SET        = f"{SYMBOL}:orderbook:asks"
BID_HASH        = f"{SYMBOL}:orderbook:bid_volumes"
ASK_HASH        = f"{SYMBOL}:orderbook:ask_volumes"
LAST_UPDATE_KEY = f"{SYMBOL}:last_update_id"

# ────────────────── Global state ──────────────────────────────────────────
r        = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT,
                             password=REDIS_PWD, decode_responses=True)
synced   = False
buffered = []
lock     = threading.Lock()
ws_app   = None

# ────────────────── Apply a single diff message to Redis ────────────────
def apply_diff(msg: dict):
    for price, qty in msg.get("b", []):   # bids
        q = float(qty)
        if q == 0:
            r.zrem(BIDS_SET, price)
            r.hdel(BID_HASH, price)
        else:
            r.zadd(BIDS_SET, {price: float(price)})
            r.hset(BID_HASH, price, qty)

    for price, qty in msg.get("a", []):   # asks
        q = float(qty)
        if q == 0:
            r.zrem(ASKS_SET, price)
            r.hdel(ASK_HASH, price)
        else:
            r.zadd(ASKS_SET, {price: float(price)})
            r.hset(ASK_HASH, price, qty)

# ────────────────── WebSocket handlers ────────────────────────────────────
def on_open(ws):
    print("WebSocket connected")

def on_message(ws, raw):
    msg = json.loads(raw).get("data", {})
    if not msg:
        return
    with lock:
        if not synced:
            buffered.append(msg)
        else:
            process_live(msg)

def process_live(msg):
    global synced
    last_id = int(r.get(LAST_UPDATE_KEY) or 0)
    U, u    = msg["U"], msg["u"]

    if u <= last_id:
        return
    if U != last_id + 1:   # gap detected
        logging.warning(f"Gap {last_id+1}→{U}, resyncing via REST")
        synced = False
        ws_app.close()
        return

    apply_diff(msg)
    r.set(LAST_UPDATE_KEY, u)

def on_error(ws, err):
    logging.error(f"WS error: {err}")

def on_close(ws, code, reason):
    pass

def start_ws():
    global ws_app
    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    threading.Thread(target=ws_app.run_forever, daemon=True).start()

# ────────────────── REST snapshot & resume logic ─────────────────────────
def rest_snapshot():
    snap = requests.get(REST_URL, timeout=5).json()
    bids, asks = snap["bids"], snap["asks"]
    last_id = snap["lastUpdateId"]

    pipe = r.pipeline()
    pipe.delete(BIDS_SET, ASKS_SET, BID_HASH, ASK_HASH, LAST_UPDATE_KEY)
    for p, q in bids:
        if float(q) > 0:
            pipe.zadd(BIDS_SET, {p: float(p)})
            pipe.hset(BID_HASH, p, q)
    for p, q in asks:
        if float(q) > 0:
            pipe.zadd(ASKS_SET, {p: float(p)})
            pipe.hset(ASK_HASH, p, q)
    pipe.set(LAST_UPDATE_KEY, last_id)
    pipe.execute()
    print("REST snapshot stored")

def resume_from_redis():
    last_id = r.get(LAST_UPDATE_KEY)
    if not last_id:
        return False
    last_id = int(last_id)
    first = None
    for msg in buffered:
        if msg["u"] > last_id:
            if first is None:
                first = msg
            if first["U"] != last_id + 1:
                return False
            apply_diff(msg)
            last_id = msg["u"]
    if first:
        r.set(LAST_UPDATE_KEY, last_id)
    return True

def initial_sync():
    global synced, buffered
    buffered = []
    start_ws()
    time.sleep(1)  # gather early diffs

    if resume_from_redis():
        print("Resumed from existing Redis snapshot")
        synced = True
        buffered.clear()
        return

    rest_snapshot()
    last_id = int(r.get(LAST_UPDATE_KEY))
    for msg in buffered:
        if msg["u"] > last_id:
            apply_diff(msg)
            last_id = msg["u"]
    r.set(LAST_UPDATE_KEY, last_id)
    synced = True
    buffered.clear()

# ────────────────── Dashboard rendering ──────────────────────────────────
def band_metrics(best_bid, best_ask):
    rows = []
    for lo, hi in BANDS:
        bid_low  = best_bid * (1 - hi/100)
        bid_high = best_bid * (1 - lo/100)
        ask_low  = best_ask * (1 + lo/100)
        ask_high = best_ask * (1 + hi/100)

        bids_px = r.zrangebyscore(BIDS_SET, bid_low, bid_high)
        asks_px = r.zrangebyscore(ASKS_SET, ask_low, ask_high)

        bid_vol = sum(float(r.hget(BID_HASH, p) or 0) for p in bids_px)
        ask_vol = sum(float(r.hget(ASK_HASH, p) or 0) for p in asks_px)
        delta   = bid_vol - ask_vol
        total   = bid_vol + ask_vol
        ratio   = (delta / total * 100) if total else 0

        rows.append((f"{lo}-{hi}%", bid_vol, ask_vol, delta, ratio))
    return rows

def show_dashboard():
    try:
        # get the exact string members for best bid/ask
        best_bid_str = r.zrange(BIDS_SET, -1, -1)[0]
        best_ask_str = r.zrange(ASKS_SET,  0,  0)[0]

        # convert to float for numeric operations
        best_bid = float(best_bid_str)
        best_ask = float(best_ask_str)

        # look up the exact quantities stored under those fields
        bid_q = float(r.hget(BID_HASH, best_bid_str) or 0)
        ask_q = float(r.hget(ASK_HASH, best_ask_str) or 0)
    except Exception:
        print("Waiting for data…")
        return

    # clear the screen if in a real terminal
    print(CLEAR, end="")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {SYMBOL}")
    print(f"Best Bid: {best_bid:,.2f}")
    print(f"Best Ask: {best_ask:,.2f}")
    print("-" * 60)
    print(f"{'Band':<8}{'BidVol':>10}{'AskVol':>10}{'Δ':>10}{'Ratio%':>10}")
    for band, bvol, avol, dlt, rat in band_metrics(best_bid, best_ask):
        print(f"{band:<8}{bvol:>10.3f}{avol:>10.3f}{dlt:>10.3f}{rat:>10.2f}")

# ────────────────── Main loop ─────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    try:
        r.ping()
        print("Redis connected")
    except redis.RedisError as e:
        print(f"Cannot connect to Redis: {e}")
        sys.exit(1)

    while True:
        try:
            if not synced:
                initial_sync()

            show_dashboard()
            time.sleep(DISPLAY_INT)

            # auto-resync on WS disconnect
            if not getattr(ws_app, "sock", None) or not ws_app.sock.connected:
                logging.warning("WebSocket disconnected – resyncing")
                synced = False

        except Exception as ex:
            logging.error(f"Main loop error: {ex}")
            synced = False
            time.sleep(1)
