#!/usr/bin/env python3
"""
BTC-USDT futures order-book tracker  –  Bigtable edition
────────────────────────────────────────────────────────────────────────────
• One REST snapshot → Cloud Bigtable   (startup or gap)
• Continuous @100 ms WebSocket diffs → Cloud Bigtable
• Dashboard refresh every 1 s (reads best bid / ask *from Bigtable*)
────────────────────────────────────────────────────────────────────────────
Prerequisites you create once (manual / Terraform):
  ▸ Bigtable instance  :  orderbook-inst   (single-region, ≥1 node)
  ▸ Table              :  btc_orderbook   with column-family  ob
     - row key “b#<price>” for bids
     - row key “a#<price>” for asks
  ▸ Service account running this code has   roles/bigtable.user
Dependencies:
  pip install google-cloud-bigtable websocket-client requests
"""

import json, time, threading, logging, sys, requests, websocket
from google.cloud import bigtable
from google.cloud.bigtable import row_filters, row

# ──────────────── Static configuration ───────────────────────────────────
PROJECT_ID      = "my-gcp-project"     # <─── change to yours
INSTANCE_ID     = "orderbook-inst"     # Bigtable instance
TABLE_ID        = "btc_orderbook"
COLUMN_FAMILY   = "ob"
SYMBOL          = "BTCUSDT"

DEPTH_LIMIT     = 5000
REST_URL        = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL}&limit={DEPTH_LIMIT}"
WS_URL          = f"wss://stream.binance.com:9443/stream?streams={SYMBOL.lower()}@depth@100ms"

DISPLAY_INT     = 1.0                  # dashboard refresh (s)

# ──────────────── Bigtable client / table objects ───────────────────────
bt_client   = bigtable.Client(project=PROJECT_ID, admin=False)
bt_instance = bt_client.instance(INSTANCE_ID)
table       = bt_instance.table(TABLE_ID)

# ──────────────── Runtime state ──────────────────────────────────────────
last_id   = 0
ws_app    = None
lock      = threading.Lock()

# ──────────────── Bigtable helpers ───────────────────────────────────────
def _row_key(side: str, price: str) -> bytes:
    """Return row-key b#<price> or a#<price>"""
    return f"{side}#{price}".encode()

def batch_mutate(side_prefix: str, price_qty_pairs):
    """Batch put/delete rows for one side."""
    rows = []
    for price, qty in price_qty_pairs:
        key = _row_key(side_prefix, price)
        if float(qty) == 0.0:
            r = row.DirectRow(row_key=key, table=table)
            r.delete()                             # delete row
        else:
            r = row.DirectRow(row_key=key, table=table)
            r.set_cell(COLUMN_FAMILY, b"q", qty.encode())
        rows.append(r)
    if rows:
        table.mutate_rows(rows)

# ──────────────── Snapshot (initial or gap) ──────────────────────────────
def store_snapshot():
    global last_id
    snap = requests.get(REST_URL, timeout=5).json()
    last_id = snap["lastUpdateId"]

    # build two DirectRow lists, then bulk-mutate once
    bids = snap["bids"]
    asks = snap["asks"]
    batch_mutate("b", bids)
    batch_mutate("a", asks)
    logging.info(f"REST snapshot stored  #{last_id:,}")

# ──────────────── Apply one WebSocket diff to Bigtable ───────────────────
def apply_diff(msg):
    batch_mutate("b", msg.get("b", []))
    batch_mutate("a", msg.get("a", []))

# ──────────────── WebSocket callbacks ────────────────────────────────────
def on_open(ws):
    logging.info("WebSocket connected")

def on_message(ws, raw):
    global last_id
    msg = json.loads(raw).get("data", {})
    if not msg:
        return
    U, u = msg["U"], msg["u"]

    with lock:
        if u <= last_id:                        # already seen
            return
        if U != last_id + 1:                    # gap detected
            logging.warning(f"Gap {last_id+1}→{U} (resync)")
            ws.close()
            store_snapshot()
            return
        apply_diff(msg)
        last_id = u

def on_error(ws, err):
    logging.error(f"WS error: {err}")

def on_close(ws, code, reason):
    logging.warning(f"WebSocket closed {code} {reason}")

def start_ws():
    global ws_app
    ws_app = websocket.WebSocketApp(
        WS_URL, on_open=on_open, on_message=on_message,
        on_error=on_error, on_close=on_close
    )
    threading.Thread(target=ws_app.run_forever, daemon=True).start()

# ──────────────── Dashboard helpers ──────────────────────────────────────
BANDS = [(0,1), (1,2.5), (2.5,5), (5,10), (10,25)]
CLEAR = "" if not sys.stdout.isatty() else "\033[H\033[J"

def fetch_side(side_prefix: bytes):
    flt = row_filters.RowKeyRegexFilter(side_prefix + b"#.*")
    rows = table.read_rows(filter_=flt)
    for row_data in rows:
        price = float(row_data.row_key.split(b"#")[1])
        qty   = float(row_data.cells[COLUMN_FAMILY][b"q"][-1].value)
        yield price, qty

def best_prices_and_vols():
    bids = list(fetch_side(b"b"))
    asks = list(fetch_side(b"a"))
    if not bids or not asks:
        return None
    best_bid = max(p for p, _ in bids)
    best_ask = min(p for p, _ in asks)

    # compute band volumes
    rows = []
    for lo, hi in BANDS:
        bid_vol = sum(q for p, q in bids if best_bid*(1-hi/100) <= p <= best_bid*(1-lo/100))
        ask_vol = sum(q for p, q in asks if best_ask*(1+lo/100) <= p <= best_ask*(1+hi/100))
        delta   = bid_vol - ask_vol
        total   = bid_vol + ask_vol
        ratio   = (delta/total*100) if total else 0
        rows.append((f"{lo}-{hi}%", bid_vol, ask_vol, delta, ratio))
    return best_bid, best_ask, rows

def show_dashboard():
    res = best_prices_and_vols()
    if not res:
        print("Waiting for data…"); return
    best_bid, best_ask, rows = res

    print(CLEAR, end="")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {SYMBOL}")
    print(f"Best Bid: {best_bid:,.2f}")
    print(f"Best Ask: {best_ask:,.2f}")
    print("-"*60)
    print(f"{'Band':<8}{'BidVol':>10}{'AskVol':>10}{'Δ':>10}{'Ratio%':>10}")
    for band, bvol, avol, dlt, rat in rows:
        print(f"{band:<8}{bvol:>10.3f}{avol:>10.3f}{dlt:>10.3f}{rat:>10.2f}")

# ──────────────── Main loop ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s — %(levelname)s — %(message)s")

    try:
        table.sample_row_keys()       # simple health check
        logging.info("Bigtable connected")
    except Exception as e:
        logging.error(f"Cannot connect to Bigtable: {e}")
        sys.exit(1)

    store_snapshot()
    start_ws()

    while True:
        try:
            live = getattr(ws_app, "sock", None) and ws_app.sock.connected
            if not live:
                logging.warning("WS down – restarting")
                start_ws()

            show_dashboard()
            time.sleep(DISPLAY_INT)
        except Exception as ex:
            logging.error(f"Main loop error: {ex}")
            time.sleep(1)
