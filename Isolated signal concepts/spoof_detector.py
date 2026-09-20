import asyncio
import json
import time
import websockets
from datetime import datetime

# ============================================================
# SETTINGS
# ============================================================

MIN_ORDER_SIZE = 1.7
MAX_ORDER_SIZE = 9.7
PRICE_PROXIMITY = 15
MIN_HOLD_TIME = 5
MAX_HOLD_TIME = 150
BOUNCE_MIN_MOVE = 4

# ============================================================
# STATE
# ============================================================

tracked_orders = {}
confirmed_spoofs = []
price_history = []

def ts():
    return datetime.now().strftime("%H:%M:%S")

async def track_prices():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(url) as ws:
        while True:
            try:
                data = json.loads(await ws.recv())
                price = float(data.get("p", 0))
                if price:
                    price_history.append({
                        "price": price,
                        "time": time.time()
                    })
                    cutoff = time.time() - 120
                    price_history[:] = [p for p in price_history if p["time"] > cutoff]
            except:
                await asyncio.sleep(0.1)

async def check_orderbook():
    url = "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms"
    print(f"[{ts()}] Connecting to BTC orderbook...")

    previous_bids = {}
    previous_asks = {}

    async with websockets.connect(url) as ws:
        while True:
            try:
                data = json.loads(await ws.recv())
                bids = data.get("bids", [])
                asks = data.get("asks", [])

                if not bids:
                    continue

                current_price = float(bids[0][0])
                now = time.time()

                current_bids = {float(b[0]): float(b[1]) for b in bids}
                current_asks = {float(a[0]): float(a[1]) for a in asks}

                # track new large orders appearing
                for price, size in current_bids.items():
                    if MIN_ORDER_SIZE <= size <= MAX_ORDER_SIZE:
                        if abs(price - current_price) <= PRICE_PROXIMITY:
                            key = f"BID_{price:.0f}"
                            if key not in tracked_orders:
                                tracked_orders[key] = {
                                    "price": price,
                                    "size": size,
                                    "side": "BID",
                                    "first_seen": now,
                                    "price_at_appearance": current_price
                                }
                             # silent tracking
                            pass

                for price, size in current_asks.items():
                    if MIN_ORDER_SIZE <= size <= MAX_ORDER_SIZE:
                        if abs(price - current_price) <= PRICE_PROXIMITY:
                            key = f"ASK_{price:.0f}"
                            if key not in tracked_orders:
                                tracked_orders[key] = {
                                    "price": price,
                                    "size": size,
                                    "side": "ASK",
                                    "first_seen": now,
                                    "price_at_appearance": current_price
                                }
                              # silent tracking
                                pass

                # check for cancelled orders
                for key in list(tracked_orders.keys()):
                    order = tracked_orders[key]
                    side = order["side"]
                    price = order["price"]
                    held_for = now - order["first_seen"]
                    price_at_appearance = order["price_at_appearance"]

                    if side == "BID" and price not in current_bids:
                        gone = True
                    elif side == "ASK" and price not in current_asks:
                        gone = True
                    else:
                        gone = False

                    if gone:
                        del tracked_orders[key]

                        if held_for < MIN_HOLD_TIME or held_for > MAX_HOLD_TIME:
                            continue

                        # check price bounced in expected direction
                        if side == "ASK":
                            price_moved_correctly = current_price > price_at_appearance + BOUNCE_MIN_MOVE
                            spoof_direction = "UP"
                        else:
                            price_moved_correctly = current_price < price_at_appearance - BOUNCE_MIN_MOVE
                            spoof_direction = "DOWN"

                        if not price_moved_correctly:
                            print(f"\n[{ts()}] Order at ${price:,.0f} cancelled after {held_for:.0f}s — no bounce, likely real")
                            continue

                        print(f"\n{'='*55}")
                        print(f"✅ CONFIRMED SPOOF [{ts()}]")
                        print(f"  {side} order at ${price:,.0f}")
                        print(f"  Size: {order['size']:.2f} BTC")
                        print(f"  Held for: {held_for:.1f}s")
                        print(f"  Price at appearance: ${price_at_appearance:,.0f}")
                        print(f"  Price now: ${current_price:,.0f}")
                        print(f"  Moved: ${abs(current_price - price_at_appearance):.0f}")
                        print(f"  SPOOF {spoof_direction} confirmed")
                        print(f"  → IGNORE ALL Polymarket signals for next 60s")
                        print(f"{'='*55}")

                        confirmed_spoofs.append({
                            "direction": spoof_direction,
                            "confirmed_at": now
                        })

                # clean up old spoofs
                confirmed_spoofs[:] = [s for s in confirmed_spoofs
                                       if now - s["confirmed_at"] < 60]

                previous_bids = current_bids
                previous_asks = current_asks

            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(0.1)

async def main():
    print("=== BTC Spoof Detector ===")
    print(f"Tracking orders: {MIN_ORDER_SIZE}-{MAX_ORDER_SIZE} BTC")
    print(f"Within ${PRICE_PROXIMITY} of current price")
    print(f"Spoof = held {MIN_HOLD_TIME}-{MAX_HOLD_TIME}s + price bounces ${BOUNCE_MIN_MOVE}+")
    print("=" * 40)

    await asyncio.gather(
        track_prices(),
        check_orderbook()
    )

if __name__ == "__main__":
    asyncio.run(main())