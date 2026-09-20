import asyncio
import aiohttp
import json
import time
from datetime import datetime

WALLET = ""
DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com/markets/slug"

POLL_INTERVAL = 0.5
MIN_COPY_PRICE = 0.85
MAX_TIME_TO_CLOSE = 300

seen_transactions = set()

def ts_to_time(ts):
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")

def seconds_until(end_date_str):
    try:
        end_ts = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        now_ts = datetime.now(end_ts.tzinfo)
        return int((end_ts - now_ts).total_seconds())
    except:
        return None

async def get_market_info(session, slug):
    try:
        async with session.get(f"{GAMMA_API}/{slug}") as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except:
        return None

async def get_latest_trades(session):
    try:
        async with session.get(f"{DATA_API}/activity", params={
            "user": WALLET,
            "limit": 20
        }) as resp:
            data = await resp.json()
            return [d for d in data if d.get("type") == "TRADE"]
    except:
        return []

async def main():
    print("=== Wallet Activity Monitor (live) ===")
    print(f"Polling every {POLL_INTERVAL}s")
    print("=" * 50)

    async with aiohttp.ClientSession() as session:

        print("Seeding known trades...")
        initial_trades = await get_latest_trades(session)
        for t in initial_trades:
            seen_transactions.add(t.get("transactionHash"))
        
        # show what the most recent trade was
        if initial_trades:
            latest = initial_trades[0]
            latest_time = ts_to_time(latest.get("timestamp", 0))
            print(f"Most recent trade in API: {latest_time} | {latest.get('title')} | {latest.get('outcome')} @ {latest.get('price')}")
        
        print(f"Loaded {len(seen_transactions)} existing trades\n")
        print("Watching... (printing every 10s to confirm still running)\n")

        tick = 0
        while True:
            trades = await get_latest_trades(session)
            tick += 1

            # every 10 seconds print status
            if tick % 20 == 0:
                now = datetime.now().strftime("%H:%M:%S")
                latest = trades[0] if trades else None
                latest_time = ts_to_time(latest.get("timestamp", 0)) if latest else "none"
                print(f"[{now}] Still watching | Latest trade in API: {latest_time} | Total seen: {len(seen_transactions)}")

            for trade in trades:
                tx_hash = trade.get("transactionHash")

                if tx_hash in seen_transactions:
                    continue

                seen_transactions.add(tx_hash)

                price = trade.get("price", 0)
                side = trade.get("side", "")
                outcome = trade.get("outcome", "")
                usdc = trade.get("usdcSize", 0)
                title = trade.get("title", "")
                slug = trade.get("slug", "")
                ts = trade.get("timestamp", 0)

                now = datetime.now().strftime("%H:%M:%S")
                print(f"[{now}] NEW TRADE DETECTED: {title} | {outcome} @ {price} | ${usdc:.2f}")

                if side != "BUY" or price < MIN_COPY_PRICE:
                    print(f"  → Skipped (price {price} below threshold or not a buy)")
                    continue

                market = await get_market_info(session, slug)
                if not market:
                    print(f"  → Skipped (couldn't fetch market)")
                    continue

                end_date = market.get("endDate", "")
                secs_left = seconds_until(end_date)
                print(f"  → Market closes in {secs_left}s")

                if secs_left is None or secs_left > MAX_TIME_TO_CLOSE or secs_left < 0:
                    print(f"  → Skipped (time left {secs_left}s outside range)")
                    continue

                print(f"\n{'='*55}")
                print(f"🚨 ACTIVITY ALERT [{ts_to_time(ts)}]")
                print(f"  Market:      {title}")
                print(f"  Outcome:     {outcome}")
                print(f"  Price:       {price*100:.1f}%")
                print(f"  Tracked wallet size: ${usdc:.2f}")
                print(f"  Time left:   {secs_left}s")
                print(f"{'='*55}")

            await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
