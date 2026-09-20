import asyncio
import aiohttp
import json
from datetime import datetime

WALLET = ""
DATA_API = "https://data-api.polymarket.com"

def ts_to_time(ts):
    return datetime.fromtimestamp(ts).strftime("%m/%d %H:%M:%S")

async def main():
    async with aiohttp.ClientSession() as session:

        # fetch much more history
        url = f"{DATA_API}/activity"
        params = {
            "user": WALLET,
            "limit": 50,
        }

        async with session.get(url, params=params) as resp:
            data = await resp.json()

        trades = [d for d in data if d.get("type") == "TRADE"]
        print(f"Found {len(trades)} trades\n")

        # find all low price high value trades
        print("=== LONGSHOT TRADES (price < 0.49) ===")
        print(f"{'Time':<20} {'Market':<45} {'Side':<6} {'Outcome':<8} {'Price':<8} {'USDC':<10} {'Shares':<10}")
        print("-" * 110)

        longshots = [t for t in trades if t.get("price", 1) < 0.49]
        longshots.sort(key=lambda x: x.get("usdcSize", 0), reverse=True)

        for t in longshots:
            time_str = ts_to_time(t["timestamp"])
            title = t.get("title", "")[:44]
            side = t.get("side", "")
            outcome = t.get("outcome", "")
            price = t.get("price", 0)
            usdc = t.get("usdcSize", 0)
            shares = usdc / price if price > 0 else 0

            print(f"{time_str:<20} {title:<45} {side:<6} {outcome:<8} {price:<8.3f} ${usdc:<10.2f} {shares:<10.0f}")

        print(f"\n=== HIGH VALUE TRADES (usdc > $200) ===")
        print(f"{'Time':<20} {'Market':<45} {'Side':<6} {'Outcome':<8} {'Price':<8} {'USDC':<10}")
        print("-" * 105)

        big_trades = [t for t in trades if t.get("usdcSize", 0) > 200]
        big_trades.sort(key=lambda x: x.get("usdcSize", 0), reverse=True)

        for t in big_trades:
            time_str = ts_to_time(t["timestamp"])
            title = t.get("title", "")[:44]
            side = t.get("side", "")
            outcome = t.get("outcome", "")
            price = t.get("price", 0)
            usdc = t.get("usdcSize", 0)

            print(f"{time_str:<20} {title:<45} {side:<6} {outcome:<8} {price:<8.3f} ${usdc:<10.2f}")

if __name__ == "__main__":
    asyncio.run(main())
