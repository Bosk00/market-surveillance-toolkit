import asyncio
import aiohttp
import json
import os
import re
import subprocess
import statistics
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

LIVE_TRADING        = False   # live orders vs SIM
DRY_RUN             = False   # if True: no orders at all, just scoring/logging

TARGET_WALLET       = ""
SELF_WALLET         = os.getenv("PROXY_WALLET")

DATA_API            = "https://data-api.polymarket.com"
GAMMA_API           = "https://gamma-api.polymarket.com"
CLOB_API            = "https://clob.polymarket.com"

MIN_PRICE           = 0.049
MAX_PRICE           = 0.07    # tightened from 0.10 — whale rarely goes above 7c

MAX_DAYS_AHEAD      = 7
SWEET_SPOT_DTR_MIN  = 1.0     # don't buy within 12h of resolution
SWEET_SPOT_DTR_MAX  = 5.0
MAX_BUCKET_WIDTH    = 1.0     # single-degree buckets only (whale avg_width ~0.19)
MIN_SCORE           = 2

TRADE_USDC          = 10.0
DAILY_SPEND_CAP     = 200.0   # max USDC to spend per calendar day

POLL_INTERVAL_HOURS = 6       # how often the loop runs
NEW_MARKET_AGE_HOURS = 48     # markets created within this window get fast-lane treatment

CUTOFF_DAYS         = 21

# Files
HISTORY_FILE           = "wallet_history.json"
FORECAST_CACHE_FILE    = "vc_forecast_cache.json"
OPEN_POSITIONS_FILE    = "weather_positions.json"
RESOLVED_POSITIONS_FILE = "weather_resolved_positions.json"
WHALE_CACHE_FILE       = "weather_whale_cache.json"
SPEND_LOG_FILE            = "daily_spend.json"
LOG_FILE                  = "weather_bot.log"
TRADES_LOG_FILE           = "weather_trades.log"
LAST_SEEN_MARKET_FILE     = "weather_last_seen_market.json"

MAX_LOG_MB             = 5

VC_API_KEY = os.getenv("VISUAL_CROSSING_API_KEY", "")

KNOWN_CITIES: Dict[str, Dict[str, str]] = {
    "hong kong":    {"coords": "22.3080,113.9185",  "tz": "Asia/Hong_Kong"},
    "toronto":      {"coords": "43.6275,-79.3962",  "tz": "America/Toronto"},
    "sao paulo":    {"coords": "-23.4356,-46.4731", "tz": "America/Sao_Paulo"},
    "munich":       {"coords": "48.3537,11.7750",   "tz": "Europe/Berlin"},
    "wellington":   {"coords": "-41.3272,174.8053", "tz": "Pacific/Auckland"},
    "shenzhen":     {"coords": "22.6393,113.8107",  "tz": "Asia/Shanghai"},
    "chengdu":      {"coords": "30.5785,103.9471",  "tz": "Asia/Shanghai"},
    "seattle":      {"coords": "47.4502,-122.3088", "tz": "America/Los_Angeles"},
    "los angeles":  {"coords": "33.9425,-118.4081", "tz": "America/Los_Angeles"},
    "lagos":        {"coords": "6.5774,3.3211",     "tz": "Africa/Lagos"},
    "moscow":       {"coords": "55.9726,37.4146",   "tz": "Europe/Moscow"},
    "kuala lumpur": {"coords": "2.7456,101.7072",   "tz": "Asia/Kuala_Lumpur"},
    "tokyo":        {"coords": "35.6762,139.6503",  "tz": "Asia/Tokyo"},
    "london":       {"coords": "51.5074,-0.1278",   "tz": "Europe/London"},
    "paris":        {"coords": "48.8566,2.3522",    "tz": "Europe/Paris"},
    "beijing":      {"coords": "39.9042,116.4074",  "tz": "Asia/Shanghai"},
    "shanghai":     {"coords": "31.2304,121.4737",  "tz": "Asia/Shanghai"},
    "singapore":    {"coords": "1.3521,103.8198",   "tz": "Asia/Singapore"},
    "miami":        {"coords": "25.7617,-80.1918",  "tz": "America/New_York"},
    "chicago":      {"coords": "41.8781,-87.6298",  "tz": "America/Chicago"},
    "new york city":{"coords": "40.7128,-74.0060",  "tz": "America/New_York"},
    "atlanta":      {"coords": "33.7490,-84.3880",  "tz": "America/New_York"},
    "dallas":       {"coords": "32.7767,-96.7970",  "tz": "America/Chicago"},
    "denver":       {"coords": "39.7392,-104.9903", "tz": "America/Denver"},
    "austin":       {"coords": "30.2672,-97.7431",  "tz": "America/Chicago"},
    "seoul":        {"coords": "37.5665,126.9780",  "tz": "Asia/Seoul"},
    "madrid":       {"coords": "40.4168,-3.7038",   "tz": "Europe/Madrid"},
    "warsaw":       {"coords": "52.2297,21.0122",   "tz": "Europe/Warsaw"},
    "ankara":       {"coords": "39.9334,32.8597",   "tz": "Europe/Istanbul"},
    "buenos aires": {"coords": "-34.6037,-58.3816", "tz": "America/Argentina/Buenos_Aires"},
    "cape town":    {"coords": "-33.9249,18.4241",  "tz": "Africa/Johannesburg"},
    "lucknow":      {"coords": "26.8467,80.9462",   "tz": "Asia/Kolkata"},
    "wuhan":        {"coords": "30.5928,114.3055",  "tz": "Asia/Shanghai"},
    "chongqing":    {"coords": "29.4316,106.9123",  "tz": "Asia/Shanghai"},
}


# ============================================================
# LOGGING
# ============================================================

def log(msg: str):
    """Write timestamped message to both console and log file."""
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass


def rotate_log_if_needed():
    if not os.path.exists(LOG_FILE):
        return
    size_mb = os.path.getsize(LOG_FILE) / (1024 * 1024)
    if size_mb >= MAX_LOG_MB:
        archive = LOG_FILE.replace(".log", f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        os.rename(LOG_FILE, archive)
        log(f"Log rotated → {archive}")


def log_trade(action: str, event: str, bucket: str, token_id: str,
              price: float, size_usdc: float, shares: float,
              lane: str = "", pnl: float = None, status: str = ""):
    """
    Append one line to weather_trades.log AND print to terminal.
    Every BUY and every RESOLVED position gets a dedicated line here.

    Example lines:
    2025-04-19 14:53:24 | BUY      | WHALE_COPY | Highest temp Hong Kong Apr 20 | 26°C | price=0.049 | $10.00 | 204.08 shares
    2025-04-19 20:11:02 | RESOLVED |            | Highest temp Hong Kong Apr 20 | 26°C | price=0.049 | $10.00 | 204.08 shares | pnl=+194.08 | WIN
    """
    ts_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pnl_str = f" | pnl={pnl:+.2f}" if pnl is not None else ""
    win_str = ""
    if status == "RESOLVED":
        win_str = " | WIN" if (pnl or 0) > 0 else " | LOSS"

    line = (
        f"{ts_str} | {action:<8} | {lane:<10} | "
        f"{event} | {bucket} | "
        f"price={price:.3f} | ${size_usdc:.2f} | {shares:.2f} shares"
        f"{pnl_str}{win_str}"
    )
    print(line)
    try:
        with open(TRADES_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass


# ============================================================
# FILE HELPERS
# ============================================================

def load_json_file(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default


def save_json_file(path: str, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except:
        pass


def load_open_positions() -> List[Dict[str, Any]]:
    return load_json_file(OPEN_POSITIONS_FILE, [])


def save_open_positions(positions: List[Dict[str, Any]]):
    save_json_file(OPEN_POSITIONS_FILE, positions)


def load_resolved_positions() -> List[Dict[str, Any]]:
    return load_json_file(RESOLVED_POSITIONS_FILE, [])


def save_resolved_positions(positions: List[Dict[str, Any]]):
    save_json_file(RESOLVED_POSITIONS_FILE, positions)


def load_whale_cache() -> Dict[str, Any]:
    default = {"buckets": [], "events": [], "last_timestamp": 0}
    return load_json_file(WHALE_CACHE_FILE, default)


def save_whale_cache(cache: Dict[str, Any]):
    save_json_file(WHALE_CACHE_FILE, cache)


def load_forecast_cache() -> Dict[str, float]:
    return load_json_file(FORECAST_CACHE_FILE, {})


def save_forecast_cache(cache: Dict[str, float]):
    save_json_file(FORECAST_CACHE_FILE, cache)


# ============================================================
# DAILY SPEND TRACKING
# ============================================================

def get_today_spend() -> float:
    log_data = load_json_file(SPEND_LOG_FILE, {})
    today = datetime.now(timezone.utc).date().isoformat()
    return log_data.get(today, 0.0)


def record_spend(amount: float):
    log_data = load_json_file(SPEND_LOG_FILE, {})
    today = datetime.now(timezone.utc).date().isoformat()
    log_data[today] = log_data.get(today, 0.0) + amount
    save_json_file(SPEND_LOG_FILE, log_data)


# ============================================================
# STATUS DASHBOARD
# ============================================================

def print_status_dashboard():
    log("=" * 55)
    log(f"  POLYBOT STATUS")
    log("=" * 55)

    whale = load_whale_cache()
    last_ts = whale.get("last_timestamp", 0)
    last_seen = (
        datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M")
        if last_ts else "never"
    )
    log(f"  WHALE  | events={len(whale.get('events', []))} "
        f"buckets={len(whale.get('buckets', []))} last_trade={last_seen}")

    open_pos = load_open_positions()
    total_cost = sum(p.get("size_usdc", 0) for p in open_pos)
    log(f"  OPEN   | count={len(open_pos)}  cost_basis=${total_cost:.2f}")

    resolved = load_resolved_positions()
    total_pnl = sum(p.get("pnl", 0) for p in resolved)
    winners = [p for p in resolved if p.get("pnl", 0) > 0]
    losers  = [p for p in resolved if p.get("pnl", 0) <= 0]
    win_rate = (len(winners) / len(resolved) * 100) if resolved else 0
    log(f"  RESOLVED | count={len(resolved)}  W={len(winners)} L={len(losers)}  "
        f"win%={win_rate:.1f}  total_pnl=${total_pnl:+.2f}")

    daily_spend = get_today_spend()
    log(f"  SPEND  | today=${daily_spend:.2f} / cap=${DAILY_SPEND_CAP:.2f}")

    fc = load_forecast_cache()
    log(f"  FORECAST | {len(fc)} cities cached")
    log("=" * 55)


# ============================================================
# NORMALIZATION HELPERS
# ============================================================

def normalize_city(city: str) -> str:
    return re.sub(r"\s+", " ", city.strip().lower())


def normalize_date_str(date_str: str) -> str:
    return re.sub(r"\s+", " ", date_str.strip().lower())


def normalize_bucket_label(label: str) -> str:
    t = label.lower().strip()
    t = t.replace("°c", "c").replace("°f", "f")
    t = re.sub(r"\s+", " ", t)
    t = t.replace(" or below", "_or_below")
    t = t.replace(" or higher", "_or_higher")
    t = t.replace(" - ", "-").replace(" – ", "-")
    t = t.replace(" ", "")
    return t


# ============================================================
# TRADE NORMALIZATION
# ============================================================

def normalize_trade(t):
    title = (
        t.get("title")
        or t.get("market", {}).get("question")
        or t.get("market", {}).get("title")
        or ""
    )
    raw_side = (t.get("side") or "").upper()
    if "BUY" in raw_side:
        side = "BUY"
    elif "SELL" in raw_side:
        side = "SELL"
    else:
        side = raw_side

    outcome = (t.get("outcome") or "").upper()
    price = (
        t.get("price")
        or t.get("avgPrice")
        or (t.get("fill") or {}).get("price")
        or ((t.get("fills") or [{}])[0] or {}).get("price")
    )
    try:
        price = float(price)
    except:
        price = None

    usdc = t.get("usdcSize") or t.get("size") or 0
    return {
        "title": title,
        "side": side,
        "outcome": outcome,
        "price": price,
        "usdc": float(usdc),
        "timestamp": t.get("timestamp"),
    }


# ============================================================
# WEATHER DETECTION + PARSING
# ============================================================

def is_weather_event(ev) -> bool:
    slug  = (ev.get("slug") or "").lower()
    title = (ev.get("title") or ev.get("question") or "").lower()
    slug_match  = "highest-temperature" in slug or "highest-temp" in slug
    title_match = (
        ("temperature" in title or "°c" in title or "°f" in title)
        and " in " in title
        and " on " in title
    )
    return slug_match or title_match


def is_weather_title(title: str) -> bool:
    t = title.lower()
    if not ("temperature" in t or "°c" in t or "°f" in t):
        return False
    if " in " not in t:
        return False
    if " on " not in t:
        return False
    return True


def extract_city(title: str) -> str:
    t = title.lower()
    m = re.search(
        r"in ([a-zA-Z\u00C0-\u017F\s]+?)(?: on | be | reach | between |,|\?|$)", t
    )
    if m:
        return m.group(1).strip()
    return ""


def extract_city_and_date(title: str):
    t = title.lower()
    m = re.search(r"in ([a-zA-Z\u00C0-\u017F\s]+?) be .* on ([^?]+)\?", t)
    if not m:
        m2 = re.search(r"in ([a-zA-Z\u00C0-\u017F\s]+?) on ([^?]+)\?", t)
        if not m2:
            return None, None
        return m2.group(1).strip(), m2.group(2).strip()
    return m.group(1).strip(), m.group(2).strip()


def extract_bucket_width(title: str) -> float:
    t = title.lower()
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)", t)
    if m:
        return abs(float(m.group(2)) - float(m.group(1)))
    return 0.0


def extract_bucket_label_from_title(title: str) -> str:
    t = title.lower()
    m = re.search(r"be (.+?) on ", t)
    if m:
        return m.group(1).strip()
    return ""


def get_event_created_ts(ev) -> Optional[float]:
    """Parse createdAt field into a UTC unix timestamp. Returns None if missing/unparseable."""
    created = ev.get("createdAt") or ev.get("created_at")
    if not created:
        return None
    try:
        if created.endswith("Z"):
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(created)
        return dt.timestamp()
    except:
        return None


def load_last_seen_market_ts() -> float:
    """Load the newest market createdAt timestamp we have ever processed."""
    data = load_json_file(LAST_SEEN_MARKET_FILE, {"ts": 0.0})
    return float(data.get("ts", 0.0))


def save_last_seen_market_ts(ts_val: float):
    save_json_file(LAST_SEEN_MARKET_FILE, {"ts": ts_val})


def is_newly_created(ev, max_age_hours: float = NEW_MARKET_AGE_HOURS) -> bool:
    """Fallback age-based check — used by the full cycle lane logic."""
    created_ts = get_event_created_ts(ev)
    if created_ts is None:
        return False
    age_hours = (datetime.now(timezone.utc).timestamp() - created_ts) / 3600
    return age_hours <= max_age_hours


# ============================================================
# EVENT DATE SCORING
# ============================================================

def event_days_to_res(ev):
    end = ev.get("endDate") or ev.get("endDateISO")
    if not end:
        return None
    try:
        if end.endswith("Z"):
            dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(end)
    except:
        return None
    now = datetime.now(timezone.utc)
    return (dt - now).total_seconds() / 86400.0


def is_today_event(ev) -> bool:
    title = ev.get("title", "") or ev.get("question", "")
    today = datetime.now().date()
    month_name = today.strftime("%B")
    day_str = str(today.day)
    return (month_name.lower() in title.lower()) and (day_str in title)


# ============================================================
# BUCKET QUALITY FILTERS
# ============================================================

def bucket_numeric_range(label: str) -> Optional[Tuple[float, float]]:
    t = label.lower().replace("°c", "").replace("°f", "").strip()
    if "or below" in t:
        try:
            v = float(t.replace("or below", "").strip())
            return float("-inf"), v
        except:
            return None
    if "or higher" in t:
        try:
            v = float(t.replace("or higher", "").strip())
            return v, float("inf")
        except:
            return None
    m = re.match(r"(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)", t)
    if m:
        return float(m.group(1)), float(m.group(2))
    try:
        v = float(t)
        return v, v
    except:
        return None


def bucket_is_plausible(label: str, bucket_prices: Dict[str, float],
                         threshold_ratio: float = 6.0) -> bool:
    """
    Returns False if this bucket is more than threshold_ratio times cheaper
    than the most expensive bucket in the same event — likely worthless.
    """
    if not bucket_prices or label not in bucket_prices:
        return True
    max_price = max(bucket_prices.values())
    this_price = bucket_prices.get(label, 0)
    if max_price <= 0 or this_price <= 0:
        return True
    return (max_price / this_price) < threshold_ratio


def already_owns(token_id: str, open_positions: List[Dict[str, Any]]) -> bool:
    return any(p["token_id"] == token_id for p in open_positions)


# ============================================================
# WALLET TRADES FETCH
# ============================================================

async def fetch_wallet_trades_since(session, wallet, since_ts: float):
    url = f"{DATA_API}/activity"
    trades = []
    offset = 0
    stop = False

    while not stop:
        params = {"user": wallet, "limit": 100, "offset": offset}
        async with session.get(url, params=params) as resp:
            batch = await resp.json()

        if not isinstance(batch, list):
            break
        if any(not isinstance(x, dict) for x in batch):
            break

        for d in batch:
            if d.get("type") != "TRADE":
                continue
            nt = normalize_trade(d)
            ts_val = nt.get("timestamp")
            if ts_val is None:
                continue
            if ts_val <= since_ts:
                stop = True
                break
            trades.append(nt)

        if len(batch) < 100:
            break
        offset += 100

    log(f"Fetched {len(trades)} new trades since {since_ts}")
    return trades


async def fetch_wallet_trades_initial(session, wallet):
    url = f"{DATA_API}/activity"
    trades = []
    offset = 0
    cutoff_ts = datetime.now(timezone.utc).timestamp() - CUTOFF_DAYS * 86400
    stop = False

    while not stop:
        params = {"user": wallet, "limit": 100, "offset": offset}
        async with session.get(url, params=params) as resp:
            batch = await resp.json()

        if not isinstance(batch, list):
            break
        if any(not isinstance(x, dict) for x in batch):
            break

        for d in batch:
            if d.get("type") != "TRADE":
                continue
            nt = normalize_trade(d)
            ts_val = nt.get("timestamp")
            if ts_val is not None and ts_val < cutoff_ts:
                stop = True
                break
            trades.append(nt)

        if len(batch) < 100:
            break
        offset += 100

    log(f"Initial backfill trades (last ~{CUTOFF_DAYS} days): {len(trades)}")
    return trades


# ============================================================
# WHALE CACHE BUILD / UPDATE
# ============================================================

def update_whale_cache_with_trades(cache: Dict[str, Any],
                                    trades: List[Dict[str, Any]]):
    buckets   = cache.get("buckets", [])
    events    = cache.get("events", [])
    last_ts   = cache.get("last_timestamp", 0)
    bucket_set = {tuple(b) for b in buckets}
    event_set  = {tuple(e) for e in events}

    for t in trades:
        title = t["title"]
        if not is_weather_title(title):
            continue
        city, date_str = extract_city_and_date(title)
        if not city or not date_str:
            continue

        city_norm   = normalize_city(city)
        date_norm   = normalize_date_str(date_str)
        bucket_label = extract_bucket_label_from_title(title)
        if not bucket_label:
            continue
        bucket_norm = normalize_bucket_label(bucket_label)

        key_event  = (city_norm, date_norm)
        key_bucket = (city_norm, date_norm, bucket_norm)

        if key_event not in event_set:
            events.append(list(key_event))
            event_set.add(key_event)
        if key_bucket not in bucket_set:
            buckets.append(list(key_bucket))
            bucket_set.add(key_bucket)
        if t["timestamp"] and t["timestamp"] > last_ts:
            last_ts = t["timestamp"]

    cache["buckets"]        = buckets
    cache["events"]         = events
    cache["last_timestamp"] = last_ts
    return cache


def build_pattern_from_whale_cache(cache: Dict[str, Any]):
    city_counts = {}
    widths = []

    for city_norm, date_norm, bucket_norm in cache.get("buckets", []):
        city_counts[city_norm] = city_counts.get(city_norm, 0) + 1
        m = re.match(r"(-?\d+(?:\.\d+)?)[cf]_-?(\d+(?:\.\d+)?)[cf]", bucket_norm)
        if m:
            try:
                widths.append(abs(float(m.group(2)) - float(m.group(1))))
            except:
                pass
        else:
            m2 = re.match(r"(-?\d+(?:\.\d+)?)[cf]-(-?\d+(?:\.\d+)?)[cf]", bucket_norm)
            if m2:
                try:
                    widths.append(abs(float(m2.group(2)) - float(m2.group(1))))
                except:
                    pass
            else:
                widths.append(0.0)

    pattern = {
        "city_counts": city_counts,
        "avg_width": sum(widths) / len(widths) if widths else None,
    }
    log(f"Pattern | cities={sorted(pattern['city_counts'].keys())} avg_width={pattern['avg_width']}")
    return pattern


def build_whale_bucket_maps_from_cache(cache: Dict[str, Any]):
    bought_buckets  = set()
    touched_events  = set()

    for city_norm, date_norm, bucket_norm in cache.get("buckets", []):
        bought_buckets.add((city_norm, date_norm, bucket_norm))
        touched_events.add((city_norm, date_norm))

    for city_norm, date_norm in cache.get("events", []):
        touched_events.add((city_norm, date_norm))

    log(f"Whale maps | touched_events={len(touched_events)} bought_buckets={len(bought_buckets)}")
    return bought_buckets, touched_events


# ============================================================
# MARKET SCANNING HELPERS
# ============================================================

async def fetch_events(session):
    events = []
    limit, offset = 100, 0

    while True:
        params = {
            "active":    "true",
            "closed":    "false",
            "limit":     str(limit),
            "offset":    str(offset),
            "order":     "id",
            "ascending": "false",
        }
        async with session.get(f"{GAMMA_API}/events", params=params) as resp:
            batch = await resp.json()
        if not batch:
            break
        events.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    log(f"Fetched active events: {len(events)}")
    return events


def parse_buckets(ev):
    buckets = []
    skipped = 0
    for m in ev.get("markets", []):
        if not m.get("enableOrderBook"):
            skipped += 1
            continue
        title = (m.get("groupItemTitle") or "").strip()
        raw_ids = m.get("clobTokenIds", "[]")
        try:
            token_ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        except:
            continue
        if not token_ids:
            continue
        buckets.append({"label": title, "token_id": token_ids[0]})
    if skipped:
        log(f"  parse_buckets | skipped {skipped} non-orderbook markets in '{ev.get('title', '')}'")
    return buckets


# ============================================================
# VISUAL CROSSING (FORECAST)
# ============================================================

async def fetch_vc_high(session: aiohttp.ClientSession,
                         coords: str) -> Optional[float]:
    if not VC_API_KEY:
        return None
    url = (
        "https://weather.visualcrossing.com/VisualCrossingWebServices/"
        "rest/services/timeline/" + coords
    )
    params = {
        "unitGroup":   "metric",
        "include":     "current,days",
        "key":         VC_API_KEY,
        "contentType": "json",
    }
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            data = await r.json()
    except Exception:
        return None
    try:
        days   = data.get("days", [{}])
        d0     = days[0] if days else {}
        tempmax = d0.get("tempmax")
        if tempmax is None:
            return None
        return float(tempmax)
    except Exception:
        return None


def forecast_alignment_score(forecast_high: float, bucket_label: str) -> float:
    rng = bucket_numeric_range(bucket_label)
    if not rng:
        return 0.0
    low, high = rng
    if low <= forecast_high <= high:
        return 2.0
    if abs(low) == float("inf") or abs(high) == float("inf"):
        center = forecast_high
    else:
        center = (low + high) / 2
    diff = abs(center - forecast_high)
    if diff <= 1.0:
        return 1.5
    if diff <= 2.0:
        return 1.0
    return 0.0


# ============================================================
# ORDERBOOK FETCH (PARALLEL)
# ============================================================

async def fetch_best_ask_parallel(session, token_ids, cache):
    async def fetch_one(tid):
        if tid in cache:
            return tid, cache[tid]
        try:
            async with session.get(
                f"{CLOB_API}/book",
                params={"token_id": tid},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                ob = await r.json()
        except:
            cache[tid] = None
            return tid, None

        prices = []
        for a in ob.get("asks", []):
            try:
                if float(a.get("size", 0)) > 0:
                    prices.append(float(a["price"]))
            except:
                pass
        best = min(prices) if prices else None
        cache[tid] = best
        return tid, best

    results = await asyncio.gather(*[fetch_one(tid) for tid in token_ids])
    return {tid: ask for tid, ask in results}


async def fetch_best_bid_parallel(session, token_ids, cache):
    async def fetch_one(tid):
        if tid in cache:
            return tid, cache[tid]
        try:
            async with session.get(
                f"{CLOB_API}/book",
                params={"token_id": tid},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                ob = await r.json()
        except:
            cache[tid] = None
            return tid, None

        prices = []
        for b in ob.get("bids", []):
            try:
                if float(b.get("size", 0)) > 0:
                    prices.append(float(b["price"]))
            except:
                pass
        best = max(prices) if prices else None
        cache[tid] = best
        return tid, best

    results = await asyncio.gather(*[fetch_one(tid) for tid in token_ids])
    return {tid: bid for tid, bid in results}


async def fetch_book_quality(session, token_id: str):
    """Returns (best_ask, best_bid, is_healthy). Healthy = bid >= 30% of ask."""
    try:
        async with session.get(
            f"{CLOB_API}/book",
            params={"token_id": token_id},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            ob = await r.json()
    except:
        return None, None, False

    asks = sorted(
        [float(a["price"]) for a in ob.get("asks", []) if float(a.get("size", 0)) > 0]
    )
    bids = sorted(
        [float(b["price"]) for b in ob.get("bids", []) if float(b.get("size", 0)) > 0],
        reverse=True,
    )
    best_ask = asks[0] if asks else None
    best_bid = bids[0] if bids else None

    if best_ask is None:
        return None, None, False
    if best_bid is None or best_bid < best_ask * 0.30:
        return best_ask, best_bid, False   # one-sided / dead market
    return best_ask, best_bid, True


# ============================================================
# OPTIMIZED SCAN + LANES
# ============================================================

async def optimized_scan(session, pattern, bought_buckets, touched_events,
                          open_positions):
    log("Optimized scan started...")
    events = await fetch_events(session)

    traded_cities   = set(pattern["city_counts"].keys())
    vc_cache: Dict[str, float] = {}
    forecast_cache  = load_forecast_cache()
    forecast_shifted: Dict[str, bool] = {}
    candidates      = []

    # ---- filter events ----
    filtered_events = []
    for ev in events:
        if not is_weather_event(ev):
            continue
        title = ev.get("title", "") or ev.get("question", "")
        city, date_str = extract_city_and_date(title)
        if not city or not date_str:
            continue

        city_norm = normalize_city(city)
        date_norm = normalize_date_str(date_str)

        dtr = event_days_to_res(ev)
        if dtr is None or dtr < SWEET_SPOT_DTR_MIN or dtr > MAX_DAYS_AHEAD:
            continue

        ev["_city"]     = city_norm
        ev["_date_str"] = date_norm
        ev["_dtr"]      = dtr
        ev["_new"]      = is_newly_created(ev)

        # include known cities AND new markets from any city
        if city_norm not in traded_cities and not ev["_new"]:
            continue

        filtered_events.append(ev)

    log(f"Filtered to {len(filtered_events)} relevant events "
        f"({sum(1 for e in filtered_events if e['_new'])} new markets)")

    # ---- collect token IDs ----
    event_token_ids: Dict[Tuple[str, str], List[str]] = {}
    bucket_map: Dict[str, Any] = {}

    for ev in filtered_events:
        city_norm  = ev["_city"]
        date_norm  = ev["_date_str"]
        dtr        = ev["_dtr"]
        is_new     = ev["_new"]
        key_event  = (city_norm, date_norm)
        is_today   = is_today_event(ev)

        # Fetch VC forecast for today's events in known cities
        if is_today and city_norm in KNOWN_CITIES:
            if city_norm not in vc_cache:
                coords  = KNOWN_CITIES[city_norm]["coords"]
                vc_high = await fetch_vc_high(session, coords)
                vc_cache[city_norm] = vc_high if vc_high is not None else float("nan")
                prev = forecast_cache.get(city_norm)
                if (prev is not None and vc_high is not None
                        and not (vc_high != vc_high)
                        and abs(vc_high - prev) >= 1.0):
                    forecast_shifted[city_norm] = True

        for b in parse_buckets(ev):
            # Tighter width gate — whale buys single-degree buckets
            if extract_bucket_width(b["label"]) > MAX_BUCKET_WIDTH:
                continue

            bucket_norm      = normalize_bucket_label(b["label"])
            key_bucket       = (city_norm, date_norm, bucket_norm)
            touched          = key_bucket in bought_buckets
            future_rep       = (key_event in touched_events) and (dtr > 0)
            forecast_play    = (
                is_today
                and city_norm in vc_cache
                and not (vc_cache.get(city_norm, float("nan")) != vc_cache.get(city_norm, float("nan")))
                and city_norm in KNOWN_CITIES
            )

            # Only queue if there's a reason to consider it
            if not (touched or forecast_play or future_rep or is_new):
                continue

            tid = b["token_id"]
            event_token_ids.setdefault(key_event, []).append(tid)
            bucket_map[tid] = {
                "ev": ev, "b": b,
                "touched": touched,
                "forecast_play": forecast_play,
                "future_rep": future_rep,
                "is_new": is_new,
                "dtr": dtr,
                "city_norm": city_norm,
                "key_event": key_event,
                "bucket_norm": bucket_norm,
            }

    total_tids = sum(len(v) for v in event_token_ids.values())
    log(f"Fetching orderbooks for {total_tids} buckets...")

    all_tids   = [tid for tids in event_token_ids.values() for tid in tids]
    ob_cache   = {}
    best_asks  = await fetch_best_ask_parallel(session, all_tids, ob_cache)

    # Build per-event price maps for plausibility check
    event_price_maps: Dict[Tuple, Dict[str, float]] = {}
    for tid, info in bucket_map.items():
        ask = best_asks.get(tid)
        if ask is not None:
            key_event = info["key_event"]
            event_price_maps.setdefault(key_event, {})[info["b"]["label"]] = ask

    # ---- lane logic ----
    for key_event, tids in event_token_ids.items():
        # Cluster median for whale-copy lane
        prices_for_median = [
            best_asks.get(tid)
            for tid in tids
            if best_asks.get(tid) is not None
            and best_asks[tid] <= MAX_PRICE
            and bucket_map[tid]["touched"]
        ]
        median_price = statistics.median(prices_for_median) if prices_for_median else None
        lower_bound  = median_price * 0.6 if median_price else None
        upper_bound  = median_price * 1.4 if median_price else None

        for tid in tids:
            info = bucket_map[tid]
            ask  = best_asks.get(tid)

            if ask is None:
                continue
            if not (MIN_PRICE <= ask <= MAX_PRICE):
                continue

            # Skip if already owned — print live price vs entry for P&L tracking
            owned = next((p for p in open_positions if p["token_id"] == tid), None)
            if owned:
                entry    = owned["entry_price"]
                shares   = owned["shares"]
                curr_ask = ask  # current market ask = best available price
                unreal   = (curr_ask - entry) * shares
                pct      = ((curr_ask - entry) / entry * 100) if entry > 0 else 0
                log(
                    f"  HOLDING | {info['b']['label']} | {info['ev'].get('title','')} | "
                    f"entry={entry:.3f} now={curr_ask:.3f} "
                    f"unrealised={unreal:+.2f} ({pct:+.1f}%)"
                )
                continue

            ev_title     = info["ev"].get("title", "")
            bucket_label = info["b"]["label"]
            dtr          = info["dtr"]
            city_norm    = info["city_norm"]
            price_map    = event_price_maps.get(key_event, {})

            # Plausibility check — skip if bucket is >6x cheaper than consensus
            if not bucket_is_plausible(bucket_label, price_map):
                log(f"  SKIP (implausible) | {bucket_label} | {ev_title}")
                continue

            # LANE 0: New market fast lane — buy all cheap buckets, no scoring
            if info["is_new"] and dtr <= SWEET_SPOT_DTR_MAX:
                candidates.append({
                    "score": 10,
                    "event": ev_title,
                    "bucket": bucket_label,
                    "token_id": tid,
                    "ask": ask,
                    "lane": "NEW_MARKET",
                })
                continue

            # LANE 1: Whale-copy with cluster filter
            if info["touched"]:
                if median_price is not None:
                    if not (lower_bound <= ask <= upper_bound):
                        continue
                s = 2
                if MIN_PRICE <= ask <= 0.055:
                    s += 3
                elif ask <= 0.07:
                    s += 2
                if SWEET_SPOT_DTR_MIN <= dtr <= SWEET_SPOT_DTR_MAX:
                    s += 1
                if city_norm in pattern.get("city_counts", {}):
                    s += 1
                if s >= MIN_SCORE:
                    candidates.append({
                        "score": s,
                        "event": ev_title,
                        "bucket": bucket_label,
                        "token_id": tid,
                        "ask": ask,
                        "lane": "WHALE_COPY",
                    })
                continue

            # LANE 2: Forecast-aligned
            if info["forecast_play"] and city_norm in vc_cache:
                vc_high = vc_cache[city_norm]
                fa = forecast_alignment_score(vc_high, bucket_label)
                shift_boost = forecast_shifted.get(city_norm, False)
                if (fa >= 1.0 or shift_boost) and ask <= 0.07 and SWEET_SPOT_DTR_MIN <= dtr <= 7:
                    candidates.append({
                        "score": 4 + fa,
                        "event": ev_title,
                        "bucket": bucket_label,
                        "token_id": tid,
                        "ask": ask,
                        "lane": "FORECAST",
                    })
                continue

            # LANE 3: Future replication (event-level)
            if info["future_rep"] and SWEET_SPOT_DTR_MIN <= dtr <= SWEET_SPOT_DTR_MAX and ask <= 0.07:
                candidates.append({
                    "score": 2,
                    "event": ev_title,
                    "bucket": bucket_label,
                    "token_id": tid,
                    "ask": ask,
                    "lane": "FUTURE_REP",
                })

    # Deduplicate: keep cheapest bucket per event per lane
    # (new market lane keeps ALL buckets; other lanes keep cheapest per event)
    new_market_candidates = [c for c in candidates if c["lane"] == "NEW_MARKET"]
    other_candidates      = [c for c in candidates if c["lane"] != "NEW_MARKET"]

    deduped: Dict[str, dict] = {}
    for c in other_candidates:
        k = c["event"]
        if k not in deduped or c["ask"] < deduped[k]["ask"]:
            deduped[k] = c
    final_candidates = new_market_candidates + list(deduped.values())
    final_candidates.sort(key=lambda x: x["score"], reverse=True)

    log(f"Found {len(final_candidates)} candidate buckets "
        f"(new={sum(1 for c in final_candidates if c['lane']=='NEW_MARKET')} "
        f"whale={sum(1 for c in final_candidates if c['lane']=='WHALE_COPY')} "
        f"forecast={sum(1 for c in final_candidates if c['lane']=='FORECAST')} "
        f"future={sum(1 for c in final_candidates if c['lane']=='FUTURE_REP')})")

    # Update forecast cache
    if vc_cache:
        forecast_cache.update(
            {k: v for k, v in vc_cache.items() if v is not None and not (v != v)}
        )
        save_forecast_cache(forecast_cache)

    return final_candidates


# ============================================================
# EXECUTION + POSITION LOGGING
# ============================================================

def place_order(token_id, price, size_usdc, label, event_title,
                open_positions: List[Dict[str, Any]], lane: str = ""):
    shares = size_usdc / price

    if DRY_RUN:
        log(f"DRY RUN | {label} | ${size_usdc:.2f} @ {price:.3f}")
        _append_position(open_positions, token_id, event_title, label, price, size_usdc, shares, lane)
        return True

    if not LIVE_TRADING:
        log(f"SIM BUY | {label} | ${size_usdc:.2f} @ {price:.3f} ({shares:.2f} shares)")
        _append_position(open_positions, token_id, event_title, label, price, size_usdc, shares, lane)
        return True

    try:
        result = subprocess.run(
            ["node", "place_order.js", token_id, str(price), str(max(size_usdc, 5.0))],
            capture_output=True, text=True, encoding="utf-8", timeout=20,
        )
        stdout    = result.stdout.strip().split("\n")
        json_line = next((l for l in stdout if l.startswith("{")), None)
        output    = json.loads(json_line) if json_line else {"success": False}

        if output.get("success") and not output.get("response", {}).get("error"):
            log(f"LIVE BUY OK | {label} | ${size_usdc:.2f} @ {price:.3f}")
            _append_position(open_positions, token_id, event_title, label, price, size_usdc, shares, lane)
            return True

        log(f"LIVE BUY FAILED | {label} | {output}")
        return False

    except Exception as e:
        log(f"ORDER ERROR | {label} | {e}")
        return False


def _append_position(open_positions, token_id, event_title, label, price,
                     size_usdc, shares, lane: str = ""):
    open_positions.append({
        "timestamp":   datetime.now(timezone.utc).timestamp(),
        "event":       event_title,
        "bucket":      label,
        "token_id":    token_id,
        "entry_price": price,
        "size_usdc":   size_usdc,
        "shares":      shares,
        "lane":        lane,
    })
    save_open_positions(open_positions)
    log_trade(
        action="BUY",
        event=event_title,
        bucket=label,
        token_id=token_id,
        price=price,
        size_usdc=size_usdc,
        shares=shares,
        lane=lane,
    )


# ============================================================
# PnL REPORTING
# ============================================================

async def compute_and_print_pnl(session, open_positions: List[Dict[str, Any]]):
    if not open_positions:
        log("PnL | No open positions")
        return open_positions

    token_ids = list({p["token_id"] for p in open_positions})
    bid_cache = {}
    best_bids = await fetch_best_bid_parallel(session, token_ids, bid_cache)

    resolved_positions = load_resolved_positions()
    still_open         = []
    total_open_pnl     = 0.0
    total_open_cost    = 0.0
    today_closed_pnl   = 0.0
    today_date         = datetime.now(timezone.utc).date()

    for p in open_positions:
        tid   = p["token_id"]
        bid   = best_bids.get(tid)
        entry = p["entry_price"]
        shares = p["shares"]
        cost   = entry * shares

        if bid is None:
            pnl    = 0.0
            status = "NO_BID"
            still_open.append(p)
        else:
            pnl = (bid - entry) * shares
            if bid >= 0.99 or bid <= 0.01:
                status = "RESOLVED"
                resolved_entry = dict(p)
                resolved_entry["resolved_price"]     = bid
                resolved_entry["pnl"]                = pnl
                resolved_entry["resolved_timestamp"] = datetime.now(timezone.utc).timestamp()
                resolved_positions.append(resolved_entry)
                log_trade(
                    action="RESOLVED",
                    event=p["event"],
                    bucket=p["bucket"],
                    token_id=p["token_id"],
                    price=p["entry_price"],
                    size_usdc=p["size_usdc"],
                    shares=p["shares"],
                    lane=p.get("lane", ""),
                    pnl=pnl,
                    status="RESOLVED",
                )
                res_date = datetime.fromtimestamp(
                    resolved_entry["resolved_timestamp"], tz=timezone.utc
                ).date()
                if res_date == today_date:
                    today_closed_pnl += pnl
            else:
                status = "OPEN"
                still_open.append(p)
                total_open_pnl  += pnl
                total_open_cost += cost

        log(f"POS | {p['event']} | {p['bucket']} | "
            f"entry={entry:.3f} bid={bid if bid is not None else 'N/A'} "
            f"status={status} pnl={pnl:+.2f}")

    save_resolved_positions(resolved_positions)
    save_open_positions(still_open)

    total_res_pnl = sum(r.get("pnl", 0) for r in resolved_positions)
    log(f"PnL summary | open={len(still_open)} resolved={len(resolved_positions)} "
        f"open_pnl={total_open_pnl:+.2f} cost={total_open_cost:.2f} "
        f"today_closed={today_closed_pnl:+.2f} all_time_resolved={total_res_pnl:+.2f}")

    return still_open


# ============================================================
# SINGLE CYCLE
# ============================================================

async def run_once(session, open_positions):
    log("CYCLE START")
    print_status_dashboard()

    # Update whale cache
    whale_cache = load_whale_cache()
    last_ts     = whale_cache.get("last_timestamp", 0)

    if last_ts == 0:
        log("No whale cache — doing initial backfill...")
        trades = await fetch_wallet_trades_initial(session, TARGET_WALLET)
    else:
        log(f"Updating whale cache from last_timestamp={last_ts}...")
        trades = await fetch_wallet_trades_since(session, TARGET_WALLET, last_ts)

    whale_cache = update_whale_cache_with_trades(whale_cache, trades)
    save_whale_cache(whale_cache)

    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(trades, f, indent=2)

    # Advance the new-market cursor so quick checks don't re-process
    # markets the full cycle already handled
    try:
        events_for_cursor = await fetch_events(session)
        newest_ts = load_last_seen_market_ts()
        for e in events_for_cursor:
            if not is_weather_event(e):
                continue
            created_ts = get_event_created_ts(e)
            if created_ts and created_ts > newest_ts:
                newest_ts = created_ts
        save_last_seen_market_ts(newest_ts)
        log(f"Full cycle: market cursor set to {datetime.fromtimestamp(newest_ts).strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        log(f"Full cycle: could not update market cursor: {e}")

    pattern                    = build_pattern_from_whale_cache(whale_cache)
    bought_buckets, touched_events = build_whale_bucket_maps_from_cache(whale_cache)

    # PnL before new trades
    open_positions = await compute_and_print_pnl(session, open_positions)

    # Scan
    candidates = await optimized_scan(
        session, pattern, bought_buckets, touched_events, open_positions
    )

    log("=== CANDIDATES ===")
    for c in candidates:
        log(f"  [{c['score']}] [{c['lane']}] {c['event']} | {c['bucket']} | ask={c['ask']:.3f}")

    # Execute with daily cap
    log("=== EXECUTION ===")
    for c in candidates:
        if get_today_spend() + TRADE_USDC > DAILY_SPEND_CAP:
            log("Daily spend cap reached — stopping execution")
            break
        placed = place_order(
            c["token_id"], c["ask"], TRADE_USDC,
            c["bucket"], c["event"], open_positions,
            lane=c.get("lane", "")
        )
        if placed:
            record_spend(TRADE_USDC)

    # PnL after new trades
    open_positions = load_open_positions()
    open_positions = await compute_and_print_pnl(session, open_positions)

    log("CYCLE DONE")
    return open_positions


# ============================================================
# QUICK NEW-MARKET CHECK (runs every hour)
# ============================================================

async def quick_new_market_check(session, open_positions):
    """
    Lightweight hourly scan — only looks for markets created AFTER the
    last time we checked. Uses a persisted timestamp so there are zero
    gaps between checks regardless of timing.
    Skips whale cache update and full PnL report to stay fast.
    """
    log("--- QUICK CHECK: scanning for new markets ---")

    last_seen_ts = load_last_seen_market_ts()
    log(f"Quick check: looking for markets newer than ts={last_seen_ts:.0f} "
        f"({datetime.fromtimestamp(last_seen_ts).strftime('%Y-%m-%d %H:%M') if last_seen_ts else 'never'})")

    try:
        events = await fetch_events(session)
    except Exception as e:
        log(f"QUICK CHECK ERROR fetching events: {e}")
        return open_positions

    # Find all weather events created after our last seen timestamp
    new_events = []
    newest_ts  = last_seen_ts
    for e in events:
        if not is_weather_event(e):
            continue
        created_ts = get_event_created_ts(e)
        if created_ts is None:
            continue
        if created_ts > newest_ts:
            newest_ts = created_ts
        if created_ts > last_seen_ts:
            new_events.append(e)

    # Always advance the cursor even if we found nothing to buy
    if newest_ts > last_seen_ts:
        save_last_seen_market_ts(newest_ts)
        log(f"Quick check: cursor advanced to {datetime.fromtimestamp(newest_ts).strftime('%Y-%m-%d %H:%M')}")

    if not new_events:
        log("Quick check: no new markets since last check")
        return open_positions

    log(f"Quick check: {len(new_events)} new market(s) found — entering fast lane")

    # Load pattern from cached whale data (no API call needed)
    whale_cache    = load_whale_cache()
    pattern        = build_pattern_from_whale_cache(whale_cache)
    bought_buckets, touched_events = build_whale_bucket_maps_from_cache(whale_cache)

    # Collect all token IDs from new events
    all_tids = []
    bucket_info = {}
    for ev in new_events:
        title = ev.get("title", "") or ev.get("question", "")
        city, date_str = extract_city_and_date(title)
        if not city or not date_str:
            continue
        dtr = event_days_to_res(ev)
        if dtr is None or dtr < SWEET_SPOT_DTR_MIN or dtr > MAX_DAYS_AHEAD:
            continue
        ev["_city"]     = normalize_city(city)
        ev["_date_str"] = normalize_date_str(date_str)
        ev["_dtr"]      = dtr
        ev["_new"]      = True
        for b in parse_buckets(ev):
            if extract_bucket_width(b["label"]) > MAX_BUCKET_WIDTH:
                continue
            tid = b["token_id"]
            all_tids.append(tid)
            bucket_info[tid] = (ev, b)

    if not all_tids:
        log("Quick check: no eligible buckets in new markets")
        return open_positions

    log(f"Quick check: fetching orderbooks for {len(all_tids)} buckets...")
    ob_cache  = {}
    best_asks = await fetch_best_ask_parallel(session, all_tids, ob_cache)

    candidates = []
    for tid, (ev, b) in bucket_info.items():
        ask = best_asks.get(tid)
        if ask is None or not (MIN_PRICE <= ask <= MAX_PRICE):
            continue
        if already_owns(tid, open_positions):
            log(f"  SKIP (already own) | {b['label']} | {ev.get('title','')}")
            continue
        candidates.append({
            "score":    10,
            "event":    ev.get("title", ""),
            "bucket":   b["label"],
            "token_id": tid,
            "ask":      ask,
            "lane":     "NEW_MARKET",
        })

    candidates.sort(key=lambda x: x["ask"])  # cheapest first within new markets
    log(f"Quick check: {len(candidates)} new-market buckets to buy")

    for c in candidates:
        if get_today_spend() + TRADE_USDC > DAILY_SPEND_CAP:
            log("Daily spend cap reached — stopping quick check execution")
            break
        log(f"  [NEW] {c['event']} | {c['bucket']} | ask={c['ask']:.3f}")
        placed = place_order(
            c["token_id"], c["ask"], TRADE_USDC,
            c["bucket"], c["event"], open_positions,
            lane="NEW_MARKET"
        )
        if placed:
            record_spend(TRADE_USDC)

    log("--- QUICK CHECK DONE ---")
    return open_positions


# ============================================================
# MAIN LOOP
# ============================================================

# How often each cycle type runs:
#   Every 1h  → quick new-market check (fast, minimal API calls)
#   Every 3h  → full cycle (whale update, PnL report, all lanes)
QUICK_CHECK_INTERVAL_HOURS = 1
FULL_CYCLE_EVERY_N          = 3   # full cycle every 3 quick checks


async def run_loop():
    rotate_log_if_needed()
    log(f"BOT STARTED | LIVE_TRADING={LIVE_TRADING} DRY_RUN={DRY_RUN} "
        f"quick_check={QUICK_CHECK_INTERVAL_HOURS}h "
        f"full_cycle=every {FULL_CYCLE_EVERY_N * QUICK_CHECK_INTERVAL_HOURS}h "
        f"cap=${DAILY_SPEND_CAP}")

    open_positions = load_open_positions()
    log(f"Recovered {len(open_positions)} open positions from disk")

    cycle = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if cycle % FULL_CYCLE_EVERY_N == 0:
                    # Full cycle: whale update + all lanes + PnL report
                    log(f"=== FULL CYCLE (#{cycle}) ===")
                    open_positions = await run_once(session, open_positions)
                else:
                    # Lightweight: new markets only
                    log(f"=== QUICK CHECK (#{cycle}) ===")
                    open_positions = await quick_new_market_check(session, open_positions)
            except Exception as e:
                log(f"ERROR in cycle #{cycle}: {e} — will retry next cycle")

            cycle += 1
            log(f"Sleeping {QUICK_CHECK_INTERVAL_HOURS}h until next check...")
            await asyncio.sleep(QUICK_CHECK_INTERVAL_HOURS * 3600)


if __name__ == "__main__":
    asyncio.run(run_loop())