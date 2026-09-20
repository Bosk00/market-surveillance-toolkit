"""
weather_trader.py

Polymarket daily temperature trading bot.

Discovery:
  - Scans ALL active "highest-temperature-in-*" events for today via the
    Gamma /events endpoint (paginated). No hardcoded city list needed.
  - Cities with a known station mapping are traded automatically.
  - Unknown cities are logged to unknown_cities.txt so you can add them.

Weather sources (in priority order):
  1. Visual Crossing Timeline API  — current temp + today's forecast high
                                     + feels-like, humidity, wind, cloud cover
  2. METAR (NOAA)                  — current temp fallback

Signal logic:
  - Tracks a self-observed rolling high from actual current readings.
  - VC forecast high used as a ceiling check: if VC still expects warmer,
    we wait rather than locking in too early.
  - Signal only fires after SIGNAL_HOUR_LOCAL (default 14:00 local).
  - Requires STABLE_POLLS consecutive readings below the observed high.
  - Only buys if ask is at least MIN_EDGE below fair value (1.0).

Station mapping:
  - KNOWN_CITIES maps each city slug (as it appears in Polymarket event slugs)
    to an ICAO code, Visual Crossing coords, and timezone.
  - Stations are chosen to match Polymarket's Wunderground resolution source.
  - Unknown cities are appended to unknown_cities.txt — check that file daily
    and add new entries to KNOWN_CITIES as you verify the correct station.

Config:
  - Tuning params are plain constants below — edit directly.
  - Only the API key lives in .env: VISUAL_CROSSING_API_KEY=your_key

Python 3.8+
Install: pip install aiohttp python-dotenv
"""

import asyncio
import aiohttp
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple
from dotenv import load_dotenv

load_dotenv()

# ==========================
# CONFIG  — edit these directly
# ==========================

PAPER_TRADE        = True
TRADE_SIZE         = 10.0
MAX_PRICE          = 0.97
MIN_EDGE           = 0.04   # only buy if ask < (1.0 - MIN_EDGE)
POLL_INTERVAL      = 60     # seconds between full poll cycles
STABLE_POLLS       = 20     # consecutive polls below observed high before signal
SIGNAL_HOUR_LOCAL  = 14     # don't fire before this local hour (24h)
STATUS_EVERY       = 5      # print status line every N polls

UNKNOWN_CITIES_LOG = "unknown_cities.txt"

# --- Keep in .env ---
VC_API_KEY = os.getenv("VISUAL_CROSSING_API_KEY", "")

# ==========================
# KNOWN CITIES
# ==========================
# Key   = city slug exactly as it appears in the Polymarket event slug.
#         e.g. "highest-temperature-in-hong-kong-on-..." -> key is "hong-kong"
# icao  = ICAO code for METAR fallback (must serve the same area as the
#         Wunderground station Polymarket uses for resolution)
# coords= "lat,lon" passed to Visual Crossing (match the resolution station)
# tz    = IANA timezone string
#
# To add a city: find the market rules on polymarket.com, note the
# Wunderground station code (e.g. EGLC), look up its coords, add a row.
# ==========================

KNOWN_CITIES: Dict[str, Dict[str, str]] = {

    # ---------- Europe ----------
    "london": {
        "icao": "EGLC", "coords": "51.5053,-0.0553",   # London City Airport — confirmed
        "tz": "Europe/London",
    },
    "paris": {
        "icao": "LFPB", "coords": "48.9694,2.4414",    # Le Bourget — confirmed from market rules page
        "tz": "Europe/Paris",
    },
    "amsterdam": {
        "icao": "EHAM", "coords": "52.3105,4.7683",
        "tz": "Europe/Amsterdam",
    },
    "berlin": {
        "icao": "EDDB", "coords": "52.3667,13.5033",
        "tz": "Europe/Berlin",
    },
    "madrid": {
        "icao": "LEMD", "coords": "40.4719,-3.5626",
        "tz": "Europe/Madrid",
    },
    "rome": {
        "icao": "LIRF", "coords": "41.8003,12.2389",
        "tz": "Europe/Rome",
    },
    "zurich": {
        "icao": "LSZH", "coords": "47.4582,8.5555",
        "tz": "Europe/Zurich",
    },
    "vienna": {
        "icao": "LOWW", "coords": "48.1103,16.5697",
        "tz": "Europe/Vienna",
    },
    "brussels": {
        "icao": "EBBR", "coords": "50.9010,4.4844",
        "tz": "Europe/Brussels",
    },
    "stockholm": {
        "icao": "ESSA", "coords": "59.6519,17.9186",
        "tz": "Europe/Stockholm",
    },
    "oslo": {
        "icao": "ENGM", "coords": "60.1939,11.1004",
        "tz": "Europe/Oslo",
    },
    "copenhagen": {
        "icao": "EKCH", "coords": "55.6180,12.6508",
        "tz": "Europe/Copenhagen",
    },
    "helsinki": {
        "icao": "EFHK", "coords": "60.3172,24.9633",
        "tz": "Europe/Helsinki",
    },
    "warsaw": {
        "icao": "EPWA", "coords": "52.1657,20.9671",
        "tz": "Europe/Warsaw",
    },
    "prague": {
        "icao": "LKPR", "coords": "50.1008,14.2600",
        "tz": "Europe/Prague",
    },
    "budapest": {
        "icao": "LHBP", "coords": "47.4298,19.2611",
        "tz": "Europe/Budapest",
    },
    "athens": {
        "icao": "LGAV", "coords": "37.9364,23.9445",
        "tz": "Europe/Athens",
    },
    "lisbon": {
        "icao": "LPPT", "coords": "38.7813,-9.1359",
        "tz": "Europe/Lisbon",
    },
    "dublin": {
        "icao": "EIDW", "coords": "53.4213,-6.2700",
        "tz": "Europe/Dublin",
    },
    "frankfurt": {
        "icao": "EDDF", "coords": "50.0264,8.5431",
        "tz": "Europe/Berlin",
    },
    "munich": {
        "icao": "EDDM", "coords": "48.3537,11.7750",
        "tz": "Europe/Berlin",
    },
    "milan": {
        "icao": "LIML", "coords": "45.4654,9.2769",
        "tz": "Europe/Rome",
    },
    "barcelona": {
        "icao": "LEBL", "coords": "41.2971,2.0785",
        "tz": "Europe/Madrid",
    },

    # ---------- North America ----------
    "nyc": {
        "icao": "KJFK", "coords": "40.6413,-73.7781",
        "tz": "America/New_York",
    },
    "new-york": {
        "icao": "KJFK", "coords": "40.6413,-73.7781",
        "tz": "America/New_York",
    },
    "toronto": {
        "icao": "CYTZ", "coords": "43.6275,-79.3962",  # Billy Bishop downtown
        "tz": "America/Toronto",
    },
    "chicago": {
        "icao": "KORD", "coords": "41.9742,-87.9073",
        "tz": "America/Chicago",
    },
    "los-angeles": {
        "icao": "KLAX", "coords": "33.9425,-118.4081",
        "tz": "America/Los_Angeles",
    },
    "miami": {
        "icao": "KMIA", "coords": "25.7959,-80.2870",
        "tz": "America/New_York",
    },
    "dallas": {
        "icao": "KDFW", "coords": "32.8998,-97.0403",
        "tz": "America/Chicago",
    },
    "houston": {
        "icao": "KIAH", "coords": "29.9902,-95.3368",
        "tz": "America/Chicago",
    },
    "seattle": {
        "icao": "KSEA", "coords": "47.4502,-122.3088",
        "tz": "America/Los_Angeles",
    },
    "denver": {
        "icao": "KDEN", "coords": "39.8561,-104.6737",
        "tz": "America/Denver",
    },
    "boston": {
        "icao": "KBOS", "coords": "42.3656,-71.0096",
        "tz": "America/New_York",
    },
    "phoenix": {
        "icao": "KPHX", "coords": "33.4373,-112.0078",
        "tz": "America/Phoenix",
    },
    "montreal": {
        "icao": "CYUL", "coords": "45.4706,-73.7408",
        "tz": "America/Toronto",
    },
    "vancouver": {
        "icao": "CYVR", "coords": "49.1967,-123.1815",
        "tz": "America/Vancouver",
    },
    "mexico-city": {
        "icao": "MMMX", "coords": "19.4363,-99.0721",
        "tz": "America/Mexico_City",
    },

    # ---------- Asia ----------
    "hong-kong": {
        "icao": "VHHH", "coords": "22.3080,113.9185",
        "tz": "Asia/Hong_Kong",
    },
    "shanghai": {
        "icao": "ZSPD", "coords": "31.1443,121.8083",
        "tz": "Asia/Shanghai",
    },
    "beijing": {
        "icao": "ZBAA", "coords": "40.0801,116.5846",
        "tz": "Asia/Shanghai",
    },
    "tokyo": {
        "icao": "RJTT", "coords": "35.5533,139.7811",
        "tz": "Asia/Tokyo",
    },
    "singapore": {
        "icao": "WSSS", "coords": "1.3644,103.9915",
        "tz": "Asia/Singapore",
    },
    "dubai": {
        "icao": "OMDB", "coords": "25.2528,55.3644",
        "tz": "Asia/Dubai",
    },
    "seoul": {
        "icao": "RKSI", "coords": "37.4602,126.4407",
        "tz": "Asia/Seoul",
    },
    "bangkok": {
        "icao": "VTBS", "coords": "13.6811,100.7470",
        "tz": "Asia/Bangkok",
    },
    "mumbai": {
        "icao": "VABB", "coords": "19.0896,72.8656",
        "tz": "Asia/Kolkata",
    },
    "delhi": {
        "icao": "VIDP", "coords": "28.5665,77.1031",
        "tz": "Asia/Kolkata",
    },
    "taipei": {
        "icao": "RCTP", "coords": "25.0777,121.2328",
        "tz": "Asia/Taipei",
    },
    "kuala-lumpur": {
        "icao": "WMKK", "coords": "2.7456,101.7072",
        "tz": "Asia/Kuala_Lumpur",
    },
    "jakarta": {
        "icao": "WIII", "coords": "-6.1275,106.6537",
        "tz": "Asia/Jakarta",
    },

    # ---------- Oceania ----------
    "sydney": {
        "icao": "YSSY", "coords": "-33.9461,151.1772",
        "tz": "Australia/Sydney",
    },
    "melbourne": {
        "icao": "YMML", "coords": "-37.6690,144.8410",
        "tz": "Australia/Melbourne",
    },
    "brisbane": {
        "icao": "YBBN", "coords": "-27.3842,153.1175",
        "tz": "Australia/Brisbane",
    },
    "auckland": {
        "icao": "NZAA", "coords": "-37.0082,174.7917",
        "tz": "Pacific/Auckland",
    },

    # ---------- Middle East / Africa ----------
    "tel-aviv": {
        "icao": "LLBG", "coords": "32.0114,34.8867",
        "tz": "Asia/Jerusalem",
    },
    "cairo": {
        "icao": "HECA", "coords": "30.1219,31.4056",
        "tz": "Africa/Cairo",
    },
    "johannesburg": {
        "icao": "FAOR", "coords": "-26.1367,28.2411",
        "tz": "Africa/Johannesburg",
    },
    "nairobi": {
        "icao": "HKJK", "coords": "-1.3192,36.9275",
        "tz": "Africa/Nairobi",
    },
    "riyadh": {
        "icao": "OERK", "coords": "24.9576,46.6988",
        "tz": "Asia/Riyadh",
    },

    # ---------- South America ----------
    "sao-paulo": {
        "icao": "SBGR", "coords": "-23.4356,-46.4731",
        "tz": "America/Sao_Paulo",
    },
    "buenos-aires": {
        "icao": "SAEZ", "coords": "-34.8222,-58.5358",
        "tz": "America/Argentina/Buenos_Aires",
    },
    "bogota": {
        "icao": "SKBO", "coords": "4.7016,-74.1469",
        "tz": "America/Bogota",
    },
    "lima": {
        "icao": "SPIM", "coords": "-12.0219,-77.1143",
        "tz": "America/Lima",
    },
}

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log_unknown_city(city_slug: str, event_slug: str, station: str = "") -> None:
    try:
        with open(UNKNOWN_CITIES_LOG, "a", encoding="utf-8") as f:
            station_str = f"  station={station}" if station else "  station=UNKNOWN (check market rules)"
            f.write(
                f"{datetime.now().date()}  {city_slug:<30}  "
                f"{event_slug}{station_str}\n"
            )
    except Exception:
        pass


# ICAO coords lookup for auto-resolving scraped stations
ICAO_COORDS: Dict[str, str] = {
    "EGLC": "51.5053,-0.0553",  "LFPB": "48.9694,2.4414",
    "LFPG": "49.0097,2.5479",   "EHAM": "52.3105,4.7683",
    "EDDB": "52.3667,13.5033",  "LEMD": "40.4719,-3.5626",
    "LIRF": "41.8003,12.2389",  "LSZH": "47.4582,8.5555",
    "LOWW": "48.1103,16.5697",  "EBBR": "50.9010,4.4844",
    "ESSA": "59.6519,17.9186",  "ENGM": "60.1939,11.1004",
    "EKCH": "55.6180,12.6508",  "EFHK": "60.3172,24.9633",
    "EPWA": "52.1657,20.9671",  "LKPR": "50.1008,14.2600",
    "LHBP": "47.4298,19.2611",  "LGAV": "37.9364,23.9445",
    "LPPT": "38.7813,-9.1359",  "EIDW": "53.4213,-6.2700",
    "EDDF": "50.0264,8.5431",   "EDDM": "48.3537,11.7750",
    "LIML": "45.4654,9.2769",   "LEBL": "41.2971,2.0785",
    "KJFK": "40.6413,-73.7781", "CYTZ": "43.6275,-79.3962",
    "KORD": "41.9742,-87.9073", "KLAX": "33.9425,-118.4081",
    "KMIA": "25.7959,-80.2870", "KDFW": "32.8998,-97.0403",
    "KIAH": "29.9902,-95.3368", "KSEA": "47.4502,-122.3088",
    "KDEN": "39.8561,-104.6737","KBOS": "42.3656,-71.0096",
    "KPHX": "33.4373,-112.0078","CYUL": "45.4706,-73.7408",
    "CYVR": "49.1967,-123.1815","MMMX": "19.4363,-99.0721",
    "VHHH": "22.3080,113.9185", "ZSPD": "31.1443,121.8083",
    "ZBAA": "40.0801,116.5846", "RJTT": "35.5533,139.7811",
    "WSSS": "1.3644,103.9915",  "OMDB": "25.2528,55.3644",
    "RKSI": "37.4602,126.4407", "VTBS": "13.6811,100.7470",
    "VABB": "19.0896,72.8656",  "VIDP": "28.5665,77.1031",
    "RCTP": "25.0777,121.2328", "WMKK": "2.7456,101.7072",
    "WIII": "-6.1275,106.6537", "YSSY": "-33.9461,151.1772",
    "YMML": "-37.6690,144.8410","YBBN": "-27.3842,153.1175",
    "NZAA": "-37.0082,174.7917","LLBG": "32.0114,34.8867",
    "HECA": "30.1219,31.4056",  "FAOR": "-26.1367,28.2411",
    "HKJK": "-1.3192,36.9275",  "OERK": "24.9576,46.6988",
    "SBGR": "-23.4356,-46.4731","SAEZ": "-34.8222,-58.5358",
    "SKBO": "4.7016,-74.1469",  "SPIM": "-12.0219,-77.1143",
}

# Regex to pull ICAO from a Wunderground history URL
# e.g. https://www.wunderground.com/history/daily/fr/paris/LFPB
_WU_ICAO_RE = re.compile(
    r"wunderground\.com/history/daily/[^/]+/[^/]+/([A-Z0-9]{4})"
)


async def scrape_resolution_station(
    session: aiohttp.ClientSession,
    event: Dict[str, Any],
) -> str:
    """
    Reads resolutionSource / description fields from the event's child markets
    and extracts the ICAO station code from any embedded Wunderground URL.
    Returns the 4-letter ICAO code (e.g. 'LFPB') or empty string if not found.
    """
    texts = []
    for m in event.get("markets", []):
        for field in ("resolutionSource", "description"):
            val = m.get(field) or ""
            if val:
                texts.append(val)
    ev_desc = event.get("description") or ""
    if ev_desc:
        texts.append(ev_desc)

    for text in texts:
        hit = _WU_ICAO_RE.search(text)
        if hit:
            return hit.group(1)
    return ""


# ==========================
# VISUAL CROSSING
# ==========================

async def fetch_vc(
    session: aiohttp.ClientSession,
    coords: str,
) -> Optional[Dict[str, Any]]:
    """
    Visual Crossing Timeline API — metric units.
    Returns a dict with:
      current_c   current observed temperature (°C)
      vc_high_c   today's forecast high (°C)
      feels_like  feels-like current temp (°C)
      humidity    relative humidity (%)
      conditions  short text e.g. "Partly cloudy"
      wind_speed  km/h
      cloud_cover %
    Returns None if the key is missing or the request fails.
    """
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
        cur  = data.get("currentConditions", {})
        days = data.get("days", [{}])
        d0   = days[0] if days else {}
        temp    = cur.get("temp")
        tempmax = d0.get("tempmax")
        if temp is None or tempmax is None:
            return None
        return {
            "current_c":   float(temp),
            "vc_high_c":   float(tempmax),
            "feels_like":  float(cur.get("feelslike", temp)),
            "humidity":    float(cur.get("humidity", 0)),
            "conditions":  str(cur.get("conditions", "")),
            "wind_speed":  float(cur.get("windspeed", 0)),
            "cloud_cover": float(cur.get("cloudcover", 0)),
        }
    except Exception:
        return None


# ==========================
# METAR FALLBACK
# ==========================

async def fetch_metar_temp_c(
    session: aiohttp.ClientSession,
    icao: str,
) -> Optional[float]:
    url = f"https://tgftp.nws.noaa.gov/data/observations/metar/stations/{icao}.TXT"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            text = await r.text()
    except Exception:
        return None
    lines = text.strip().splitlines()
    if not lines:
        return None
    for part in lines[-1].split():
        if "/" in part and len(part) >= 3:
            try:
                raw = part.split("/")[0]
                return -float(raw[1:]) if raw.startswith("M") else float(raw)
            except Exception:
                continue
    return None


# ==========================
# DISCOVERY
# ==========================

def _make_date_str(d: date) -> str:
    try:
        return d.strftime("%B-%-d-%Y").lower()   # Linux/macOS
    except ValueError:
        return d.strftime("%B-%#d-%Y").lower()   # Windows


def _guess_tz(icao: str) -> str:
    """
    Best-guess timezone from ICAO prefix. Used only for auto-discovered cities.
    Add the city to KNOWN_CITIES with the correct tz once you verify the station.
    """
    prefix_map = {
        "EG": "Europe/London",   "EI": "Europe/Dublin",
        "LF": "Europe/Paris",    "ED": "Europe/Berlin",
        "LE": "Europe/Madrid",   "LI": "Europe/Rome",
        "LS": "Europe/Zurich",   "LO": "Europe/Vienna",
        "EB": "Europe/Brussels", "ES": "Europe/Stockholm",
        "EN": "Europe/Oslo",     "EK": "Europe/Copenhagen",
        "EF": "Europe/Helsinki", "EP": "Europe/Warsaw",
        "LK": "Europe/Prague",   "LH": "Europe/Budapest",
        "LG": "Europe/Athens",   "LP": "Europe/Lisbon",
        "KJ": "America/New_York","KB": "America/New_York",
        "KO": "America/Chicago", "KL": "America/Los_Angeles",
        "KP": "America/Phoenix", "KS": "America/Los_Angeles",
        "KD": "America/Denver",  "KI": "America/Chicago",
        "KM": "America/New_York","CY": "America/Toronto",
        "MM": "America/Mexico_City",
        "VH": "Asia/Hong_Kong",  "ZS": "Asia/Shanghai",
        "ZB": "Asia/Shanghai",   "RJ": "Asia/Tokyo",
        "WS": "Asia/Singapore",  "OM": "Asia/Dubai",
        "RK": "Asia/Seoul",      "VT": "Asia/Bangkok",
        "VA": "Asia/Kolkata",    "VI": "Asia/Kolkata",
        "RC": "Asia/Taipei",     "WM": "Asia/Kuala_Lumpur",
        "WI": "Asia/Jakarta",    "YS": "Australia/Sydney",
        "YM": "Australia/Melbourne","YB": "Australia/Brisbane",
        "NZ": "Pacific/Auckland",
        "LL": "Asia/Jerusalem",  "HE": "Africa/Cairo",
        "FA": "Africa/Johannesburg","HK": "Africa/Nairobi",
        "OE": "Asia/Riyadh",     "SB": "America/Sao_Paulo",
        "SA": "America/Argentina/Buenos_Aires",
        "SK": "America/Bogota",  "SP": "America/Lima",
    }
    return prefix_map.get(icao[:2], "UTC")


async def discover_events(
    session: aiohttp.ClientSession,
) -> List[Dict[str, Any]]:
    """
    Paginates through ALL active Gamma events and finds every
    'highest-temperature-in-*-on-<today>' event.

    Splits results into:
      tradeable — city slug is in KNOWN_CITIES, returned for trading
      unknown   — city slug is new, appended to unknown_cities.txt
    """
    today    = datetime.now().date()
    date_str = _make_date_str(today)
    suffix   = f"-on-{date_str}"

    tradeable: List[Dict[str, Any]] = []
    unknown_seen: set = set()
    limit, offset = 100, 0

    print(f"[{ts()}] 🔍 Scanning all active events (today = {date_str})...")

    while True:
        try:
            async with session.get(
                f"{GAMMA_API}/events",
                params={
                    "active":    "true",
                    "closed":    "false",
                    "limit":     str(limit),
                    "offset":    str(offset),
                    "order":     "id",
                    "ascending": "false",
                },
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                if r.status != 200:
                    print(f"[{ts()}] ⚠️  /events returned HTTP {r.status}")
                    break
                batch = await r.json()
        except Exception as e:
            print(f"[{ts()}] ⚠️  Discovery request failed: {e}")
            break

        if not batch:
            break

        for ev in batch:
            slug = ev.get("slug", "")
            if not (slug.startswith("highest-temperature-in-") and slug.endswith(suffix)):
                continue

            prefix    = "highest-temperature-in-"
            city_slug = slug[len(prefix):-len(suffix)]
            markets   = ev.get("markets", [])
            if not markets:
                continue

            if city_slug in KNOWN_CITIES:
                # Cross-check scraped station against our config, warn on mismatch
                scraped = await scrape_resolution_station(session, ev)
                cfg = KNOWN_CITIES[city_slug]
                if scraped and scraped != cfg["icao"]:
                    print(f"[{ts()}] WARNING {city_slug}: configured={cfg['icao']} "
                          f"but market resolves against {scraped} -- update KNOWN_CITIES!")
                tradeable.append({
                    "city_slug": city_slug,
                    "slug":      slug,
                    "markets":   markets,
                    "cfg":       cfg,
                    "station":   scraped or cfg["icao"],
                })
            elif city_slug not in unknown_seen:
                unknown_seen.add(city_slug)
                scraped = await scrape_resolution_station(session, ev)
                log_unknown_city(city_slug, slug, scraped)
                # Auto-trade if we scraped a station we have coords for
                if scraped and scraped in ICAO_COORDS:
                    tz_guess = _guess_tz(scraped)
                    auto_cfg = {"icao": scraped, "coords": ICAO_COORDS[scraped], "tz": tz_guess}
                    print(f"[{ts()}] AUTO {city_slug} -> station {scraped} (added for this session)")
                    tradeable.append({
                        "city_slug": city_slug,
                        "slug":      slug,
                        "markets":   markets,
                        "cfg":       auto_cfg,
                        "station":   scraped,
                        "auto":      True,
                    })

        if len(batch) < limit:
            break
        offset += limit

    print(f"[{ts()}] Tradeable: {len(tradeable)}  |  Unknown logged: {len(unknown_seen)}")
    for ev in tradeable:
        auto_tag = " [auto]" if ev.get("auto") else ""
        print(f"[{ts()}]   {ev['city_slug']:<22} | station:{ev.get('station','?'):<6} | "
              f"{len(ev['markets'])} buckets{auto_tag}")
    if unknown_seen:
        print(f"[{ts()}] Unknown cities appended to {UNKNOWN_CITIES_LOG}:")
        for cs in sorted(unknown_seen):
            print(f"[{ts()}]   {cs}")

    return tradeable


# ==========================
# BUCKET PARSING
# ==========================

_RANGE_RE = re.compile(r"^(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)$")


def parse_buckets(markets: List[Dict]) -> List[Dict[str, Any]]:
    buckets = []
    for m in markets:
        if not m.get("enableOrderBook"):
            continue
        title = (m.get("groupItemTitle") or "").strip()
        try:
            raw_ids   = m.get("clobTokenIds", "[]")
            token_ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
            if not token_ids:
                continue
            yes_token = token_ids[0]
        except Exception:
            continue

        unit = "C" if "°C" in title else ("F" if "°F" in title else None)
        if not unit:
            continue
        s = title.replace("°C", "").replace("°F", "").strip().lower()

        if "or below" in s:
            try:
                val = float(s.replace("or below", "").strip())
                buckets.append({"label": title, "token_id": yes_token, "unit": unit,
                                "low": float("-inf"), "high": val})
            except Exception:
                pass
        elif "or higher" in s:
            try:
                val = float(s.replace("or higher", "").strip())
                buckets.append({"label": title, "token_id": yes_token, "unit": unit,
                                "low": val, "high": float("inf")})
            except Exception:
                pass
        else:
            rm = _RANGE_RE.match(s)
            if rm:
                buckets.append({"label": title, "token_id": yes_token, "unit": unit,
                                "low": float(rm.group(1)), "high": float(rm.group(2))})
            else:
                try:
                    val = float(s)
                    buckets.append({"label": title, "token_id": yes_token, "unit": unit,
                                    "low": val, "high": val})
                except Exception:
                    pass
    return buckets


def sanity_check_buckets(buckets: List[Dict]) -> Tuple[bool, str]:
    if not buckets:
        return False, "No tradeable buckets"
    units = {b["unit"] for b in buckets}
    if len(units) != 1:
        return False, f"Mixed units: {units}"
    for b in buckets:
        if b["low"] > b["high"]:
            return False, f"low > high in '{b['label']}'"
    sorted_b = sorted(
        (b for b in buckets if b["low"] != float("-inf")),
        key=lambda x: x["low"],
    )
    last_high = None
    for b in sorted_b:
        if last_high is not None and b["low"] <= last_high:
            return False, f"Overlap at '{b['label']}'"
        last_high = b["high"]
    return True, "OK"


# ==========================
# ORDERBOOK
# ==========================

async def fetch_best_ask(
    session: aiohttp.ClientSession,
    token_id: str,
) -> Optional[float]:
    try:
        async with session.get(
            f"{CLOB_API}/book",
            params={"token_id": token_id},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return None
            ob = await r.json()
    except Exception:
        return None
    prices = []
    for a in ob.get("asks", []):
        try:
            if float(a.get("size", 0)) > 0:
                prices.append(float(a["price"]))
        except Exception:
            pass
    return min(prices) if prices else None


# ==========================
# ORDER PLACEMENT
# ==========================

def place_order(token_id: str, price: float, size: float, label: str) -> bool:
    if PAPER_TRADE:
        shares = size / price
        print(f"  📝 PAPER BUY YES '{label}' | ${size:.2f} @ {price:.3f} "
              f"({shares:.2f} shares)")
        return True
    try:
        result = subprocess.run(
            ["node", "place_order.js", token_id, str(price), str(max(size, 5.0))],
            capture_output=True, text=True, encoding="utf-8", timeout=20,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        stdout    = result.stdout.strip().split("\n")
        json_line = next((l for l in stdout if l.startswith("{")), None)
        output    = json.loads(json_line) if json_line else {"success": False}
        if output.get("success") and not output.get("response", {}).get("error"):
            print(f"  ✅ LIVE BUY YES '{label}'")
            return True
        print(f"  ❌ Order failed: {output.get('response', {}).get('error')}")
        return False
    except Exception as e:
        print(f"  ❌ Order exception: {e}")
        return False


# ==========================
# TRADING LOOP
# ==========================

async def trade_one_day(
    session: aiohttp.ClientSession,
    events: List[Dict],
) -> None:

    # Build state and pre-validate buckets
    state: Dict[str, Dict] = {}
    for ev in events:
        cs      = ev["city_slug"]
        buckets = parse_buckets(ev["markets"])
        ok, msg = sanity_check_buckets(buckets)
        if ok:
            print(f"[{ts()}] ✅ {cs}: {len(buckets)} valid buckets")
        else:
            print(f"[{ts()}] ❌ {cs}: bucket parse failed — {msg}")
            for m in ev["markets"]:
                print(f"       title={m.get('groupItemTitle')!r}  "
                      f"ids={m.get('clobTokenIds')!r}")
        state[cs] = {
            "observed_high_c": None,
            "vc_high_c":       None,
            "stable_polls":    0,
            "traded":          False,
            "buckets":         buckets if ok else [],
            "buckets_ok":      ok,
        }

    poll_count = 0
    end_of_day = (datetime.now() + timedelta(days=1)).replace(
        hour=0, minute=5, second=0, microsecond=0
    )

    while datetime.now() < end_of_day:
        poll_count += 1

        for ev in events:
            cs  = ev["city_slug"]
            cfg = ev["cfg"]
            st  = state[cs]

            local_now  = datetime.now(ZoneInfo(cfg["tz"]))
            local_hour = local_now.hour

            if local_hour < 8 or local_hour > 21:
                continue

            # --- Fetch weather ---
            vc       = await fetch_vc(session, cfg["coords"])
            vc_extra = ""
            if vc is not None:
                current_c      = vc["current_c"]
                st["vc_high_c"] = vc["vc_high_c"]
                source         = "VC"
                vc_extra = (
                    f"FL:{vc['feels_like']:.0f}°  "
                    f"Hum:{vc['humidity']:.0f}%  "
                    f"Cld:{vc['cloud_cover']:.0f}%  "
                    f"Wnd:{vc['wind_speed']:.0f}km/h  "
                    f"\"{vc['conditions']}\""
                )
            else:
                current_c = await fetch_metar_temp_c(session, cfg["icao"])
                source    = "METAR"
                vc_extra  = "(VC unavailable)"

            if current_c is None:
                print(f"[{ts()}] ⚠️  {cs}: all sources failed")
                continue

            # Rolling observed high — only ever increases; resets stable counter
            if st["observed_high_c"] is None or current_c > st["observed_high_c"]:
                st["observed_high_c"] = current_c
                st["stable_polls"]    = 0
            elif current_c < st["observed_high_c"]:
                st["stable_polls"] += 1

            if poll_count % STATUS_EVERY == 0:
                vc_hi = f"{st['vc_high_c']:5.1f}" if st["vc_high_c"] else "  N/A"
                print(
                    f"[{ts()}] {cs:<22} | {source:<5} | "
                    f"Now:{current_c:5.1f}°C  "
                    f"ObsHi:{st['observed_high_c']:5.1f}°C  "
                    f"VCHi:{vc_hi}°C  "
                    f"Stbl:{st['stable_polls']:3d}/{STABLE_POLLS}  "
                    + vc_extra
                )

            if st["traded"] or not st["buckets_ok"]:
                continue

            # --- Signal gates ---
            if local_hour < SIGNAL_HOUR_LOCAL:
                continue

            # VC ceiling check: if forecast still expects >1°C more, wait
            if st["vc_high_c"] is not None:
                if st["vc_high_c"] > st["observed_high_c"] + 1.0:
                    if poll_count % STATUS_EVERY == 0:
                        print(f"  ⏳ {cs}: VC forecasts {st['vc_high_c']:.1f} "
                              f"> observed {st['observed_high_c']:.1f} — waiting")
                    continue

            if st["stable_polls"] < STABLE_POLLS:
                continue

            # --- Match bucket ---
            high    = st["observed_high_c"]
            matches = [b for b in st["buckets"] if b["low"] <= high <= b["high"]]

            print("\n" + "=" * 70)
            print(f"[{ts()}] 🌡️  SIGNAL: {cs}  "
                  f"ObsHigh={high:.1f}°C  VCHigh={st['vc_high_c']}°C")
            if vc_extra:
                print(f"  Conditions: {vc_extra}")

            if len(matches) != 1:
                reason = "no match" if not matches else "multiple matches"
                print(f"  ❌ {reason} for {high:.1f}°C — aborting")
                for b in st["buckets"]:
                    print(f"       {b['label']:<20} [{b['low']} – {b['high']}]")
                st["traded"] = True
                print("=" * 70 + "\n")
                continue

            bucket = matches[0]
            print(f"  Bucket: {bucket['label']}")

            ask = await fetch_best_ask(session, bucket["token_id"])
            if ask is None:
                print("  ❌ No asks in orderbook")
                print("=" * 70 + "\n")
                continue

            edge = 1.0 - ask
            print(f"  Ask: {ask:.3f}  Edge: {edge:.3f}  (need ≥ {MIN_EDGE:.2f})")

            if edge < MIN_EDGE:
                print("  ❌ Insufficient edge")
                st["traded"] = True
                print("=" * 70 + "\n")
                continue

            if ask > MAX_PRICE:
                print(f"  ❌ Ask {ask:.3f} > MAX_PRICE {MAX_PRICE}")
                st["traded"] = True
                print("=" * 70 + "\n")
                continue

            if place_order(bucket["token_id"], ask, TRADE_SIZE, bucket["label"]):
                st["traded"] = True
            print("=" * 70 + "\n")

        await asyncio.sleep(POLL_INTERVAL)

    print(f"[{ts()}] 🔄 End of day — done.")


# ==========================
# MAIN
# ==========================

async def main():
    print("=== Weather Bot ===")
    print(f"Mode:           {'PAPER' if PAPER_TRADE else 'LIVE'}")
    print(f"Known cities:   {len(KNOWN_CITIES)}")
    print(f"Signal after:   {SIGNAL_HOUR_LOCAL}:00 local  "
          f"StablePolls: {STABLE_POLLS}  Edge: {MIN_EDGE}")
    print("=" * 50)

    async with aiohttp.ClientSession() as session:
        while True:
            print(f"\n[{ts()}] 🗓️  New day starting...")
            events = await discover_events(session)

            if not events:
                print(f"[{ts()}] ⚠️  No tradeable events. Retrying in 30 min.")
                await asyncio.sleep(1800)
                continue

            await trade_one_day(session, events)


if __name__ == "__main__":
    asyncio.run(main())