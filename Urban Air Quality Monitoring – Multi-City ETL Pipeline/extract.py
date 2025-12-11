# extract.py
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
RAW_DIR = Path(os.getenv("RAW_DIR", Path(__file__).resolve().parents[0] / "data" / "raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = os.getenv("OPENAQ_API_BASE", "https://air-quality-api.open-meteo.com/v1/air-quality")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", 10))
SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", 0.5))

# Cities with coordinates
CITIES_COORDS = {
    "Delhi": (28.7041, 77.1025),
    "Mumbai": (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Kolkata": (22.5726, 88.3639),
}

HOURLY_PARAMS = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index"


def _now_ts() -> str:
    """UTC compact timestamp for filenames."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _save_raw(payload: object, city: str) -> str:
    """Save JSON payload to RAW_DIR and return path."""
    ts = _now_ts()
    filename = f"{city.replace(' ', '_').lower()}_raw_{ts}.json"
    path = RAW_DIR / filename
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        # fallback to plain text
        path = RAW_DIR / f"{city.replace(' ', '_').lower()}_raw_{ts}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(repr(payload))
    return str(path.resolve())


def _fetch_city(city: str, lat: float, lon: float) -> Dict[str, Optional[str]]:
    """Fetch Open-Meteo AQI data for one city with retries."""
    attempt = 0
    last_error: Optional[str] = None

    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": HOURLY_PARAMS
            }
            resp = requests.get(API_BASE, params=params, timeout=TIMEOUT_SECONDS)
            resp.raise_for_status()
            payload = resp.json()
            saved_path = _save_raw(payload, city)
            print(f"✅ [{city}] fetched and saved to: {saved_path}")
            return {"city": city, "success": "true", "raw_path": saved_path}
        except requests.RequestException as e:
            last_error = str(e)
            print(f"⚠️ [{city}] attempt {attempt}/{MAX_RETRIES} failed: {e}")
        except Exception as e:
            last_error = str(e)
            print(f"⚠️ [{city}] unexpected error: {e}")

        backoff = 2 ** (attempt - 1)
        print(f"⏳ [{city}] retrying in {backoff}s ...")
        time.sleep(backoff)

    print(f"❌ [{city}] failed after {MAX_RETRIES} attempts. Last error: {last_error}")
    return {"city": city, "success": "false", "error": last_error}


def fetch_all_cities() -> List[Dict[str, Optional[str]]]:
    results = []
    for city, (lat, lon) in CITIES_COORDS.items():
        res = _fetch_city(city, lat, lon)
        results.append(res)
        time.sleep(SLEEP_BETWEEN_CALLS)
    return results


if __name__ == "__main__":
    print("Starting extraction for Open-Meteo Air Quality API")
    out = fetch_all_cities()
    print("Extraction complete. Summary:")
    for r in out:
        if r.get("success") == "true":
            print(f" - {r['city']}: saved -> {r['raw_path']}")
        else:
            print(f" - {r['city']}: ERROR -> {r.get('error')}")
