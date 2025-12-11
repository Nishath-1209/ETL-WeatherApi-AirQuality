# transform.py
import json
from pathlib import Path
from datetime import datetime
from typing import List
import pandas as pd
import os

BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
STAGED_DIR = BASE_DIR / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

# AQI categories based on PM2.5
def classify_aqi(pm2_5: float) -> str:
    if pm2_5 <= 50:
        return "Good"
    elif pm2_5 <= 100:
        return "Moderate"
    elif pm2_5 <= 200:
        return "Unhealthy"
    elif pm2_5 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

# Severity score calculation
def compute_severity(row: pd.Series) -> float:
    return (
        row.get("pm2_5", 0) * 5 +
        row.get("pm10", 0) * 3 +
        row.get("nitrogen_dioxide", 0) * 4 +
        row.get("sulphur_dioxide", 0) * 4 +
        row.get("carbon_monoxide", 0) * 2 +
        row.get("ozone", 0) * 3
    )

# Risk classification
def risk_classification(severity: float) -> str:
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"
def flatten_city_json(json_path: str) -> pd.DataFrame:
    """Flatten Open-Meteo city JSON into DataFrame with one row per hour"""
    with open(json_path, "r") as f:
        payload = json.load(f)

    # If payload is a list, take the first element
    if isinstance(payload, list) and payload:
        payload = payload[0]
    elif isinstance(payload, list) and not payload:
        # empty list
        print(f"⚠️ {json_path} is empty list, skipping.")
        return pd.DataFrame()  # return empty DF

    # Determine city name from payload or filename
    city_name = payload.get("city") or Path(json_path).stem.split("_")[0].capitalize()

    hourly = payload.get("hourly", {})
    if not hourly:
        print(f"⚠️ {city_name} has no 'hourly' data, skipping.")
        return pd.DataFrame()

    # Columns of interest
    cols = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
            "sulphur_dioxide", "ozone", "uv_index"]

    # Determine max length
    max_len = max(len(hourly.get(c, [])) for c in cols + ["time"])

    # Pad shorter arrays
    data = {}
    for c in ["time"] + cols:
        arr = hourly.get(c, [])
        if len(arr) < max_len:
            arr.extend([None] * (max_len - len(arr)))
        data[c] = arr

    # Convert to DataFrame
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["city"] = city_name
    df["hour"] = df["time"].dt.hour

    # --- Feature engineering ---
    df["AQI_category"] = df["pm2_5"].apply(classify_aqi)
    df["severity_score"] = df.apply(compute_severity, axis=1)
    df["risk"] = df["severity_score"].apply(risk_classification)

    # Drop rows where all pollutants are missing
    df = df.dropna(subset=cols, how="all")

    return df

def transform_all(raw_json_paths: List[str]) -> str:
    """Transform all raw JSON files into a single staged CSV"""
    dfs = []
    for path in raw_json_paths:
        print(f"🔁 Transforming {path} ...")
        df = flatten_city_json(path)
        dfs.append(df)

    if not dfs:
        raise ValueError("No raw JSON files found for transformation")

    final_df = pd.concat(dfs, ignore_index=True)

    staged_path = STAGED_DIR / f"air_quality_transformed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    final_df.to_csv(staged_path, index=False)
    print(f"✅ Transformed AQI data saved to: {staged_path}")
    return str(staged_path)

if __name__ == "__main__":
    raw_files = sorted([str(p) for p in RAW_DIR.glob("*_raw_*.json")])
    if not raw_files:
        print("No raw files found. Run extract.py first.")
    else:
        transform_all(raw_files)
