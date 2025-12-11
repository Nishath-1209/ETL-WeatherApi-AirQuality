# etl_analysis.py
"""
ETL Analysis for Urban Air Quality Monitoring

- Fetches air_quality_data from Supabase
- Computes KPIs:
    * City with highest average PM2.5
    * City with highest average severity_score
    * Percentage of High/Moderate/Low risk hours
    * Hour of day with worst (highest avg) PM2.5
- Prepares pollution trend table (time -> pm2_5, pm10, ozone) per city
- Saves CSVs to data/processed/:
    - summary_metrics.csv
    - city_risk_distribution.csv
    - pollution_trends.csv
- Saves PNG plots to data/processed/:
    - pm25_histogram.png
    - risk_bar_by_city.png
    - hourly_pm25_trends.png
    - severity_vs_pm25_scatter.png
"""
from dotenv import load_dotenv
from supabase import create_client
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import os
from typing import Any

load_dotenv()

# Directories
BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Supabase config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "air_quality_data"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in your .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def _extract_data_from_response(res: Any) -> list:
    """Robustly extract list-of-dicts from different supabase response shapes."""
    data = getattr(res, "data", None)
    if isinstance(data, list):
        return data

    # fallback: if res is dict-like with 'data'
    try:
        if isinstance(res, dict) and "data" in res and isinstance(res["data"], list):
            return res["data"]
    except Exception:
        pass

    # if res itself is a list
    if isinstance(res, (list, tuple)):
        return list(res)

    # last try: json()
    json_like = getattr(res, "json", None)
    if callable(json_like):
        try:
            j = res.json()
            if isinstance(j, dict) and "data" in j and isinstance(j["data"], list):
                return j["data"]
        except Exception:
            pass

    return []


def fetch_table(limit: int | None = None) -> pd.DataFrame:
    """Fetch all rows from Supabase table and return cleaned DataFrame."""
    print(f"🔍 Fetching data from Supabase table '{TABLE_NAME}'...")
    query = supabase.table(TABLE_NAME).select("*")
    if limit:
        query = query.limit(limit)
    res = query.execute()

    rows = _extract_data_from_response(res)
    df = pd.DataFrame(rows)

    if df.empty:
        print("⚠️  No rows returned from Supabase.")
        return df

    # normalize column names to consistent lowercase
    df.columns = [c.lower() for c in df.columns]

    # ensure time column is datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # numeric conversions
    numeric_cols = [
        "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
        "sulphur_dioxide", "ozone", "uv_index", "severity_score", "hour"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ensure text columns exist and are strings
    for c in ["city", "risk_flag", "aqi_category"]:
        if c in df.columns:
            df[c] = df[c].astype(object)

    return df


def compute_kpis(df: pd.DataFrame) -> dict:
    """Compute KPIs and return a dictionary for saving/reporting."""
    kpis = {}

    if df.empty:
        return kpis

    # City with highest average PM2.5
    if {"city", "pm2_5"}.issubset(df.columns):
        pm25_by_city = df.groupby("city", as_index=False)["pm2_5"].mean().dropna()
        if not pm25_by_city.empty:
            top = pm25_by_city.sort_values("pm2_5", ascending=False).iloc[0]
            kpis["city_highest_avg_pm2_5"] = {"city": top["city"], "avg_pm2_5": float(top["pm2_5"])}

    # City with highest average severity_score
    if {"city", "severity_score"}.issubset(df.columns):
        sev_by_city = df.groupby("city", as_index=False)["severity_score"].mean().dropna()
        if not sev_by_city.empty:
            top = sev_by_city.sort_values("severity_score", ascending=False).iloc[0]
            kpis["city_highest_severity_score"] = {"city": top["city"], "avg_severity_score": float(top["severity_score"])}

    # Percentage of High/Moderate/Low risk hours
    if "risk_flag" in df.columns:
        total = len(df)
        risk_counts = df["risk_flag"].fillna("Unknown").value_counts()
        risk_pct = {str(k): round((v / total) * 100, 2) for k, v in risk_counts.items()}
        kpis["risk_percentage"] = risk_pct

    # Hour of day with worst AQI (by avg pm2_5)
    if {"time", "pm2_5"}.issubset(df.columns):
        temp = df.dropna(subset=["time", "pm2_5"]).copy()
        if not temp.empty:
            temp["hour_of_day"] = temp["time"].dt.hour
            hour_avg = temp.groupby("hour_of_day", as_index=False)["pm2_5"].mean()
            worst = hour_avg.sort_values("pm2_5", ascending=False).iloc[0]
            kpis["worst_hour_by_avg_pm2_5"] = {"hour": int(worst["hour_of_day"]), "avg_pm2_5": float(worst["pm2_5"])}

    return kpis


def save_summary_metrics(kpis: dict):
    """Save simplified KPI table to CSV."""
    out = []
    if not kpis:
        print("No KPIs to save.")
        return
    if "city_highest_avg_pm2_5" in kpis:
        out.append({
            "metric": "city_highest_avg_pm2_5",
            "value": kpis["city_highest_avg_pm2_5"]["city"],
            "detail": kpis["city_highest_avg_pm2_5"]["avg_pm2_5"]
        })
    if "city_highest_severity_score" in kpis:
        out.append({
            "metric": "city_highest_severity_score",
            "value": kpis["city_highest_severity_score"]["city"],
            "detail": kpis["city_highest_severity_score"]["avg_severity_score"]
        })
    if "worst_hour_by_avg_pm2_5" in kpis:
        out.append({
            "metric": "worst_hour_by_avg_pm2_5",
            "value": kpis["worst_hour_by_avg_pm2_5"]["hour"],
            "detail": kpis["worst_hour_by_avg_pm2_5"]["avg_pm2_5"]
        })
    if "risk_percentage" in kpis:
        for k, v in kpis["risk_percentage"].items():
            out.append({"metric": "risk_percentage", "value": k, "detail": v})

    pd.DataFrame(out).to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)
    print(f"✅ Saved summary_metrics.csv to {PROCESSED_DIR}")


def save_city_risk_distribution(df: pd.DataFrame):
    """Save CSV with counts & percentages of risk_flag by city."""
    if df.empty:
        print("No data for city risk distribution.")
        return
    pivot = df.groupby(["city", "risk_flag"]).size().reset_index(name="count")
    totals = df.groupby("city").size().reset_index(name="total")
    merged = pivot.merge(totals, on="city", how="left")
    merged["percent"] = (merged["count"] / merged["total"] * 100).round(2)
    merged.to_csv(PROCESSED_DIR / "city_risk_distribution.csv", index=False)
    print(f"✅ Saved city_risk_distribution.csv to {PROCESSED_DIR}")


def save_pollution_trends(df: pd.DataFrame):
    """Save time -> pm2_5, pm10, ozone per city (long format)."""
    if df.empty:
        print("No data for pollution trends.")
        return
    cols = ["city", "time", "pm2_5", "pm10", "ozone"]
    trends = df[[c for c in cols if c in df.columns]].copy()
    # keep rows with at least one pollutant value
    trends = trends.dropna(subset=["pm2_5", "pm10", "ozone"], how="all")
    trends.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)
    print(f"✅ Saved pollution_trends.csv to {PROCESSED_DIR}")


def create_plots(df: pd.DataFrame):
    """Create and save requested PNG plots."""
    if df.empty:
        print("No data to plot.")
        return

    # Histogram of PM2.5
    if "pm2_5" in df.columns:
        plt.figure(figsize=(8, 4))
        df["pm2_5"].dropna().plot(kind="hist", bins=30)
        plt.title("PM2.5 Distribution")
        plt.xlabel("PM2.5")
        plt.tight_layout()
        plt.savefig(PROCESSED_DIR / "pm25_histogram.png")
        plt.close()
        print("✅ Saved pm25_histogram.png")

    # Bar chart of risk flags per city
    if {"city", "risk_flag"}.issubset(df.columns):
        agg = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
        plt.figure(figsize=(10, 6))
        agg.plot(kind="bar")
        plt.title("Risk Flags per City")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(PROCESSED_DIR / "risk_bar_by_city.png")
        plt.close()
        print("✅ Saved risk_bar_by_city.png")

    # Line chart of hourly PM2.5 trends (per city)
    if {"time", "pm2_5", "city"}.issubset(df.columns):
        temp = df.dropna(subset=["time", "pm2_5"]).copy()
        temp["date_hour"] = temp["time"].dt.floor("h")  # lowercase 'h' to fix deprecation
        hourly = temp.groupby(["date_hour", "city"], as_index=False)["pm2_5"].mean()
        pivot = hourly.pivot(index="date_hour", columns="city", values="pm2_5").ffill()  # use df.ffill() instead of fillna(method="ffill")
        plt.figure(figsize=(12, 6))
        pivot.plot()
        plt.title("Hourly Average PM2.5 by City")
        plt.ylabel("PM2.5")
        plt.xlabel("Time")
        plt.tight_layout()
        plt.savefig(PROCESSED_DIR / "hourly_pm25_trends.png")
        plt.close()
        print("✅ Saved hourly_pm25_trends.png")


    # Scatter: severity_score vs pm2_5
    if {"severity_score", "pm2_5"}.issubset(df.columns):
        tmp = df.dropna(subset=["severity_score", "pm2_5"])
        plt.figure(figsize=(8, 6))
        plt.scatter(tmp["pm2_5"], tmp["severity_score"], alpha=0.6)
        plt.title("Severity Score vs PM2.5")
        plt.xlabel("PM2.5")
        plt.ylabel("Severity Score")
        plt.tight_layout()
        plt.savefig(PROCESSED_DIR / "severity_vs_pm25_scatter.png")
        plt.close()
        print("✅ Saved severity_vs_pm25_scatter.png")


def run_analysis(limit: int | None = None):
    df = fetch_table(limit=limit)
    if df.empty:
        print("No data available in Supabase for analysis.")
        return

    # compute and save KPIs
    kpis = compute_kpis(df)
    save_summary_metrics(kpis)

    # save city risk distribution
    save_city_risk_distribution(df)

    # save pollution trends
    save_pollution_trends(df)

    # save processed main table (convenience)
    df.to_csv(PROCESSED_DIR / "air_quality_processed_main.csv", index=False)
    print(f"✅ Saved air_quality_processed_main.csv to {PROCESSED_DIR}")

    # create plots
    create_plots(df)


if __name__ == "__main__":
    run_analysis()
