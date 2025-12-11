import pandas as pd
from supabase import create_client, Client
from pathlib import Path
from dotenv import load_dotenv
import os
import math

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"


# -------------------------------------------------------
# 🔹 Clean values so Supabase JSON can accept them
# -------------------------------------------------------
def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    return v


# -------------------------------------------------------
# 🔹 Load CSV into Supabase
# -------------------------------------------------------
def load_csv_to_supabase(csv_path):
    print(f"📥 Loading CSV: {csv_path}")

    df = pd.read_csv(csv_path)

    # STEP 1 — Replace string NaN
    df = df.replace(["nan", "NaN", "None", ""], pd.NA)

    # STEP 2 — Fix column names
    df.rename(columns={
        "AQI_category": "aqi_category",
        "risk": "risk_flag"
    }, inplace=True)

    # STEP 3 — Convert datetime → ISO string
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # STEP 4 — Normalize float columns
    float_cols = [
        "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
        "sulphur_dioxide", "ozone", "uv_index", "severity_score"
    ]

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # STEP 5 — Replace NaN/inf with None
    df = df.astype(object)
    for col in df.columns:
        df[col] = df[col].apply(clean_value)

    print(f"📦 Total records: {len(df)}")

    # STEP 6 — Convert to Python dict list
    records = df.to_dict(orient="records")

    BATCH_SIZE = 200

    # STEP 7 — Batch insert
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]

        # Clean each row again before send
        cleaned_batch = [
            {k: clean_value(v) for k, v in row.items()}
            for row in batch
        ]

        try:
            supabase.table(TABLE_NAME).insert(cleaned_batch).execute()
            print(f"   ✅ Inserted rows {i} → {i + len(batch)}")

        except Exception as e:
            print("❌ Insert Error:", e)
            print("   🔎 Example cleaned row:", cleaned_batch[0])

    print("🎉 Data successfully loaded into Supabase!")


# -------------------------------------------------------
# 🔹 Main — Pick the latest staged CSV
# -------------------------------------------------------
if __name__ == "__main__":
    staged_dir = Path("data/staged")
    csv_files = sorted(staged_dir.glob("air_quality_transformed_*.csv"))

    if not csv_files:
        print("❌ No CSV found in data/staged/")
        exit()

    latest = csv_files[-1]
    load_csv_to_supabase(latest)
