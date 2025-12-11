# run_pipeline.py
import sys
import os
import locale
from extract import fetch_all_cities
from transform import transform_all
from load import load_csv_to_supabase
from etl_analysis import run_analysis  # KPI & visualization functions

# Fix Unicode issue for Windows consoles
if os.name == "nt":
    # Set UTF-8 output for Windows terminal
    sys.stdout.reconfigure(encoding='utf-8')

def run_pipeline():
    print("🚀 Starting Urban Air Quality ETL Pipeline\n")

    # 1️⃣ Extract
    print("1️⃣ Extracting data from Open-Meteo API ...")
    try:
        extracted_files = fetch_all_cities()
        success_count = sum(1 for f in extracted_files if f.get("success") == "true")
        print(f"✅ Extraction complete. {success_count} files saved.\n")
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        sys.exit(1)

    # 2️⃣ Transform
    print("2️⃣ Transforming data ...")
    try:
        # Pass only successful raw file paths
        raw_files = [f["raw_path"] for f in extracted_files if f.get("success") == "true"]
        if not raw_files:
            raise ValueError("No successful extracted files to transform.")
        staged_csv = transform_all(raw_files)
        print(f"✅ Transformation complete. Staged CSV: {staged_csv}\n")
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        sys.exit(1)

    # 3️⃣ Load
    print("3️⃣ Loading data into Supabase ...")
    try:
        load_csv_to_supabase(staged_csv)
        print("✅ Loading complete.\n")
    except Exception as e:
        print(f"❌ Loading failed: {e}")
        sys.exit(1)

    # 4️⃣ Analysis
    print("4️⃣ Running ETL Analysis ...")
    try:
        run_analysis()
        print("✅ Analysis complete.\n")
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        sys.exit(1)

    print("🎯 ETL Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
