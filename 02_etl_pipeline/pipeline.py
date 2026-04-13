# ============================================================
# FILE: 02_etl_pipeline/pipeline.py
# PURPOSE: Master script — runs full ETL in one command
# ============================================================

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from datetime import datetime
from extract.extractor     import extract_all
from transform.transformer import transform_all
from load.loader           import load_all


def run_pipeline():
    start_time = datetime.now()
    print("=" * 55)
    print("   HR DATA ENGINEERING — ETL PIPELINE")
    print(f"   Started : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    try:
        # Step 1 — Extract
        raw_data = extract_all()

        # Step 2 — Transform
        transformed_data = transform_all(raw_data)

        # Step 3 — Load
        load_all(transformed_data)

        end_time  = datetime.now()
        duration  = (end_time - start_time).seconds

        print("\n" + "=" * 55)
        print("   ✅ PIPELINE COMPLETED SUCCESSFULLY")
        print(f"   Finished : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Duration : {duration} seconds")
        print("=" * 55)

    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()