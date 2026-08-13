"""
Bulk ingest DVF files across multiple years and departments.
Calls download_dvf.py and upload_to_minio.py unchanged, via subprocess,
so extending scope is just editing YEARS/DEPARTMENTS below, not the
ingestion scripts themselves.
"""

import subprocess
import sys
import time

YEARS = ["2023", "2024"]
DEPARTMENTS = ["75", "92", "93", "94"]

start = time.time()

for year in YEARS:
    for dept in DEPARTMENTS:
        print(f"\n=== {year} / {dept} ===")
        subprocess.run(
            [sys.executable, "src/ingestion/download_dvf.py", "--year", year, "--dept", dept],
            check=True,
        )
        subprocess.run(
            [sys.executable, "src/ingestion/upload_to_minio.py", "--year", year, "--dept", dept],
            check=True,
        )

elapsed = time.time() - start
print(f"\nDone. Total time: {elapsed:.1f}s")