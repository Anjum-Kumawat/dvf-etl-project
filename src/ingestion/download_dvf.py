"""
Download one raw DVF file from data.gouv.fr.
Scope: department 75 (Paris), year 2024 (pilot scope).
The file is streamed to disk in chunks rather than loaded into memory,
so the same script works on larger departments without saturating RAM.
This script only downloads. Upload to MinIO is handled separately.
"""

import requests
from pathlib import Path


URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/2024/departements/75.csv.gz"
DEST = Path("data_raw/75.csv.gz")

DEST.parent.mkdir(parents=True, exist_ok=True)

print(f"Downloading {URL}")

response = requests.get(URL, stream=True)
response.raise_for_status()

with open(DEST, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

size_mb = DEST.stat().st_size / 1024 / 1024

print(f"Saved to {DEST} ({size_mb:.1f} MB)")