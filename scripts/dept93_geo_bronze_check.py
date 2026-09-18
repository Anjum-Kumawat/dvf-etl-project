"""Diagnostic (not part of the pipeline): check whether commune 93059
(Pierrefitte-sur-Seine) is present in (a) the live geo.api.gouv.fr response
and (b) the actual Bronze snapshot already stored in MinIO for department
93 -- to find out whether the gap originates upstream (API/ingestion) or
downstream (Silver join).

Run with: python -m scripts.dept93_geo_bronze_check
"""
import json

import requests

from src.ingestion.upload_to_minio import get_client

BUCKET = "bronze"
KEY = "geo/extraction_date=2026-09-18/department=93/93.json"

print("=" * 70)
print("1. Live geo.api.gouv.fr response for codeDepartement=93")
print("=" * 70)
resp = requests.get(
    "https://geo.api.gouv.fr/communes",
    params={"codeDepartement": "93", "fields": "nom,code,codeDepartement,population,epci,centre", "format": "json"},
    timeout=30,
)
resp.raise_for_status()
live_communes = resp.json()
print(f"Total communes returned live: {len(live_communes)}")
live_match = [c for c in live_communes if c.get("code") == "93059"]
print(f"Commune 93059 present in live response: {bool(live_match)}")
if live_match:
    print(json.dumps(live_match[0], ensure_ascii=False, indent=2))

print("\n" + "=" * 70)
print("2. Stored Bronze snapshot in MinIO")
print("=" * 70)
client = get_client()
obj = client.get_object(Bucket=BUCKET, Key=KEY)
snapshot = json.loads(obj["Body"].read())
bronze_communes = snapshot["communes"]
print(f"Total communes in stored Bronze snapshot: {len(bronze_communes)}")
bronze_match = [c for c in bronze_communes if c.get("code") == "93059"]
print(f"Commune 93059 present in stored Bronze snapshot: {bool(bronze_match)}")
if bronze_match:
    print(json.dumps(bronze_match[0], ensure_ascii=False, indent=2))

live_codes = {c["code"] for c in live_communes}
bronze_codes = {c["code"] for c in bronze_communes}
missing_from_bronze = live_codes - bronze_codes
print(f"\nCodes present live but missing from stored Bronze snapshot: {sorted(missing_from_bronze)}")