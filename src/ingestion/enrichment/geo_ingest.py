"""geo.api.gouv.fr administrative-division ingestion — Bronze layer.

Fetches the department record, its parent region, and the communes within the
department, and stores them together as one Bronze JSON snapshot under the
`geo/` prefix.

Known join-key mismatch: this API's commune codes are the canonical INSEE
codes. For Paris, Lyon, and Marseille, DVF/BAN/DPE instead use per-
arrondissement fiscal codes (e.g. 75101-75120 for Paris), which do not exist
as communes in this API -- querying codeDepartement=75 here returns exactly
one commune, "75056" (Paris as a whole). Joining Silver DVF rows to this
reference data requires mapping the fiscal arrondissement code back to its
parent INSEE commune (any 751xx/691xx/132xx code -> 75056/69123/13055) rather
than a direct code match. This is a Silver-layer join concern, not a Bronze
ingestion bug -- documented here since it's discovered at ingestion time.
"""
import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from validation import validate_json  # noqa: E402
from checksum import compute_sha256  # noqa: E402
from upload_to_minio import get_client, get_remote_checksum, BUCKET  # noqa: E402

GEO_API_BASE = "https://geo.api.gouv.fr"
COMMUNE_FIELDS = "nom,code,codeDepartement,codeRegion,codesPostaux,population,epci,centre"
DEPARTEMENT_FIELDS = "nom,code,codeRegion"
REGION_FIELDS = "nom,code"


def geo_local_path(dept: str, extraction_date: Optional[str] = None) -> Path:
    extraction_date = extraction_date or date.today().isoformat()
    return Path(f"data_raw/geo/{extraction_date}/{dept}.json")


def geo_object_key(dept: str, extraction_date: Optional[str] = None) -> str:
    extraction_date = extraction_date or date.today().isoformat()
    return f"geo/extraction_date={extraction_date}/department={dept}/{dept}.json"


def fetch_department(dept: str) -> dict:
    response = requests.get(
        f"{GEO_API_BASE}/departements/{dept}", params={"fields": DEPARTEMENT_FIELDS}, timeout=30
    )
    response.raise_for_status()
    return response.json()


def fetch_region(region_code: str) -> dict:
    response = requests.get(
        f"{GEO_API_BASE}/regions/{region_code}", params={"fields": REGION_FIELDS}, timeout=30
    )
    response.raise_for_status()
    return response.json()


def fetch_communes(dept: str) -> list:
    response = requests.get(
        f"{GEO_API_BASE}/communes",
        params={"codeDepartement": dept, "fields": COMMUNE_FIELDS, "format": "json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def build_snapshot(dept: str) -> dict:
    department = fetch_department(dept)
    region = fetch_region(department["codeRegion"])
    communes = fetch_communes(dept)
    return {"department": department, "region": region, "communes": communes}


def save_snapshot(snapshot: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, sort_keys=True, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dept", required=True)
    args = parser.parse_args()

    print(f"Fetching administrative divisions for department {args.dept}...")
    snapshot = build_snapshot(args.dept)
    result_path = geo_local_path(args.dept)
    save_snapshot(snapshot, result_path)
    print(f"Saved {len(snapshot['communes'])} commune(s) to {result_path}")

    is_valid, reason = validate_json(result_path)
    if not is_valid:
        print(f"ERROR: {reason}", file=sys.stderr)
        sys.exit(1)

    checksum = compute_sha256(result_path)
    key = geo_object_key(args.dept)
    client = get_client()
    remote_checksum = get_remote_checksum(client, BUCKET, key)

    if remote_checksum == checksum:
        print(f"Skipped: {BUCKET}/{key} unchanged (sha256={checksum})")
    else:
        client.upload_file(str(result_path), BUCKET, key, ExtraArgs={"Metadata": {"sha256": checksum}})
        if remote_checksum is None:
            print(f"Uploaded: {BUCKET}/{key} (sha256={checksum})")
        else:
            print(f"Changed: {BUCKET}/{key} checksum differed (old={remote_checksum}, new={checksum}) — re-uploaded")


if __name__ == "__main__":
    main()