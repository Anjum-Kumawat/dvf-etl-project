"""ADEME DPE (Diagnostic de Performance Énergétique) ingestion — Bronze layer.

Paginates through the public data-fair API for the "DPE Logements existants"
dataset, filtered to one department, and stores the raw JSON Lines response in
MinIO Bronze under the `dpe/` prefix.

Unlike DVF (a fixed biannual publication), the DPE dataset is updated weekly and
has no publication version to key off of. Bronze objects are partitioned by
extraction date instead of DVF's publication label — a deliberate, documented
divergence from the DVF/BAN pattern, not an oversight.

Field selection: the source dataset carries ~150 columns per record. Pulling
all of them for a full department (800k+ records for Paris) would produce a
multi-gigabyte Bronze object for a pilot-scope project. A documented subset
covering address/geocode join keys, the DPE/GES labels, and basic building
context is captured instead — an MVP scope decision, consistent with the
project's existing pilot-geography and quality-tooling decisions.
"""
import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Iterator, Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from validation import validate_non_empty  # noqa: E402
from checksum import compute_sha256  # noqa: E402
from upload_to_minio import get_client, get_remote_checksum, BUCKET  # noqa: E402

DPE_API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
PAGE_SIZE = 10000
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 5
SELECT_FIELDS = [
    "numero_dpe",
    "identifiant_ban",
    "adresse_ban",
    "code_insee_ban",
    "code_postal_ban",
    "nom_commune_ban",
    "numero_voie_ban",
    "nom_rue_ban",
    "etiquette_dpe",
    "etiquette_ges",
    "surface_habitable_logement",
    "type_batiment",
    "periode_construction",
    "date_etablissement_dpe",
    "date_reception_dpe",
    "coordonnee_cartographique_x_ban",
    "coordonnee_cartographique_y_ban",
]


def dpe_local_path(dept: str, extraction_date: Optional[str] = None) -> Path:
    extraction_date = extraction_date or date.today().isoformat()
    return Path(f"data_raw/dpe/{extraction_date}/{dept}.jsonl")


def dpe_object_key(dept: str, extraction_date: Optional[str] = None) -> str:
    extraction_date = extraction_date or date.today().isoformat()
    return f"dpe/extraction_date={extraction_date}/department={dept}/{dept}.jsonl"


def _get_with_retry(url: str, params: Optional[dict] = None, timeout: int = 90):
    """GET with retry-and-backoff — a single transient timeout shouldn't kill an
    85-page run."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt == MAX_RETRIES:
                break
            wait = RETRY_BACKOFF_SECONDS * attempt
            print(
                f"  request failed ({exc.__class__.__name__}), retrying in {wait}s "
                f"(attempt {attempt}/{MAX_RETRIES})",
                file=sys.stderr,
            )
            time.sleep(wait)
    raise last_exc


def fetch_dpe_records(dept: str, max_pages: Optional[int] = None) -> Iterator[dict]:
    """Yield DPE records for the given department, paginating through the API."""
    params = {
        "qs": f"code_insee_ban:{dept}*",
        "size": PAGE_SIZE,
        "select": ",".join(SELECT_FIELDS),
    }
    url = DPE_API_URL
    page = 0
    while url:
        response = _get_with_retry(url, params=params if url == DPE_API_URL else None)
        payload = response.json()
        results = payload.get("results", [])
        for record in results:
            yield record
        page += 1
        print(f"  fetched page {page} ({len(results)} records)")
        url = payload.get("next")
        if max_pages and page >= max_pages:
            break


def save_jsonl(records: Iterator[dict], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            f.write("\n")
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dept", required=True)
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages fetched, for quick testing")
    args = parser.parse_args()

    print(f"Fetching DPE records for department {args.dept}...")
    records = fetch_dpe_records(args.dept, max_pages=args.max_pages)
    result_path = dpe_local_path(args.dept)
    count = save_jsonl(records, result_path)
    print(f"Saved {count} records to {result_path}")

    if not validate_non_empty(result_path):
        print("ERROR: DPE extract is empty", file=sys.stderr)
        sys.exit(1)

    checksum = compute_sha256(result_path)
    key = dpe_object_key(args.dept)
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