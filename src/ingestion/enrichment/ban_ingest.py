"""BAN (Base Adresse Nationale) geocoding ingestion — Bronze layer.

Extracts unique addresses from the Bronze DVF file for a given year/department,
sends them to the IGN Géoplateforme batch geocoding API, and stores the raw
response CSV in MinIO Bronze under the `ban/` prefix, following the same
validation, checksum, and idempotency pattern as DVF ingestion.
"""
import argparse
import csv
import gzip
import sys
from io import StringIO
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from paths import current_publication, local_path as dvf_local_path  # noqa: E402
from validation import validate_non_empty  # noqa: E402
from checksum import compute_sha256  # noqa: E402
from upload_to_minio import get_client, get_remote_checksum, BUCKET  # noqa: E402

BAN_BATCH_URL = "https://data.geopf.fr/geocodage/search/csv"
ADDRESS_COLUMNS = ["adresse_numero", "adresse_nom_voie", "code_postal", "code_commune"]
SCORE_COLUMNS = ["result_score", "result_score_next"]
ROUND_DIGITS = 4


def ban_local_path(year: str, dept: str, publication: Optional[str] = None) -> Path:
    publication = publication or current_publication()
    return Path(f"data_raw/{publication}/{year}/ban_{dept}.csv")


def ban_object_key(year: str, dept: str, publication: Optional[str] = None) -> str:
    publication = publication or current_publication()
    return f"ban/publication={publication}/year={year}/department={dept}/{dept}.csv"


def extract_unique_addresses(dvf_path: Path, output_path: Path) -> int:
    """Read the Bronze DVF file, dedupe addresses, write an input CSV for geocoding.

    Returns the number of unique addresses written.
    """
    seen = set()
    rows = []
    with gzip.open(dvf_path, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = tuple(row.get(col, "") for col in ADDRESS_COLUMNS)
            if key in seen or not key[1]:  # skip dupes and rows with no street name
                continue
            seen.add(key)
            rows.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id"] + ADDRESS_COLUMNS)
        writer.writeheader()
        for i, row in enumerate(rows):
            writer.writerow({"id": i, **{col: row.get(col, "") for col in ADDRESS_COLUMNS}})
    return len(rows)


def _stabilize_scores(csv_bytes: bytes) -> bytes:
    """Round volatile match-confidence score columns to stabilize checksums.

    The BAN API returns result_score / result_score_next with full floating-
    point precision, which can differ by tiny amounts between two calls with
    identical input (server-side scoring isn't bit-for-bit reproducible).
    Rounding to 4 decimal places keeps the signal (match confidence) while
    eliminating jitter that would otherwise make Bronze uploads non-idempotent
    for unchanged data.
    """
    text = csv_bytes.decode("utf-8")
    reader = csv.DictReader(StringIO(text))
    fieldnames = reader.fieldnames
    rows = []
    for row in reader:
        for col in SCORE_COLUMNS:
            value = row.get(col, "")
            if value:
                row[col] = str(round(float(value), ROUND_DIGITS))
        rows.append(row)

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def geocode_csv(input_path: Path, output_path: Path) -> None:
    """POST the address CSV to the BAN batch endpoint and save the raw response."""
    with open(input_path, "rb") as f:
        files = {"data": (input_path.name, f, "text/csv")}
        data = [
            ("columns", "adresse_numero"),
            ("columns", "adresse_nom_voie"),
            ("columns", "code_postal"),
            ("citycode", "code_commune"),
        ]
        response = requests.post(BAN_BATCH_URL, files=files, data=data, timeout=120)

    if response.status_code != 200:
        print(f"BAN API returned {response.status_code}:", file=sys.stderr)
        print(response.text[:2000], file=sys.stderr)
        response.raise_for_status()

    stabilized = _stabilize_scores(response.content)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(stabilized)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--dept", required=True)
    args = parser.parse_args()

    dvf_path = dvf_local_path(args.year, args.dept)
    if not dvf_path.exists():
        print(f"ERROR: DVF Bronze file not found at {dvf_path} — run download_dvf.py first", file=sys.stderr)
        sys.exit(1)

    addresses_path = Path(f"data_raw/tmp/ban_input_{args.year}_{args.dept}.csv")
    n = extract_unique_addresses(dvf_path, addresses_path)
    print(f"Extracted {n} unique addresses")

    result_path = ban_local_path(args.year, args.dept)
    geocode_csv(addresses_path, result_path)

    if not validate_non_empty(result_path):
        print("ERROR: geocoding response is empty", file=sys.stderr)
        sys.exit(1)

    checksum = compute_sha256(result_path)
    key = ban_object_key(args.year, args.dept)
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