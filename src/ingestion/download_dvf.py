"""
Download one raw DVF file from data.gouv.fr.
Validates the response is non-empty and a readable gzip archive before
accepting it as a valid Bronze source.
"""

import argparse
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validation import validate_archive
from paths import local_path

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz"


def download(year: str, dept: str) -> Path:
    """
    Download and validate one DVF partition.
    Raises RuntimeError with a clear message on any HTTP or validation
    failure. Returns the local path on success.
    """
    url = BASE_URL.format(year=year, dept=dept)
    dest = local_path(year, dept)
    dest.parent.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {url}")
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"download failed: {exc}") from exc

    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    is_valid, reason = validate_archive(dest)
    if not is_valid:
        dest.unlink(missing_ok=True)
        raise RuntimeError(f"downloaded file failed validation: {reason}")

    return dest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--dept", required=True)
    args = parser.parse_args()

    try:
        dest = download(args.year, args.dept)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    size_mb = dest.stat().st_size / 1024 / 1024
    print(f"Saved to {dest} ({size_mb:.1f} MB) — validated as a non-empty gzip archive")


if __name__ == "__main__":
    main()