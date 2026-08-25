"""
Download a DVF departmental csv.gz file for a given year and department.
Validates the HTTP response and the archive integrity before accepting the file.

Usage:
    python -m src.ingestion.dvf --year 2024 --department 75

Configuration (no hardcoded credentials or local paths):
    DATA_RAW_DIR   Base directory for downloaded files. Defaults to "data_raw".
    DVF_TIMEOUT    HTTP timeout in seconds. Defaults to 30.
"""

import argparse
import gzip
import logging
import os
import sys
from pathlib import Path

import requests

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{department}.csv.gz"
DATA_DIR = os.environ.get("DATA_RAW_DIR", "data_raw")
TIMEOUT = int(os.environ.get("DVF_TIMEOUT", "30"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("dvf_download")


def build_url(year: str, department: str) -> str:
    return BASE_URL.format(year=year, department=department)


def validate_archive(path: Path) -> bool:
    """Confirm the downloaded file is a complete, readable gzip archive."""
    try:
        with gzip.open(path, "rb") as f:
            while f.read(1024 * 1024):
                pass
        return True
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
        logger.error("Archive integrity check failed for %s: %s", path, exc)
        return False


def download_dvf(year: str, department: str) -> Path:
    url = build_url(year, department)
    dest = Path(DATA_DIR) / year / f"{department}.csv.gz"
    dest.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Starting download: %s -> %s", url, dest)

    try:
        response = requests.get(url, stream=True, timeout=TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Download failed: request timed out after %ss (%s)", TIMEOUT, url)
        raise
    except requests.exceptions.HTTPError:
        logger.error("Download failed: HTTP %s for %s", response.status_code, url)
        raise
    except requests.exceptions.RequestException as exc:
        logger.error("Download failed: %s (%s)", exc, url)
        raise

    expected_size = response.headers.get("Content-Length")

    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    actual_size = dest.stat().st_size

    if expected_size is not None and int(expected_size) != actual_size:
        logger.error(
            "Size mismatch for %s: expected %s bytes, got %s bytes",
            dest, expected_size, actual_size,
        )
        raise ValueError(f"Incomplete download: {dest}")

    if not validate_archive(dest):
        raise ValueError(f"Corrupt or invalid archive: {dest}")

    size_mb = actual_size / 1024 / 1024
    logger.info("Download succeeded and archive validated: %s (%.1f MB)", dest, size_mb)
    return dest


def main():
    parser = argparse.ArgumentParser(description="Download a DVF departmental csv.gz file.")
    parser.add_argument("--year", required=True, help="Transaction year, e.g. 2024")
    parser.add_argument("--department", required=True, help="Department code, e.g. 75")
    args = parser.parse_args()

    try:
        download_dvf(args.year, args.department)
    except (requests.exceptions.RequestException, ValueError):
        sys.exit(1)


if __name__ == "__main__":
    main()