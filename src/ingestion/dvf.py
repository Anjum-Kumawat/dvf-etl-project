"""
Download a DVF departmental csv.gz file for a given year and department.
Validates the HTTP response and archive integrity, then computes and stores
a SHA-256 checksum in a manifest for future idempotency checks.

Usage:
    python -m src.ingestion.dvf --year 2024 --department 75

Configuration (no hardcoded credentials or local paths):
    DATA_RAW_DIR   Base directory for downloaded files. Defaults to "data_raw".
    DVF_TIMEOUT    HTTP timeout in seconds. Defaults to 30.
"""

import argparse
import gzip
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{department}.csv.gz"
DATA_DIR = os.environ.get("DATA_RAW_DIR", "data_raw")
TIMEOUT = int(os.environ.get("DVF_TIMEOUT", "30"))
MANIFEST_PATH = Path(DATA_DIR) / "manifest.json"

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


def compute_checksum(path: Path) -> str:
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r") as f:
            return json.load(f)
    return {}


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)


def record_manifest_entry(year: str, department: str, path: Path, checksum: str, status: str) -> None:
    manifest = load_manifest()
    key = f"{year}/{department}"
    manifest[key] = {
        "year": year,
        "department": department,
        "path": str(path),
        "checksum_sha256": checksum,
        "size_bytes": path.stat().st_size,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
    }
    save_manifest(manifest)
    logger.info("Manifest updated for %s: %s (%s)", key, checksum, status)


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
        record_manifest_entry(year, department, dest, checksum="", status="failed_size_mismatch")
        raise ValueError(f"Incomplete download: {dest}")

    if not validate_archive(dest):
        record_manifest_entry(year, department, dest, checksum="", status="failed_corrupt")
        raise ValueError(f"Corrupt or invalid archive: {dest}")

    checksum = compute_checksum(dest)
    record_manifest_entry(year, department, dest, checksum=checksum, status="success")

    size_mb = actual_size / 1024 / 1024
    logger.info(
        "Download succeeded and archive validated: %s (%.1f MB, sha256=%s)",
        dest, size_mb, checksum,
    )
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