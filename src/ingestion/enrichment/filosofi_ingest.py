"""INSEE Filosofi (income) ingestion — Bronze layer.

Downloads the commune-level Filosofi income/living-standards file and stores
it unchanged in MinIO Bronze under the `filosofi/` prefix.

Unlike DVF, Filosofi is not a periodic biannual publication with a predictable
next release: the 2021 vintage is, as of this ingestion, the last one INSEE has
published. Production of the 2022 vintage was cancelled — INSEE's stated reason
is that the abolition of the taxe d'habitation broke the method Filosofi used to
link tax households to a dwelling, and the replacement sourcing wasn't reliable
enough statistically. This is a real data-freshness limitation for the project
(Filosofi income data will run 2-3 years stale relative to 2023-2024 DVF
transactions) and belongs in the report's limitations section, not silently
absorbed.

Also unlike DVF/BAN/DPE, this source is not filtered by department at ingestion
time: INSEE distributes one national commune-level file per vintage, not one
file per department. Filtering to the pilot department happens in Silver —
Bronze preserves the file exactly as published.
"""
import argparse
import sys
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from validation import validate_zip  # noqa: E402
from checksum import compute_sha256  # noqa: E402
from upload_to_minio import get_client, get_remote_checksum, BUCKET  # noqa: E402

FILOSOFI_VINTAGE = "2021"  # last available vintage; 2022 production was cancelled by INSEE
FILOSOFI_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/7756855/"
    "indic-struct-distrib-revenu-2021-COMMUNES_csv.zip"
)


def filosofi_local_path(vintage: Optional[str] = None) -> Path:
    vintage = vintage or FILOSOFI_VINTAGE
    return Path(f"data_raw/filosofi/{vintage}/communes.zip")


def filosofi_object_key(vintage: Optional[str] = None) -> str:
    vintage = vintage or FILOSOFI_VINTAGE
    return f"filosofi/vintage={vintage}/communes.zip"


def download(vintage: Optional[str] = None) -> Path:
    vintage = vintage or FILOSOFI_VINTAGE
    dest = filosofi_local_path(vintage)
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {FILOSOFI_URL}")
    try:
        response = requests.get(FILOSOFI_URL, stream=True, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"download failed: {exc}") from exc

    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    is_valid, reason = validate_zip(dest)
    if not is_valid:
        dest.unlink(missing_ok=True)
        raise RuntimeError(f"downloaded file failed validation: {reason}")
    return dest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vintage", default=FILOSOFI_VINTAGE)
    args = parser.parse_args()

    try:
        dest = download(args.vintage)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    size_mb = dest.stat().st_size / 1024 / 1024
    print(f"Saved to {dest} ({size_mb:.1f} MB) — validated as a non-empty zip archive")

    checksum = compute_sha256(dest)
    key = filosofi_object_key(args.vintage)
    client = get_client()
    remote_checksum = get_remote_checksum(client, BUCKET, key)

    if remote_checksum == checksum:
        print(f"Skipped: {BUCKET}/{key} unchanged (sha256={checksum})")
    else:
        client.upload_file(str(dest), BUCKET, key, ExtraArgs={"Metadata": {"sha256": checksum}})
        if remote_checksum is None:
            print(f"Uploaded: {BUCKET}/{key} (sha256={checksum})")
        else:
            print(f"Changed: {BUCKET}/{key} checksum differed (old={remote_checksum}, new={checksum}) — re-uploaded")


if __name__ == "__main__":
    main()