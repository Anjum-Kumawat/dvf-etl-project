"""
Upload the raw DVF file to the MinIO bronze bucket, unchanged, and store its
SHA-256 checksum as MinIO object metadata so future ingestion runs can
detect whether the source has changed (see RETL0-33).
"""

import argparse
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent))
from checksum import compute_sha256

BUCKET = "bronze"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--dept", required=True)
    args = parser.parse_args()

    source = Path(f"data_raw/{args.year}/{args.dept}.csv.gz")
    key = f"dvf/{args.year}/{args.dept}.csv.gz"

    if not source.exists():
        print(f"ERROR: local file not found: {source}", file=sys.stderr)
        sys.exit(1)

    local_checksum = compute_sha256(source)
    print(f"Local SHA-256: {local_checksum}")

    client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    print(f"Uploading {source} to {BUCKET}/{key}")
    client.upload_file(
        str(source),
        BUCKET,
        key,
        ExtraArgs={"Metadata": {"sha256": local_checksum}},
    )
    print("Upload complete")

    response = client.head_object(Bucket=BUCKET, Key=key)
    stored_checksum = response["Metadata"].get("sha256", "")
    size_mb = response["ContentLength"] / 1024 / 1024

    print(f"Object in MinIO: {size_mb:.1f} MB")
    print(f"Stored SHA-256 metadata: {stored_checksum}")

    if stored_checksum != local_checksum:
        print("ERROR: stored checksum does not match local checksum", file=sys.stderr)
        sys.exit(1)

    print("Checksum verified: local and stored SHA-256 match")


if __name__ == "__main__":
    main()