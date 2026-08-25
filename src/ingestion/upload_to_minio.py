"""
Upload the raw DVF file to the MinIO bronze bucket, unchanged, storing its
SHA-256 checksum as MinIO object metadata. Skips the transfer when an
object already exists with an identical checksum (idempotent upload).
"""

import argparse
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from checksum import compute_sha256

BUCKET = "bronze"


def get_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )


def get_remote_checksum(client, bucket, key):
    """Return the stored sha256 metadata for an existing object, or None if missing."""
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return response["Metadata"].get("sha256")


def upload_with_checksum(client, source: Path, bucket: str, key: str, checksum: str):
    client.upload_file(
        str(source),
        bucket,
        key,
        ExtraArgs={"Metadata": {"sha256": checksum}},
    )


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
    client = get_client()
    remote_checksum = get_remote_checksum(client, BUCKET, key)

    if remote_checksum is None:
        upload_with_checksum(client, source, BUCKET, key, local_checksum)
        print(f"Uploaded: {BUCKET}/{key} (sha256={local_checksum})")
    elif remote_checksum == local_checksum:
        print(f"Skipped: {BUCKET}/{key} unchanged (sha256={local_checksum})")
    else:
        upload_with_checksum(client, source, BUCKET, key, local_checksum)
        print(
            f"Changed: {BUCKET}/{key} checksum differed "
            f"(old={remote_checksum}, new={local_checksum}) — re-uploaded"
        )

    response = client.head_object(Bucket=BUCKET, Key=key)
    stored_checksum = response["Metadata"].get("sha256", "")
    size_mb = response["ContentLength"] / 1024 / 1024
    print(f"Object in MinIO: {size_mb:.1f} MB, sha256={stored_checksum}")

    if stored_checksum != local_checksum:
        print("ERROR: stored checksum does not match local checksum after operation", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()