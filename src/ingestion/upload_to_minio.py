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
from paths import local_path, object_key

BUCKET = "bronze"


def get_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )


def get_remote_checksum(client, bucket: str, key: str):
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return response["Metadata"].get("sha256")


def upload(year: str, dept: str, client=None):
    """
    Upload one DVF partition to Bronze if missing or changed.
    Returns (status, local_checksum, remote_checksum_before) where status
    is one of "uploaded", "skipped", "changed".
    """
    client = client or get_client()
    source = local_path(year, dept)
    key = object_key(year, dept)

    if not source.exists():
        raise RuntimeError(f"local file not found: {source}")

    local_checksum = compute_sha256(source)
    remote_checksum = get_remote_checksum(client, BUCKET, key)

    if remote_checksum is None:
        client.upload_file(str(source), BUCKET, key, ExtraArgs={"Metadata": {"sha256": local_checksum}})
        return "uploaded", local_checksum, remote_checksum
    elif remote_checksum == local_checksum:
        return "skipped", local_checksum, remote_checksum
    else:
        client.upload_file(str(source), BUCKET, key, ExtraArgs={"Metadata": {"sha256": local_checksum}})
        return "changed", local_checksum, remote_checksum


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--dept", required=True)
    args = parser.parse_args()

    key = object_key(args.year, args.dept)

    try:
        status, local_checksum, remote_checksum_before = upload(args.year, args.dept)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    if status == "uploaded":
        print(f"Uploaded: {BUCKET}/{key} (sha256={local_checksum})")
    elif status == "skipped":
        print(f"Skipped: {BUCKET}/{key} unchanged (sha256={local_checksum})")
    else:
        print(
            f"Changed: {BUCKET}/{key} checksum differed "
            f"(old={remote_checksum_before}, new={local_checksum}) — re-uploaded"
        )

    client = get_client()
    response = client.head_object(Bucket=BUCKET, Key=key)
    stored_checksum = response["Metadata"].get("sha256", "")
    size_mb = response["ContentLength"] / 1024 / 1024
    print(f"Object in MinIO: {size_mb:.1f} MB, sha256={stored_checksum}")

    if stored_checksum != local_checksum:
        print("ERROR: stored checksum does not match local checksum after operation", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()