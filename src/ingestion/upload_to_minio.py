"""
Upload the raw DVF file to the MinIO bronze bucket.
The file is uploaded as-is: still gzipped, no parsing, no cleaning.
Bronze stores raw data. Transformation happens later, in Silver.
"""

import boto3
from pathlib import Path

SOURCE = Path("data_raw/75.csv.gz")
BUCKET = "bronze"
KEY    = "dvf/2024/75.csv.gz"

client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

print(f"Uploading {SOURCE} to {BUCKET}/{KEY}")
client.upload_file(str(SOURCE), BUCKET, KEY)
print("Done")

response = client.head_object(Bucket=BUCKET, Key=KEY)
size_mb = response["ContentLength"] / 1024 / 1024
print(f"Object in MinIO: {size_mb:.1f} MB")