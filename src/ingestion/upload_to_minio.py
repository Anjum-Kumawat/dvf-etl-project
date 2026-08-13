import argparse
import boto3
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--year", required=True)
parser.add_argument("--dept", required=True)
args = parser.parse_args()

SOURCE = Path(f"data_raw/{args.year}/{args.dept}.csv.gz")
BUCKET = "bronze"
KEY = f"dvf/{args.year}/{args.dept}.csv.gz"

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