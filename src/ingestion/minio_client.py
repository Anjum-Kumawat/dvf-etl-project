"""
Verify that we can reach MinIO from Python.

This script transfers nothing. It only opens a connection and lists the
buckets, so that a later failure can be traced to the upload rather than
to the connection.
"""

import boto3


client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",  # Without this, boto3 would call AWS
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

buckets = client.list_buckets()["Buckets"]

print([bucket["Name"] for bucket in buckets])