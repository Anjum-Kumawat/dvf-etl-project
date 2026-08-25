"""Minimal fake S3 client for testing upload logic without a live MinIO."""

from pathlib import Path

from botocore.exceptions import ClientError


class FakeS3Client:
    def __init__(self):
        self.objects = {}
        self.uploaded = []

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return self.objects[Key]

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        metadata = (ExtraArgs or {}).get("Metadata", {})
        size = Path(Filename).stat().st_size
        self.objects[Key] = {"Metadata": metadata, "ContentLength": size}
        self.uploaded.append(Key)