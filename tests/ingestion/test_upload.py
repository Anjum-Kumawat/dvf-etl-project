import gzip

from src.ingestion.upload_to_minio import upload
from src.ingestion.paths import local_path, object_key
from tests.ingestion.fake_s3 import FakeS3Client


def _write_gzip(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as f:
        f.write(content)


def test_upload_missing_object_returns_uploaded(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_gzip(local_path("2024", "75"), b"data")

    client = FakeS3Client()
    status, local_checksum, remote_before = upload("2024", "75", client=client)

    assert status == "uploaded"
    assert remote_before is None
    assert object_key("2024", "75") in client.uploaded


def test_upload_existing_same_checksum_returns_skipped(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_gzip(local_path("2024", "75"), b"data")

    client = FakeS3Client()
    upload("2024", "75", client=client)
    uploads_before = list(client.uploaded)

    status, local_checksum, remote_before = upload("2024", "75", client=client)

    assert status == "skipped"
    assert remote_before == local_checksum
    assert client.uploaded == uploads_before


def test_upload_changed_checksum_returns_changed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dest = local_path("2024", "75")
    _write_gzip(dest, b"data v1")

    client = FakeS3Client()
    upload("2024", "75", client=client)

    _write_gzip(dest, b"data v2 - different content")
    status, local_checksum, remote_before = upload("2024", "75", client=client)

    assert status == "changed"
    assert remote_before != local_checksum


def test_stored_metadata_matches_local_checksum(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_gzip(local_path("2024", "75"), b"data")

    client = FakeS3Client()
    status, local_checksum, _ = upload("2024", "75", client=client)

    stored = client.objects[object_key("2024", "75")]["Metadata"]["sha256"]
    assert stored == local_checksum


def test_upload_does_not_modify_local_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dest = local_path("2024", "75")
    _write_gzip(dest, b"some raw gzip-ish bytes")
    before = dest.read_bytes()

    client = FakeS3Client()
    upload("2024", "75", client=client)

    assert dest.read_bytes() == before


def test_upload_respects_publication_override(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_gzip(local_path("2024", "75", "2026-10"), b"data")

    client = FakeS3Client()
    status, local_checksum, remote_before = upload("2024", "75", publication="2026-10", client=client)

    assert status == "uploaded"
    assert remote_before is None
    assert object_key("2024", "75", "2026-10") in client.uploaded