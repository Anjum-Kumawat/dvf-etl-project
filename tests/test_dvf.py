import gzip
import hashlib
from pathlib import Path

import pytest

from src.ingestion import dvf


def test_build_url():
    url = dvf.build_url("2024", "75")
    assert url == "https://files.data.gouv.fr/geo-dvf/latest/csv/2024/departements/75.csv.gz"


def test_validate_archive_accepts_valid_gzip(tmp_path):
    path = tmp_path / "valid.csv.gz"
    with gzip.open(path, "wb") as f:
        f.write(b"some,csv,content\n" * 100)
    assert dvf.validate_archive(path) is True


def test_validate_archive_rejects_plain_text(tmp_path):
    path = tmp_path / "not_gzip.csv.gz"
    path.write_bytes(b"this is not gzip data")
    assert dvf.validate_archive(path) is False


def test_validate_archive_rejects_truncated_gzip(tmp_path):
    path = tmp_path / "truncated.csv.gz"
    with gzip.open(path, "wb") as f:
        f.write(b"some,csv,content\n" * 1000)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])
    assert dvf.validate_archive(path) is False


def test_compute_checksum_matches_known_hash(tmp_path):
    path = tmp_path / "file.bin"
    content = b"hello world"
    path.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    assert dvf.compute_checksum(path) == expected


def test_compute_checksum_differs_for_different_content(tmp_path):
    path_a = tmp_path / "a.bin"
    path_b = tmp_path / "b.bin"
    path_a.write_bytes(b"content A")
    path_b.write_bytes(b"content B")
    assert dvf.compute_checksum(path_a) != dvf.compute_checksum(path_b)


@pytest.fixture
def manifest_env(tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.json"
    monkeypatch.setattr(dvf, "MANIFEST_PATH", manifest_path)
    return manifest_path


def test_already_ingested_false_when_no_manifest_entry(tmp_path, manifest_env):
    dest = tmp_path / "75.csv.gz"
    with gzip.open(dest, "wb") as f:
        f.write(b"data")
    assert dvf.already_ingested("2024", "75", dest) is False


def test_already_ingested_true_when_checksum_matches(tmp_path, manifest_env):
    dest = tmp_path / "75.csv.gz"
    with gzip.open(dest, "wb") as f:
        f.write(b"data")
    checksum = dvf.compute_checksum(dest)
    dvf.record_manifest_entry("2024", "75", dest, checksum, "success")
    assert dvf.already_ingested("2024", "75", dest) is True


def test_already_ingested_false_when_checksum_differs(tmp_path, manifest_env):
    dest = tmp_path / "75.csv.gz"
    with gzip.open(dest, "wb") as f:
        f.write(b"data")
    dvf.record_manifest_entry("2024", "75", dest, "wrong_checksum", "success")
    assert dvf.already_ingested("2024", "75", dest) is False


def test_already_ingested_false_when_file_missing(tmp_path, manifest_env):
    dest = tmp_path / "75.csv.gz"
    with gzip.open(dest, "wb") as f:
        f.write(b"data")
    checksum = dvf.compute_checksum(dest)
    dvf.record_manifest_entry("2024", "75", dest, checksum, "success")
    dest.unlink()
    assert dvf.already_ingested("2024", "75", dest) is False