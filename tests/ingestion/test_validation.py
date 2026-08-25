import gzip

from src.ingestion.validation import validate_archive


def test_validate_archive_accepts_valid_gzip(tmp_path):
    path = tmp_path / "valid.csv.gz"
    with gzip.open(path, "wb") as f:
        f.write(b"some,csv,content\n" * 100)
    is_valid, reason = validate_archive(path)
    assert is_valid is True
    assert reason == ""


def test_validate_archive_rejects_empty_file(tmp_path):
    path = tmp_path / "empty.csv.gz"
    path.write_bytes(b"")
    is_valid, reason = validate_archive(path)
    assert is_valid is False
    assert "empty" in reason.lower()


def test_validate_archive_rejects_plain_text(tmp_path):
    path = tmp_path / "not_gzip.csv.gz"
    path.write_bytes(b"this is not gzip data")
    is_valid, reason = validate_archive(path)
    assert is_valid is False


def test_validate_archive_rejects_truncated_gzip(tmp_path):
    path = tmp_path / "truncated.csv.gz"
    with gzip.open(path, "wb") as f:
        f.write(b"some,csv,content\n" * 1000)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])
    is_valid, reason = validate_archive(path)
    assert is_valid is False