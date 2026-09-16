import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "ingestion"))
from validation import validate_zip_archive, validate_zip


def make_valid_zip(path: Path):
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("communes.csv", "code_commune,revenu\n75101,25000\n")


def test_validate_zip_archive_accepts_valid_zip(tmp_path):
    path = tmp_path / "valid.zip"
    make_valid_zip(path)
    assert validate_zip_archive(path) is True


def test_validate_zip_archive_rejects_empty_file(tmp_path):
    path = tmp_path / "empty.zip"
    path.write_bytes(b"")
    assert validate_zip_archive(path) is False


def test_validate_zip_archive_rejects_plain_text(tmp_path):
    path = tmp_path / "notazip.zip"
    path.write_text("this is not a zip file")
    assert validate_zip_archive(path) is False


def test_validate_zip_wrapper_returns_reason_on_failure(tmp_path):
    path = tmp_path / "missing.zip"
    is_valid, reason = validate_zip(path)
    assert is_valid is False
    assert "empty or missing" in reason


def test_validate_zip_wrapper_accepts_valid_zip(tmp_path):
    path = tmp_path / "valid.zip"
    make_valid_zip(path)
    is_valid, reason = validate_zip(path)
    assert is_valid is True
    assert reason == ""