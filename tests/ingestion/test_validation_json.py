import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "ingestion"))
from validation import validate_json_file, validate_json


def test_validate_json_file_accepts_valid_json(tmp_path):
    path = tmp_path / "valid.json"
    path.write_text('{"a": 1}', encoding="utf-8")
    assert validate_json_file(path) is True


def test_validate_json_file_rejects_malformed_json(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text('{"a": 1', encoding="utf-8")
    assert validate_json_file(path) is False


def test_validate_json_wrapper_returns_reason_on_missing_file(tmp_path):
    path = tmp_path / "missing.json"
    is_valid, reason = validate_json(path)
    assert is_valid is False
    assert "empty or missing" in reason


def test_validate_json_wrapper_accepts_valid_json(tmp_path):
    path = tmp_path / "valid.json"
    path.write_text('{"a": 1}', encoding="utf-8")
    is_valid, reason = validate_json(path)
    assert is_valid is True
    assert reason == ""