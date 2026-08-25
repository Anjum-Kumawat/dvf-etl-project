import hashlib

from src.ingestion.checksum import compute_sha256


def test_compute_sha256_matches_known_hash(tmp_path):
    path = tmp_path / "file.bin"
    content = b"hello world"
    path.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    assert compute_sha256(path) == expected


def test_compute_sha256_is_deterministic(tmp_path):
    path = tmp_path / "file.bin"
    path.write_bytes(b"same content")
    assert compute_sha256(path) == compute_sha256(path)


def test_compute_sha256_differs_for_different_content(tmp_path):
    path_a = tmp_path / "a.bin"
    path_b = tmp_path / "b.bin"
    path_a.write_bytes(b"content A")
    path_b.write_bytes(b"content B")
    assert compute_sha256(path_a) != compute_sha256(path_b)