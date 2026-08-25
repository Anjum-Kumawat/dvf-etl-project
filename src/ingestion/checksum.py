"""
SHA-256 checksum helper for raw Bronze source files.
"""

import hashlib
from pathlib import Path


def compute_sha256(path: Path) -> str:
    """Compute the SHA-256 hex digest of a file without modifying it."""
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()