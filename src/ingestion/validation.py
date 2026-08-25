"""
Reusable validation helpers for raw Bronze source files.
"""

import gzip
from pathlib import Path


def validate_non_empty(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def validate_gzip_archive(path: Path) -> bool:
    """True if path is a complete, readable gzip archive."""
    try:
        with gzip.open(path, "rb") as f:
            while f.read(1024 * 1024):
                pass
        return True
    except (gzip.BadGzipFile, EOFError, OSError):
        return False


def validate_archive(path: Path):
    """
    Validate a downloaded raw archive.
    Returns (is_valid: bool, reason: str). reason is "" when valid.
    """
    if not validate_non_empty(path):
        return False, f"File is empty or missing: {path}"
    if not validate_gzip_archive(path):
        return False, f"File is not a valid gzip archive: {path}"
    return True, ""