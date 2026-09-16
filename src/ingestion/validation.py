import gzip
import zipfile
from pathlib import Path


def validate_non_empty(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def validate_gzip_archive(path: Path) -> bool:
    try:
        with gzip.open(path, "rb") as f:
            while f.read(1024 * 1024):
                pass
        return True
    except (gzip.BadGzipFile, EOFError, OSError):
        return False


def validate_archive(path: Path):
    if not validate_non_empty(path):
        return False, f"File is empty or missing: {path}"
    if not validate_gzip_archive(path):
        return False, f"File is not a valid gzip archive: {path}"
    return True, ""


def validate_zip_archive(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        if not zipfile.is_zipfile(path):
            return False
        with zipfile.ZipFile(path) as zf:
            bad_file = zf.testzip()
            return bad_file is None
    except (zipfile.BadZipFile, OSError):
        return False


def validate_zip(path: Path):
    if not validate_non_empty(path):
        return False, f"File is empty or missing: {path}"
    if not validate_zip_archive(path):
        return False, f"File is not a valid zip archive: {path}"
    return True, ""