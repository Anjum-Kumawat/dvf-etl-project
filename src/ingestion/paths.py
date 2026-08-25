"""Path/key construction helpers shared by download and upload scripts."""

from pathlib import Path


def local_path(year: str, dept: str) -> Path:
    return Path(f"data_raw/{year}/{dept}.csv.gz")


def object_key(year: str, dept: str) -> str:
    return f"dvf/{year}/{dept}.csv.gz"