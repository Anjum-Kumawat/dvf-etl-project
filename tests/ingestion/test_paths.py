from pathlib import Path

from src.ingestion.paths import local_path, object_key


def test_local_path_pattern():
    assert local_path("2024", "75") == Path("data_raw/2024/75.csv.gz")


def test_object_key_pattern():
    assert object_key("2024", "75") == "dvf/2024/75.csv.gz"