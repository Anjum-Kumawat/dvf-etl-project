import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "ingestion"))
from paths import current_publication, local_path, object_key


def test_current_publication_after_october():
    assert current_publication(date(2026, 11, 15)) == "2026-10"


def test_current_publication_after_april():
    assert current_publication(date(2026, 6, 1)) == "2026-04"


def test_current_publication_before_april():
    assert current_publication(date(2026, 2, 1)) == "2025-10"


def test_current_publication_on_october_boundary():
    assert current_publication(date(2026, 10, 1)) == "2026-10"


def test_current_publication_on_april_boundary():
    assert current_publication(date(2026, 4, 1)) == "2026-04"


def test_local_path_with_explicit_publication():
    assert local_path("2024", "75", publication="2026-04") == Path("data_raw/2026-04/2024/75.csv.gz")


def test_object_key_with_explicit_publication():
    assert object_key("2024", "75", publication="2026-04") == "dvf/publication=2026-04/year=2024/department=75/75.csv.gz"


def test_local_path_defaults_to_current_publication():
    result = local_path("2024", "75")
    assert result.as_posix().endswith("2024/75.csv.gz")
    assert result.parts[0] == "data_raw"