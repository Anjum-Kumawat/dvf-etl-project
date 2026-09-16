import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "ingestion" / "enrichment"))
from geo_ingest import build_snapshot, save_snapshot, geo_object_key, geo_local_path


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_build_snapshot_combines_department_region_communes(monkeypatch):
    def fake_get(url, params=None, timeout=30):
        if "departements" in url:
            return FakeResponse({"nom": "Paris", "code": "75", "codeRegion": "11"})
        if "regions" in url:
            return FakeResponse({"nom": "Île-de-France", "code": "11"})
        if "communes" in url:
            return FakeResponse([{"nom": "Paris", "code": "75056"}])
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr("geo_ingest.requests.get", fake_get)

    snapshot = build_snapshot("75")

    assert snapshot["department"]["code"] == "75"
    assert snapshot["region"]["code"] == "11"
    assert snapshot["communes"] == [{"nom": "Paris", "code": "75056"}]


def test_save_snapshot_writes_valid_json(tmp_path):
    output_path = tmp_path / "snapshot.json"
    snapshot = {"department": {"code": "75"}, "region": {"code": "11"}, "communes": []}

    save_snapshot(snapshot, output_path)

    with open(output_path, encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded == snapshot


def test_geo_object_key_format():
    assert geo_object_key("75", extraction_date="2026-09-17") == "geo/extraction_date=2026-09-17/department=75/75.json"


def test_geo_local_path_format():
    assert str(geo_local_path("75", extraction_date="2026-09-17")) == str(Path("data_raw/geo/2026-09-17/75.json"))