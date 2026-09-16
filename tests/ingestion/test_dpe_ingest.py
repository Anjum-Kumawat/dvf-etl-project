import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "ingestion" / "enrichment"))
from dpe_ingest import fetch_dpe_records, save_jsonl, dpe_object_key, dpe_local_path


def test_fetch_dpe_records_follows_pagination(monkeypatch):
    page1 = {
        "results": [{"numero_dpe": "A"}, {"numero_dpe": "B"}],
        "next": "https://data.ademe.fr/data-fair/api/v1/datasets/meg-xyz/lines?after=1",
    }
    page2 = {"results": [{"numero_dpe": "C"}]}  # no "next" -> last page
    calls = {"n": 0}

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self._payload

    def fake_get(url, params=None, timeout=60):
        calls["n"] += 1
        return FakeResponse(page1) if calls["n"] == 1 else FakeResponse(page2)

    monkeypatch.setattr("dpe_ingest.requests.get", fake_get)

    records = list(fetch_dpe_records("75"))

    assert [r["numero_dpe"] for r in records] == ["A", "B", "C"]
    assert calls["n"] == 2


def test_fetch_dpe_records_respects_max_pages(monkeypatch):
    page1 = {
        "results": [{"numero_dpe": "A"}],
        "next": "https://data.ademe.fr/data-fair/api/v1/datasets/meg-xyz/lines?after=1",
    }

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self._payload

    def fake_get(url, params=None, timeout=60):
        return FakeResponse(page1)

    monkeypatch.setattr("dpe_ingest.requests.get", fake_get)

    records = list(fetch_dpe_records("75", max_pages=1))

    assert len(records) == 1


def test_save_jsonl_writes_one_record_per_line(tmp_path):
    output_path = tmp_path / "out.jsonl"
    records = [{"numero_dpe": "A"}, {"numero_dpe": "B"}]

    count = save_jsonl(iter(records), output_path)

    assert count == 2
    lines = output_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["numero_dpe"] == "A"
    assert json.loads(lines[1])["numero_dpe"] == "B"


def test_dpe_object_key_format():
    assert dpe_object_key("75", extraction_date="2026-09-16") == "dpe/extraction_date=2026-09-16/department=75/75.jsonl"


def test_dpe_local_path_format():
    assert str(dpe_local_path("75", extraction_date="2026-09-16")) == str(Path("data_raw/dpe/2026-09-16/75.jsonl"))