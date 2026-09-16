import csv
import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "ingestion" / "enrichment"))
from ban_ingest import extract_unique_addresses, geocode_csv, ban_object_key


def make_dvf_fixture(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["adresse_numero", "adresse_nom_voie", "code_postal", "code_commune"])
        writer.writerow(["12", "RUE DE RIVOLI", "75001", "75101"])
        writer.writerow(["12", "RUE DE RIVOLI", "75001", "75101"])  # duplicate
        writer.writerow(["5", "AVENUE FOCH", "75116", "75116"])
        writer.writerow(["", "", "", ""])  # missing address, should be skipped


def test_extract_unique_addresses_dedupes(tmp_path):
    dvf_path = tmp_path / "75.csv.gz"
    make_dvf_fixture(dvf_path)
    output_path = tmp_path / "ban_input.csv"

    count = extract_unique_addresses(dvf_path, output_path)

    assert count == 2
    with open(output_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    streets = {row["adresse_nom_voie"] for row in rows}
    assert streets == {"RUE DE RIVOLI", "AVENUE FOCH"}


def test_geocode_csv_posts_and_saves_response(tmp_path, monkeypatch):
    input_path = tmp_path / "input.csv"
    input_path.write_text(
        "id,adresse_numero,adresse_nom_voie,code_postal,code_commune\n0,12,RUE DE RIVOLI,75001,75101\n"
    )
    output_path = tmp_path / "output.csv"

    class FakeResponse:
        status_code = 200
        content = b"id,latitude,longitude\n0,48.8606,2.3376\n"

        def raise_for_status(self):
            pass

    captured = {}

    def fake_post(url, files, data, timeout):
        captured["url"] = url
        return FakeResponse()

    monkeypatch.setattr("ban_ingest.requests.post", fake_post)

    geocode_csv(input_path, output_path)

    assert captured["url"] == "https://data.geopf.fr/geocodage/search/csv"
    assert output_path.read_bytes() == b"id,latitude,longitude\n0,48.8606,2.3376\n"


def test_geocode_csv_stabilizes_score_jitter(tmp_path, monkeypatch):
    input_path = tmp_path / "input.csv"
    input_path.write_text("id,adresse_numero\n0,4\n")

    responses = [
        b"id,result_score\n0,0.79605528138528140\n",
        b"id,result_score\n0,0.79605529999999999\n",  # tiny jitter, same address
    ]

    class FakeResponse:
        status_code = 200

        def __init__(self, content):
            self.content = content

        def raise_for_status(self):
            pass

    call_count = {"n": 0}

    def fake_post(url, files, data, timeout):
        content = responses[call_count["n"]]
        call_count["n"] += 1
        return FakeResponse(content)

    monkeypatch.setattr("ban_ingest.requests.post", fake_post)

    out1, out2 = tmp_path / "out1.csv", tmp_path / "out2.csv"
    geocode_csv(input_path, out1)
    geocode_csv(input_path, out2)

    assert out1.read_bytes() == out2.read_bytes()


def test_ban_object_key_format():
    key = ban_object_key("2024", "75", publication="2026-04")
    assert key == "ban/publication=2026-04/year=2024/department=75/75.csv"