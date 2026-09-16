import io
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "ingestion" / "enrichment"))
from filosofi_ingest import download, filosofi_local_path, filosofi_object_key


def make_zip_bytes():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("communes.csv", "code_commune,revenu\n75101,25000\n")
    return buf.getvalue()


def test_download_succeeds_with_valid_zip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    zip_bytes = make_zip_bytes()

    class FakeResponse:
        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield zip_bytes

    def fake_get(url, stream=True, timeout=60):
        return FakeResponse()

    monkeypatch.setattr("filosofi_ingest.requests.get", fake_get)

    dest = download("2021")

    assert dest == filosofi_local_path("2021")
    assert dest.exists()
    assert dest.read_bytes() == zip_bytes


def test_download_raises_on_invalid_archive(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class FakeResponse:
        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield b"not a zip file"

    def fake_get(url, stream=True, timeout=60):
        return FakeResponse()

    monkeypatch.setattr("filosofi_ingest.requests.get", fake_get)

    try:
        download("2021")
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "validation" in str(exc)


def test_filosofi_object_key_format():
    assert filosofi_object_key("2021") == "filosofi/vintage=2021/communes.zip"


def test_filosofi_local_path_format():
    assert str(filosofi_local_path("2021")) == str(Path("data_raw/filosofi/2021/communes.zip"))