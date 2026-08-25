import gzip
import io

import pytest
import requests

from src.ingestion.download_dvf import download
from src.ingestion.paths import local_path


class FakeResponse:
    def __init__(self, content=b"", raise_exc=None):
        self._content = content
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc

    def iter_content(self, chunk_size=8192):
        yield self._content


def test_download_raises_on_http_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def fake_get(url, stream=True, timeout=30):
        return FakeResponse(raise_exc=requests.exceptions.HTTPError("404 Client Error"))

    monkeypatch.setattr("src.ingestion.download_dvf.requests.get", fake_get)

    with pytest.raises(RuntimeError, match="download failed"):
        download("2024", "999")


def test_download_raises_on_invalid_archive(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def fake_get(url, stream=True, timeout=30):
        return FakeResponse(content=b"not gzip data")

    monkeypatch.setattr("src.ingestion.download_dvf.requests.get", fake_get)

    with pytest.raises(RuntimeError, match="failed validation"):
        download("2024", "75")


def test_download_succeeds_with_valid_gzip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(b"valid,csv,content\n" * 10)
    gzip_bytes = buf.getvalue()

    def fake_get(url, stream=True, timeout=30):
        return FakeResponse(content=gzip_bytes)

    monkeypatch.setattr("src.ingestion.download_dvf.requests.get", fake_get)

    dest = download("2024", "75")
    assert dest == local_path("2024", "75")
    assert dest.exists()