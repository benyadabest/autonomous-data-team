from __future__ import annotations

import zipfile
from pathlib import Path

import httpx

from autonomous_data_team.probe import probe_url


def test_probe_csv_fixture(settings) -> None:
    fixture = str(Path("tests/fixtures/data/sample.csv"))
    result = probe_url(fixture, settings)

    assert result.downloadable is True
    assert result.tabular_hint is True
    assert result.row_estimate == 3
    assert result.column_estimate == 3


def test_probe_json_fixture(settings) -> None:
    fixture = str(Path("tests/fixtures/data/sample.json"))
    result = probe_url(fixture, settings)

    assert result.downloadable is True
    assert result.column_estimate == 3
    assert {item["name"] for item in result.basic_schema} == {"school", "calories", "protein"}


def test_probe_single_csv_zip(tmp_path: Path, settings) -> None:
    archive_path = tmp_path / "sample.zip"
    csv_path = tmp_path / "inside.csv"
    csv_path.write_text("col_a,col_b\n1,2\n3,4\n", encoding="utf-8")
    with zipfile.ZipFile(archive_path, "w") as zf:
        zf.write(csv_path, arcname="inside.csv")

    result = probe_url(str(archive_path), settings)

    assert result.downloadable is True
    assert result.row_estimate == 2
    assert result.column_estimate == 2


def test_probe_url_returns_blocked_result_on_http_error(settings, monkeypatch) -> None:
    request = httpx.Request("GET", "https://example.com/blocked")
    response = httpx.Response(403, request=request)

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def get(self, url):
            raise httpx.HTTPStatusError("blocked", request=request, response=response)

    monkeypatch.setattr("autonomous_data_team.probe.httpx.Client", lambda **kwargs: _Client())

    result = probe_url("https://example.com/blocked", settings)

    assert result.downloadable is False
    assert result.blocked_reason == "http_403"
