from __future__ import annotations

import sys
import types

from autonomous_data_team.extractor import ApiExtractor, BrowserExtractor, NoneExtractor, TavilyExtractor
from autonomous_data_team.models import DatasetEntry


class _FakeResponse:
    def __init__(self, text="", url="https://example.com/page", headers=None, json_data=None):
        self.text = text
        self.url = url
        self.headers = headers or {}
        self._json_data = json_data or {}
        self.content = text.encode("utf-8")

    def raise_for_status(self):
        return None

    def json(self):
        return self._json_data


class _FakeClient:
    def __init__(self, response):
        self.response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def get(self, url):
        return self.response

    def post(self, url, json=None, headers=None):
        return self.response


def _entry() -> DatasetEntry:
    return DatasetEntry(
        entry_id="entry-1",
        edition_date="2025-01-08",
        ordinal=0,
        title="Traffic crashes",
        description="Crash records",
        source_links=["https://example.com/page"],
        as_seen_in_links=[],
        raw_markdown="Traffic crashes.",
    )


def test_none_extractor_returns_disabled_marker() -> None:
    result = NoneExtractor().extract(_entry(), "https://example.com/page")
    assert result.blocked_reason == "extractor_disabled"
    assert result.downloadable is False


def test_browser_extractor_detects_download_link(settings, monkeypatch) -> None:
    html = '<html><body><a href="/downloads/crashes.csv">Download CSV</a></body></html>'
    monkeypatch.setattr(
        "autonomous_data_team.extractor.httpx.Client",
        lambda **kwargs: _FakeClient(_FakeResponse(text=html, headers={"content-type": "text/html"})),
    )

    result = BrowserExtractor(settings).extract(_entry(), "https://example.com/page")

    assert result.downloadable is True
    assert result.resolved_url == "https://example.com/downloads/crashes.csv"


def test_api_extractor_uses_configured_response(settings, monkeypatch) -> None:
    settings.extractor_provider = "api"
    settings.extractor_api_key = "extractor-key"
    settings.extractor_base_url = "https://extractor.example.com"
    response = _FakeResponse(
        json_data={
            "resolved_url": "https://example.com/resolved.csv",
            "content_type": "text/csv",
            "tabular_hint": True,
            "structured_text": "Dataset landing page",
            "blocked_reason": None,
            "downloadable": True,
        }
    )
    monkeypatch.setattr(
        "autonomous_data_team.extractor.httpx.Client",
        lambda **kwargs: _FakeClient(response),
    )

    result = ApiExtractor(settings).extract(_entry(), "https://example.com/page")

    assert result.downloadable is True
    assert result.resolved_url == "https://example.com/resolved.csv"


def test_tavily_extractor_uses_raw_content(settings, monkeypatch) -> None:
    settings.extractor_provider = "tavily"
    settings.tavily_api_key = "tvly-test"

    class FakeTavilyClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def extract(self, urls, extract_depth="advanced"):
            assert urls == "https://example.com/page"
            assert extract_depth == "advanced"
            return {
                "results": [
                    {
                        "url": "https://example.com/page",
                        "raw_content": "This dataset page lists columns, rows, and a downloadable CSV dataset.",
                    }
                ]
            }

    fake_module = types.SimpleNamespace(TavilyClient=FakeTavilyClient)
    monkeypatch.setitem(sys.modules, "tavily", fake_module)

    result = TavilyExtractor(settings).extract(_entry(), "https://example.com/page")

    assert result.downloadable is False
    assert result.tabular_hint is True
    assert "downloadable CSV" in result.structured_text
