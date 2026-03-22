from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import httpx
from bs4 import BeautifulSoup

from .config import Settings
from .models import DatasetEntry, ExtractorResult

TABULAR_EXTENSIONS = (".csv", ".tsv", ".json", ".parquet", ".zip")


class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, entry: DatasetEntry, url: str) -> ExtractorResult:
        raise NotImplementedError


class NoneExtractor(BaseExtractor):
    def extract(self, entry: DatasetEntry, url: str) -> ExtractorResult:
        return ExtractorResult(
            resolved_url=url,
            blocked_reason="extractor_disabled",
            downloadable=False,
        )


class BrowserExtractor(BaseExtractor):
    def __init__(self, settings: Settings) -> None:
        self._timeout = settings.extractor_timeout_seconds

    def extract(self, entry: DatasetEntry, url: str) -> ExtractorResult:
        with httpx.Client(timeout=self._timeout, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(" ", strip=True)[:4000]
        resolved = str(response.url)
        downloadable = resolved.lower().endswith(TABULAR_EXTENSIONS)
        if not downloadable:
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                if href.lower().endswith(TABULAR_EXTENSIONS):
                    resolved = str(httpx.URL(response.url).join(href))
                    downloadable = True
                    break
        return ExtractorResult(
            resolved_url=resolved,
            content_type=response.headers.get("content-type"),
            tabular_hint=downloadable,
            structured_text=text,
            blocked_reason=None if downloadable or text else "no_structured_content",
            downloadable=downloadable,
        )


class ApiExtractor(BaseExtractor):
    def __init__(self, settings: Settings) -> None:
        self._base_url = settings.extractor_base_url
        self._api_key = settings.extractor_api_key
        self._timeout = settings.extractor_timeout_seconds

    def extract(self, entry: DatasetEntry, url: str) -> ExtractorResult:
        if not self._base_url or not self._api_key:
            return ExtractorResult(
                resolved_url=url,
                blocked_reason="extractor_api_not_configured",
                downloadable=False,
            )
        payload = {
            "entry": {
                "entry_id": entry.entry_id,
                "title": entry.title,
                "description": entry.description,
            },
            "url": url,
        }
        headers = {"Authorization": f"Bearer {self._api_key}"}
        with httpx.Client(timeout=self._timeout, follow_redirects=True) as client:
            response = client.post(self._base_url, json=payload, headers=headers)
            response.raise_for_status()
        data = response.json()
        return ExtractorResult(
            resolved_url=data.get("resolved_url", url),
            content_type=data.get("content_type"),
            tabular_hint=bool(data.get("tabular_hint")),
            structured_text=data.get("structured_text"),
            blocked_reason=data.get("blocked_reason"),
            downloadable=bool(data.get("downloadable")),
        )


class TavilyExtractor(BaseExtractor):
    def __init__(self, settings: Settings) -> None:
        self._api_key = settings.tavily_api_key
        self._extract_depth = settings.tavily_extract_depth

    def extract(self, entry: DatasetEntry, url: str) -> ExtractorResult:
        if not self._api_key:
            return ExtractorResult(
                resolved_url=url,
                blocked_reason="tavily_not_configured",
                downloadable=False,
            )

        try:
            from tavily import TavilyClient
        except ImportError:
            return ExtractorResult(
                resolved_url=url,
                blocked_reason="tavily_package_missing",
                downloadable=False,
            )

        client = TavilyClient(api_key=self._api_key)
        response = client.extract(urls=url, extract_depth=self._extract_depth)
        result = _extract_tavily_result(response, url)
        raw_content = result.get("raw_content")
        resolved_url = result.get("url", url)
        downloadable = str(resolved_url).lower().endswith(TABULAR_EXTENSIONS)
        tabular_hint = downloadable or _has_tabular_hint(raw_content)
        blocked_reason = result.get("error")
        if not raw_content and not blocked_reason:
            blocked_reason = "tavily_empty_result"
        return ExtractorResult(
            resolved_url=resolved_url,
            content_type=None,
            tabular_hint=tabular_hint,
            structured_text=raw_content[:4000] if isinstance(raw_content, str) else None,
            blocked_reason=blocked_reason,
            downloadable=downloadable,
        )


def build_extractor(settings: Settings) -> BaseExtractor:
    provider = settings.extractor_provider.lower()
    if provider == "browser":
        return BrowserExtractor(settings)
    if provider == "api":
        return ApiExtractor(settings)
    if provider == "tavily":
        return TavilyExtractor(settings)
    return NoneExtractor()


def _extract_tavily_result(response: Any, fallback_url: str) -> dict[str, Any]:
    if isinstance(response, dict):
        results = response.get("results") or []
        failed = response.get("failed_results") or []
        if results:
            first = results[0]
            if isinstance(first, dict):
                return first
        if failed:
            first_failed = failed[0]
            if isinstance(first_failed, dict):
                return {"url": fallback_url, "error": first_failed.get("error", "tavily_failed")}
    return {"url": fallback_url, "error": "tavily_unexpected_response"}


def _has_tabular_hint(text: Any) -> bool:
    if not isinstance(text, str):
        return False
    lower = text.lower()
    return any(token in lower for token in ("csv", "tsv", "parquet", "table", "columns", "rows", "dataset"))
