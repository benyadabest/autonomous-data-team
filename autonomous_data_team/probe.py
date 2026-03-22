from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd

from .config import Settings
from .extractor import BaseExtractor
from .models import DatasetEntry, ProbeResult


def probe_entry(entry: DatasetEntry, settings: Settings, extractor: BaseExtractor) -> ProbeResult:
    links = list(dict.fromkeys(entry.source_links + entry.as_seen_in_links))
    if not links:
        return ProbeResult(probe_mode_used="text_only", blocked_reason="no_links_available")

    for url in links:
        direct = probe_url(url, settings)
        if direct.downloadable or direct.tabular_hint:
            return direct
        extracted = extractor.extract(entry, url)
        if extracted.downloadable and extracted.resolved_url:
            fallback = probe_url(extracted.resolved_url, settings, mode_override=settings.extractor_provider)
            fallback.structured_text = extracted.structured_text
            fallback.source_url = url
            return fallback
        if extracted.structured_text:
            return ProbeResult(
                probe_mode_used=settings.extractor_provider,
                resolved_url=extracted.resolved_url,
                content_type=extracted.content_type,
                tabular_hint=extracted.tabular_hint,
                structured_text=extracted.structured_text,
                blocked_reason=extracted.blocked_reason,
                downloadable=False,
                source_url=url,
            )
    return ProbeResult(probe_mode_used="text_only", blocked_reason="no_supported_dataset_found")


def probe_url(url: str, settings: Settings, mode_override: Optional[str] = None) -> ProbeResult:
    mode = mode_override or "direct"
    if url.startswith(("http://", "https://")):
        try:
            with httpx.Client(timeout=settings.extractor_timeout_seconds, follow_redirects=True) as client:
                response = client.get(url)
                response.raise_for_status()
            return _probe_bytes(
                raw_bytes=response.content[: settings.sample_download_bytes_limit],
                source_url=url,
                resolved_url=str(response.url),
                content_type=response.headers.get("content-type"),
                mode=mode,
            )
        except httpx.HTTPStatusError as exc:
            return ProbeResult(
                probe_mode_used=mode,
                resolved_url=str(exc.response.url) if exc.response is not None else url,
                content_type=exc.response.headers.get("content-type") if exc.response is not None else None,
                blocked_reason=f"http_{exc.response.status_code}" if exc.response is not None else "http_error",
                downloadable=False,
                source_url=url,
            )
        except httpx.HTTPError as exc:
            return ProbeResult(
                probe_mode_used=mode,
                resolved_url=url,
                blocked_reason=f"network_error:{exc.__class__.__name__}",
                downloadable=False,
                source_url=url,
            )

    path = Path(url)
    raw_bytes = path.read_bytes()[: settings.sample_download_bytes_limit]
    return _probe_bytes(
        raw_bytes=raw_bytes,
        source_url=url,
        resolved_url=str(path.resolve()),
        content_type=None,
        mode=mode,
    )


def _probe_bytes(
    raw_bytes: bytes,
    source_url: str,
    resolved_url: str,
    content_type: Optional[str],
    mode: str,
) -> ProbeResult:
    suffix = Path(resolved_url).suffix.lower()
    try:
        if suffix in {".csv", ".tsv"}:
            sep = "\t" if suffix == ".tsv" else ","
            frame = pd.read_csv(io.BytesIO(raw_bytes), sep=sep, nrows=200)
            return _probe_frame(frame, source_url, resolved_url, content_type, mode, downloadable=True)
        if suffix == ".json":
            data = json.loads(raw_bytes.decode("utf-8"))
            frame = _frame_from_json(data)
            return _probe_frame(frame, source_url, resolved_url, content_type, mode, downloadable=True)
        if suffix == ".parquet":
            frame = pd.read_parquet(io.BytesIO(raw_bytes))
            return _probe_frame(frame, source_url, resolved_url, content_type, mode, downloadable=True)
        if suffix == ".zip":
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                csv_names = [name for name in zf.namelist() if name.lower().endswith((".csv", ".tsv"))]
                if len(csv_names) == 1:
                    with zf.open(csv_names[0]) as handle:
                        sep = "\t" if csv_names[0].lower().endswith(".tsv") else ","
                        frame = pd.read_csv(handle, sep=sep, nrows=200)
                    return _probe_frame(frame, source_url, resolved_url, content_type, mode, downloadable=True)
            return ProbeResult(
                probe_mode_used=mode,
                resolved_url=resolved_url,
                content_type=content_type,
                blocked_reason="zip_without_single_table_csv",
                downloadable=False,
                source_url=source_url,
            )
    except Exception as exc:  # pragma: no cover - exercised through result contract
        return ProbeResult(
            probe_mode_used=mode,
            resolved_url=resolved_url,
            content_type=content_type,
            blocked_reason=f"probe_failed:{exc.__class__.__name__}",
            downloadable=False,
            source_url=source_url,
        )

    return ProbeResult(
        probe_mode_used=mode,
        resolved_url=resolved_url,
        content_type=content_type,
        blocked_reason="unsupported_format",
        downloadable=False,
        source_url=source_url,
    )


def _frame_from_json(data: object) -> pd.DataFrame:
    if isinstance(data, list):
        return pd.json_normalize(data[:200])
    if isinstance(data, dict):
        if all(isinstance(value, list) for value in data.values()):
            return pd.DataFrame(data)
        return pd.json_normalize([data])
    raise ValueError("JSON payload is not tabular")


def _probe_frame(
    frame: pd.DataFrame,
    source_url: str,
    resolved_url: str,
    content_type: Optional[str],
    mode: str,
    downloadable: bool,
) -> ProbeResult:
    schema = [{"name": str(name), "dtype": str(dtype)} for name, dtype in frame.dtypes.items()]
    return ProbeResult(
        probe_mode_used=mode,
        resolved_url=resolved_url,
        content_type=content_type,
        tabular_hint=True,
        downloadable=downloadable,
        row_estimate=int(frame.shape[0]),
        column_estimate=int(frame.shape[1]),
        basic_schema=schema,
        source_url=source_url,
    )
