from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Iterable

from .models import DatasetEntry, Edition

DATE_PATTERNS = [
    re.compile(r"(20\d{2})[-_.](\d{2})[-_.](\d{2})"),
    re.compile(r"(20\d{2})\.(\d{2})\.(\d{2})"),
]
LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
URL_RE = re.compile(r"https?://[^\s)>]+")
AS_SEEN_IN_RE = re.compile(r"As seen in\s*:\s*(.*)", re.IGNORECASE | re.DOTALL)


def parse_edition_file(path: Path) -> tuple[Edition, list[DatasetEntry]]:
    raw_markdown = path.read_text(encoding="utf-8")
    edition_date = extract_edition_date(path, raw_markdown)
    title = extract_title(raw_markdown, edition_date)
    edition = Edition(
        edition_date=edition_date,
        title=title,
        path=str(path),
        raw_markdown=raw_markdown,
    )
    entries = list(parse_dataset_entries(raw_markdown, edition_date))
    return edition, entries


def extract_edition_date(path: Path, raw_markdown: str) -> str:
    candidates = [str(path)]
    candidates.extend(raw_markdown.splitlines()[:10])
    for candidate in candidates:
        for pattern in DATE_PATTERNS:
            match = pattern.search(candidate)
            if match:
                year, month, day = match.groups()
                return f"{year}-{month}-{day}"
    raise ValueError(f"Could not determine edition date for {path}")


def extract_title(raw_markdown: str, edition_date: str) -> str:
    for line in raw_markdown.splitlines():
        line = line.strip()
        if line.startswith("# "):
            return line[2:].strip()
        if line.startswith("## "):
            return line[3:].strip()
    return f"Data Is Plural {edition_date}"


def parse_dataset_entries(raw_markdown: str, edition_date: str) -> Iterable[DatasetEntry]:
    paragraphs = [chunk.strip() for chunk in re.split(r"\n\s*\n", raw_markdown) if chunk.strip()]
    ordinal = 0
    for chunk in paragraphs:
        if _skip_chunk(chunk):
            continue
        source_links, as_seen_in_links = extract_links(chunk)
        if not source_links and not as_seen_in_links:
            continue
        title = extract_entry_title(chunk)
        description = summarize_chunk(chunk)
        entry_id = make_entry_id(edition_date, ordinal, title)
        yield DatasetEntry(
            entry_id=entry_id,
            edition_date=edition_date,
            ordinal=ordinal,
            title=title,
            description=description,
            source_links=source_links,
            as_seen_in_links=as_seen_in_links,
            raw_markdown=chunk,
        )
        ordinal += 1


def extract_links(chunk: str) -> tuple[list[str], list[str]]:
    markdown_links = LINK_RE.findall(chunk)
    urls = [url for _, url in markdown_links]
    bare_urls = [url for url in URL_RE.findall(LINK_RE.sub("", chunk)) if url not in urls]
    all_links = urls + bare_urls
    as_seen_in_links: list[str] = []
    match = AS_SEEN_IN_RE.search(chunk)
    if match:
        seen_text = match.group(1)
        seen_links = [url for _, url in LINK_RE.findall(seen_text)]
        seen_links.extend(URL_RE.findall(LINK_RE.sub("", seen_text)))
        as_seen_in_links = list(dict.fromkeys(seen_links))
    source_links = [url for url in all_links if url not in as_seen_in_links]
    return list(dict.fromkeys(source_links)), as_seen_in_links


def extract_entry_title(chunk: str) -> str:
    cleaned = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", chunk)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned.startswith(("#", "*", "-")):
        cleaned = cleaned.lstrip("#*- ").strip()
    as_seen_index = cleaned.lower().find("as seen in:")
    if as_seen_index != -1:
        cleaned = cleaned[:as_seen_index].strip()
    first_sentence = cleaned.split(". ", 1)[0].strip().strip(".")
    if len(first_sentence) > 120:
        first_sentence = first_sentence[:117].rstrip() + "..."
    return first_sentence or "Untitled dataset"


def summarize_chunk(chunk: str) -> str:
    cleaned = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", chunk)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def make_entry_id(edition_date: str, ordinal: int, title: str) -> str:
    digest = hashlib.sha1(f"{edition_date}:{ordinal}:{title}".encode("utf-8")).hexdigest()[:12]
    return f"{edition_date}-{ordinal:03d}-{digest}"


def _skip_chunk(chunk: str) -> bool:
    normalized = chunk.strip()
    lower = normalized.lower()
    if normalized.startswith("#"):
        return True
    if "jsvine@gmail.com" in lower:
        return True
    return any(
        lower.startswith(prefix) or prefix in lower
        for prefix in (
            "data is plural",
            "previous editions",
            "about",
            "support data is plural",
            "tip jar",
            "dataset suggestions? criticism? praise?",
            "or just reply to this email",
        )
    )
