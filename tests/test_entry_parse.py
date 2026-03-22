from __future__ import annotations

from pathlib import Path

from autonomous_data_team.entry_parse import parse_edition_file


def test_parse_edition_file_extracts_entries() -> None:
    fixture = Path("tests/fixtures/archive/editions/2025-01-08.md")
    edition, entries = parse_edition_file(fixture)

    assert edition.edition_date == "2025-01-08"
    assert len(entries) == 2
    assert entries[0].title == "Traffic crashes"
    assert entries[0].source_links == ["https://example.com/crashes.csv"]
    assert entries[0].as_seen_in_links == ["https://example.com/metro-analysis"]
    assert "Citywide crash records" in entries[0].description
    assert entries[1].title == "School lunches"


def test_parse_edition_file_skips_feedback_footer(tmp_path: Path) -> None:
    fixture = tmp_path / "2025-08-27.md"
    fixture.write_text(
        "# Data Is Plural\n\n"
        "## 2025.08.27 edition\n\n"
        "Useful data. [CSV](https://example.com/data.csv).\n\n"
        "Dataset suggestions? Criticism? Praise? Send full-bodied feedback to jsvine@gmail.com, or just reply to this email.\n",
        encoding="utf-8",
    )

    _, entries = parse_edition_file(fixture)

    assert len(entries) == 1
    assert entries[0].title == "Useful data"
