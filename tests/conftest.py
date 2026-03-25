from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_data_team.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        openai_api_key=None,
        openai_model="gpt-4.1-mini",
        openai_base_url="https://api.openai.com/v1",
        agentmail_api_key="test-agentmail-key",
        agentmail_base_url="https://api.agentmail.to/v0",
        agentmail_inbox_id="autonomous-data-team@agentmail.to",
        authorized_senders=["owner@example.com"],
        archive_repo_url="https://github.com/data-is-plural/newsletter-archive.git",
        archive_cache_dir=tmp_path / "archive_cache",
        runs_dir=tmp_path / "runs",
        sample_download_bytes_limit=5_242_880,
        extractor_provider="none",
        extractor_api_key=None,
        extractor_base_url=None,
        extractor_timeout_seconds=5.0,
        tavily_api_key=None,
        tavily_extract_depth="advanced",
        swarm_orchestrator="heuristic",
        crewai_home_dir=tmp_path / "crewai_home",
        max_dataset_rows=50000,
        bind_host="127.0.0.1",
        port=8000,
        worker_poll_interval=300,
        db_path=tmp_path / "state" / "opportunities.sqlite3",
        recent_default_count=25,
    )
