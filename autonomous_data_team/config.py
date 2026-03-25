from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class Settings:
    openai_api_key: str | None
    openai_model: str
    openai_base_url: str
    agentmail_api_key: str | None
    agentmail_base_url: str
    agentmail_inbox_id: str
    authorized_senders: list[str]
    archive_repo_url: str
    archive_cache_dir: Path
    runs_dir: Path
    sample_download_bytes_limit: int
    extractor_provider: str
    extractor_api_key: str | None
    extractor_base_url: str | None
    extractor_timeout_seconds: float
    tavily_api_key: str | None
    tavily_extract_depth: str
    swarm_orchestrator: str
    crewai_home_dir: Path
    max_dataset_rows: int
    bind_host: str
    port: int
    worker_poll_interval: int
    db_path: Path
    recent_default_count: int

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        archive_cache_dir = Path(os.getenv("ARCHIVE_CACHE_DIR", "./archive_cache")).resolve()
        runs_dir = Path(os.getenv("RUNS_DIR", "./runs")).resolve()
        return cls(
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            openai_base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            agentmail_api_key=os.getenv("AGENTMAIL_API_KEY"),
            agentmail_base_url=os.getenv("AGENTMAIL_BASE_URL", "https://api.agentmail.to/v0"),
            agentmail_inbox_id=os.getenv("AGENTMAIL_INBOX_ID", "autonomous-data-team@agentmail.to"),
            authorized_senders=_split_csv(os.getenv("AUTHORIZED_SENDERS", "")),
            archive_repo_url=os.getenv(
                "ARCHIVE_REPO_URL",
                "https://github.com/data-is-plural/newsletter-archive.git",
            ),
            archive_cache_dir=archive_cache_dir,
            runs_dir=runs_dir,
            sample_download_bytes_limit=int(os.getenv("SAMPLE_DOWNLOAD_BYTES_LIMIT", "5242880")),
            extractor_provider=os.getenv("EXTRACTOR_PROVIDER", "none"),
            extractor_api_key=os.getenv("EXTRACTOR_API_KEY"),
            extractor_base_url=os.getenv("EXTRACTOR_BASE_URL"),
            extractor_timeout_seconds=float(os.getenv("EXTRACTOR_TIMEOUT_SECONDS", "20")),
            tavily_api_key=os.getenv("TAVILY_API_KEY") or os.getenv("EXTRACTOR_API_KEY"),
            tavily_extract_depth=os.getenv("TAVILY_EXTRACT_DEPTH", "advanced"),
            swarm_orchestrator=os.getenv("SWARM_ORCHESTRATOR", "crewai"),
            crewai_home_dir=Path(
                os.getenv("CREWAI_HOME_DIR", str(runs_dir / ".crewai_home"))
            ).resolve(),
            max_dataset_rows=int(os.getenv("MAX_DATASET_ROWS", "50000")),
            bind_host=os.getenv("BIND_HOST", "0.0.0.0"),
            port=int(os.getenv("PORT", "8000")),
            worker_poll_interval=int(os.getenv("WORKER_POLL_INTERVAL", "300")),
            db_path=Path(os.getenv("DB_PATH", str(runs_dir / "opportunities.sqlite3"))).resolve(),
            recent_default_count=int(os.getenv("RECENT_DEFAULT_COUNT", "25")),
        )

    def ensure_directories(self) -> None:
        self.archive_cache_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.crewai_home_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


def _split_csv(value: str) -> list[str]:
    return [item.strip().lower() for item in value.split(",") if item.strip()]
