from __future__ import annotations

import subprocess
from pathlib import Path

from .config import Settings


def sync_archive_repo(settings: Settings) -> Path:
    settings.ensure_directories()
    repo_dir = settings.archive_cache_dir / "newsletter-archive"
    if (repo_dir / ".git").exists():
        subprocess.run(
            ["git", "-C", str(repo_dir), "pull", "--ff-only"],
            check=True,
            capture_output=True,
            text=True,
        )
    elif repo_dir.exists():
        raise RuntimeError(f"Archive directory exists but is not a git repo: {repo_dir}")
    else:
        subprocess.run(
            ["git", "clone", "--depth", "1", settings.archive_repo_url, str(repo_dir)],
            check=True,
            capture_output=True,
            text=True,
        )
    return repo_dir


def find_edition_files(repo_dir: Path) -> list[Path]:
    paths = []
    for path in repo_dir.rglob("*.md"):
        if path.name.lower() == "readme.md":
            continue
        if "archive" in path.parts or "editions" in path.parts or _looks_like_edition(path.name):
            paths.append(path)
    return sorted(paths)


def _looks_like_edition(name: str) -> bool:
    return any(ch.isdigit() for ch in name) and any(sep in name for sep in ("-", ".", "_"))

