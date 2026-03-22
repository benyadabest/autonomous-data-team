from __future__ import annotations

import json
from pathlib import Path

import typer

from .config import Settings
from .service import inbox_worker, process_inbox_once, score_archive, score_edition, sync_archive, top_opportunities
from .storage import Store
from .swarm import run_local_dataset_swarm

app = typer.Typer(add_completion=False, help="Archive-first dataset opportunity scoper.")


def _settings_and_store() -> tuple[Settings, Store]:
    settings = Settings.from_env()
    settings.ensure_directories()
    return settings, Store(settings.db_path)


@app.command("sync-archive")
def sync_archive_command() -> None:
    settings, store = _settings_and_store()
    result = sync_archive(settings, store)
    typer.echo(json.dumps(result, indent=2))


@app.command("score-archive")
def score_archive_command(
    mode: str = typer.Option("full", "--mode"),
    limit: int | None = typer.Option(None, "--limit"),
) -> None:
    settings, store = _settings_and_store()
    result = score_archive(settings, store, mode=mode, limit=limit)
    typer.echo(json.dumps(result, indent=2))


@app.command("score-edition")
def score_edition_command(date: str = typer.Option(..., "--date")) -> None:
    settings, store = _settings_and_store()
    result = score_edition(settings, store, edition_date=date)
    typer.echo(json.dumps(result, indent=2))


@app.command("top-opportunities")
def top_opportunities_command(limit: int = typer.Option(50, "--limit")) -> None:
    settings, store = _settings_and_store()
    result = top_opportunities(store, limit=limit)
    typer.echo(json.dumps(result, indent=2))


@app.command("inbox-worker")
def inbox_worker_command(
    poll_interval: int = typer.Option(300, "--poll-interval"),
    once: bool = typer.Option(False, "--once"),
) -> None:
    settings, store = _settings_and_store()
    if once:
        result = process_inbox_once(settings, store)
        typer.echo(json.dumps(result, indent=2))
        return
    inbox_worker(settings, store, poll_interval)


@app.command("analyze-dataset")
def analyze_dataset_command(
    path: str = typer.Option(..., "--path"),
    notes: str = typer.Option("", "--notes"),
) -> None:
    settings, store = _settings_and_store()
    result = run_local_dataset_swarm(path=str(Path(path).resolve()), requester_notes=notes, settings=settings, store=store)
    typer.echo(json.dumps(result, indent=2))
