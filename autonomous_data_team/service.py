from __future__ import annotations

import json
import time
from pathlib import Path

from .agents import AgentRunner
from .archive_sync import find_edition_files, sync_archive_repo
from .config import Settings
from .entry_parse import parse_edition_file
from .extractor import build_extractor
from .mail import AgentMailAPI, has_supported_dataset_attachment, is_authorized_sender, parse_command
from .probe import probe_entry
from .reporting import write_run_artifacts
from .storage import Store
from .swarm import run_attachment_swarm


def sync_archive(settings: Settings, store: Store, repo_dir: Path | None = None) -> dict[str, int]:
    repo = repo_dir or sync_archive_repo(settings)
    edition_files = find_edition_files(repo)
    edition_count = 0
    entry_count = 0
    for path in edition_files:
        try:
            edition, entries = parse_edition_file(path)
        except ValueError:
            continue
        store.upsert_edition(edition)
        edition_count += 1
        store.replace_entries_for_edition(edition.edition_date, entries)
        entry_count += len(entries)
    return {"editions": edition_count, "entries": entry_count}


def score_archive(settings: Settings, store: Store, mode: str, limit: int | None = None) -> dict[str, str]:
    settings.ensure_directories()
    run = store.create_run(mode=mode, metadata={"limit": limit})
    run_dir = settings.runs_dir / run.run_id
    try:
        entries = _entries_for_mode(store, mode, limit, settings.recent_default_count)
        scored = _score_entries(entries, settings)
        scored.sort(key=lambda item: item.overall_priority_score, reverse=True)
        _, _, summary_path = write_run_artifacts(run_dir, scored)
        for score in scored:
            store.insert_score(run.run_id, score)
        store.complete_run(run.run_id, str(summary_path))
        return {
            "run_id": run.run_id,
            "summary_path": str(summary_path),
            "scored_entries": str(len(scored)),
        }
    except Exception as exc:
        store.fail_run(run.run_id, str(exc))
        raise


def score_edition(settings: Settings, store: Store, edition_date: str) -> dict[str, str]:
    settings.ensure_directories()
    run = store.create_run(mode="edition", metadata={"edition_date": edition_date})
    run_dir = settings.runs_dir / run.run_id
    try:
        entries = store.list_entries(edition_date=edition_date)
        scored = _score_entries(entries, settings)
        scored.sort(key=lambda item: item.overall_priority_score, reverse=True)
        _, _, summary_path = write_run_artifacts(run_dir, scored)
        for score in scored:
            store.insert_score(run.run_id, score)
        store.complete_run(run.run_id, str(summary_path))
        return {
            "run_id": run.run_id,
            "summary_path": str(summary_path),
            "scored_entries": str(len(scored)),
        }
    except Exception as exc:
        store.fail_run(run.run_id, str(exc))
        raise


def top_opportunities(store: Store, limit: int) -> list[dict[str, object]]:
    rows = store.top_scores(limit=limit)
    results = []
    for row in rows:
        results.append(
            {
                "entry_id": row["entry_id"],
                "edition_date": row["edition_date"],
                "title": row["title"],
                "overall_priority_score": row["overall_priority_score"],
                "next_step_recommendation": row["next_step_recommendation"],
                "probe_mode_used": row["probe_mode_used"],
                "application_ideas": json.loads(row["application_ideas_json"]),
            }
        )
    return results


def process_inbox_once(settings: Settings, store: Store) -> list[str]:
    client = AgentMailAPI(settings)
    responses: list[str] = []
    for message in client.list_unread_messages():
        if not is_authorized_sender(message, settings):
            client.reply_all(message.message_id, "Sender is not authorized for this inbox.")
            client.update_labels(message.message_id, ["failed"], ["unread"])
            responses.append(f"{message.message_id}:unauthorized")
            continue
        if has_supported_dataset_attachment(message):
            client.update_labels(message.message_id, ["processing"], ["unread"])
            try:
                reply_text, reply_attachments, run_id = run_attachment_swarm(message, client, settings, store)
                client.reply_all(message.message_id, reply_text, attachments=reply_attachments)
                client.update_labels(message.message_id, ["processed"], ["processing"])
                responses.append(f"{message.message_id}:attachment_processed:{run_id}")
            except Exception as exc:
                client.reply_all(message.message_id, f"Attachment swarm run failed: {exc}")
                client.update_labels(message.message_id, ["failed"], ["processing"])
                responses.append(f"{message.message_id}:attachment_failed")
            continue
        command = parse_command(message)
        if command is None:
            client.reply_all(
                message.message_id,
                "Unsupported command. Use RUN FULL ARCHIVE, RUN RECENT <N>, RUN EDITION <YYYY-MM-DD>, or TOP <N>.",
            )
            client.update_labels(message.message_id, ["failed"], ["unread"])
            responses.append(f"{message.message_id}:invalid")
            continue

        client.update_labels(message.message_id, ["processing"], ["unread"])
        try:
            result_text = _execute_mail_command(command.action, command.arg, settings, store)
            client.reply_all(message.message_id, result_text)
            client.update_labels(message.message_id, ["processed"], ["processing"])
            responses.append(f"{message.message_id}:processed")
        except Exception as exc:
            client.reply_all(message.message_id, f"Run failed: {exc}")
            client.update_labels(message.message_id, ["failed"], ["processing"])
            responses.append(f"{message.message_id}:failed")
    return responses


def inbox_worker(settings: Settings, store: Store, poll_interval: int) -> None:
    while True:
        process_inbox_once(settings, store)
        time.sleep(poll_interval)


def _execute_mail_command(action: str, arg: str | None, settings: Settings, store: Store) -> str:
    if action == "run_full_archive":
        sync_archive(settings, store)
        result = score_archive(settings, store, mode="full")
        return f"Scored full archive. Run ID: {result['run_id']}. Summary: {result['summary_path']}"
    if action == "run_recent":
        sync_archive(settings, store)
        result = score_archive(settings, store, mode="recent", limit=int(arg or settings.recent_default_count))
        return f"Scored recent archive window. Run ID: {result['run_id']}. Summary: {result['summary_path']}"
    if action == "run_edition":
        sync_archive(settings, store)
        result = score_edition(settings, store, edition_date=str(arg))
        return f"Scored edition {arg}. Run ID: {result['run_id']}. Summary: {result['summary_path']}"
    if action == "top":
        top = top_opportunities(store, int(arg or 10))
        if not top:
            return "No scored opportunities found yet."
        lines = ["Top opportunities:"]
        for item in top:
            lines.append(
                f"- {item['title']} ({item['edition_date']}): {item['overall_priority_score']:.3f} -> {item['next_step_recommendation']}"
            )
        return "\n".join(lines)
    raise ValueError(f"Unsupported action: {action}")


def _entries_for_mode(store: Store, mode: str, limit: int | None, recent_default_count: int):
    if mode == "full":
        return store.list_entries(limit=limit)
    if mode == "recent":
        edition_limit = limit or recent_default_count
        edition_dates = set(store.list_recent_edition_dates(edition_limit))
        entries = store.list_entries()
        return [entry for entry in entries if entry.edition_date in edition_dates]
    raise ValueError(f"Unsupported scoring mode: {mode}")


def _score_entries(entries, settings: Settings):
    runner = AgentRunner(settings)
    extractor = build_extractor(settings)
    scored = []
    for entry in entries:
        probe = probe_entry(entry, settings, extractor)
        scored.append(runner.score_entry(entry, probe))
    return scored
