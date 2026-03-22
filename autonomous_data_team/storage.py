from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime
from pathlib import Path

from .models import DatasetEntry, Edition, RankedOpportunity, RunRecord


class Store:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS editions (
                    edition_date TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    path TEXT NOT NULL,
                    raw_markdown TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dataset_entries (
                    entry_id TEXT PRIMARY KEY,
                    edition_date TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    source_links_json TEXT NOT NULL,
                    as_seen_in_links_json TEXT NOT NULL,
                    raw_markdown TEXT NOT NULL,
                    UNIQUE (edition_date, ordinal)
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    summary_path TEXT,
                    metadata_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS opportunity_scores (
                    run_id TEXT NOT NULL,
                    entry_id TEXT NOT NULL,
                    edition_date TEXT NOT NULL,
                    title TEXT NOT NULL,
                    application_ideas_json TEXT NOT NULL,
                    likely_findings_json TEXT NOT NULL,
                    ml_task_candidates_json TEXT NOT NULL,
                    audiences_json TEXT NOT NULL,
                    accessibility_score REAL NOT NULL,
                    novelty_score REAL NOT NULL,
                    ml_fitness_score REAL NOT NULL,
                    storytelling_score REAL NOT NULL,
                    overall_priority_score REAL NOT NULL,
                    next_step_recommendation TEXT NOT NULL,
                    skepticism_summary TEXT NOT NULL,
                    risks_json TEXT NOT NULL,
                    probe_mode_used TEXT NOT NULL,
                    probe_result_json TEXT NOT NULL,
                    scored_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, entry_id)
                );
                CREATE TABLE IF NOT EXISTS swarm_tasks (
                    task_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    worker_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    input_payload_json TEXT NOT NULL,
                    output_payload_json TEXT,
                    error_text TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS run_artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    artifact_type TEXT NOT NULL,
                    path TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            conn.commit()

    def upsert_edition(self, edition: Edition) -> None:
        now = utcnow()
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO editions (edition_date, title, path, raw_markdown, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(edition_date) DO UPDATE SET
                    title=excluded.title,
                    path=excluded.path,
                    raw_markdown=excluded.raw_markdown,
                    updated_at=excluded.updated_at
                """,
                (edition.edition_date, edition.title, edition.path, edition.raw_markdown, now),
            )
            conn.commit()

    def upsert_entry(self, entry: DatasetEntry) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO dataset_entries (
                    entry_id, edition_date, ordinal, title, description,
                    source_links_json, as_seen_in_links_json, raw_markdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entry_id) DO UPDATE SET
                    title=excluded.title,
                    description=excluded.description,
                    source_links_json=excluded.source_links_json,
                    as_seen_in_links_json=excluded.as_seen_in_links_json,
                    raw_markdown=excluded.raw_markdown
                """,
                (
                    entry.entry_id,
                    entry.edition_date,
                    entry.ordinal,
                    entry.title,
                    entry.description,
                    json.dumps(entry.source_links),
                    json.dumps(entry.as_seen_in_links),
                    entry.raw_markdown,
                ),
            )
            conn.commit()

    def replace_entries_for_edition(self, edition_date: str, entries: list[DatasetEntry]) -> None:
        with closing(self._connect()) as conn:
            conn.execute("DELETE FROM dataset_entries WHERE edition_date = ?", (edition_date,))
            conn.executemany(
                """
                INSERT INTO dataset_entries (
                    entry_id, edition_date, ordinal, title, description,
                    source_links_json, as_seen_in_links_json, raw_markdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        entry.entry_id,
                        entry.edition_date,
                        entry.ordinal,
                        entry.title,
                        entry.description,
                        json.dumps(entry.source_links),
                        json.dumps(entry.as_seen_in_links),
                        entry.raw_markdown,
                    )
                    for entry in entries
                ],
            )
            conn.commit()

    def create_run(self, mode: str, metadata: dict) -> RunRecord:
        run_id = str(uuid.uuid4())
        record = RunRecord(run_id=run_id, mode=mode, status="running", started_at=utcnow(), metadata=metadata)
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO runs (run_id, mode, status, started_at, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (record.run_id, record.mode, record.status, record.started_at, json.dumps(record.metadata)),
            )
            conn.commit()
        return record

    def complete_run(self, run_id: str, summary_path: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                UPDATE runs
                SET status='completed', completed_at=?, summary_path=?
                WHERE run_id=?
                """,
                (utcnow(), summary_path, run_id),
            )
            conn.commit()

    def fail_run(self, run_id: str, error: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                UPDATE runs
                SET status='failed', completed_at=?, metadata_json=?
                WHERE run_id=?
                """,
                (utcnow(), json.dumps({"error": error}), run_id),
            )
            conn.commit()

    def insert_score(self, run_id: str, score: RankedOpportunity) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO opportunity_scores (
                    run_id, entry_id, edition_date, title,
                    application_ideas_json, likely_findings_json, ml_task_candidates_json,
                    audiences_json, accessibility_score, novelty_score, ml_fitness_score,
                    storytelling_score, overall_priority_score, next_step_recommendation,
                    skepticism_summary, risks_json, probe_mode_used, probe_result_json, scored_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    score.entry_id,
                    score.edition_date,
                    score.title,
                    json.dumps(score.application_ideas),
                    json.dumps(score.likely_findings),
                    json.dumps(score.ml_task_candidates),
                    json.dumps(score.audiences),
                    score.accessibility_score,
                    score.novelty_score,
                    score.ml_fitness_score,
                    score.storytelling_score,
                    score.overall_priority_score,
                    score.next_step_recommendation,
                    score.skepticism_summary,
                    json.dumps(score.risks),
                    score.probe_mode_used,
                    json.dumps(score.probe_result),
                    score.scored_at,
                ),
            )
            conn.commit()

    def create_swarm_task(
        self,
        run_id: str,
        worker_name: str,
        dataset_name: str,
        input_payload: dict,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = utcnow()
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO swarm_tasks (
                    task_id, run_id, dataset_name, worker_name, status,
                    input_payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    run_id,
                    dataset_name,
                    worker_name,
                    "running",
                    json.dumps(input_payload),
                    now,
                    now,
                ),
            )
            conn.commit()
        return task_id

    def complete_swarm_task(self, task_id: str, output_payload: dict) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                UPDATE swarm_tasks
                SET status='completed', output_payload_json=?, updated_at=?
                WHERE task_id=?
                """,
                (json.dumps(output_payload), utcnow(), task_id),
            )
            conn.commit()

    def fail_swarm_task(self, task_id: str, error_text: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                UPDATE swarm_tasks
                SET status='failed', error_text=?, updated_at=?
                WHERE task_id=?
                """,
                (error_text, utcnow(), task_id),
            )
            conn.commit()

    def record_artifact(
        self,
        run_id: str,
        dataset_name: str,
        artifact_type: str,
        path: str,
        metadata: dict,
    ) -> str:
        artifact_id = str(uuid.uuid4())
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO run_artifacts (
                    artifact_id, run_id, dataset_name, artifact_type, path, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (artifact_id, run_id, dataset_name, artifact_type, path, json.dumps(metadata), utcnow()),
            )
            conn.commit()
        return artifact_id

    def list_entries(self, edition_date: str | None = None, limit: int | None = None) -> list[DatasetEntry]:
        query = """
            SELECT entry_id, edition_date, ordinal, title, description,
                   source_links_json, as_seen_in_links_json, raw_markdown
            FROM dataset_entries
        """
        params: list[object] = []
        if edition_date:
            query += " WHERE edition_date = ?"
            params.append(edition_date)
        query += " ORDER BY edition_date DESC, ordinal ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with closing(self._connect()) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            DatasetEntry(
                entry_id=row["entry_id"],
                edition_date=row["edition_date"],
                ordinal=row["ordinal"],
                title=row["title"],
                description=row["description"],
                source_links=json.loads(row["source_links_json"]),
                as_seen_in_links=json.loads(row["as_seen_in_links_json"]),
                raw_markdown=row["raw_markdown"],
            )
            for row in rows
        ]

    def list_recent_edition_dates(self, limit: int) -> list[str]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT edition_date
                FROM editions
                ORDER BY edition_date DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [row["edition_date"] for row in rows]

    def top_scores(self, limit: int, run_id: str | None = None) -> list[sqlite3.Row]:
        if run_id is None:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT run_id
                    FROM runs
                    WHERE status='completed'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()
            if row is None:
                return []
            run_id = row["run_id"]
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM opportunity_scores
                WHERE run_id=?
                ORDER BY overall_priority_score DESC, edition_date DESC, title ASC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        return rows


def utcnow() -> str:
    return datetime.utcnow().isoformat() + "Z"
