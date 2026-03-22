from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable

from .models import RankedOpportunity


def write_run_artifacts(run_dir: Path, scored: Iterable[RankedOpportunity]) -> tuple[Path, Path, Path]:
    run_dir.mkdir(parents=True, exist_ok=True)
    scores = list(scored)
    json_path = run_dir / "top_opportunities.json"
    csv_path = run_dir / "top_opportunities.csv"
    summary_path = run_dir / "summary.md"

    json_path.write_text(json.dumps([score.__dict__ for score in scores], indent=2), encoding="utf-8")
    _write_csv(csv_path, scores)
    summary_path.write_text(render_summary(scores), encoding="utf-8")
    return json_path, csv_path, summary_path


def _write_csv(path: Path, scores: list[RankedOpportunity]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "entry_id",
                "edition_date",
                "title",
                "overall_priority_score",
                "accessibility_score",
                "novelty_score",
                "ml_fitness_score",
                "storytelling_score",
                "next_step_recommendation",
                "probe_mode_used",
            ],
        )
        writer.writeheader()
        for score in scores:
            writer.writerow(
                {
                    "entry_id": score.entry_id,
                    "edition_date": score.edition_date,
                    "title": score.title,
                    "overall_priority_score": score.overall_priority_score,
                    "accessibility_score": score.accessibility_score,
                    "novelty_score": score.novelty_score,
                    "ml_fitness_score": score.ml_fitness_score,
                    "storytelling_score": score.storytelling_score,
                    "next_step_recommendation": score.next_step_recommendation,
                    "probe_mode_used": score.probe_mode_used,
                }
            )


def render_summary(scores: list[RankedOpportunity]) -> str:
    lines = ["# Opportunity Summary", ""]
    if not scores:
        lines.append("No opportunities were scored.")
        return "\n".join(lines)
    for index, score in enumerate(scores, start=1):
        lines.extend(
            [
                f"## {index}. {score.title}",
                f"- Edition date: `{score.edition_date}`",
                f"- Overall priority: `{score.overall_priority_score:.3f}`",
                f"- Next step: `{score.next_step_recommendation}`",
                f"- Probe mode: `{score.probe_mode_used}`",
                f"- Applications: {', '.join(score.application_ideas)}",
                f"- Likely findings: {', '.join(score.likely_findings)}",
                f"- Risks: {', '.join(score.risks)}",
                "",
            ]
        )
    return "\n".join(lines)

