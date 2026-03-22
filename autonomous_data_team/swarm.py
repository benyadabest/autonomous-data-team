from __future__ import annotations

import base64
import json
from dataclasses import asdict
from pathlib import Path

from .config import Settings
from .crewai_bridge import build_critique, build_problem_frame, build_report
from .dataset_ingestion import save_message_attachments
from .experiment_runner import build_dataset_profile, run_experiments
from .mail import AgentMailAPI, extract_body_text
from .models import DatasetProfile, MailAttachment, MailMessage, ReportResult, SavedAttachment
from .storage import Store

EDA_ONLY = "eda_only"
EDA_AND_EXPERIMENTS = "eda_and_experiments"
EDA_ONLY_HINTS = ("mode: eda", "eda only", "only eda", "just eda")


def run_attachment_swarm(
    message: MailMessage,
    client: AgentMailAPI,
    settings: Settings,
    store: Store,
) -> tuple[str, list[dict[str, str]], str]:
    requester_notes = extract_body_text(message)
    analysis_mode = requested_analysis_mode(requester_notes)
    run = store.create_run(
        mode="dataset_email",
        metadata={
            "message_id": message.message_id,
            "thread_id": message.thread_id,
            "sender": message.sender,
            "subject": message.subject,
            "analysis_mode": analysis_mode,
        },
    )
    run_dir = settings.runs_dir / run.run_id
    attachments_dir = run_dir / "attachments"
    attachments = save_message_attachments(message, client, attachments_dir)
    if not attachments:
        raise ValueError("No supported dataset attachments were found in the email.")

    dataset_reports = [
        _run_dataset_workers(
            run.run_id,
            run_dir,
            attachment,
            requester_notes,
            analysis_mode,
            settings,
            store,
        )
        for attachment in attachments
    ]

    summary_path = _write_run_summary(run_dir, dataset_reports)
    store.complete_run(run.run_id, str(summary_path))
    email_body = _email_body(dataset_reports)
    reply_attachments = [_path_to_attachment(summary_path)]
    return email_body, reply_attachments, run.run_id


def run_local_dataset_swarm(
    path: str,
    requester_notes: str,
    settings: Settings,
    store: Store,
) -> dict[str, str]:
    analysis_mode = requested_analysis_mode(requester_notes)
    pseudo_message = MailMessage(
        message_id="local",
        thread_id="local",
        sender="local@example.com",
        subject=Path(path).name,
        labels=[],
        text=requester_notes,
        attachments=[
            MailAttachment(
                attachment_id="local-attachment",
                filename=Path(path).name,
                content_type=None,
            )
        ],
    )
    run = store.create_run(
        mode="local_dataset",
        metadata={"path": path, "subject": pseudo_message.subject, "analysis_mode": analysis_mode},
    )
    run_dir = settings.runs_dir / run.run_id
    attachments_dir = run_dir / "attachments"
    attachments_dir.mkdir(parents=True, exist_ok=True)
    local_target = attachments_dir / Path(path).name
    local_target.write_bytes(Path(path).read_bytes())
    attachment = SavedAttachment(
        attachment_id="local-attachment",
        filename=local_target.name,
        content_type=None,
        local_path=str(local_target),
    )
    report = _run_dataset_workers(
        run.run_id,
        run_dir,
        attachment,
        requester_notes,
        analysis_mode,
        settings,
        store,
    )
    summary_path = _write_run_summary(run_dir, [report])
    store.complete_run(run.run_id, str(summary_path))
    return {"run_id": run.run_id, "summary_path": str(summary_path), "report_path": report["report_path"]}


def _run_dataset_workers(
    run_id: str,
    run_dir: Path,
    attachment: SavedAttachment,
    requester_notes: str,
    analysis_mode: str,
    settings: Settings,
    store: Store,
) -> dict[str, str]:
    dataset_dir = run_dir / Path(attachment.filename).stem
    dataset_dir.mkdir(parents=True, exist_ok=True)

    acquisition_task = store.create_swarm_task(
        run_id,
        worker_name="acquisition",
        dataset_name=attachment.filename,
        input_payload={"attachment": asdict(attachment)},
    )
    store.complete_swarm_task(
        acquisition_task,
        output_payload={"local_path": attachment.local_path, "dataset_dir": str(dataset_dir)},
    )

    eda_task = store.create_swarm_task(
        run_id,
        worker_name="eda",
        dataset_name=attachment.filename,
        input_payload={"path": attachment.local_path},
    )
    profile = build_dataset_profile(attachment.local_path, settings)
    profile_path = dataset_dir / "profile.json"
    _write_json(profile_path, asdict(profile))
    store.record_artifact(run_id, attachment.filename, "dataset_profile", str(profile_path), {"worker": "eda"})
    store.complete_swarm_task(eda_task, output_payload={"profile_path": str(profile_path)})

    if analysis_mode == EDA_ONLY:
        return _finish_eda_only_run(
            run_id,
            attachment,
            dataset_dir,
            requester_notes,
            profile,
            store,
        )

    coordinator_task = store.create_swarm_task(
        run_id,
        worker_name="coordinator",
        dataset_name=attachment.filename,
        input_payload={"profile": asdict(profile), "requester_notes": requester_notes},
    )
    frame = build_problem_frame(settings, profile, requester_notes)
    frame_path = dataset_dir / "problem_frame.json"
    _write_json(frame_path, asdict(frame))
    store.record_artifact(run_id, attachment.filename, "problem_frame", str(frame_path), {"worker": "coordinator"})
    store.complete_swarm_task(coordinator_task, output_payload={"problem_frame_path": str(frame_path)})

    modeling_task = store.create_swarm_task(
        run_id,
        worker_name="modeling",
        dataset_name=attachment.filename,
        input_payload={"problem_frame": asdict(frame), "dataset_path": attachment.local_path},
    )
    experiment = run_experiments(attachment.local_path, frame, settings)
    experiment_path = dataset_dir / "experiment_result.json"
    _write_json(experiment_path, asdict(experiment))
    store.record_artifact(run_id, attachment.filename, "experiment_result", str(experiment_path), {"worker": "modeling"})
    store.complete_swarm_task(modeling_task, output_payload={"experiment_path": str(experiment_path)})

    critic_task = store.create_swarm_task(
        run_id,
        worker_name="critic",
        dataset_name=attachment.filename,
        input_payload={"problem_frame": asdict(frame), "experiment": asdict(experiment)},
    )
    critique = build_critique(settings, profile, frame, experiment)
    critique_path = dataset_dir / "critique.json"
    _write_json(critique_path, asdict(critique))
    store.record_artifact(run_id, attachment.filename, "critique", str(critique_path), {"worker": "critic"})
    store.complete_swarm_task(critic_task, output_payload={"critique_path": str(critique_path)})

    reporting_task = store.create_swarm_task(
        run_id,
        worker_name="reporting",
        dataset_name=attachment.filename,
        input_payload={"problem_frame": asdict(frame), "experiment": asdict(experiment), "critique": asdict(critique)},
    )
    report = build_report(settings, profile, frame, experiment, critique)
    report_path = dataset_dir / "report.md"
    report_path.write_text(report.body_markdown, encoding="utf-8")
    store.record_artifact(run_id, attachment.filename, "report", str(report_path), {"worker": "reporting"})
    store.complete_swarm_task(reporting_task, output_payload={"report_path": str(report_path)})

    return {
        "dataset_name": attachment.filename,
        "report_path": str(report_path),
        "summary": report.executive_summary,
        "analysis_mode": analysis_mode,
        "best_model": experiment.best_model,
        "best_score": str(experiment.best_score),
        "primary_metric": experiment.primary_metric,
    }


def _write_run_summary(run_dir: Path, dataset_reports: list[dict[str, str]]) -> Path:
    summary_path = run_dir / "attachment_swarm_summary.md"
    lines = ["# Attachment Swarm Summary", ""]
    for report in dataset_reports:
        lines.append(f"## {report['dataset_name']}")
        lines.append(f"- Executive summary: {report['summary']}")
        lines.append(f"- Analysis mode: `{report['analysis_mode']}`")
        if report["analysis_mode"] == EDA_ONLY:
            lines.append("- Experiments: not requested")
        else:
            lines.extend(
                [
                    f"- Best model: `{report['best_model']}`",
                    f"- Metric: `{report['primary_metric']}`",
                    f"- Best score: `{report['best_score']}`",
                ]
            )
        lines.extend([f"- Report path: `{report['report_path']}`", ""])
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    return summary_path


def _email_body(dataset_reports: list[dict[str, str]]) -> str:
    lines = ["Your dataset analysis run is complete.", ""]
    for report in dataset_reports:
        lines.append(f"- {report['dataset_name']}: {report['summary']}")
        if report["analysis_mode"] == EDA_ONLY:
            lines.append("  Scope: EDA only. No experiments were run.")
        else:
            lines.append(
                f"  Best model: {report['best_model']} ({report['primary_metric']}={report['best_score']})"
            )
    lines.append("")
    lines.append("A markdown summary is attached.")
    return "\n".join(lines)


def requested_analysis_mode(requester_notes: str) -> str:
    normalized = requester_notes.lower()
    if any(hint in normalized for hint in EDA_ONLY_HINTS):
        return EDA_ONLY
    return EDA_AND_EXPERIMENTS


def _finish_eda_only_run(
    run_id: str,
    attachment: SavedAttachment,
    dataset_dir: Path,
    requester_notes: str,
    profile: DatasetProfile,
    store: Store,
) -> dict[str, str]:
    reporting_task = store.create_swarm_task(
        run_id,
        worker_name="reporting",
        dataset_name=attachment.filename,
        input_payload={"profile": asdict(profile), "requester_notes": requester_notes, "analysis_mode": EDA_ONLY},
    )
    report = _build_eda_only_report(profile, requester_notes)
    report_path = dataset_dir / "report.md"
    report_path.write_text(report.body_markdown, encoding="utf-8")
    store.record_artifact(run_id, attachment.filename, "report", str(report_path), {"worker": "reporting"})
    store.complete_swarm_task(reporting_task, output_payload={"report_path": str(report_path)})
    return {
        "dataset_name": attachment.filename,
        "report_path": str(report_path),
        "summary": report.executive_summary,
        "analysis_mode": EDA_ONLY,
        "best_model": "not_run",
        "best_score": "not_run",
        "primary_metric": "not_run",
    }


def _build_eda_only_report(profile: DatasetProfile, requester_notes: str) -> ReportResult:
    top_missing = [
        item for item in sorted(profile.missingness, key=lambda value: value["missing_pct"], reverse=True) if item["missing_count"]
    ][:5]
    body_lines = [
        f"# EDA for {profile.dataset_name}",
        "",
        "## Scope",
        "- Requested mode: `eda_only`",
        "",
        "## Dataset shape",
        f"- Rows: `{profile.rows}`",
        f"- Columns: `{profile.columns}`",
        "",
        "## Schema sample",
    ]
    body_lines.extend(
        f"- `{column['name']}`: `{column['dtype']}`"
        for column in profile.dtypes[:10]
    )
    if top_missing:
        body_lines.extend(["", "## Missingness hotspots"])
        body_lines.extend(
            f"- `{item['column']}`: {item['missing_count']} missing ({item['missing_pct']:.2%})"
            for item in top_missing
        )
    if profile.target_candidates:
        body_lines.extend(["", "## Potential target columns"])
        body_lines.extend(
            f"- `{item['column']}` ({item['task_type']}, {item['unique_values']} unique values)"
            for item in profile.target_candidates[:5]
        )
    if profile.notes:
        body_lines.extend(["", "## Notes"])
        body_lines.extend(f"- {note}" for note in profile.notes)
    if requester_notes.strip():
        body_lines.extend(["", "## Request notes", requester_notes.strip()])
    return ReportResult(
        subject_line=f"EDA results for {profile.dataset_name}",
        body_markdown="\n".join(body_lines),
        executive_summary="EDA completed; experiments were not run.",
    )


def _path_to_attachment(path: Path) -> dict[str, str]:
    raw = path.read_bytes()
    return {
        "content": base64.b64encode(raw).decode(),
        "filename": path.name,
        "content_type": "text/markdown",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
