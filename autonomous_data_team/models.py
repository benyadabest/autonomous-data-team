from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Optional


def dataclass_to_dict(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    if isinstance(value, list):
        return [dataclass_to_dict(item) for item in value]
    if isinstance(value, dict):
        return {key: dataclass_to_dict(item) for key, item in value.items()}
    return value


@dataclass
class Edition:
    edition_date: str
    title: str
    path: str
    raw_markdown: str


@dataclass
class DatasetEntry:
    entry_id: str
    edition_date: str
    ordinal: int
    title: str
    description: str
    source_links: list[str]
    as_seen_in_links: list[str]
    raw_markdown: str


@dataclass
class ExtractorResult:
    resolved_url: Optional[str] = None
    content_type: Optional[str] = None
    tabular_hint: bool = False
    structured_text: Optional[str] = None
    blocked_reason: Optional[str] = None
    downloadable: bool = False


@dataclass
class ProbeResult:
    probe_mode_used: str
    resolved_url: Optional[str] = None
    content_type: Optional[str] = None
    tabular_hint: bool = False
    structured_text: Optional[str] = None
    blocked_reason: Optional[str] = None
    downloadable: bool = False
    row_estimate: Optional[int] = None
    column_estimate: Optional[int] = None
    basic_schema: list[dict[str, str]] = field(default_factory=list)
    source_url: Optional[str] = None


@dataclass
class OpportunityAssessment:
    application_ideas: list[str]
    audiences: list[str]
    ml_task_candidates: list[str]
    accessibility_score: float
    novelty_score: float
    storytelling_score: float
    rationale: str


@dataclass
class FindingsAssessment:
    likely_findings: list[str]
    ml_fitness_score: float
    rationale: str


@dataclass
class SkepticAssessment:
    risks: list[str]
    skepticism_summary: str
    score_adjustment: float


@dataclass
class RankedOpportunity:
    entry_id: str
    edition_date: str
    title: str
    application_ideas: list[str]
    likely_findings: list[str]
    ml_task_candidates: list[str]
    audiences: list[str]
    accessibility_score: float
    novelty_score: float
    ml_fitness_score: float
    storytelling_score: float
    overall_priority_score: float
    next_step_recommendation: str
    skepticism_summary: str
    risks: list[str]
    probe_mode_used: str
    probe_result: dict[str, Any]
    scored_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


@dataclass
class RunRecord:
    run_id: str
    mode: str
    status: str
    started_at: str
    completed_at: Optional[str] = None
    summary_path: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MailCommand:
    action: str
    arg: Optional[str] = None


@dataclass
class MailMessage:
    message_id: str
    thread_id: str
    sender: str
    subject: str
    labels: list[str]
    text: Optional[str] = None
    html: Optional[str] = None
    attachments: list["MailAttachment"] = field(default_factory=list)


@dataclass
class MailAttachment:
    attachment_id: str
    filename: Optional[str] = None
    content_type: Optional[str] = None
    size: Optional[int] = None
    inline: bool = False


@dataclass
class SavedAttachment:
    attachment_id: str
    filename: str
    content_type: Optional[str]
    local_path: str
    extracted_from: Optional[str] = None


@dataclass
class DatasetProfile:
    dataset_name: str
    path: str
    rows: int
    columns: int
    dtypes: list[dict[str, str]]
    missingness: list[dict[str, Any]]
    numeric_summary: list[dict[str, Any]]
    target_candidates: list[dict[str, Any]]
    notes: list[str] = field(default_factory=list)


@dataclass
class ProblemFrame:
    task_type: str
    target_column: Optional[str]
    primary_metric: str
    exit_criterion: str
    ground_truth_anchor: str
    problem_statement: str
    rationale: str


@dataclass
class ExperimentResult:
    dataset_name: str
    dataset_path: str
    task_type: str
    target_column: Optional[str]
    primary_metric: str
    exit_criterion: str
    baseline_model: str
    best_model: str
    baseline_score: Optional[float]
    best_score: Optional[float]
    experiments: list[dict[str, Any]]
    error_analysis: list[str]
    caveats: list[str]


@dataclass
class CritiqueResult:
    verdict: str
    strengths: list[str]
    weaknesses: list[str]
    recommended_next_steps: list[str]


@dataclass
class ReportResult:
    subject_line: str
    body_markdown: str
    executive_summary: str
