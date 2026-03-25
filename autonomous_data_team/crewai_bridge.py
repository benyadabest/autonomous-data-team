from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from typing import Any

from .config import Settings
from .experiment_runner import heuristic_problem_frame
from .models import CritiqueResult, DatasetProfile, ExperimentResult, ProblemFrame, ReportResult


def crewai_is_available(settings: Settings) -> bool:
    if settings.swarm_orchestrator != "crewai":
        return False
    if sys.version_info < (3, 10):
        return False
    try:
        _prepare_crewai_environment(settings)
        import crewai  # noqa: F401
    except Exception:
        return False
    return True


def build_eda_insights(
    settings: Settings, profile: DatasetProfile, requester_notes: str
) -> dict[str, Any] | None:
    return _run_json_task(
        settings,
        role="Creative Data Strategist",
        goal="Analyze a dataset profile and brainstorm both practical and unconventional ideas for what to do with the data.",
        task_description=(
            "You are a creative data strategist reviewing the EDA profile of a dataset someone emailed in. "
            "Based on the schema, shape, missingness, and any requester notes, generate insights about "
            "what this dataset could be used for.\n\n"
            "Go beyond the obvious. Include practical ideas AND wild, out-of-the-box ones — "
            "cross-domain mashups, unexpected correlations to investigate, novel visualizations, "
            "unconventional ML applications, or creative products/apps that could be built from this data. "
            "Think like a startup founder, an investigative journalist, and a data artist all at once.\n\n"
            "Return strict JSON with these keys:\n"
            "- executive_summary: a 1-2 sentence summary of the dataset and its potential\n"
            "- project_ideas: list of 3-5 concrete, practical project ideas (dashboards, reports, apps, automations)\n"
            "- wild_ideas: list of 2-3 unconventional or creative ideas that most people wouldn't think of "
            "(cross-domain applications, art projects, surprising analyses, novel products)\n"
            "- ml_opportunities: list of 2-4 specific ML tasks that could be attempted "
            "(mention target columns, task types, and why they'd be interesting)\n"
            "- data_quality_concerns: list of 1-3 issues worth addressing before analysis\n"
            "- recommended_next_steps: list of 2-4 prioritized actions the user should take next\n\n"
            f"Dataset profile: {json.dumps(asdict(profile), ensure_ascii=True)}\n"
            f"Requester notes: {requester_notes or 'none provided'}"
        ),
    )


def build_problem_frame(settings: Settings, profile: DatasetProfile, requester_notes: str) -> ProblemFrame:
    payload = _run_json_task(
        settings,
        role="Coordinator",
        goal="Frame a pragmatic machine learning problem from a dataset profile.",
        task_description=(
            "You are the coordinator in a data science swarm. "
            "Follow these principles: define the problem before modeling, choose an appropriate metric, "
            "set a good-enough exit criterion, identify a ground truth anchor, and prefer simple baselines first. "
            "Return strict JSON with keys task_type, target_column, primary_metric, exit_criterion, "
            "ground_truth_anchor, problem_statement, rationale.\n"
            f"Dataset profile: {json.dumps(asdict(profile), ensure_ascii=True)}\n"
            f"Requester notes: {requester_notes}"
        ),
    )
    if payload is None:
        return heuristic_problem_frame(profile)
    return ProblemFrame(**payload)


def build_critique(settings: Settings, profile: DatasetProfile, frame: ProblemFrame, experiment: ExperimentResult) -> CritiqueResult:
    fallback = _fallback_critique(experiment)
    payload = _run_json_task(
        settings,
        role="Critic",
        goal="Challenge experiment design and identify ML risks.",
        task_description=(
            "You are the critic in a data science swarm. Review the dataset profile, problem framing, "
            "and experiment results. Focus on target validity, metric choice, leakage risk, split quality, "
            "and whether the model beat the baseline in a meaningful way. "
            "Return strict JSON with keys verdict, strengths, weaknesses, recommended_next_steps.\n"
            f"Dataset profile: {json.dumps(asdict(profile), ensure_ascii=True)}\n"
            f"Problem frame: {json.dumps(asdict(frame), ensure_ascii=True)}\n"
            f"Experiment result: {json.dumps(asdict(experiment), ensure_ascii=True)}"
        ),
    )
    if payload is None:
        return fallback
    return CritiqueResult(**payload)


def _fallback_critique(experiment: ExperimentResult) -> CritiqueResult:
    return CritiqueResult(
        verdict="needs_human_review" if experiment.best_score is None else "usable_with_caveats",
        strengths=[
            "The pipeline established a baseline before comparing more complex models.",
            "A holdout split was used before model selection.",
        ],
        weaknesses=experiment.caveats or ["Additional manual review is recommended."],
        recommended_next_steps=[
            "Review the target column choice and confirm it matches the real business question.",
            "Inspect the highest-error segments before trusting the result.",
        ],
    )


def build_report(
    settings: Settings,
    profile: DatasetProfile,
    frame: ProblemFrame,
    experiment: ExperimentResult,
    critique: CritiqueResult,
) -> ReportResult:
    fallback = ReportResult(
        subject_line=f"Dataset analysis results for {profile.dataset_name}",
        body_markdown=_fallback_report(profile, frame, experiment, critique),
        executive_summary=critique.verdict,
    )
    payload = _run_json_task(
        settings,
        role="Reporter",
        goal="Write a concise data science findings report for email.",
        task_description=(
            "You are the reporting worker in a data science swarm. Summarize the dataset, EDA, chosen modeling frame, "
            "best experiment, critique, and clear next steps. Return strict JSON with keys subject_line, "
            "body_markdown, executive_summary.\n"
            f"Dataset profile: {json.dumps(asdict(profile), ensure_ascii=True)}\n"
            f"Problem frame: {json.dumps(asdict(frame), ensure_ascii=True)}\n"
            f"Experiment result: {json.dumps(asdict(experiment), ensure_ascii=True)}\n"
            f"Critique: {json.dumps(asdict(critique), ensure_ascii=True)}"
        ),
    )
    if payload is None:
        return fallback
    return ReportResult(**payload)


def _run_json_task(
    settings: Settings,
    role: str,
    goal: str,
    task_description: str,
) -> dict[str, Any] | None:
    if not settings.openai_api_key or not crewai_is_available(settings):
        return None
    try:
        result = _run_single_agent_task(settings, role, goal, task_description)
        return _extract_json(result)
    except Exception:
        return None


def _run_single_agent_task(settings: Settings, role: str, goal: str, task_description: str) -> Any:
    _prepare_crewai_environment(settings)
    from crewai import Agent, Crew, Process, Task

    agent = Agent(
        role=role,
        goal=goal,
        backstory=f"You are the {role.lower()} in an autonomous data science swarm.",
        llm=settings.openai_model,
        verbose=False,
        allow_delegation=False,
    )
    task = Task(
        description=task_description,
        expected_output="Strict JSON only.",
        agent=agent,
    )
    crew = Crew(
        agents=[agent],
        tasks=[task],
        process=Process.sequential,
        verbose=False,
    )
    return crew.kickoff()


def _prepare_crewai_environment(settings: Settings) -> None:
    settings.crewai_home_dir.mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = str(settings.crewai_home_dir)
    os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")
    os.environ.setdefault("CREWAI_DISABLE_TRACKING", "true")
    os.environ.setdefault("CREWAI_TRACING_ENABLED", "false")
    os.environ.setdefault("OTEL_SDK_DISABLED", "true")
    os.environ.setdefault("CREWAI_DISABLE_VERSION_CHECK", "true")


def _extract_json(result: Any) -> dict[str, Any]:
    text = getattr(result, "raw", None) or str(result)
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if "\n" in text:
            text = text.split("\n", 1)[1]
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("CrewAI result did not contain JSON")
    return json.loads(text[start : end + 1])


def _fallback_report(profile: DatasetProfile, frame: ProblemFrame, experiment: ExperimentResult, critique: CritiqueResult) -> str:
    return "\n".join(
        [
            f"# Results for {profile.dataset_name}",
            "",
            "## Problem framing",
            f"- Task type: `{frame.task_type}`",
            f"- Target column: `{frame.target_column or 'none'}`",
            f"- Primary metric: `{frame.primary_metric}`",
            f"- Exit criterion: {frame.exit_criterion}",
            "",
            "## Dataset profile",
            f"- Rows: `{profile.rows}`",
            f"- Columns: `{profile.columns}`",
            f"- Target candidates: {', '.join(item['column'] for item in profile.target_candidates) or 'none'}",
            "",
            "## Experiment summary",
            f"- Best model: `{experiment.best_model}`",
            f"- Baseline model: `{experiment.baseline_model}`",
            f"- Best score: `{experiment.best_score}`",
            f"- Baseline score: `{experiment.baseline_score}`",
            "",
            "## Critique",
            f"- Verdict: `{critique.verdict}`",
            f"- Strengths: {', '.join(critique.strengths)}",
            f"- Weaknesses: {', '.join(critique.weaknesses)}",
            f"- Recommended next steps: {', '.join(critique.recommended_next_steps)}",
        ]
    )
