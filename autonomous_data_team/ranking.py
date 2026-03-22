from __future__ import annotations

from .models import (
    FindingsAssessment,
    OpportunityAssessment,
    ProbeResult,
    RankedOpportunity,
    SkepticAssessment,
)


def combine_assessments(
    entry_id: str,
    edition_date: str,
    title: str,
    opportunity: OpportunityAssessment,
    findings: FindingsAssessment,
    skeptic: SkepticAssessment,
    probe: ProbeResult,
) -> RankedOpportunity:
    accessibility = _clamp(opportunity.accessibility_score + (0.15 if probe.downloadable else 0.0))
    novelty = _clamp(opportunity.novelty_score)
    ml_fitness = _clamp(findings.ml_fitness_score + (0.1 if probe.tabular_hint else 0.0))
    storytelling = _clamp(opportunity.storytelling_score)
    base_score = (
        accessibility * 0.30
        + novelty * 0.20
        + ml_fitness * 0.30
        + storytelling * 0.20
    )
    overall = _clamp(base_score + skeptic.score_adjustment)
    next_step = recommend_next_step(probe, overall)
    return RankedOpportunity(
        entry_id=entry_id,
        edition_date=edition_date,
        title=title,
        application_ideas=opportunity.application_ideas,
        likely_findings=findings.likely_findings,
        ml_task_candidates=opportunity.ml_task_candidates,
        audiences=opportunity.audiences,
        accessibility_score=round(accessibility, 3),
        novelty_score=round(novelty, 3),
        ml_fitness_score=round(ml_fitness, 3),
        storytelling_score=round(storytelling, 3),
        overall_priority_score=round(overall, 3),
        next_step_recommendation=next_step,
        skepticism_summary=skeptic.skepticism_summary,
        risks=skeptic.risks,
        probe_mode_used=probe.probe_mode_used,
        probe_result={
            "resolved_url": probe.resolved_url,
            "content_type": probe.content_type,
            "tabular_hint": probe.tabular_hint,
            "blocked_reason": probe.blocked_reason,
            "downloadable": probe.downloadable,
            "row_estimate": probe.row_estimate,
            "column_estimate": probe.column_estimate,
            "source_url": probe.source_url,
        },
    )


def recommend_next_step(probe: ProbeResult, score: float) -> str:
    if probe.downloadable and score >= 0.75:
        return "promote_to_deep_dive"
    if probe.tabular_hint and score >= 0.6:
        return "manual_sample_validation"
    if probe.structured_text:
        return "review_landing_page_and_access_requirements"
    return "text_only_triage"


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))

