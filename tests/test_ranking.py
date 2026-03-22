from __future__ import annotations

from autonomous_data_team.models import FindingsAssessment, OpportunityAssessment, ProbeResult, SkepticAssessment
from autonomous_data_team.ranking import combine_assessments


def test_combine_assessments_prefers_downloadable_data() -> None:
    opportunity = OpportunityAssessment(
        application_ideas=["Idea 1", "Idea 2", "Idea 3"],
        audiences=["Audience 1", "Audience 2", "Audience 3"],
        ml_task_candidates=["classification", "regression", "clustering"],
        accessibility_score=0.7,
        novelty_score=0.6,
        storytelling_score=0.9,
        rationale="Strong civic interest.",
    )
    findings = FindingsAssessment(
        likely_findings=["Finding 1", "Finding 2", "Finding 3"],
        ml_fitness_score=0.75,
        rationale="Has structured labels.",
    )
    skeptic = SkepticAssessment(
        risks=["Licensing unclear"],
        skepticism_summary="Check license.",
        score_adjustment=-0.05,
    )
    probe = ProbeResult(
        probe_mode_used="direct",
        downloadable=True,
        tabular_hint=True,
        row_estimate=100,
        column_estimate=8,
    )

    ranked = combine_assessments("entry-1", "2025-01-08", "Traffic crashes", opportunity, findings, skeptic, probe)

    assert ranked.overall_priority_score > 0.7
    assert ranked.next_step_recommendation == "promote_to_deep_dive"

