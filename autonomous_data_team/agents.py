from __future__ import annotations

import json
import re
from dataclasses import asdict

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import Settings
from .models import (
    DatasetEntry,
    FindingsAssessment,
    OpportunityAssessment,
    ProbeResult,
    SkepticAssessment,
)
from .ranking import combine_assessments


class AgentRunner:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def score_entry(self, entry: DatasetEntry, probe: ProbeResult):
        if self.settings.openai_api_key:
            try:
                return self._score_with_openai(entry, probe)
            except Exception:
                pass
        return self._score_with_heuristics(entry, probe)

    def _score_with_openai(self, entry: DatasetEntry, probe: ProbeResult):
        context = json.dumps(
            {
                "entry": asdict(entry),
                "probe": asdict(probe),
            },
            ensure_ascii=True,
        )
        opportunity = OpportunityAssessment(**self._complete_json(OPPORTUNITY_PROMPT, context))
        findings = FindingsAssessment(**self._complete_json(FINDINGS_PROMPT, context))
        skeptic = SkepticAssessment(**self._complete_json(SKEPTIC_PROMPT, context))
        ranked_raw = self._complete_json(
            RANKER_PROMPT,
            json.dumps(
                {
                    "entry": asdict(entry),
                    "probe": asdict(probe),
                    "opportunity": asdict(opportunity),
                    "findings": asdict(findings),
                    "skeptic": asdict(skeptic),
                }
            ),
        )
        combined = combine_assessments(
            entry.entry_id,
            entry.edition_date,
            entry.title,
            opportunity,
            findings,
            skeptic,
            probe,
        )
        if "next_step_recommendation" in ranked_raw:
            combined.next_step_recommendation = ranked_raw["next_step_recommendation"]
        if "overall_priority_score" in ranked_raw:
            combined.overall_priority_score = float(ranked_raw["overall_priority_score"])
        return combined

    def _score_with_heuristics(self, entry: DatasetEntry, probe: ProbeResult):
        theme = infer_theme(entry.title + " " + entry.description)
        opportunity = OpportunityAssessment(
            application_ideas=[
                f"Build a {theme} monitoring brief for weekly trend tracking.",
                f"Use the dataset to prioritize investigations and interventions in {theme}.",
                f"Create a public-facing explainer or benchmark around {theme}.",
            ],
            audiences=audiences_for_theme(theme),
            ml_task_candidates=task_candidates_for_probe(probe),
            accessibility_score=0.85 if probe.downloadable else 0.55 if probe.structured_text else 0.35,
            novelty_score=0.72 if theme in {"climate", "health", "economics"} else 0.64,
            storytelling_score=0.82 if len(entry.description) > 120 else 0.68,
            rationale=f"Theme inferred as {theme} from the archive text and probe metadata.",
        )
        findings = FindingsAssessment(
            likely_findings=[
                f"Outliers and concentrated hotspots in {theme} records.",
                f"Strong segmentation by geography, time, or category for {theme}.",
                f"Unexpected correlations that could support follow-up reporting on {theme}.",
            ],
            ml_fitness_score=ml_fitness_for_probe(probe),
            rationale="Heuristic score based on tabular access, schema width, and sample availability.",
        )
        risks = []
        if not probe.downloadable:
            risks.append("Dataset is not directly downloadable from the linked source.")
        if not probe.basic_schema:
            risks.append("Schema is unknown, so ML suitability is estimated from text only.")
        if "license" not in entry.description.lower():
            risks.append("Usage rights and licensing are not explicit in the archive snippet.")
        skeptic = SkepticAssessment(
            risks=risks or ["None beyond normal data quality checks."],
            skepticism_summary="Prioritize manual access and licensing review before deep analysis.",
            score_adjustment=-0.08 if not probe.downloadable else -0.02,
        )
        return combine_assessments(
            entry.entry_id,
            entry.edition_date,
            entry.title,
            opportunity,
            findings,
            skeptic,
            probe,
        )

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    def _complete_json(self, system_prompt: str, user_payload: str) -> dict:
        url = self.settings.openai_base_url.rstrip("/") + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.settings.openai_api_key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": self.settings.openai_model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
        }
        with httpx.Client(timeout=60) as client:
            response = client.post(url, headers=headers, json=body)
            response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        if isinstance(content, list):
            content = "".join(part.get("text", "") for part in content if isinstance(part, dict))
        return json.loads(extract_json(content))


def extract_json(text: str) -> str:
    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("No JSON object found in model output")
    candidate = match.group(0)
    json.loads(candidate)
    return candidate


def infer_theme(text: str) -> str:
    lower = text.lower()
    theme_map = {
        "health": ("health", "disease", "hospital", "nutrition", "medical"),
        "climate": ("climate", "weather", "temperature", "emissions", "wildfire"),
        "transportation": ("traffic", "transit", "mobility", "road", "crash"),
        "economics": ("income", "jobs", "finance", "trade", "economy", "tax"),
        "education": ("school", "student", "teacher", "learning", "university"),
        "civic": ("election", "government", "crime", "policy", "justice"),
    }
    for theme, keywords in theme_map.items():
        if any(keyword in lower for keyword in keywords):
            return theme
    return "public-interest"


def audiences_for_theme(theme: str) -> list[str]:
    shared = ["Journalists", "Policy analysts", "Researchers"]
    if theme == "health":
        return ["Public health teams", "Healthcare researchers", "Journalists"]
    if theme == "climate":
        return ["Climate analysts", "Local governments", "Investigative reporters"]
    return shared


def task_candidates_for_probe(probe: ProbeResult) -> list[str]:
    if probe.basic_schema:
        dtypes = " ".join(column["dtype"] for column in probe.basic_schema).lower()
        if "object" in dtypes and any(token in dtypes for token in ("int", "float")):
            return ["classification", "regression", "clustering"]
        if "int" in dtypes or "float" in dtypes:
            return ["regression", "clustering", "ranking"]
    return ["clustering", "retrieval", "descriptive segmentation"]


def ml_fitness_for_probe(probe: ProbeResult) -> float:
    if probe.downloadable and (probe.column_estimate or 0) >= 4:
        return 0.78
    if probe.tabular_hint:
        return 0.62
    return 0.38


OPPORTUNITY_PROMPT = """You are OpportunityAgent.
Return JSON with keys:
application_ideas: list of 3 strings
audiences: list of 3 strings
ml_task_candidates: list of 3 strings
accessibility_score: float from 0 to 1
novelty_score: float from 0 to 1
storytelling_score: float from 0 to 1
rationale: string
Do not include any extra keys."""

FINDINGS_PROMPT = """You are FindingsAgent.
Return JSON with keys:
likely_findings: list of 3 strings
ml_fitness_score: float from 0 to 1
rationale: string
Do not include any extra keys."""

SKEPTIC_PROMPT = """You are SkepticAgent.
Return JSON with keys:
risks: list of 3 strings
skepticism_summary: string
score_adjustment: float from -0.3 to 0
Do not include any extra keys."""

RANKER_PROMPT = """You are RankerAgent.
Return JSON with keys:
overall_priority_score: float from 0 to 1
next_step_recommendation: string
Do not include any extra keys."""

