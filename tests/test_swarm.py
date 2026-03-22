from __future__ import annotations

from pathlib import Path

from autonomous_data_team.storage import Store
from autonomous_data_team.swarm import EDA_AND_EXPERIMENTS, EDA_ONLY, requested_analysis_mode, run_local_dataset_swarm


def test_requested_analysis_mode_defaults_to_eda_and_experiments() -> None:
    assert requested_analysis_mode("Please analyze the attached dataset.") == EDA_AND_EXPERIMENTS
    assert requested_analysis_mode("Run EDA and experiments.") == EDA_AND_EXPERIMENTS


def test_requested_analysis_mode_detects_eda_only() -> None:
    assert requested_analysis_mode("EDA only") == EDA_ONLY
    assert requested_analysis_mode("Please do just EDA for this attachment.") == EDA_ONLY
    assert requested_analysis_mode("Mode: EDA") == EDA_ONLY


def test_run_local_dataset_swarm_skips_experiments_for_eda_only(settings, monkeypatch) -> None:
    store = Store(settings.db_path)
    sample_path = Path("tests/fixtures/data/sample.csv").resolve()

    def fail_if_called(*args, **kwargs):
        raise AssertionError("run_experiments should not be called for EDA-only runs")

    monkeypatch.setattr("autonomous_data_team.swarm.run_experiments", fail_if_called)

    result = run_local_dataset_swarm(
        path=str(sample_path),
        requester_notes="EDA only",
        settings=settings,
        store=store,
    )

    report_path = Path(result["report_path"])
    summary_path = Path(result["summary_path"])
    assert report_path.exists()
    assert summary_path.exists()
    assert "Requested mode: `eda_only`" in report_path.read_text(encoding="utf-8")
    summary_text = summary_path.read_text(encoding="utf-8")
    assert "EDA completed; experiments were not run." in summary_text
    assert "Experiments: not requested" in summary_text
