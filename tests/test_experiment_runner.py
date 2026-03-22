from __future__ import annotations

from pathlib import Path

from autonomous_data_team.experiment_runner import build_dataset_profile, heuristic_problem_frame, run_experiments


def test_build_profile_and_run_supervised_experiment(settings) -> None:
    path = str(Path("tests/fixtures/data/sample.csv"))
    profile = build_dataset_profile(path, settings)
    frame = heuristic_problem_frame(profile)
    result = run_experiments(path, frame, settings)

    assert profile.rows == 3
    assert frame.task_type in {"classification", "regression", "clustering"}
    assert result.best_model
    assert result.experiments or result.best_model == "descriptive_only"
