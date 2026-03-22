from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier, DummyRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_squared_error,
    r2_score,
    silhouette_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .config import Settings
from .models import DatasetProfile, ExperimentResult, ProblemFrame


def load_dataframe(path: str, max_rows: int | None = None) -> pd.DataFrame:
    path_obj = Path(path)
    suffix = path_obj.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path_obj, nrows=max_rows)
    if suffix == ".tsv":
        return pd.read_csv(path_obj, sep="\t", nrows=max_rows)
    if suffix == ".json":
        data = json.loads(path_obj.read_text(encoding="utf-8"))
        if isinstance(data, list):
            frame = pd.json_normalize(data)
        elif isinstance(data, dict):
            if all(isinstance(value, list) for value in data.values()):
                frame = pd.DataFrame(data)
            else:
                frame = pd.json_normalize([data])
        else:
            raise ValueError("Unsupported JSON dataset shape")
        return frame.head(max_rows) if max_rows is not None else frame
    if suffix == ".parquet":
        frame = pd.read_parquet(path_obj)
        return frame.head(max_rows) if max_rows is not None else frame
    raise ValueError(f"Unsupported dataset format: {path}")


def build_dataset_profile(path: str, settings: Settings) -> DatasetProfile:
    frame = load_dataframe(path, max_rows=settings.max_dataset_rows)
    rows, cols = frame.shape
    dtypes = [{"name": str(name), "dtype": str(dtype)} for name, dtype in frame.dtypes.items()]
    numeric_frame = frame.select_dtypes(include=["number"])
    missingness = [
        {
            "column": str(column),
            "missing_count": int(frame[column].isna().sum()),
            "missing_pct": round(float(frame[column].isna().mean()), 4),
        }
        for column in frame.columns
    ]
    numeric_summary: list[dict[str, Any]] = []
    if not numeric_frame.empty:
        described = numeric_frame.describe().transpose().fillna(0)
        for column, row in described.iterrows():
            numeric_summary.append(
                {
                    "column": str(column),
                    "mean": round(float(row.get("mean", 0.0)), 4),
                    "std": round(float(row.get("std", 0.0)), 4),
                    "min": round(float(row.get("min", 0.0)), 4),
                    "max": round(float(row.get("max", 0.0)), 4),
                }
            )
    target_candidates = _target_candidates(frame)
    notes: list[str] = []
    if rows < 50:
        notes.append("Dataset is small; model evaluation will be noisy.")
    if not target_candidates:
        notes.append("No obvious supervised target found; clustering may be the fallback.")
    return DatasetProfile(
        dataset_name=Path(path).name,
        path=path,
        rows=int(rows),
        columns=int(cols),
        dtypes=dtypes,
        missingness=missingness,
        numeric_summary=numeric_summary,
        target_candidates=target_candidates,
        notes=notes,
    )


def heuristic_problem_frame(profile: DatasetProfile) -> ProblemFrame:
    if profile.target_candidates:
        candidate = profile.target_candidates[0]
        task_type = candidate["task_type"]
        target_column = candidate["column"]
        metric = "f1_weighted" if task_type == "classification" else "rmse"
        exit_criterion = (
            "Beat the dummy baseline by at least 10% relative improvement on the primary metric."
            if task_type == "classification"
            else "Reduce RMSE by at least 10% versus the dummy baseline."
        )
        anchor = f"Use `{target_column}` as the supervised ground truth anchor."
        statement = f"Predict `{target_column}` from the remaining features."
    else:
        task_type = "clustering"
        target_column = None
        metric = "silhouette"
        exit_criterion = "Produce a positive silhouette score with interpretable segments."
        anchor = "No ground truth label is available; use unsupervised segmentation."
        statement = "Find meaningful clusters and segments in the dataset."
    return ProblemFrame(
        task_type=task_type,
        target_column=target_column,
        primary_metric=metric,
        exit_criterion=exit_criterion,
        ground_truth_anchor=anchor,
        problem_statement=statement,
        rationale="Heuristic problem framing based on target cardinality and available schema.",
    )


def run_experiments(path: str, frame: ProblemFrame, settings: Settings) -> ExperimentResult:
    dataset = load_dataframe(path, max_rows=settings.max_dataset_rows).copy()
    dataset = dataset.dropna(axis=1, how="all")
    dataset = dataset.dropna(how="all")
    if frame.task_type == "classification":
        return _run_supervised(dataset, path, frame, classification=True)
    if frame.task_type == "regression":
        return _run_supervised(dataset, path, frame, classification=False)
    return _run_clustering(dataset, path, frame)


def _run_supervised(dataset: pd.DataFrame, path: str, frame: ProblemFrame, classification: bool) -> ExperimentResult:
    if frame.target_column is None or frame.target_column not in dataset.columns:
        raise ValueError("Target column is required for supervised experiments")
    target = dataset[frame.target_column]
    features = dataset.drop(columns=[frame.target_column])
    usable_features = features.loc[:, features.nunique(dropna=False) > 1]
    if usable_features.empty:
        raise ValueError("No usable feature columns found after dropping constant columns")

    numeric_features = usable_features.select_dtypes(include=["number", "bool"]).columns.tolist()
    categorical_features = [col for col in usable_features.columns if col not in numeric_features]
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, numeric_features),
            ("cat", categorical_pipe, categorical_features),
        ]
    )

    test_size = 0.2
    stratify = None
    if classification and target.nunique() > 1:
        class_counts = target.value_counts(dropna=False)
        if not class_counts.empty and int(class_counts.min()) >= 2:
            min_test_fraction = target.nunique() / max(len(dataset), 1)
            test_size = min(0.5, max(test_size, min_test_fraction))
            stratify = target
    x_train, x_test, y_train, y_test = train_test_split(
        usable_features,
        target,
        test_size=test_size,
        random_state=42,
        stratify=stratify,
    )

    candidates: list[tuple[str, Any]] = []
    if classification:
        baseline = DummyClassifier(strategy="most_frequent")
        candidates = [
            ("dummy_most_frequent", baseline),
            ("logistic_regression", LogisticRegression(max_iter=1000)),
            ("random_forest", RandomForestClassifier(n_estimators=200, random_state=42)),
        ]
    else:
        baseline = DummyRegressor(strategy="mean")
        candidates = [
            ("dummy_mean", baseline),
            ("ridge", Ridge(alpha=1.0)),
            ("random_forest", RandomForestRegressor(n_estimators=200, random_state=42)),
        ]

    experiments: list[dict[str, Any]] = []
    best_row: dict[str, Any] | None = None
    baseline_row: dict[str, Any] | None = None
    for name, estimator in candidates:
        pipeline = Pipeline(steps=[("prep", preprocessor), ("model", estimator)])
        pipeline.fit(x_train, y_train)
        predictions = pipeline.predict(x_test)
        if classification:
            score = float(f1_score(y_test, predictions, average="weighted"))
            extra = {"accuracy": round(float(accuracy_score(y_test, predictions)), 4)}
        else:
            rmse = float(np.sqrt(mean_squared_error(y_test, predictions)))
            score = -rmse
            extra = {"r2": round(float(r2_score(y_test, predictions)), 4), "rmse": round(rmse, 4)}
        row = {"model": name, "primary_metric": round(score, 4), **extra}
        experiments.append(row)
        if baseline_row is None:
            baseline_row = row
        if best_row is None or row["primary_metric"] > best_row["primary_metric"]:
            best_row = row
    if best_row is None or baseline_row is None:
        raise RuntimeError("No experiments were produced for the supervised run")
    if classification:
        baseline_score = float(baseline_row["primary_metric"])
        best_score = float(best_row["primary_metric"])
        error_analysis = [
            f"Weighted F1 improved from {baseline_score:.4f} to {best_score:.4f}.",
            f"Test set size: {len(y_test)} rows across {y_test.nunique()} classes.",
        ]
        caveats = _supervised_caveats(dataset, frame.target_column, classification=True)
    else:
        baseline_score = float(baseline_row["rmse"])
        best_score = float(best_row["rmse"])
        error_analysis = [
            f"RMSE improved from {baseline_score:.4f} to {best_score:.4f}.",
            f"Test set size: {len(y_test)} rows with target `{frame.target_column}`.",
        ]
        caveats = _supervised_caveats(dataset, frame.target_column, classification=False)
    return ExperimentResult(
        dataset_name=Path(path).name,
        dataset_path=path,
        task_type=frame.task_type,
        target_column=frame.target_column,
        primary_metric=frame.primary_metric,
        exit_criterion=frame.exit_criterion,
        baseline_model=str(baseline_row["model"]),
        best_model=str(best_row["model"]),
        baseline_score=baseline_score,
        best_score=best_score,
        experiments=experiments,
        error_analysis=error_analysis,
        caveats=caveats,
    )


def _run_clustering(dataset: pd.DataFrame, path: str, frame: ProblemFrame) -> ExperimentResult:
    encoded = pd.get_dummies(dataset.dropna(axis=1, how="all"), dummy_na=True)
    encoded = encoded.select_dtypes(include=["number", "bool"]).fillna(0)
    if encoded.shape[1] < 2 or len(encoded) < 10:
        return ExperimentResult(
            dataset_name=Path(path).name,
            dataset_path=path,
            task_type="clustering",
            target_column=None,
            primary_metric="silhouette",
            exit_criterion=frame.exit_criterion,
            baseline_model="descriptive_only",
            best_model="descriptive_only",
            baseline_score=None,
            best_score=None,
            experiments=[],
            error_analysis=["Dataset is too small for reliable clustering; returning descriptive-only findings."],
            caveats=[
                "No supervised target was available.",
                "The dataset did not have enough rows for a stable clustering experiment.",
            ],
        )
    max_clusters = min(6, len(encoded) - 1)
    experiments: list[dict[str, Any]] = []
    best_row: dict[str, Any] | None = None
    for n_clusters in range(2, max_clusters + 1):
        model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        labels = model.fit_predict(encoded)
        score = float(silhouette_score(encoded, labels))
        row = {"model": f"kmeans_{n_clusters}", "primary_metric": round(score, 4), "clusters": n_clusters}
        experiments.append(row)
        if best_row is None or score > best_row["primary_metric"]:
            best_row = row
    if best_row is None:
        raise RuntimeError("No clustering experiments were produced")
    return ExperimentResult(
        dataset_name=Path(path).name,
        dataset_path=path,
        task_type="clustering",
        target_column=None,
        primary_metric="silhouette",
        exit_criterion=frame.exit_criterion,
        baseline_model="kmeans_2",
        best_model=str(best_row["model"]),
        baseline_score=float(experiments[0]["primary_metric"]),
        best_score=float(best_row["primary_metric"]),
        experiments=experiments,
        error_analysis=[f"Best silhouette score was {best_row['primary_metric']:.4f}."],
        caveats=[
            "No labeled ground truth was available, so clustering quality is heuristic.",
            "Interpretability should be checked manually before trusting segments.",
        ],
    )


def _target_candidates(frame: pd.DataFrame) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    rows = len(frame)
    preferred_names = {"target", "label", "class", "outcome", "y", "response"}
    for column in frame.columns:
        series = frame[column]
        unique = int(series.nunique(dropna=True))
        if unique < 2:
            continue
        if unique == rows:
            continue
        name_bonus = 100 if str(column).lower() in preferred_names else 0
        if unique <= max(10, int(rows * 0.2)):
            candidates.append(
                {
                    "column": str(column),
                    "task_type": "classification",
                    "unique_values": unique,
                    "priority": name_bonus + (50 - unique),
                }
            )
            continue
        if pd.api.types.is_numeric_dtype(series) and unique < rows:
            candidates.append(
                {
                    "column": str(column),
                    "task_type": "regression",
                    "unique_values": unique,
                    "priority": name_bonus + min(unique, 50),
                }
            )
    candidates.sort(key=lambda item: item.get("priority", 0), reverse=True)
    for candidate in candidates:
        candidate.pop("priority", None)
    return candidates


def _supervised_caveats(dataset: pd.DataFrame, target_column: str, classification: bool) -> list[str]:
    caveats = []
    target = dataset[target_column]
    if target.isna().mean() > 0:
        caveats.append("Rows with missing target values may reduce effective training data.")
    if classification and target.nunique() > 10:
        caveats.append("High-cardinality classification targets may be unstable for simple baselines.")
    if dataset.shape[0] < 250:
        caveats.append("Small sample size means the holdout split may be noisy.")
    return caveats
