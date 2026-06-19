"""Vertex AI custom training job: XGBoost/LightGBM with geospatial block CV.

Entry point for Vertex AI Training. Reads features from BigQuery snapshot,
runs geospatial block cross-validation, Optuna hyperparameter search,
and logs everything to MLflow. Promotes winning model to Vertex Model Registry.

Usage (local test):
  python training/train_xgb.py --project_id=vz-pci --market_id=nyc --feature_date=2025-01-01
"""

import argparse
import logging
import os
import tempfile
from datetime import date
from typing import Optional

import mlflow
import numpy as np
import optuna
import pandas as pd
import xgboost as xgb
from google.cloud import bigquery
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from src.features.feature_schema import (
    ALL_FEATURE_COLS,
    BOOL_FEATURE_COLS,
    FEATURE_DATE_COL,
    H3_INDEX_COL,
    LABEL_COL,
    MARKET_COL,
    NUMERIC_FEATURE_COLS,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Minimum recall on coverage gaps — we prefer to catch real gaps over precision
MIN_ACCEPTABLE_RECALL = 0.70
CLASSIFICATION_THRESHOLD = 0.40  # lower threshold biases toward recall


def load_features(
    project_id: str,
    feature_date: str,
    market_id: Optional[str] = None,
    bq_dataset: str = "pci_features",
    bq_table: str = "h3_features_snapshot",
) -> pd.DataFrame:
    client = bigquery.Client(project=project_id)

    market_filter = f"AND market_id = '{market_id}'" if market_id else ""
    query = f"""
        SELECT
            {H3_INDEX_COL},
            {MARKET_COL},
            {','.join(ALL_FEATURE_COLS)},
            {LABEL_COL},
            SUBSTR({H3_INDEX_COL}, 1, 2) AS h3_region_prefix
        FROM `{project_id}.{bq_dataset}.{bq_table}`
        WHERE {FEATURE_DATE_COL} = '{feature_date}'
          AND {LABEL_COL} IS NOT NULL
          {market_filter}
    """
    logger.info("Loading features from BigQuery for date=%s", feature_date)
    df = client.query(query).to_dataframe()
    logger.info("Loaded %d labeled rows", len(df))
    return df


def geospatial_block_folds(df: pd.DataFrame, n_folds: int = 5) -> list[tuple]:
    """
    Create spatial block CV folds by partitioning H3 region prefixes.
    Each fold holds out a contiguous geographic block, preventing
    spatial autocorrelation from inflating validation metrics.
    """
    prefixes = sorted(df["h3_region_prefix"].unique())
    fold_size = max(1, len(prefixes) // n_folds)
    folds = []
    for i in range(n_folds):
        test_prefixes = set(prefixes[i * fold_size: (i + 1) * fold_size])
        train_idx = df.index[~df["h3_region_prefix"].isin(test_prefixes)].tolist()
        test_idx = df.index[df["h3_region_prefix"].isin(test_prefixes)].tolist()
        if train_idx and test_idx:
            folds.append((train_idx, test_idx))
    return folds


def prepare_matrices(df: pd.DataFrame, feature_cols: list[str]) -> tuple:
    X = df[feature_cols].copy()
    # Encode booleans as 0/1 for XGBoost
    for col in BOOL_FEATURE_COLS:
        if col in X.columns:
            X[col] = X[col].astype(float)
    y = df[LABEL_COL].values
    return X, y


def objective(trial: optuna.Trial, df: pd.DataFrame, feature_cols: list[str], folds: list) -> float:
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 200, 1000),
        "max_depth": trial.suggest_int("max_depth", 3, 8),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 20.0),
        "tree_method": "hist",
        "eval_metric": "aucpr",
        "use_label_encoder": False,
    }

    fold_aucs = []
    for train_idx, test_idx in folds:
        train_df = df.iloc[train_idx]
        test_df = df.iloc[test_idx]
        X_train, y_train = prepare_matrices(train_df, feature_cols)
        X_test, y_test = prepare_matrices(test_df, feature_cols)

        model = xgb.XGBClassifier(**params, random_state=42)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        proba = model.predict_proba(X_test)[:, 1]
        fold_aucs.append(roc_auc_score(y_test, proba))

    return float(np.mean(fold_aucs))


def train_and_evaluate(
    df: pd.DataFrame,
    feature_cols: list[str],
    best_params: dict,
    folds: list,
) -> tuple[xgb.XGBClassifier, dict]:
    """Train final model on all data and report per-fold metrics."""
    all_metrics: dict[str, list] = {
        "auc": [], "recall": [], "precision": [], "f1": [], "avg_precision": []
    }

    for train_idx, test_idx in folds:
        train_df = df.iloc[train_idx]
        test_df = df.iloc[test_idx]
        X_train, y_train = prepare_matrices(train_df, feature_cols)
        X_test, y_test = prepare_matrices(test_df, feature_cols)

        model = xgb.XGBClassifier(**best_params, random_state=42)
        model.fit(X_train, y_train)

        proba = model.predict_proba(X_test)[:, 1]
        preds = (proba >= CLASSIFICATION_THRESHOLD).astype(int)

        all_metrics["auc"].append(roc_auc_score(y_test, proba))
        all_metrics["recall"].append(recall_score(y_test, preds, zero_division=0))
        all_metrics["precision"].append(precision_score(y_test, preds, zero_division=0))
        all_metrics["f1"].append(f1_score(y_test, preds, zero_division=0))
        all_metrics["avg_precision"].append(average_precision_score(y_test, proba))

    mean_metrics = {k: float(np.mean(v)) for k, v in all_metrics.items()}

    # Final model trained on full dataset
    X_all, y_all = prepare_matrices(df, feature_cols)
    final_model = xgb.XGBClassifier(**best_params, random_state=42)
    final_model.fit(X_all, y_all)

    return final_model, mean_metrics


def main(args: argparse.Namespace) -> None:
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment("pci-coverage-gap-classifier")

    df = load_features(
        project_id=args.project_id,
        feature_date=args.feature_date,
        market_id=args.market_id,
    )

    feature_cols = [c for c in ALL_FEATURE_COLS if c in df.columns]
    folds = geospatial_block_folds(df, n_folds=args.n_folds)
    logger.info("Created %d geospatial CV folds", len(folds))

    with mlflow.start_run(run_name=f"xgb-{args.feature_date}-{args.market_id or 'all'}"):
        mlflow.log_params({
            "feature_date": args.feature_date,
            "market_id": args.market_id or "all",
            "n_folds": len(folds),
            "n_samples": len(df),
            "n_features": len(feature_cols),
            "label_positive_rate": float(df[LABEL_COL].mean()),
            "threshold": CLASSIFICATION_THRESHOLD,
        })

        study = optuna.create_study(direction="maximize")
        study.optimize(
            lambda trial: objective(trial, df, feature_cols, folds),
            n_trials=args.n_trials,
            show_progress_bar=True,
        )

        best_params = study.best_params
        best_params.update({"tree_method": "hist", "use_label_encoder": False})
        mlflow.log_params({f"best_{k}": v for k, v in study.best_params.items()})

        final_model, metrics = train_and_evaluate(df, feature_cols, best_params, folds)

        mlflow.log_metrics(metrics)
        logger.info("CV metrics: %s", metrics)

        if metrics["recall"] < MIN_ACCEPTABLE_RECALL:
            logger.warning(
                "Recall %.3f below minimum %.3f — model should not be promoted",
                metrics["recall"], MIN_ACCEPTABLE_RECALL,
            )

        with tempfile.TemporaryDirectory() as tmp:
            model_path = f"{tmp}/model.json"
            final_model.save_model(model_path)
            mlflow.log_artifact(model_path, artifact_path="model")

        mlflow.xgboost.log_model(
            final_model,
            artifact_path="xgb_model",
            registered_model_name="pci-coverage-gap-classifier",
        )

        logger.info("Training complete. Run ID: %s", mlflow.active_run().info.run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--feature_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--market_id", default=None)
    parser.add_argument("--n_folds", type=int, default=5)
    parser.add_argument("--n_trials", type=int, default=50)
    main(parser.parse_args())
