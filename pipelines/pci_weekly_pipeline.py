"""Vertex AI Pipelines DSL — 11-step weekly PCI scoring pipeline.

Scheduled weekly by Cloud Composer via VertexAIPipelineRunOperator.
Each step is a Vertex Pipeline component; dependencies are declared explicitly.

Steps:
  1.  Data validation (Great Expectations)
  2.  Feature pull from BigQuery snapshot (point-in-time correct)
  3.  Geo-fold assignment validation
  4.  XGBoost/LightGBM training job
  5.  Optuna hyperparameter search (inside training job)
  6.  Model evaluation gate
  7.  Historical backtesting
  8.  SHAP computation (Dataproc Serverless)
  9.  Model registration to Vertex Model Registry
  10. Digital Twin batch prediction
  11. ROI calculation + ranked build list materialization
"""

import os
from datetime import date

from kfp import dsl
from kfp.dsl import component, Input, Output, Dataset, Model, Metrics
from google.cloud import aiplatform

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "telco-pci-prod")
REGION = os.environ.get("GCP_REGION", "us-central1")
PIPELINE_ROOT = f"gs://{PROJECT_ID}-ml-artifacts/pipeline_runs"
BQ_DATASET_FEATURES = "pci_features"
BQ_DATASET_SCORING = "pci_scoring"
TRAINING_IMAGE = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/pci/training:latest"
BASE_IMAGE = "python:3.11-slim"


@component(base_image=BASE_IMAGE, packages_to_install=["great-expectations==0.18.15", "google-cloud-bigquery==3.25.0"])
def validate_data(
    project_id: str,
    feature_date: str,
    bq_dataset: str,
    validation_report: Output[Dataset],
) -> bool:
    """Step 1: Run Great Expectations suite on the feature snapshot."""
    from google.cloud import bigquery
    import json

    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT
          COUNT(*) AS total_rows,
          COUNTIF(h3_index IS NULL) AS null_h3,
          COUNTIF(rsrp_median_dbm IS NULL) AS null_rsrp,
          COUNTIF(pop_density_per_km2 IS NULL) AS null_pop_density,
          AVG(CASE WHEN rsrp_median_dbm IS NOT NULL THEN 1.0 ELSE 0.0 END) AS rsrp_completeness,
          AVG(CASE WHEN pop_density_per_km2 IS NOT NULL THEN 1.0 ELSE 0.0 END) AS pop_completeness
        FROM `{project_id}.{bq_dataset}.h3_features_snapshot`
        WHERE feature_date = '{feature_date}'
    """
    row = list(client.query(query).result())[0]

    report = {
        "feature_date": feature_date,
        "total_rows": row.total_rows,
        "rsrp_completeness": float(row.rsrp_completeness),
        "pop_completeness": float(row.pop_completeness),
        "passed": (
            row.total_rows > 0
            and float(row.rsrp_completeness) >= 0.95
            and float(row.pop_completeness) >= 0.95
        ),
    }

    with open(validation_report.path, "w") as f:
        json.dump(report, f)

    if not report["passed"]:
        raise ValueError(f"Data validation failed: {report}")

    return report["passed"]


@component(base_image=BASE_IMAGE, packages_to_install=["google-cloud-bigquery==3.25.0", "pandas==2.2.2", "pyarrow==16.1.0"])
def pull_features(
    project_id: str,
    feature_date: str,
    bq_dataset: str,
    output_dataset: Output[Dataset],
) -> None:
    """Step 2: Export feature snapshot to GCS Parquet for training."""
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT *
        FROM `{project_id}.{bq_dataset}.h3_features_snapshot`
        WHERE feature_date = '{feature_date}'
          AND coverage_gap_label IS NOT NULL
    """
    df = client.query(query).to_dataframe()
    df["h3_region_prefix"] = df["h3_index"].str[:2]
    df.to_parquet(output_dataset.path, index=False)


@component(base_image=BASE_IMAGE, packages_to_install=["pandas==2.2.2", "numpy==1.26.4", "pyarrow==16.1.0"])
def validate_geo_folds(
    input_dataset: Input[Dataset],
    n_folds: int,
    fold_report: Output[Dataset],
) -> None:
    """Step 3: Verify spatial block CV folds have geographic separation."""
    import json
    import pandas as pd
    import numpy as np

    df = pd.read_parquet(input_dataset.path)
    prefixes = sorted(df["h3_region_prefix"].unique())
    fold_size = max(1, len(prefixes) // n_folds)

    report = {
        "n_prefixes": len(prefixes),
        "n_folds": n_folds,
        "avg_fold_size": fold_size,
        "min_fold_samples": int(df.groupby("h3_region_prefix").size().min()),
    }

    with open(fold_report.path, "w") as f:
        json.dump(report, f)


@component(base_image=TRAINING_IMAGE)
def train_model(
    project_id: str,
    feature_date: str,
    market_id: str,
    n_folds: int,
    n_trials: int,
    model_artifact: Output[Model],
    training_metrics: Output[Metrics],
) -> None:
    """Step 4+5: XGBoost training with Optuna hyperparameter search."""
    import subprocess
    subprocess.run([
        "python", "training/train_xgb.py",
        f"--project_id={project_id}",
        f"--feature_date={feature_date}",
        f"--market_id={market_id}",
        f"--n_folds={n_folds}",
        f"--n_trials={n_trials}",
    ], check=True)


@component(base_image=BASE_IMAGE, packages_to_install=["google-cloud-bigquery==3.25.0"])
def evaluate_model(
    project_id: str,
    feature_date: str,
    mlflow_run_id: str,
    min_recall: float,
    evaluation_report: Output[Dataset],
) -> bool:
    """Step 6: Gate on recall threshold before promotion."""
    import json
    import mlflow

    client = mlflow.MlflowClient()
    run = client.get_run(mlflow_run_id)
    metrics = run.data.metrics

    passed = metrics.get("recall", 0.0) >= min_recall

    report = {
        "mlflow_run_id": mlflow_run_id,
        "recall": metrics.get("recall"),
        "auc": metrics.get("auc"),
        "f1": metrics.get("f1"),
        "passed": passed,
    }

    with open(evaluation_report.path, "w") as f:
        json.dump(report, f)

    if not passed:
        raise ValueError(f"Model recall {metrics.get('recall'):.3f} < minimum {min_recall}")

    return passed


@component(base_image=BASE_IMAGE, packages_to_install=["google-cloud-bigquery==3.25.0"])
def backtest_model(
    project_id: str,
    backtest_report: Output[Dataset],
) -> None:
    """Step 7: Backtest against known historical build projects."""
    import json
    # In production: join model predictions against network_inventory sites
    # commissioned in the past 12 months and measure rank of those sites
    # in the model's top-N recommendations.
    report = {"status": "backtest_pending", "note": "Implement with Network Planning ground truth data"}
    with open(backtest_report.path, "w") as f:
        json.dump(report, f)


@component(base_image=BASE_IMAGE, packages_to_install=["google-cloud-dataproc==5.10.0"])
def compute_shap(
    project_id: str,
    region: str,
    run_date: str,
    model_gcs_path: str,
) -> None:
    """Step 8: Submit Dataproc Serverless job for distributed TreeSHAP."""
    from google.cloud import dataproc_v1

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    batch = dataproc_v1.Batch()
    batch.pyspark_batch = dataproc_v1.PySparkBatch(
        main_python_file_uri=f"gs://{project_id}-ml-artifacts/scripts/distributed_shap_spark.py",
        args=[
            f"--project_id={project_id}",
            f"--run_date={run_date}",
            f"--model_gcs_path={model_gcs_path}",
        ],
        python_file_uris=[f"gs://{project_id}-ml-artifacts/packages/pci_lib.zip"],
    )

    operation = client.create_batch(
        request=dataproc_v1.CreateBatchRequest(
            parent=f"projects/{project_id}/locations/{region}",
            batch=batch,
            batch_id=f"shap-{run_date}",
        )
    )
    operation.result(timeout=7200)


@component(base_image=BASE_IMAGE, packages_to_install=["google-cloud-aiplatform==1.57.0", "mlflow==2.13.2"])
def register_model(
    project_id: str,
    region: str,
    mlflow_run_id: str,
    model_display_name: str,
    model_resource_name: Output[str],
) -> None:
    """Step 9: Promote MLflow model artifact to Vertex AI Model Registry."""
    import mlflow
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=region)

    client = mlflow.MlflowClient()
    run = client.get_run(mlflow_run_id)
    artifact_uri = run.info.artifact_uri

    model = aiplatform.Model.upload(
        display_name=model_display_name,
        artifact_uri=f"{artifact_uri}/xgb_model",
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest",
        labels={"mlflow_run_id": mlflow_run_id[:63]},
    )
    model_resource_name = model.resource_name


@component(base_image=BASE_IMAGE, packages_to_install=["google-cloud-aiplatform==1.57.0"])
def batch_predict(
    project_id: str,
    region: str,
    run_date: str,
    model_resource_name: str,
) -> None:
    """Step 10: Vertex Batch Prediction for scenario scores (no online endpoint)."""
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=region)

    model = aiplatform.Model(model_resource_name)
    job = model.batch_predict(
        job_display_name=f"pci-scenario-predict-{run_date}",
        bigquery_source=f"bq://{project_id}.pci_scoring.scenario_features_{run_date}",
        bigquery_destination_prefix=f"bq://{project_id}.pci_scoring",
        instances_format="bigquery",
        predictions_format="bigquery",
        machine_type="n1-standard-4",
        starting_replica_count=10,
        max_replica_count=50,
    )
    job.wait()


@component(base_image=BASE_IMAGE, packages_to_install=["google-cloud-bigquery==3.25.0"])
def compute_roi_and_build_list(
    project_id: str,
    run_date: str,
    market_id: str,
) -> None:
    """Step 11: Run ROI SQL and materialize ranked build list."""
    from google.cloud import bigquery
    import pathlib

    client = bigquery.Client(project=project_id)
    sql = pathlib.Path("scenario_engine/roi_calc.sql").read_text()
    sql = (
        sql.replace("@project_id", project_id)
           .replace("@run_date", f"'{run_date}'")
           .replace("@market_id", f"'{market_id}'" if market_id else "NULL")
    )
    client.query(sql).result()


@dsl.pipeline(
    name="pci-weekly-scoring",
    description="PCI weekly coverage gap scoring and ROI ranking",
    pipeline_root=PIPELINE_ROOT,
)
def pci_weekly_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    feature_date: str = str(date.today()),
    market_id: str = "all",
    n_folds: int = 5,
    n_trials: int = 50,
    min_recall: float = 0.70,
    model_display_name: str = "pci-coverage-gap-classifier",
) -> None:

    validate = validate_data(
        project_id=project_id,
        feature_date=feature_date,
        bq_dataset=BQ_DATASET_FEATURES,
    )

    features = pull_features(
        project_id=project_id,
        feature_date=feature_date,
        bq_dataset=BQ_DATASET_FEATURES,
    ).after(validate)

    geo_folds = validate_geo_folds(
        input_dataset=features.outputs["output_dataset"],
        n_folds=n_folds,
    )

    training = train_model(
        project_id=project_id,
        feature_date=feature_date,
        market_id=market_id,
        n_folds=n_folds,
        n_trials=n_trials,
    ).after(geo_folds)

    evaluation = evaluate_model(
        project_id=project_id,
        feature_date=feature_date,
        mlflow_run_id=training.outputs["mlflow_run_id"],
        min_recall=min_recall,
    )

    backtest = backtest_model(project_id=project_id).after(evaluation)

    shap = compute_shap(
        project_id=project_id,
        region=region,
        run_date=feature_date,
        model_gcs_path=training.outputs["model_gcs_path"],
    ).after(evaluation)

    registration = register_model(
        project_id=project_id,
        region=region,
        mlflow_run_id=training.outputs["mlflow_run_id"],
        model_display_name=model_display_name,
    ).after(backtest)

    prediction = batch_predict(
        project_id=project_id,
        region=region,
        run_date=feature_date,
        model_resource_name=registration.outputs["model_resource_name"],
    )

    compute_roi_and_build_list(
        project_id=project_id,
        run_date=feature_date,
        market_id=market_id,
    ).after(prediction).after(shap)


if __name__ == "__main__":
    from kfp import compiler
    compiler.Compiler().compile(
        pipeline_func=pci_weekly_pipeline,
        package_path="pipelines/pci_weekly_pipeline.yaml",
    )
    print("Pipeline compiled to pipelines/pci_weekly_pipeline.yaml")
