"""Distributed TreeSHAP computation via Dataproc Serverless PySpark.

Loads the XGBoost model artifact from GCS, broadcasts it to all Spark workers,
partitions H3 cells by geographic region prefix, and computes TreeSHAP values
per partition. Outputs to BigQuery shap_values table.

Usage (submit to Dataproc Serverless):
  gcloud dataproc batches submit pyspark shap/distributed_shap_spark.py \
    --project=vz-pci \
    --region=us-central1 \
    -- --project_id=vz-pci --run_date=2025-01-01 --model_gcs_path=gs://...
"""

import argparse
import logging
import os
import tempfile

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, FloatType, StringType, StructField, StructType,
)

logger = logging.getLogger(__name__)

SHAP_SCHEMA = StructType([
    StructField("h3_index",      StringType(), False),
    StructField("run_date",      StringType(), False),
    StructField("feature_name",  StringType(), False),
    StructField("shap_value",    FloatType(),  True),
    StructField("base_value",    FloatType(),  True),
])


def load_model(model_gcs_path: str):
    """Download model artifact from GCS and load as XGBClassifier."""
    import xgboost as xgb
    from google.cloud import storage

    client = storage.Client()
    bucket_name, blob_path = model_gcs_path.replace("gs://", "").split("/", 1)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        blob.download_to_filename(tmp.name)
        model = xgb.XGBClassifier()
        model.load_model(tmp.name)
    return model


def compute_shap_partition(rows, broadcast_model, feature_cols: list[str], run_date: str):
    """Called per partition; model is a broadcast variable (not reloaded from GCS)."""
    import shap

    records = list(rows)
    if not records:
        return

    model = broadcast_model.value
    explainer = shap.TreeExplainer(model)

    df = pd.DataFrame(records)
    X = df[feature_cols].astype(float)

    shap_values = explainer.shap_values(X)  # ndarray shape (n_samples, n_features)
    base_value = float(explainer.expected_value)

    for i, row in df.iterrows():
        h3_idx = str(row["h3_index"])
        for j, feat in enumerate(feature_cols):
            yield (h3_idx, run_date, feat, float(shap_values[i][j]), base_value)


def main(args: argparse.Namespace) -> None:
    spark = (
        SparkSession.builder
        .appName(f"pci-shap-{args.run_date}")
        .config("spark.sql.shuffle.partitions", "200")
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.37.0",
        )
        .getOrCreate()
    )

    from src.features.feature_schema import ALL_FEATURE_COLS, BOOL_FEATURE_COLS

    feature_cols = ALL_FEATURE_COLS  # must match training feature order

    # Load and broadcast model — single GCS read for all workers
    model = load_model(args.model_gcs_path)
    broadcast_model = spark.sparkContext.broadcast(model)

    # Read feature snapshot from BigQuery
    feature_df = (
        spark.read.format("bigquery")
        .option("project", args.project_id)
        .option("dataset", "pci_features")
        .option("table", "h3_features_snapshot")
        .load()
        .filter(F.col("feature_date") == args.run_date)
        .select(["h3_index", "h3_region_prefix"] + feature_cols)
    )

    # Repartition by geographic region prefix for locality
    feature_df = feature_df.repartition(200, "h3_region_prefix")

    shap_rdd = feature_df.rdd.mapPartitions(
        lambda rows: compute_shap_partition(
            rows, broadcast_model, feature_cols, args.run_date
        )
    )

    shap_df = spark.createDataFrame(shap_rdd, schema=SHAP_SCHEMA)

    (
        shap_df.write.format("bigquery")
        .option("project", args.project_id)
        .option("dataset", "pci_scoring")
        .option("table", "shap_values")
        .option("writeMethod", "direct")
        .option("partitionField", "run_date")
        .mode("append")
        .save()
    )

    logger.info("SHAP computation complete for run_date=%s", args.run_date)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id",      required=True)
    parser.add_argument("--run_date",        required=True, help="YYYY-MM-DD")
    parser.add_argument("--model_gcs_path",  required=True, help="gs://bucket/path/model.json")
    main(parser.parse_args())
