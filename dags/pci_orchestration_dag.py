"""Cloud Composer 2 (Airflow) DAG — PCI weekly orchestration.

Handles cross-system data readiness gates (OSS/BSS ingestion complete)
before handing off to the Vertex AI Pipeline for all ML steps.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    RunPipelineJobOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "verizon-pci-prod")
REGION = os.environ.get("GCP_REGION", "us-central1")
PIPELINE_TEMPLATE = f"gs://{PROJECT_ID}-ml-artifacts/pipelines/pci_weekly_pipeline.yaml"

DEFAULT_ARGS = {
    "owner": "pci-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["pci-oncall@verizon.com"],
}

with DAG(
    dag_id="pci_weekly_scoring",
    default_args=DEFAULT_ARGS,
    description="PCI weekly coverage gap scoring pipeline",
    schedule_interval="0 4 * * 1",  # Monday 04:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    tags=["pci", "ml", "production"],
) as dag:

    # ── Step 1: Check that all 4 ingestion pipelines completed this week ───────

    check_network_inventory = BigQueryCheckOperator(
        task_id="check_network_inventory_freshness",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.pci_curated.network_inventory`
            WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        """,
        use_legacy_sql=False,
    )

    check_rf_performance = BigQueryCheckOperator(
        task_id="check_rf_performance_freshness",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.pci_curated.rf_performance`
            WHERE DATE(window_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """,
        use_legacy_sql=False,
    )

    check_customer_data = BigQueryCheckOperator(
        task_id="check_customer_data_freshness",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.pci_curated.customer_commercial`
            WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        """,
        use_legacy_sql=False,
    )

    check_geospatial_data = BigQueryCheckOperator(
        task_id="check_geospatial_data_freshness",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.pci_curated.geospatial_external`
            WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        """,
        use_legacy_sql=False,
    )

    # ── Step 2: Trigger Vertex AI Pipeline for all ML steps ───────────────────

    run_vertex_pipeline = RunPipelineJobOperator(
        task_id="run_pci_vertex_pipeline",
        project_id=PROJECT_ID,
        region=REGION,
        display_name=f"pci-weekly-{{{{ ds }}}}",
        template_path=PIPELINE_TEMPLATE,
        parameter_values={
            "project_id": PROJECT_ID,
            "region": REGION,
            "feature_date": "{{ ds }}",
            "market_id": "all",
            "n_folds": 5,
            "n_trials": 50,
            "min_recall": 0.70,
        },
        failure_policy="PIPELINE_FAILURE_POLICY_FAIL_SLOW",
    )

    # ── Step 3: Verify output was written ─────────────────────────────────────

    check_scores_written = BigQueryCheckOperator(
        task_id="check_scores_written",
        sql=f"""
            SELECT COUNT(*) > 1000
            FROM `{PROJECT_ID}.pci_scoring.ranked_build_list`
            WHERE DATE(scored_at) = '{{{{ ds }}}}'
        """,
        use_legacy_sql=False,
    )

    # ── Step 4: Alert on completion ───────────────────────────────────────────

    def _notify_success(**context):
        run_date = context["ds"]
        print(f"PCI weekly scoring complete for {run_date}")

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
    )

    # ── DAG dependencies ──────────────────────────────────────────────────────

    [check_network_inventory, check_rf_performance, check_customer_data, check_geospatial_data] >> run_vertex_pipeline >> check_scores_written >> notify
