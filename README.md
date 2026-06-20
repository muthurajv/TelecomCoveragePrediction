# TelecomCoveragePrediction

Predictive Coverage Intelligence (PCI) system that identifies telecom coverage gaps and ranks infrastructure investments by ROI. Built on GCP with BigQuery as the data lakehouse and Vertex AI for ML.

## What It Does

- Ingests 4 data domains (network inventory, RF performance, customer/commercial, geospatial)
- Engineers features on an H3 hexagonal grid (~0.74 km² cells)
- Trains XGBoost/LightGBM models to score coverage gaps (0–100 Coverage Opportunity Score)
- Runs a Digital Twin scenario engine comparing 4 intervention types (macro tower, small cell, spectrum upgrade, fiber/backhaul) ranked by ROI
- Computes SHAP explanations per cell so planners know *why* a cell was ranked high
- Serves results via a FastAPI REST API and weekly BigQuery batch pipeline

## Architecture

```
RF Telemetry ──► Pub/Sub Lite ──► Dataflow ──► BigQuery (pci_raw)
OSS Export   ──► GCS ──────────► Dataflow ──► BigQuery (pci_curated)
CRM Export   ──► GCS ──────────► Dataflow ──┘
Geospatial   ──► GCS (COG) ────► BigLake ──┘
                                      │
                              Dataproc Serverless
                         (H3 grid, Sedona spatial joins,
                          feature aggregation)
                                      │
                         BigQuery (pci_features)
                      H3 snapshot tables, feature_date partitioned
                                      │
                         Vertex AI Pipeline (weekly, 11 steps)
                      ┌───────────────┴────────────────┐
                  XGBoost/LightGBM            Dataproc TreeSHAP
                  + Optuna tuning             (broadcast model)
                  + Geospatial block CV                │
                      └───────────────┬────────────────┘
                                      │
                         BigQuery (pci_scoring)
                    Coverage Opportunity Scores + SHAP
                    Digital Twin scenarios + ROI ranks
                                      │
                              FastAPI (Cloud Run)
                         /score  /ranked-list  /scenario
```

**Orchestration**: Cloud Composer 2 handles cross-system data readiness gates; Vertex AI Pipelines handles the ML workflow.

## Project Structure

```
├── config/
│   └── grid_definition.yaml        # H3 resolutions, market bboxes, BQ dataset names
├── dags/
│   └── pci_orchestration_dag.py    # Cloud Composer DAG (data gates → Vertex Pipeline)
├── infrastructure/terraform/
│   ├── main.tf                     # GCS, BigQuery, Pub/Sub, Dataplex, Cloud Run, IAM
│   ├── variables.tf
│   └── outputs.tf
├── monitoring/
│   └── drift_monitor.py            # Score + feature drift → Cloud Monitoring
├── pipelines/
│   └── pci_weekly_pipeline.py      # Vertex AI Pipelines DSL (11 steps)
├── scenario_engine/
│   ├── scenario_feature_gen.sql    # 4-intervention feature perturbation matrix
│   └── roi_calc.sql                # ROI ranking (revenue uplift / CapEx)
├── schemas/
│   └── bigquery_feature_table_schema.json
├── shap/
│   └── distributed_shap_spark.py  # Dataproc Serverless TreeSHAP job
├── src/
│   ├── api/main.py                 # FastAPI service
│   ├── features/feature_schema.py  # Canonical 35-feature spec (training + serving)
│   └── ingestion/
│       ├── base_pipeline.py        # Abstract Dataflow base (validation, dead-letter)
│       ├── network_inventory_pipeline.py
│       └── rf_performance_pipeline.py
├── training/
│   └── train_xgb.py               # XGBoost + Optuna + geospatial block CV + MLflow
├── Dockerfile                      # FastAPI API image (Cloud Run)
├── implementation-plan.md          # Full 64-week phased implementation plan
└── requirements.txt
```

## Prerequisites

- Python 3.12+
- GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Terraform >= 1.7

## Environment Setup

All configuration is driven by environment variables. A template is provided:

```bash
cp .env.example .env
```

Then edit `.env` with your values:

| Variable | Description | Example |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | GCP project ID | `telco-pci-prod` |
| `GCP_REGION` | GCP region | `us-central1` |
| `BQ_DATASET_SCORING` | BigQuery scoring dataset | `pci_scoring` |
| `GCS_DATA_LAKE_BUCKET` | Raw data lake bucket | `telco-pci-prod-data-lake` |
| `GCS_ML_ARTIFACTS_BUCKET` | ML artifacts bucket | `telco-pci-prod-ml-artifacts` |
| `PUBSUB_RF_TELEMETRY_TOPIC` | RF telemetry ingest topic | `rf-telemetry-ingest` |
| `MLFLOW_TRACKING_URI` | MLflow server URI | `http://localhost:5000` |
| `VERTEX_PIPELINE_ROOT` | GCS root for pipeline runs | `gs://…/pipeline_runs` |
| `API_PORT` | Local API port | `8000` |

See `.env.example` for the full list. The `.env` file is gitignored — never commit it.

Load the env before running any component:

```bash
export $(cat .env | xargs)
```

Or use `gcloud` for local GCP auth:

```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

## Quick Start (Local API)

```bash
# 1. Set up environment
cp .env.example .env   # fill in your values
export $(cat .env | xargs)

# 2. Authenticate to GCP
gcloud auth application-default login

# 3. Install dependencies and start
pip install fastapi "uvicorn[standard]" pydantic google-cloud-bigquery h3

uvicorn src.api.main:app --host 0.0.0.0 --port $API_PORT --reload
```

API docs at `http://localhost:8000/docs`. BQ-connected endpoints require GCP credentials and a provisioned dataset.

## GCP Deployment

### 1. Provision infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform apply -var="project_id=YOUR_PROJECT_ID"
```

### 2. Build and push the API image

```bash
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/pci/api:latest
```

### 3. Compile and register the Vertex AI Pipeline

```bash
python pipelines/pci_weekly_pipeline.py
# Outputs: pipelines/pci_weekly_pipeline.yaml

gsutil cp pipelines/pci_weekly_pipeline.yaml \
  gs://YOUR_PROJECT_ID-ml-artifacts/pipelines/
```

### 4. Run the weekly pipeline manually

```bash
export GCP_PROJECT_ID=YOUR_PROJECT_ID
export GCP_REGION=us-central1

gcloud ai pipeline-jobs run \
  --project=$GCP_PROJECT_ID \
  --region=$GCP_REGION \
  --display-name=pci-manual-run \
  --template-uri=gs://$GCP_PROJECT_ID-ml-artifacts/pipelines/pci_weekly_pipeline.yaml
```

The Cloud Composer DAG (`pci_weekly_scoring`) runs automatically every Monday at 04:00 UTC.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Liveness probe |
| `GET` | `/score/{h3_cell_id}` | Coverage Opportunity Score + top SHAP features for a cell |
| `GET` | `/ranked-list?market_id=nyc&top_n=100` | ROI-ranked build list for a market |
| `POST` | `/scenario` | Async Digital Twin: score 4 interventions at a lat/lon |

Full schema at `/docs` (Swagger UI).

## ML Pipeline (11 Steps)

1. Data validation (Great Expectations)
2. Point-in-time feature pull from BigQuery snapshot
3. Geospatial block CV fold validation (Moran's I)
4. XGBoost/LightGBM custom training (Vertex AI)
5. Optuna hyperparameter search (logged to MLflow)
6. Evaluation gate (recall ≥ 0.70 required)
7. Historical backtesting against known build projects
8. Distributed TreeSHAP via Dataproc Serverless
9. Model registration to Vertex AI Model Registry
10. Digital Twin batch prediction (4 interventions × all H3 cells)
11. ROI calculation + ranked build list materialization

## Key Design Decisions

**Geospatial block CV over random CV** — spatial autocorrelation inflates random-CV metrics; folds hold out contiguous H3 regions.

**TreeSHAP via Dataproc, not Vertex Explainability** — Vertex uses Sampled Shapley which distorts feature importance rank-order; TreeSHAP is exact.

**BigQuery feature tables over Vertex Feature Store** — batch-only scoring doesn't need online serving; BQ snapshot tables are cheaper and simpler.

**Dataproc Sedona for H3 spatial joins** — Dataflow lacks vectorized geometry performance for polygon-to-H3 intersections at scale.

**ROI computed in SQL post-scoring** — keeps the model pure and the ROI formula auditable without retraining.

## Top Risks

| Risk | Mitigation |
|------|------------|
| No agreed "coverage gap" label definition | Label Strategy Document signed by Network Planning before model training begins |
| BigQuery cost overrun at national H3 scale (~30M cells at res-9) | Slot Reservations + partition/cluster by H3 prefix; cost modeled before Phase 2 |
| Adoption failure if output doesn't match planners' workflow | Workflow audit with planning engineers; output schema matches their capital review format |
