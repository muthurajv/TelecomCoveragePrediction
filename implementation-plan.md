# Telco Predictive Coverage Intelligence — Implementation Plan (GCP/BigQuery)
> Validated and refined by 3-agent ultraplan review

## Context

`Project description.docx` specifies a PCI system that identifies telecom coverage gaps and ranks infrastructure investments by ROI. This plan implements it on **GCP + BigQuery as the data lakehouse**, Vertex AI for ML, Looker/Looker Studio for BI.

---

## Team

| Role | Responsibility |
|---|---|
| Data Engineering Lead | Pipeline architecture, ingestion, data quality, GCP provisioning |
| ML Engineer (×2) | Feature engineering, model training, Vertex AI Pipelines |
| **Geospatial Data Engineer** (×1) | H3 grid ops, BigQuery GIS, Dataproc Sedona spatial joins |
| **RF/Propagation SME** (×1 part-time) | Propagation model simulation features — front-loaded in Phase 2 |
| Data Scientist (×1) | SHAP analysis, business KPI mapping, Digital Twin scenarios |
| Backend Engineer (×2) | FastAPI/Cloud Run layer, orchestration wiring |
| Frontend Engineer (×1) | Map dashboard, scenario comparison UI |
| MLOps Engineer (×1) | Vertex AI Pipelines, Model Registry, monitoring |
| Product Owner | Stakeholder alignment, DoD sign-offs, pilot coordination |
| Data Governance Lead | PII, Dataplex policies, column-level security, IAM |

> Split the original "Geospatial Analyst" into Geospatial Data Engineer + part-time RF SME — distinct specializations, combining them creates a single point of failure in Phase 2.

---

## GCP Tech Stack

**Data Foundation (Lakehouse)**
- **GCS** — raw landing zone; Iceberg table format for RF telemetry
- **BigQuery** — curated analytics warehouse; feature tables; scoring outputs
- **BigLake** — unified query access over GCS + BigQuery
- **Dataplex** — data governance, quality rules, metadata catalog, lineage
- **Pub/Sub Lite** — high-volume RF telemetry streaming

**ETL / Processing**
- **Dataflow (Apache Beam)** — streaming ETL (Pub/Sub → BigQuery Storage Write API) + simple batch
- **Dataproc Serverless (Spark + Apache Sedona)** — heavy geospatial/H3 batch ops (Dataflow lacks vectorized geometry performance)
- **Cloud Composer 2** — cross-system orchestration (OSS/BSS data readiness gates)

**ML / AI**
- **MLflow on Cloud Run** — experiment tracking, Optuna logging, geospatial CV fold metadata
- **Vertex AI Training** — custom XGBoost/LightGBM jobs; CNN embeddings for raster features
- **Vertex AI Model Registry** — champion/challenger lifecycle, audit trail
- **Vertex AI Pipelines** (KFP v2) — 11-step weekly ML workflow
- **Dataproc Serverless + TreeSHAP** — distributed SHAP; do NOT use Vertex Explainability (uses Sampled Shapley, wrong for ranking)
- **Vertex Batch Prediction** — batch-only inference, no idle online endpoint cost
- **BigQuery feature tables** (snapshot-partitioned by `feature_date`) — defer Vertex Feature Store until real-time scoring is needed
- **BigQuery ML** — smoke-test baseline only

**Serving / Visualization**
- FastAPI on Cloud Run, Deck.gl / Mapbox GL JS, Looker Studio, Terraform, Cloud Monitoring

---

## Revised Timeline (~64 weeks)

| Phase | Name | Weeks | Notes |
|---|---|---|---|
| 0 | Foundation and Discovery | 1–6 | GCP cost modeling pilot wk 4–5 |
| 1 | Data Ingestion and Quality | 7–14 | |
| 2 | Geospatial Grid + Feature Engineering | 15–26 | **Extended to 12 wks** (was 8) |
| 3a | Model Prototyping *(parallel with Ph2)* | 19–26 | Single market, proxy features |
| 3b | Full Model Training and Validation | 27–38 | After feature freeze |
| 4 | Productionization and API Layer | 37–46 | Overlaps Ph3b tail |
| 5 | Visualization and BI Layer | 45–52 | |
| 6 | Pilot, Feedback Loop, and Handoff | 52–64 | |

> Original estimate was 54 weeks — **realistically 64 weeks** given data access delays, H3 scale complexity, and carrier governance.

**Added feedback milestones (zero added scope)**:
- **Wk 10** — Data Readiness Demo to Network Planning
- **Wk 20** — Prototype Demo: H3 hex scores for one metro market
- **Wk 36** — Internal Beta: full pipeline for 2–3 markets via API

---

## Phase 0 — Foundation and Discovery (Wks 1–6)

- M0.1 (Wk 2): Stakeholder workshops — Coverage Opportunity Score formula, target markets, ROI definition, DoD criteria for every phase
- M0.2 (Wk 4): Data landscape assessment → **Label Strategy Document** (signed by Network Planning before Phase 3b)
- M0.3 (Wk 4–5): **GCP cost modeling pilot** — 1-market mini-pipeline, extrapolate BQ slot/storage costs to national H3 scale
- M0.4 (Wk 5): Workflow audit with 2–3 Network Planning engineers — design output schema to match their capital review format
- M0.5 (Wk 6): Provision GCP environments; BigQuery Reservations (separate `streaming_ingest` / `batch_analytics` slot pools); Dataplex; Composer 2; MLflow on Cloud Run; Terraform IaC

**Critical**: Set BQ column-level security and Reservations before any workloads run — retrofitting both breaks things.

**Done When**: All 4 data domains have confirmed read access in dev; infrastructure passes security review; KPI targets + label strategy signed off; GCP cost model approved by Finance.

---

## Phase 1 — Data Ingestion and Quality (Wks 7–14)

**GCP pipelines per domain**:
1. **Network Inventory** — OSS → GCS → Dataflow → BigQuery; daily incremental refresh
2. **RF Performance** — Pub/Sub Lite → Dataflow → BigQuery Storage Write API (COMMITTED, exactly-once); dead-letter topic mandatory; 5-min sliding windows
3. **Customer & Commercial** — CRM → Dataflow → BigQuery; PII anonymized at boundary (aggregated to H3 cell)
4. **Geospatial & External** — DEMs, building footprints, population, FCC + Ookla/Comlinkdata competitor coverage, regulatory zones → GCS (COG format) → BigLake external tables

No separate Cloud SQL PostGIS. H3 indexing via Dataproc Serverless bootstrap job (not BQ UDFs — too slow for bulk).

**Done When**: All 4 pipelines run 3 consecutive weeks without manual intervention; Dataplex dashboard shows >95% completeness on critical fields; row counts reconcile against source systems within 2%.

---

## Phase 2 — Geospatial Grid + Feature Engineering (Wks 15–26, 12 wks)

**Spatial grid**: H3 res-8 urban (~0.74 km²), res-7 rural. Partition BQ feature tables by H3 parent region (first 2 chars), cluster by H3 index.

| Feature Family | Key Features | Service |
|---|---|---|
| Distance | Nearest tower, competitor, road, residential centroid, fiber | Dataproc Sedona |
| Density | Population, subscribers, buildings, tower density (5km), competitor | Dataproc + BQ |
| RF | RSRP/SINR/CQI P10/50/90, weak-signal %, dropped calls, throughput degradation | BQ SQL |
| Business | Revenue/km², churn rate, complaint rate, NPS, ARPU tier | BQ SQL |
| Simulation | Hata/COST-231 propagation model features (RF SME) — labeled synthetic | RF SME + BQ |

Feature store = BigQuery snapshot tables partitioned by `feature_date`. No Vertex Feature Store in V1.

**Done When**: Feature dataset covers all market cells with <5% missing on critical features; no temporal leakage confirmed by audit (features from T-90d, labels from T to T+180d); Network Planning signs off on spatial grid; BQ slot costs within Phase 0 budget.

---

## Phase 3a — Model Prototyping (Wks 19–26, parallel with Phase 2)

One mid-size metro, 5–10% H3 cell sample, proxy features, Jupyter notebook only. Goals: lock label definition with Network Planning, validate score distribution, pick XGBoost vs LightGBM, identify top features to feed back into Phase 2. Removes 6–8 weeks from the critical path.

---

## Phase 3b — Full Model Training and Validation (Wks 27–38)

**11-step Vertex AI Pipeline** (weekly):
1. Great Expectations data validation
2. Point-in-time feature pull from BQ snapshots
3. Geo-fold assignment validation (Moran's I on fold boundaries)
4. XGBoost/LightGBM Custom Training Job (`n1-highmem-16`)
5. Optuna hyperparameter search (MLflow logging)
6. Evaluation gate (AUC, gap recall, urban/suburban/rural disaggregated)
7. Historical backtesting against ≥1 known build project *(required DoD gate)*
8. Distributed TreeSHAP via Dataproc Serverless (broadcast model, partition by H3 parent)
9. Register to Vertex Model Registry if evaluation passes
10. Digital Twin batch scoring (4 interventions × all cells; ROI = revenue uplift / CapEx in BQ SQL post-scoring)
11. Ranked build list materialized to BigQuery

**SHAP scale**: 5M cells nationally = 45–90 min on Dataproc Serverless (50–100 executors). Broadcast model as Spark variable — do not reload from GCS per partition.

**Done When**: Model beats all baselines on held-out geographic region; geospatial block CV confirms cross-region generalization; backtesting accepted by Network Planning; model card filed in Vertex Model Registry.

---

## Phase 4 — Productionization and API Layer (Wks 37–46)

**FastAPI on Cloud Run endpoints**:
- `GET /score/{h3_cell_id}` — score + SHAP breakdown (200ms P99)
- `GET /ranked-list?market={id}&top_n={n}` — ranked build opportunities (2s P99 for top 100)
- `POST /scenario` — async Digital Twin trigger, returns job ID
- `GET /health`

**Batch scoring pipeline** (Cloud Composer triggers Vertex Pipelines weekly): feature pull → Vertex Batch Prediction → Dataproc SHAP → Digital Twin scoring → ROI BQ SQL → ranked list to BigQuery. Idempotent.

**Monitoring**: Vertex Model Monitoring on **output score distribution** (not just input features — output drift is the meaningful signal).

**Done When**: Pipeline runs 3 consecutive weekly production cycles; API latency SLAs met; parity test suite confirms no training-serving skew; prediction drift monitoring active with alert drill completed.

---

## Phase 5 — Visualization and BI Layer (Wks 45–52)

- **Deck.gl H3 choropleth map**: coverage score heatmap, filter panel, click-to-drill SHAP breakdown, layer toggles (towers, competitors, population, roads)
- **Ranked build list**: sort/filter/export (CSV + PowerPoint)
- **Scenario comparison UI**: draw point on map → async `POST /scenario` → 4-intervention side-by-side bar charts + plain-language recommendation card
- **Looker Studio executive KPI dashboard**: coverage %, churn reduction, revenue uplift, CapEx productivity; export-ready for quarterly business reviews

**Done When**: UAT with ≥5 Network Planning users achieves >70% task completion; map loads <3s for full market view; dashboards pass WCAG 2.1 AA.

---

## Phase 6 — Pilot, Feedback Loop, and Handoff (Wks 52–64)

- **Wk 54**: Activate 2–3 pilot markets; Network Planning uses ranked list alongside (not replacing) existing process; log every model agreement/override
- **Wk 58**: Ground truth collection — drive-test coverage improvement as 90-day leading proxy (full revenue uplift attribution takes 12–18 months post-build)
- **Wk 60**: Retrain with pilot data; champion/challenger promotion via Vertex Model Registry
- **Wk 62**: Formalize model governance (update approvals, quarterly review cadence, retirement criteria)
- **Wk 64**: Operational handoff; 3-month hypercare from ML engineer

**Done When**: Pilot evaluation report accepted by stakeholders; ops team independently executes retraining and pipeline recovery; system has run 8+ consecutive weeks without Sev-1 incident.

---

## Critical Files to Create First

1. `infrastructure/terraform/main.tf` — GCP environments, BQ datasets, GCS buckets, Reservations, IAM, Dataplex
2. `infrastructure/bigquery_reservations.tf` — separate slot pools (`streaming_ingest` / `batch_analytics`)
3. `config/grid_definition.yaml` — H3 resolution per market tier, market boundaries, CRS
4. `src/ingestion/base_pipeline.py` — abstract base for all 4 Dataflow ingestion jobs
5. `src/features/feature_schema.py` — canonical feature schema (shared by training and serving)
6. `pipelines/pci_weekly_pipeline.py` — Vertex AI Pipelines DSL (11-step weekly workflow)
7. `training/train_xgb.py` — custom training job (geospatial CV, Optuna, MLflow)
8. `shap/distributed_shap_spark.py` — Dataproc Serverless TreeSHAP job
9. `scenario_engine/scenario_feature_gen.sql` — 4-intervention feature perturbation matrix
10. `schemas/bigquery_feature_table_schema.json` — H3-partitioned feature table schema

---

## Top 3 Risks

| Risk | Mitigation |
|---|---|
| **Label/ground truth definition** — no agreed "coverage gap" definition blocks Phase 3b | Label Strategy Document as Phase 0 deliverable, signed by Network Planning |
| **GCP cost overrun** — res-9 = ~30M cells, naive BQ scans are expensive | Phase 0 cost modeling pilot; BQ Reservations + partitioning before Phase 2; slot budget alerts |
| **Organizational adoption** — Network Planning has existing vendor tools; PCI may be ignored if output doesn't fit their workflow | Phase 0 workflow audit with 2–3 Planning engineers; output schema matches their capital review format |

---

## Verification (End-to-End Test Plan)

1. **Phase 1**: All 4 ingestion DAGs run; Dataplex shows >95% completeness; row counts reconcile vs source
2. **Phase 2**: Leakage audit notebook confirms temporal cutoff; BQ slot costs within Phase 0 estimate
3. **Phase 3a**: Prototype AUC on single market; label definition signed off; BQML parity check passes
4. **Phase 3b**: Geospatial block CV metrics; urban/suburban/rural disaggregated performance; historical backtesting accepted; SHAP rank-order validated with domain expert
5. **Phase 4**: Parity test suite (same H3 cell + as-of date → same features at train and serve time); API load test; alert drill
6. **Phase 5**: UAT task completion rate; map 3-second load; scenario comparison vs API match
7. **Phase 6**: Ops team retraining dry run without build team; pilot adoption rate tracked
