"""PCI FastAPI service — Coverage Opportunity Score endpoints.

Endpoints:
  GET  /score/{h3_cell_id}          — score + SHAP for a single cell
  GET  /ranked-list                  — ranked build list for a market
  POST /scenario                     — async Digital Twin scenario trigger
  GET  /health                       — liveness/readiness probe

All data is served from BigQuery pci_scoring tables (batch-computed weekly).
The /scenario endpoint triggers an async Vertex AI Pipeline run.
"""

import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from google.cloud import bigquery
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Telco PCI API",
    description="Predictive Coverage Intelligence — Coverage Opportunity Scores",
    version="1.0.0",
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "telco-pci-prod")
BQ_DATASET = os.environ.get("BQ_DATASET_SCORING", "pci_scoring")

_bq_client: Optional[bigquery.Client] = None


def bq() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


# ─── Response models ────────────────────────────────────────────────────────────

class ShapFeature(BaseModel):
    feature_name: str
    shap_value: float


class CellScore(BaseModel):
    h3_index: str
    market_id: str
    coverage_opportunity_score: float
    scored_at: str
    top_shap_features: list[ShapFeature]


class BuildListItem(BaseModel):
    h3_index: str
    market_id: str
    intervention_name: str
    coverage_opportunity_score: float
    score_uplift: float
    predicted_annual_revenue_uplift_usd: float
    roi_ratio: float
    roi_rank_in_market: int
    capex_usd: float


class ScenarioRequest(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    market_id: str
    h3_resolution: int = Field(default=8, ge=7, le=9)
    intervention_types: list[int] = Field(default=[1, 2, 3, 4])


class ScenarioResponse(BaseModel):
    job_id: str
    status: str
    message: str


# ─── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/score/{h3_cell_id}", response_model=CellScore)
def get_cell_score(h3_cell_id: str):
    """Return the latest Coverage Opportunity Score and top SHAP features for a cell."""
    score_query = f"""
        SELECT
          h3_index,
          market_id,
          coverage_opportunity_score,
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', scored_at) AS scored_at
        FROM `{PROJECT_ID}.{BQ_DATASET}.ranked_build_list`
        WHERE h3_index = @h3_index
          AND intervention_type = 0
        ORDER BY scenario_date DESC
        LIMIT 1
    """
    shap_query = f"""
        SELECT feature_name, shap_value
        FROM `{PROJECT_ID}.{BQ_DATASET}.shap_values`
        WHERE h3_index = @h3_index
        ORDER BY ABS(shap_value) DESC
        LIMIT 10
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("h3_index", "STRING", h3_cell_id)]
    )

    scores = list(bq().query(score_query, job_config=job_config).result())
    if not scores:
        raise HTTPException(status_code=404, detail=f"No score found for cell {h3_cell_id}")

    shap_rows = list(bq().query(shap_query, job_config=job_config).result())

    row = scores[0]
    return CellScore(
        h3_index=row.h3_index,
        market_id=row.market_id,
        coverage_opportunity_score=row.coverage_opportunity_score,
        scored_at=row.scored_at,
        top_shap_features=[
            ShapFeature(feature_name=s.feature_name, shap_value=s.shap_value)
            for s in shap_rows
        ],
    )


@app.get("/ranked-list", response_model=list[BuildListItem])
def get_ranked_list(
    market_id: str = Query(..., description="Market identifier (e.g. nyc, la)"),
    top_n: int = Query(default=100, ge=1, le=1000),
    intervention_type: Optional[int] = Query(default=None, description="Filter by intervention type (1–4)"),
    min_roi: Optional[float] = Query(default=None),
):
    """Return the ROI-ranked build list for a market."""
    intervention_filter = (
        f"AND intervention_type = {intervention_type}" if intervention_type else ""
    )
    roi_filter = f"AND roi_ratio >= {min_roi}" if min_roi else ""

    query = f"""
        SELECT
          h3_index,
          market_id,
          intervention_name,
          coverage_opportunity_score,
          score_uplift,
          predicted_annual_revenue_uplift_usd,
          roi_ratio,
          roi_rank_in_market,
          capex_usd
        FROM `{PROJECT_ID}.{BQ_DATASET}.ranked_build_list`
        WHERE market_id = @market_id
          {intervention_filter}
          {roi_filter}
        ORDER BY roi_rank_in_market
        LIMIT {top_n}
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("market_id", "STRING", market_id)]
    )

    rows = list(bq().query(query, job_config=job_config).result())
    if not rows:
        raise HTTPException(status_code=404, detail=f"No build list found for market {market_id}")

    return [
        BuildListItem(
            h3_index=r.h3_index,
            market_id=r.market_id,
            intervention_name=r.intervention_name,
            coverage_opportunity_score=r.coverage_opportunity_score,
            score_uplift=r.score_uplift,
            predicted_annual_revenue_uplift_usd=r.predicted_annual_revenue_uplift_usd,
            roi_ratio=r.roi_ratio,
            roi_rank_in_market=r.roi_rank_in_market,
            capex_usd=r.capex_usd,
        )
        for r in rows
    ]


@app.post("/scenario", response_model=ScenarioResponse)
def trigger_scenario(request: ScenarioRequest):
    """Async trigger for a Digital Twin what-if scenario at a proposed lat/lon.

    The actual scoring runs as a Vertex AI Pipeline job. The caller polls
    BigQuery for results using the returned job_id.
    """
    import h3
    import uuid
    from google.cloud import aiplatform

    h3_index = h3.geo_to_h3(request.latitude, request.longitude, request.h3_resolution)
    job_id = str(uuid.uuid4())[:8]

    try:
        aiplatform.init(project=PROJECT_ID, location=os.environ.get("GCP_REGION", "us-central1"))
        job = aiplatform.PipelineJob(
            display_name=f"pci-scenario-{job_id}",
            template_path=f"gs://{PROJECT_ID}-ml-artifacts/pipelines/pci_weekly_pipeline.yaml",
            parameter_values={
                "market_id": request.market_id,
                "feature_date": str(__import__("datetime").date.today()),
            },
            labels={"scenario_job_id": job_id, "h3_index": h3_index},
        )
        job.submit()
    except Exception as exc:
        logger.error("Failed to submit scenario pipeline: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to trigger scenario pipeline")

    return ScenarioResponse(
        job_id=job_id,
        status="submitted",
        message=f"Scenario pipeline submitted. Poll BigQuery pci_scoring.scenario_predictions WHERE job_id='{job_id}'",
    )
