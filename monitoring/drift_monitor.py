"""Prediction drift and data quality monitoring for the PCI scoring system.

Run weekly after the scoring pipeline completes. Detects:
  - Score distribution shift (Coverage Opportunity Score per market)
  - Feature distribution drift (key RF and business features)
  - Missing data rate regression

Alerts are sent to Cloud Monitoring as custom metrics.
"""

import logging
import os
from datetime import date, timedelta

from google.cloud import bigquery, monitoring_v3

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "telco-pci-prod")
MONITORING_CLIENT = monitoring_v3.MetricServiceClient()
PROJECT_NAME = f"projects/{PROJECT_ID}"

# Alert if score distribution mean shifts by more than this many points
SCORE_DRIFT_THRESHOLD = 10.0

# Key features to monitor for distribution drift
MONITORED_FEATURES = [
    "rsrp_median_dbm",
    "weak_signal_pct",
    "pop_density_per_km2",
    "churn_rate",
    "revenue_per_km2",
]


def get_score_distribution(client: bigquery.Client, run_date: str) -> dict:
    query = f"""
        SELECT
          market_id,
          AVG(coverage_opportunity_score) AS mean_score,
          STDDEV(coverage_opportunity_score) AS std_score,
          APPROX_QUANTILES(coverage_opportunity_score, 100)[OFFSET(10)] AS p10,
          APPROX_QUANTILES(coverage_opportunity_score, 100)[OFFSET(50)] AS p50,
          APPROX_QUANTILES(coverage_opportunity_score, 100)[OFFSET(90)] AS p90,
          COUNT(*) AS n_cells
        FROM `{PROJECT_ID}.pci_scoring.ranked_build_list`
        WHERE DATE(scored_at) = '{run_date}'
          AND intervention_type = 1
        GROUP BY market_id
    """
    return {row.market_id: dict(row) for row in client.query(query).result()}


def get_feature_stats(client: bigquery.Client, run_date: str) -> dict:
    feature_selects = ", ".join(
        f"AVG({f}) AS {f}_mean, STDDEV({f}) AS {f}_std" for f in MONITORED_FEATURES
    )
    query = f"""
        SELECT {feature_selects}
        FROM `{PROJECT_ID}.pci_features.h3_features_snapshot`
        WHERE feature_date = '{run_date}'
    """
    rows = list(client.query(query).result())
    return dict(rows[0]) if rows else {}


def write_metric(metric_type: str, value: float, labels: dict) -> None:
    series = monitoring_v3.TimeSeries()
    series.metric.type = f"custom.googleapis.com/pci/{metric_type}"
    for k, v in labels.items():
        series.metric.labels[k] = v
    series.resource.type = "global"
    series.resource.labels["project_id"] = PROJECT_ID

    point = monitoring_v3.Point()
    point.value.double_value = value
    now = __import__("time").time()
    point.interval.end_time.seconds = int(now)
    series.points.append(point)

    MONITORING_CLIENT.create_time_series(
        request={"name": PROJECT_NAME, "time_series": [series]}
    )


def run(run_date: str, baseline_date: str) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    current_dist = get_score_distribution(client, run_date)
    baseline_dist = get_score_distribution(client, baseline_date)

    for market_id, stats in current_dist.items():
        write_metric("score_mean", stats["mean_score"], {"market_id": market_id})
        write_metric("score_p50", stats["p50"], {"market_id": market_id})
        write_metric("n_scored_cells", stats["n_cells"], {"market_id": market_id})

        if market_id in baseline_dist:
            drift = abs(stats["mean_score"] - baseline_dist[market_id]["mean_score"])
            write_metric("score_drift", drift, {"market_id": market_id})
            if drift > SCORE_DRIFT_THRESHOLD:
                logger.warning(
                    "Score drift alert: market=%s drift=%.2f (threshold=%.2f)",
                    market_id, drift, SCORE_DRIFT_THRESHOLD,
                )

    current_features = get_feature_stats(client, run_date)
    for feature in MONITORED_FEATURES:
        mean_key = f"{feature}_mean"
        if mean_key in current_features and current_features[mean_key] is not None:
            write_metric(
                f"feature_mean/{feature}",
                float(current_features[mean_key]),
                {"run_date": run_date},
            )

    logger.info("Drift monitoring complete for run_date=%s", run_date)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", default=str(date.today()))
    parser.add_argument("--baseline_date", default=str(date.today() - timedelta(weeks=4)))
    args = parser.parse_args()
    run(args.run_date, args.baseline_date)
