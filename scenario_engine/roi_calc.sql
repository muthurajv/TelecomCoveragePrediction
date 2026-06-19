-- ROI calculation: combine model scores with CapEx to produce ranked build list.
--
-- Run after Vertex Batch Prediction has scored the scenario_features table.
-- Joins baseline score (intervention_type=0) against each intervention score
-- to compute the predicted uplift, then divides by CapEx for ROI ranking.

CREATE OR REPLACE TABLE `@project_id.pci_scoring.ranked_build_list` AS

WITH scores AS (
  SELECT
    p.h3_index,
    p.market_id,
    p.scenario_date,
    p.intervention_type,
    p.capex_usd,
    p.predicted_gap_score,  -- output from Vertex Batch Prediction (0–1 probability)
    -- Normalize to 0–100 Coverage Opportunity Score
    ROUND(p.predicted_gap_score * 100, 1) AS coverage_opportunity_score
  FROM `@project_id.pci_scoring.scenario_predictions` p
  WHERE p.scenario_date = @run_date
),

baseline AS (
  SELECT h3_index, market_id, coverage_opportunity_score AS baseline_score
  FROM scores
  WHERE intervention_type = 0
),

interventions AS (
  SELECT
    s.h3_index,
    s.market_id,
    s.scenario_date,
    s.intervention_type,
    s.capex_usd,
    s.coverage_opportunity_score,
    b.baseline_score,
    s.coverage_opportunity_score - b.baseline_score AS score_uplift,

    -- Revenue uplift estimate: score_uplift × revenue_per_km2 × pop_density proxy
    -- This is a simplified formula; replace with actuals from Finance model
    ROUND(
      (s.coverage_opportunity_score - b.baseline_score)
      / 100.0
      * f.revenue_per_km2
      * 12,  -- annualize
      2
    ) AS predicted_annual_revenue_uplift_usd,

    -- ROI = annual revenue uplift / CapEx
    SAFE_DIVIDE(
      (s.coverage_opportunity_score - b.baseline_score) / 100.0 * f.revenue_per_km2 * 12,
      s.capex_usd
    ) AS roi_ratio

  FROM scores s
  JOIN baseline b USING (h3_index, market_id)
  JOIN `@project_id.pci_features.h3_features_snapshot` f
    ON s.h3_index = f.h3_index
    AND f.feature_date = @run_date
  WHERE s.intervention_type > 0  -- exclude baseline rows
),

intervention_labels AS (
  SELECT *,
    CASE intervention_type
      WHEN 1 THEN 'macro_tower'
      WHEN 2 THEN 'small_cell'
      WHEN 3 THEN 'spectrum_upgrade'
      WHEN 4 THEN 'fiber_backhaul'
    END AS intervention_name
  FROM interventions
)

SELECT
  h3_index,
  market_id,
  scenario_date,
  intervention_type,
  intervention_name,
  capex_usd,
  baseline_score,
  coverage_opportunity_score,
  score_uplift,
  predicted_annual_revenue_uplift_usd,
  roi_ratio,
  RANK() OVER (
    PARTITION BY market_id
    ORDER BY roi_ratio DESC
  ) AS roi_rank_in_market,
  RANK() OVER (
    ORDER BY roi_ratio DESC
  ) AS roi_rank_global,
  CURRENT_TIMESTAMP() AS scored_at

FROM intervention_labels
WHERE score_uplift > 0  -- only cells where an intervention improves the score
ORDER BY roi_rank_global;
