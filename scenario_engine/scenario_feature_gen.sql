-- Digital Twin Scenario Engine: 4-intervention feature perturbation matrix
--
-- For each H3 cell that is a candidate for investment, generate 4 rows
-- representing the four intervention types. The model scores each row
-- independently; ROI is computed post-scoring in BigQuery SQL.
--
-- Intervention types:
--   0 = baseline (no action)
--   1 = new macro tower
--   2 = small cell deployment
--   3 = spectrum upgrade (add band)
--   4 = fiber/backhaul upgrade
--
-- Usage:
--   Replace @run_date and @market_id with actual values, or parameterize
--   via the Vertex AI Pipeline component that calls this query.

DECLARE run_date DATE DEFAULT @run_date;
DECLARE market_id STRING DEFAULT @market_id;

-- Macro tower cost assumptions (USD, from Finance model)
DECLARE macro_tower_capex FLOAT64 DEFAULT 350000.0;
DECLARE small_cell_capex  FLOAT64 DEFAULT  45000.0;
DECLARE spectrum_capex    FLOAT64 DEFAULT  80000.0;
DECLARE fiber_capex       FLOAT64 DEFAULT 120000.0;

CREATE OR REPLACE TABLE `@project_id.pci_scoring.scenario_features_@run_date` AS

WITH base AS (
  SELECT
    h3_index,
    market_id,
    feature_date,

    -- Distance features
    dist_nearest_tower_km,
    dist_nearest_competitor_km,
    dist_nearest_road_km,
    dist_nearest_residential_km,
    dist_nearest_fiber_km,

    -- Density features
    pop_density_per_km2,
    subscriber_density_per_km2,
    building_density_per_km2,
    tower_density_5km,
    competitor_density_5km,

    -- RF features (observed)
    rsrp_median_dbm,
    rsrp_p10_dbm,
    rsrp_p90_dbm,
    sinr_median_db,
    sinr_p10_db,
    cqi_median,
    weak_signal_pct,
    dropped_call_rate,
    throughput_degradation_ratio,

    -- Business features
    revenue_per_km2,
    churn_rate,
    complaint_rate_per_1k,
    nps_promoter_pct,
    nps_detractor_pct,
    arpu_tier,

    -- Simulation features (from propagation model)
    sim_coverage_radius_km,
    sim_pop_reach,
    sim_interference_delta,

    -- Terrain
    terrain_elevation_m,
    terrain_slope_deg,
    urban_clutter_index,

    -- Meta
    is_regulatory_constrained,
    has_coverage_hole

  FROM `@project_id.pci_features.h3_features_snapshot`
  WHERE feature_date = run_date
    AND (market_id = market_id OR market_id IS NULL)
    AND is_regulatory_constrained = FALSE
),

interventions AS (
  SELECT
    b.*,
    intervention_type,
    capex_usd,

    -- Perturb features per intervention type to simulate post-build state
    CASE intervention_type
      WHEN 1 THEN GREATEST(b.dist_nearest_tower_km - b.sim_coverage_radius_km, 0.1)
      WHEN 2 THEN GREATEST(b.dist_nearest_tower_km - 0.3, 0.05)   -- small cells have ~300m radius
      ELSE b.dist_nearest_tower_km
    END AS dist_nearest_tower_km_sim,

    CASE intervention_type
      WHEN 1 THEN b.tower_density_5km + 1.0
      WHEN 2 THEN b.tower_density_5km + 0.5
      ELSE b.tower_density_5km
    END AS tower_density_5km_sim,

    -- Spectrum upgrade improves SINR; macro tower improves RSRP
    CASE intervention_type
      WHEN 1 THEN COALESCE(b.rsrp_median_dbm, -115.0) + 10.0   -- +10 dB from new macro
      WHEN 2 THEN COALESCE(b.rsrp_median_dbm, -115.0) + 6.0    -- +6 dB from small cell
      WHEN 3 THEN COALESCE(b.rsrp_median_dbm, -115.0) + 3.0    -- +3 dB from spectrum
      ELSE b.rsrp_median_dbm
    END AS rsrp_median_dbm_sim,

    CASE intervention_type
      WHEN 1 THEN GREATEST(COALESCE(b.weak_signal_pct, 1.0) - 0.60, 0.0)
      WHEN 2 THEN GREATEST(COALESCE(b.weak_signal_pct, 1.0) - 0.40, 0.0)
      WHEN 3 THEN GREATEST(COALESCE(b.weak_signal_pct, 1.0) - 0.20, 0.0)
      ELSE b.weak_signal_pct
    END AS weak_signal_pct_sim,

    CASE intervention_type
      WHEN 4 THEN GREATEST(b.dist_nearest_fiber_km - 1.0, 0.1)
      ELSE b.dist_nearest_fiber_km
    END AS dist_nearest_fiber_km_sim

  FROM base b
  CROSS JOIN UNNEST([
    STRUCT(0 AS intervention_type, 0.0        AS capex_usd),
    STRUCT(1 AS intervention_type, macro_tower_capex AS capex_usd),
    STRUCT(2 AS intervention_type, small_cell_capex  AS capex_usd),
    STRUCT(3 AS intervention_type, spectrum_capex    AS capex_usd),
    STRUCT(4 AS intervention_type, fiber_capex       AS capex_usd)
  ]) AS iv
)

SELECT
  h3_index,
  market_id,
  run_date                        AS scenario_date,
  intervention_type,
  capex_usd,

  -- Pass simulated features as the model input columns
  dist_nearest_tower_km_sim       AS dist_nearest_tower_km,
  dist_nearest_competitor_km,
  dist_nearest_road_km,
  dist_nearest_residential_km,
  dist_nearest_fiber_km_sim       AS dist_nearest_fiber_km,

  pop_density_per_km2,
  subscriber_density_per_km2,
  building_density_per_km2,
  tower_density_5km_sim           AS tower_density_5km,
  competitor_density_5km,

  rsrp_median_dbm_sim             AS rsrp_median_dbm,
  rsrp_p10_dbm,
  rsrp_p90_dbm,
  sinr_median_db,
  sinr_p10_db,
  cqi_median,
  weak_signal_pct_sim             AS weak_signal_pct,
  dropped_call_rate,
  throughput_degradation_ratio,

  revenue_per_km2,
  churn_rate,
  complaint_rate_per_1k,
  nps_promoter_pct,
  nps_detractor_pct,
  arpu_tier,

  sim_coverage_radius_km,
  sim_pop_reach,
  sim_interference_delta,

  terrain_elevation_m,
  terrain_slope_deg,
  urban_clutter_index,

  is_regulatory_constrained,
  has_coverage_hole

FROM interventions
ORDER BY h3_index, intervention_type;
