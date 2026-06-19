"""Canonical feature schema shared by training and serving.

Any feature added here must also be added to:
  schemas/bigquery_feature_table_schema.json
  training/train_xgb.py  (FEATURE_COLS list)
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class FeatureFamily(str, Enum):
    DISTANCE = "distance"
    DENSITY = "density"
    RF = "rf"
    BUSINESS = "business"
    SIMULATION = "simulation"
    TERRAIN = "terrain"
    META = "meta"


@dataclass
class FeatureSpec:
    name: str
    family: FeatureFamily
    dtype: str          # "float64" | "int64" | "bool" | "string"
    nullable: bool
    description: str
    is_synthetic: bool = False  # True for simulation features — prevents leakage confusion


FEATURE_SPECS: list[FeatureSpec] = [
    # ── Distance ────────────────────────────────────────────────────────────────
    FeatureSpec("dist_nearest_tower_km",      FeatureFamily.DISTANCE, "float64", True,  "Nearest telco tower (km)"),
    FeatureSpec("dist_nearest_competitor_km", FeatureFamily.DISTANCE, "float64", True,  "Nearest competitor site (km)"),
    FeatureSpec("dist_nearest_road_km",       FeatureFamily.DISTANCE, "float64", True,  "Nearest major road (km)"),
    FeatureSpec("dist_nearest_residential_km",FeatureFamily.DISTANCE, "float64", True,  "Nearest residential zone centroid (km)"),
    FeatureSpec("dist_nearest_fiber_km",      FeatureFamily.DISTANCE, "float64", True,  "Nearest fiber/backhaul access point (km)"),

    # ── Density ─────────────────────────────────────────────────────────────────
    FeatureSpec("pop_density_per_km2",        FeatureFamily.DENSITY,  "float64", True,  "Population per km²"),
    FeatureSpec("subscriber_density_per_km2", FeatureFamily.DENSITY,  "float64", True,  "telco subscribers per km²"),
    FeatureSpec("building_density_per_km2",   FeatureFamily.DENSITY,  "float64", True,  "Building footprints per km²"),
    FeatureSpec("tower_density_5km",          FeatureFamily.DENSITY,  "float64", True,  "telco tower count within 5 km"),
    FeatureSpec("competitor_density_5km",     FeatureFamily.DENSITY,  "float64", True,  "Competitor site count within 5 km"),

    # ── RF ──────────────────────────────────────────────────────────────────────
    FeatureSpec("rsrp_median_dbm",            FeatureFamily.RF,       "float64", True,  "Median RSRP (dBm)"),
    FeatureSpec("rsrp_p10_dbm",              FeatureFamily.RF,       "float64", True,  "P10 RSRP (dBm)"),
    FeatureSpec("rsrp_p90_dbm",              FeatureFamily.RF,       "float64", True,  "P90 RSRP (dBm)"),
    FeatureSpec("sinr_median_db",            FeatureFamily.RF,       "float64", True,  "Median SINR (dB)"),
    FeatureSpec("sinr_p10_db",               FeatureFamily.RF,       "float64", True,  "P10 SINR (dB)"),
    FeatureSpec("cqi_median",                FeatureFamily.RF,       "float64", True,  "Median CQI"),
    FeatureSpec("weak_signal_pct",           FeatureFamily.RF,       "float64", True,  "% samples with RSRP < -110 dBm"),
    FeatureSpec("dropped_call_rate",         FeatureFamily.RF,       "float64", True,  "Dropped calls per 1000 attempts"),
    FeatureSpec("throughput_degradation_ratio", FeatureFamily.RF,    "float64", True,  "Peak-to-off-peak throughput ratio"),

    # ── Business ────────────────────────────────────────────────────────────────
    FeatureSpec("revenue_per_km2",           FeatureFamily.BUSINESS, "float64", True,  "Revenue (USD) per km²"),
    FeatureSpec("churn_rate",                FeatureFamily.BUSINESS, "float64", True,  "Subscriber churn rate (0–1)"),
    FeatureSpec("complaint_rate_per_1k",     FeatureFamily.BUSINESS, "float64", True,  "Complaints per 1000 subscribers"),
    FeatureSpec("nps_promoter_pct",          FeatureFamily.BUSINESS, "float64", True,  "NPS promoter %"),
    FeatureSpec("nps_detractor_pct",         FeatureFamily.BUSINESS, "float64", True,  "NPS detractor %"),
    FeatureSpec("arpu_tier",                 FeatureFamily.BUSINESS, "int64",   True,  "ARPU bucket (1=low, 2=mid, 3=high)"),

    # ── Simulation (synthetic — labeled to prevent leakage confusion) ───────────
    FeatureSpec("sim_coverage_radius_km",    FeatureFamily.SIMULATION, "float64", True, "Predicted coverage radius of hypothetical new tower", is_synthetic=True),
    FeatureSpec("sim_pop_reach",             FeatureFamily.SIMULATION, "float64", True, "Estimated additional population reached if gap closed", is_synthetic=True),
    FeatureSpec("sim_interference_delta",    FeatureFamily.SIMULATION, "float64", True, "Estimated interference change from new site (dB)", is_synthetic=True),

    # ── Terrain ─────────────────────────────────────────────────────────────────
    FeatureSpec("terrain_elevation_m",       FeatureFamily.TERRAIN,  "float64", True,  "Median terrain elevation (m)"),
    FeatureSpec("terrain_slope_deg",         FeatureFamily.TERRAIN,  "float64", True,  "Mean terrain slope (degrees)"),
    FeatureSpec("urban_clutter_index",       FeatureFamily.TERRAIN,  "float64", True,  "Urban clutter density index (0–1)"),

    # ── Meta flags ──────────────────────────────────────────────────────────────
    FeatureSpec("is_regulatory_constrained", FeatureFamily.META,     "bool",    True,  "Overlaps FAA/environmental constraint zone"),
    FeatureSpec("has_coverage_hole",         FeatureFamily.META,     "bool",    True,  "No telco tower within propagation distance"),
]

# Feature name lists for quick access
ALL_FEATURE_COLS: list[str] = [s.name for s in FEATURE_SPECS]
NUMERIC_FEATURE_COLS: list[str] = [s.name for s in FEATURE_SPECS if s.dtype in ("float64", "int64")]
BOOL_FEATURE_COLS: list[str] = [s.name for s in FEATURE_SPECS if s.dtype == "bool"]
SYNTHETIC_FEATURE_COLS: list[str] = [s.name for s in FEATURE_SPECS if s.is_synthetic]

LABEL_COL = "coverage_gap_label"
ROI_LABEL_COL = "roi_label_usd"
H3_INDEX_COL = "h3_index"
FEATURE_DATE_COL = "feature_date"
MARKET_COL = "market_id"


def validate_feature_row(row: dict) -> list[str]:
    """Return a list of validation error strings (empty = valid)."""
    errors = []
    for spec in FEATURE_SPECS:
        val = row.get(spec.name)
        if val is None and not spec.nullable:
            errors.append(f"{spec.name} is required but null")
    return errors
