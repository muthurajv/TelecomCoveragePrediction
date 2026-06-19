"""RF Performance streaming ingestion: Pub/Sub Lite → Dataflow → BigQuery.

Uses exactly-once STORAGE_WRITE_API (COMMITTED mode) with a 5-minute
sliding window for KPI aggregation. Dead-letter topic captures malformed records.
"""

import json
import logging
from typing import Any

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.transforms.window import SlidingWindows
from apache_beam.options.pipeline_options import StandardOptions

from src.ingestion.base_pipeline import BasePCIPipeline, PipelineConfig

logger = logging.getLogger(__name__)

WINDOW_SIZE_SECONDS = 300   # 5-minute windows
WINDOW_PERIOD_SECONDS = 60  # slide every 1 minute

RSRP_WEAK_THRESHOLD = -110.0  # dBm
RSRP_VALID_RANGE = (-140.0, -44.0)
SINR_VALID_RANGE = (-20.0, 30.0)
CQI_VALID_RANGE = (0, 15)


class ParseRFRecord(beam.DoFn):
    """Deserialize JSON from Pub/Sub and apply range validation."""

    DEAD_LETTER_TAG = "dead_letter"

    def process(self, element: bytes, *args, **kwargs):
        try:
            record = json.loads(element.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER_TAG,
                {"domain": "rf_performance", "record": str(element), "error": str(exc), "ingested_at": ""},
            )
            return

        rsrp = record.get("rsrp_dbm")
        sinr = record.get("sinr_db")
        cqi = record.get("cqi")

        if rsrp is not None and not (RSRP_VALID_RANGE[0] <= rsrp <= RSRP_VALID_RANGE[1]):
            record["rsrp_dbm"] = None
            record["_rsrp_clipped"] = True

        if sinr is not None and not (SINR_VALID_RANGE[0] <= sinr <= SINR_VALID_RANGE[1]):
            record["sinr_db"] = None

        if cqi is not None and not (CQI_VALID_RANGE[0] <= cqi <= CQI_VALID_RANGE[1]):
            record["cqi"] = None

        yield record


class AggregateRFWindow(beam.CombineFn):
    """Compute per-cell KPI aggregates over a sliding window."""

    def create_accumulator(self):
        return {
            "rsrp_values": [],
            "sinr_values": [],
            "cqi_values": [],
            "throughput_peak_mbps": [],
            "throughput_offpeak_mbps": [],
            "call_attempts": 0,
            "dropped_calls": 0,
            "sector_id": None,
            "h3_index": None,
        }

    def add_input(self, acc, element):
        if element.get("rsrp_dbm") is not None:
            acc["rsrp_values"].append(element["rsrp_dbm"])
        if element.get("sinr_db") is not None:
            acc["sinr_values"].append(element["sinr_db"])
        if element.get("cqi") is not None:
            acc["cqi_values"].append(element["cqi"])
        if element.get("is_peak_hour"):
            if element.get("throughput_mbps"):
                acc["throughput_peak_mbps"].append(element["throughput_mbps"])
        else:
            if element.get("throughput_mbps"):
                acc["throughput_offpeak_mbps"].append(element["throughput_mbps"])
        acc["call_attempts"] += element.get("call_attempts", 0)
        acc["dropped_calls"] += element.get("dropped_calls", 0)
        acc["sector_id"] = element.get("sector_id")
        acc["h3_index"] = element.get("h3_index")
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["rsrp_values"].extend(acc["rsrp_values"])
            merged["sinr_values"].extend(acc["sinr_values"])
            merged["cqi_values"].extend(acc["cqi_values"])
            merged["throughput_peak_mbps"].extend(acc["throughput_peak_mbps"])
            merged["throughput_offpeak_mbps"].extend(acc["throughput_offpeak_mbps"])
            merged["call_attempts"] += acc["call_attempts"]
            merged["dropped_calls"] += acc["dropped_calls"]
            merged["sector_id"] = merged["sector_id"] or acc["sector_id"]
            merged["h3_index"] = merged["h3_index"] or acc["h3_index"]
        return merged

    def extract_output(self, acc):
        import statistics
        import datetime

        def pct(values, p):
            if not values:
                return None
            sorted_v = sorted(values)
            idx = int(len(sorted_v) * p / 100)
            return sorted_v[min(idx, len(sorted_v) - 1)]

        def safe_median(values):
            return statistics.median(values) if values else None

        rsrp = acc["rsrp_values"]
        weak_pct = (
            sum(1 for v in rsrp if v < RSRP_WEAK_THRESHOLD) / len(rsrp)
            if rsrp else None
        )

        peak = acc["throughput_peak_mbps"]
        offpeak = acc["throughput_offpeak_mbps"]
        throughput_degradation = (
            (safe_median(peak) / safe_median(offpeak))
            if peak and offpeak and safe_median(offpeak)
            else None
        )

        dropped_rate = (
            acc["dropped_calls"] / acc["call_attempts"]
            if acc["call_attempts"] > 0 else None
        )

        return {
            "sector_id": acc["sector_id"],
            "h3_index": acc["h3_index"],
            "rsrp_median_dbm": safe_median(rsrp),
            "rsrp_p10_dbm": pct(rsrp, 10),
            "rsrp_p90_dbm": pct(rsrp, 90),
            "sinr_median_db": safe_median(acc["sinr_values"]),
            "sinr_p10_db": pct(acc["sinr_values"], 10),
            "cqi_median": safe_median(acc["cqi_values"]),
            "weak_signal_pct": weak_pct,
            "dropped_call_rate": dropped_rate,
            "throughput_degradation_ratio": throughput_degradation,
            "sample_count": len(rsrp),
            "window_start": datetime.datetime.utcnow().isoformat() + "Z",
        }


class RFPerformancePipeline(BasePCIPipeline):
    @property
    def domain(self) -> str:
        return "rf_performance"

    @property
    def required_fields(self) -> list[str]:
        return ["sector_id", "h3_index"]

    @property
    def curated_table_schema(self) -> dict:
        return {
            "fields": [
                {"name": "sector_id",                  "type": "STRING"},
                {"name": "h3_index",                   "type": "STRING"},
                {"name": "rsrp_median_dbm",            "type": "FLOAT64"},
                {"name": "rsrp_p10_dbm",               "type": "FLOAT64"},
                {"name": "rsrp_p90_dbm",               "type": "FLOAT64"},
                {"name": "sinr_median_db",             "type": "FLOAT64"},
                {"name": "sinr_p10_db",                "type": "FLOAT64"},
                {"name": "cqi_median",                 "type": "FLOAT64"},
                {"name": "weak_signal_pct",            "type": "FLOAT64"},
                {"name": "dropped_call_rate",          "type": "FLOAT64"},
                {"name": "throughput_degradation_ratio","type": "FLOAT64"},
                {"name": "sample_count",               "type": "INTEGER"},
                {"name": "window_start",               "type": "TIMESTAMP"},
                {"name": "ingested_at",                "type": "TIMESTAMP"},
            ]
        }

    def _is_streaming(self) -> bool:
        return True

    def read_source(self, pipeline: beam.Pipeline) -> beam.PCollection:
        subscription = (
            f"projects/{self.config.project_id}"
            f"/subscriptions/rf-telemetry-dataflow-sub"
        )
        return (
            pipeline
            | "ReadPubSub" >> ReadFromPubSub(subscription=subscription)
            | "ParseJSON" >> beam.ParDo(ParseRFRecord()).with_outputs(
                ParseRFRecord.DEAD_LETTER_TAG, main="valid"
            )
        )

    def transform(self, raw: beam.PCollection) -> beam.PCollection:
        return (
            raw
            | "Window" >> beam.WindowInto(
                SlidingWindows(WINDOW_SIZE_SECONDS, WINDOW_PERIOD_SECONDS)
            )
            | "KeyBySector" >> beam.Map(lambda r: (r.get("sector_id"), r))
            | "AggregateWindow" >> beam.CombinePerKey(AggregateRFWindow())
            | "Unkey" >> beam.Map(lambda kv: kv[1])
        )
