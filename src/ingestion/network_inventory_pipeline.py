"""Network Inventory batch ingestion: OSS export on GCS → Dataflow → BigQuery."""

import csv
import io
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.gcsio import GcsIO

from src.ingestion.base_pipeline import BasePCIPipeline, PipelineConfig

logger = logging.getLogger(__name__)

VALID_SPECTRUM_BANDS = {"700", "850", "1700", "1900", "2100", "2500", "3500", "mmWave"}
LAT_RANGE = (-90.0, 90.0)
LON_RANGE = (-180.0, 180.0)


class ParseInventoryCSV(beam.DoFn):
    def process(self, line: str, *args, **kwargs):
        try:
            reader = csv.DictReader(io.StringIO(line))
            for row in reader:
                yield dict(row)
        except Exception:
            yield line


class ValidateInventoryRecord(beam.DoFn):
    DEAD_LETTER_TAG = "dead_letter"

    def process(self, element: dict, *args, **kwargs):
        errors = []

        try:
            lat = float(element.get("latitude", ""))
            lon = float(element.get("longitude", ""))
            if not (LAT_RANGE[0] <= lat <= LAT_RANGE[1]):
                errors.append(f"latitude {lat} out of range")
            if not (LON_RANGE[0] <= lon <= LON_RANGE[1]):
                errors.append(f"longitude {lon} out of range")
            element["latitude"] = lat
            element["longitude"] = lon
        except (ValueError, TypeError):
            errors.append("invalid lat/lon")

        band = element.get("spectrum_band", "").strip()
        if band not in VALID_SPECTRUM_BANDS:
            element["spectrum_band"] = None

        if errors:
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER_TAG,
                {
                    "domain": "network_inventory",
                    "record": str(element),
                    "error": "; ".join(errors),
                    "ingested_at": "",
                },
            )
        else:
            yield element


class NetworkInventoryPipeline(BasePCIPipeline):
    def __init__(self, config: PipelineConfig, source_gcs_path: str):
        super().__init__(config)
        self.source_gcs_path = source_gcs_path

    @property
    def domain(self) -> str:
        return "network_inventory"

    @property
    def required_fields(self) -> list[str]:
        return ["tower_id", "sector_id", "latitude", "longitude"]

    @property
    def curated_table_schema(self) -> dict:
        return {
            "fields": [
                {"name": "tower_id",          "type": "STRING"},
                {"name": "sector_id",         "type": "STRING"},
                {"name": "latitude",          "type": "FLOAT64"},
                {"name": "longitude",         "type": "FLOAT64"},
                {"name": "antenna_height_m",  "type": "FLOAT64"},
                {"name": "spectrum_band",     "type": "STRING"},
                {"name": "transmit_power_dbm","type": "FLOAT64"},
                {"name": "azimuth_deg",       "type": "FLOAT64"},
                {"name": "tilt_deg",          "type": "FLOAT64"},
                {"name": "market_id",         "type": "STRING"},
                {"name": "site_type",         "type": "STRING"},
                {"name": "status",            "type": "STRING"},
                {"name": "commissioned_date", "type": "DATE"},
                {"name": "ingested_at",       "type": "TIMESTAMP"},
            ]
        }

    def read_source(self, pipeline: beam.Pipeline) -> beam.PCollection:
        return (
            pipeline
            | "ReadGCS" >> ReadFromText(self.source_gcs_path, skip_header_lines=0)
            | "ParseCSV" >> beam.ParDo(ParseInventoryCSV())
        )

    def transform(self, raw: beam.PCollection) -> beam.PCollection:
        return (
            raw
            | "ValidateInventory" >> beam.ParDo(ValidateInventoryRecord()).with_outputs(
                ValidateInventoryRecord.DEAD_LETTER_TAG, main="valid"
            )
        ).valid
