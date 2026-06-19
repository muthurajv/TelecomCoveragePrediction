"""Abstract base class for all four PCI Dataflow ingestion pipelines."""

import abc
import logging
from dataclasses import dataclass
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    project_id: str
    region: str
    gcs_bucket: str
    dataflow_temp_location: str
    bq_dataset_raw: str
    bq_dataset_curated: str
    environment: str = "prod"
    max_num_workers: int = 50
    machine_type: str = "n1-standard-4"


class SchemaValidationError(Exception):
    pass


class FieldValidator(beam.DoFn):
    """Routes records to main output or dead-letter based on validation rules."""

    DEAD_LETTER_TAG = "dead_letter"

    def __init__(self, required_fields: list[str], domain: str):
        self.required_fields = required_fields
        self.domain = domain

    def process(self, element: dict, *args, **kwargs):
        missing = [f for f in self.required_fields if element.get(f) is None]
        if missing:
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER_TAG,
                {
                    "domain": self.domain,
                    "record": str(element),
                    "error": f"Missing required fields: {missing}",
                    "ingested_at": beam.utils.timestamp.Timestamp.now().to_rfc3339(),
                },
            )
        else:
            yield element


class AddIngestionTimestamp(beam.DoFn):
    def process(self, element: dict, *args, **kwargs):
        import datetime
        element["ingested_at"] = datetime.datetime.utcnow().isoformat() + "Z"
        yield element


class BasePCIPipeline(abc.ABC):
    """
    All four ingestion pipelines (Network Inventory, RF Performance,
    Customer/Commercial, Geospatial) extend this base class.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config

    @property
    @abc.abstractmethod
    def domain(self) -> str:
        """Short domain name used in table names and logging (e.g. 'network_inventory')."""

    @property
    @abc.abstractmethod
    def required_fields(self) -> list[str]:
        """Fields that must be non-null for a record to pass validation."""

    @property
    @abc.abstractmethod
    def curated_table_schema(self) -> dict:
        """BigQuery table schema dict for the curated table."""

    @abc.abstractmethod
    def read_source(self, pipeline: beam.Pipeline) -> beam.PCollection:
        """Return a PCollection of raw dicts from the source system."""

    @abc.abstractmethod
    def transform(self, raw: beam.PCollection) -> beam.PCollection:
        """Apply domain-specific normalization and enrichment."""

    def _dead_letter_table(self) -> str:
        return f"{self.config.project_id}:{self.config.bq_dataset_raw}.dead_letter_{self.domain}"

    def _curated_table(self) -> str:
        return f"{self.config.project_id}:{self.config.bq_dataset_curated}.{self.domain}"

    def _pipeline_options(self) -> PipelineOptions:
        opts = PipelineOptions(
            project=self.config.project_id,
            region=self.config.region,
            temp_location=self.config.dataflow_temp_location,
            runner="DataflowRunner",
            max_num_workers=self.config.max_num_workers,
            machine_type=self.config.machine_type,
            save_main_session=True,
            streaming=self._is_streaming(),
            dataflow_service_options=["enable_prime"],
        )
        return opts

    def _is_streaming(self) -> bool:
        return False

    def run(self) -> None:
        logger.info("Starting %s ingestion pipeline", self.domain)
        options = self._pipeline_options()

        with beam.Pipeline(options=options) as p:
            raw = self.read_source(p)

            validated, dead_letters = (
                raw
                | "AddTimestamp" >> beam.ParDo(AddIngestionTimestamp())
                | "Validate" >> beam.ParDo(
                    FieldValidator(self.required_fields, self.domain)
                ).with_outputs(FieldValidator.DEAD_LETTER_TAG, main="valid")
            )

            transformed = self.transform(validated)

            transformed | "WriteCurated" >> WriteToBigQuery(
                table=self._curated_table(),
                schema=self.curated_table_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method="STORAGE_WRITE_API",
            )

            dead_letters | "WriteDeadLetter" >> WriteToBigQuery(
                table=self._dead_letter_table(),
                schema={
                    "fields": [
                        {"name": "domain", "type": "STRING"},
                        {"name": "record", "type": "STRING"},
                        {"name": "error", "type": "STRING"},
                        {"name": "ingested_at", "type": "TIMESTAMP"},
                    ]
                },
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )

        logger.info("Finished %s ingestion pipeline", self.domain)
