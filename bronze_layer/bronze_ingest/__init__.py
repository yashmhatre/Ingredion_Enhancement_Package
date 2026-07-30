from .config import IngestionConfig
from .pipeline import BronzeIngestion, ingest_json_to_bronze
from .directory_ingestion import ingest_directory_to_bronze, sanitize_table_name, build_table_name
from .quality import DataQualityError
from .streaming_reader import JsonLinesTruncationError
from .replay import reprocess_quarantine, reprocess_quarantined_files
from .catalog_metadata import apply_catalog_metadata
from .logging_utils import get_logger

__all__ = [
    "IngestionConfig",
    "BronzeIngestion",
    "ingest_json_to_bronze",
    "ingest_directory_to_bronze",
    "sanitize_table_name",
    "build_table_name",
    "DataQualityError",
    "JsonLinesTruncationError",
    "reprocess_quarantine",
    "reprocess_quarantined_files",
    "apply_catalog_metadata",
    "get_logger",
]
# Single source of truth for the package version: setup.py parses this
# string rather than declaring its own. They previously disagreed (wheel
# built as 0.4.0 while the package reported 0.3.0), which defeats the point
# of shipping a versioned artifact - you couldn't tell what was deployed
# from inside a running job.
__version__ = "0.4.0"
