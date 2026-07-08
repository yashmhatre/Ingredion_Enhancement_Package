"""
Configuration schema for the bronze JSON ingestion package.

A single IngestionConfig object drives the whole pipeline: where the JSON
comes from, how nested fields should be handled, and where/how the result
is written as a Delta bronze table.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
import json

try:
    import yaml
except ImportError:  # pyyaml is optional - only needed if you load .yaml configs
    yaml = None


VALID_FLATTEN_MODES = ("raw", "flatten", "auto")
VALID_WRITE_MODES = ("append", "overwrite", "merge")
VALID_INGESTION_MODES = ("batch", "streaming")
VALID_SCHEMA_EVOLUTION_MODES = ("addNewColumns", "rescue", "failOnNewColumns", "none")
VALID_TRIGGER_MODES = ("availableNow", "once", "processingTime")


@dataclass
class IngestionConfig:
    # --- Source ---
    source_path: str                       # any Spark-readable URI: abfss://, s3://, gs://, dbfs:/, /Volumes/..., file:/...
    multiline: bool = True                 # set True if each file is a single JSON document (not JSON-lines)
    schema_hint_ddl: Optional[str] = None   # optional DDL string to enforce a read schema instead of inferring it
    reader_options: Dict[str, Any] = field(default_factory=dict)  # extra options passed straight to spark.read.options()

    # --- Ingestion mode (batch one-off read vs incremental Auto Loader) ---
    ingestion_mode: str = "batch"           # "batch" | "streaming"
    checkpoint_location: Optional[str] = None   # required for streaming - Auto Loader progress + foreachBatch checkpoint
    schema_location: Optional[str] = None       # required for streaming - Auto Loader inferred schema store
    schema_evolution_mode: str = "addNewColumns"  # cloudFiles.schemaEvolutionMode for streaming
    rescued_data_column: str = "_rescued_data"    # column that captures fields that don't fit the inferred/enforced schema
    corrupt_record_column: str = "_corrupt_record"  # column that captures unparseable JSON records (batch mode, PERMISSIVE)
    max_files_per_trigger: Optional[int] = None
    trigger_mode: str = "availableNow"      # "availableNow" | "once" | "processingTime"
    trigger_processing_time: Optional[str] = None  # e.g. "30 seconds", required if trigger_mode == "processingTime"

    # --- Data quality ---
    required_columns: List[str] = field(default_factory=list)   # columns that must be non-null in every row
    fail_on_quality_error: bool = True      # if False, bad rows are quarantined instead of failing the run
    quarantine_table: Optional[str] = None  # e.g. "bronze.orders_raw_quarantine" - defaults to f"{table}_quarantine"

    # --- Reliability ---
    retry_attempts: int = 3
    retry_delay_seconds: float = 10.0

    # --- Nested field handling ---
    flatten_mode: str = "raw"              # "raw" | "flatten" | "auto"
    flatten_separator: str = "_"           # separator used when flattening struct field names
    explode_arrays: bool = False           # explode array columns during flatten (only used when flatten_mode != "raw")
    max_flatten_depth: Optional[int] = None  # None = flatten fully; otherwise stop after N levels
    auto_flatten_threshold: int = 5        # for flatten_mode="auto": flatten if nested depth <= this, else keep raw

    # --- Target table ---
    catalog: Optional[str] = None          # Unity Catalog catalog name, omit for hive_metastore
    schema_name: str = "bronze"            # target schema/database
    table: str = ""                        # target table name (required)
    write_mode: str = "append"             # "append" | "overwrite" | "merge"
    merge_keys: Optional[List[str]] = None  # required when write_mode == "merge"
    partition_by: Optional[List[str]] = None
    merge_schema: bool = True              # allow schema evolution on write (mergeSchema)

    # --- Audit / lineage columns added automatically ---
    add_audit_columns: bool = True
    audit_ingest_ts_col: str = "_ingested_at"
    audit_source_file_col: str = "_source_file"
    audit_batch_id_col: str = "_batch_id"
    batch_id: Optional[str] = None         # if None, an ISO timestamp is generated at run time

    def __post_init__(self):
        if not self.source_path:
            raise ValueError("source_path is required")
        if not self.table:
            raise ValueError("table is required")
        if self.flatten_mode not in VALID_FLATTEN_MODES:
            raise ValueError(f"flatten_mode must be one of {VALID_FLATTEN_MODES}, got {self.flatten_mode!r}")
        if self.write_mode not in VALID_WRITE_MODES:
            raise ValueError(f"write_mode must be one of {VALID_WRITE_MODES}, got {self.write_mode!r}")
        if self.write_mode == "merge" and not self.merge_keys:
            raise ValueError("merge_keys must be provided when write_mode='merge'")
        if self.ingestion_mode not in VALID_INGESTION_MODES:
            raise ValueError(f"ingestion_mode must be one of {VALID_INGESTION_MODES}, got {self.ingestion_mode!r}")
        if self.schema_evolution_mode not in VALID_SCHEMA_EVOLUTION_MODES:
            raise ValueError(
                f"schema_evolution_mode must be one of {VALID_SCHEMA_EVOLUTION_MODES}, got {self.schema_evolution_mode!r}"
            )
        if self.trigger_mode not in VALID_TRIGGER_MODES:
            raise ValueError(f"trigger_mode must be one of {VALID_TRIGGER_MODES}, got {self.trigger_mode!r}")
        if self.trigger_mode == "processingTime" and not self.trigger_processing_time:
            raise ValueError("trigger_processing_time is required when trigger_mode='processingTime'")
        if self.ingestion_mode == "streaming":
            if not self.checkpoint_location:
                raise ValueError("checkpoint_location is required when ingestion_mode='streaming'")
            if not self.schema_location:
                raise ValueError("schema_location is required when ingestion_mode='streaming'")

    @property
    def resolved_quarantine_table(self) -> str:
        if self.quarantine_table:
            return self.quarantine_table
        base = f"{self.table}_quarantine"
        parts = [p for p in (self.catalog, self.schema_name, base) if p]
        return ".".join(parts)

    @property
    def full_table_name(self) -> str:
        parts = [p for p in (self.catalog, self.schema_name, self.table) if p]
        return ".".join(parts)

    # ---- constructors ----
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "IngestionConfig":
        known = {f for f in cls.__dataclass_fields__}
        clean = {k: v for k, v in d.items() if k in known}
        return cls(**clean)

    @classmethod
    def from_json(cls, path: str) -> "IngestionConfig":
        with open(path, "r") as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_yaml(cls, path: str) -> "IngestionConfig":
        if yaml is None:
            raise ImportError("pyyaml is required to load YAML configs: pip install pyyaml")
        with open(path, "r") as fh:
            return cls.from_dict(yaml.safe_load(fh))

    @classmethod
    def load(cls, path: str) -> "IngestionConfig":
        """Auto-detect based on extension (.yaml/.yml/.json)."""
        if path.endswith((".yaml", ".yml")):
            return cls.from_yaml(path)
        if path.endswith(".json"):
            return cls.from_json(path)
        raise ValueError("Config file must end in .yaml, .yml, or .json")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
