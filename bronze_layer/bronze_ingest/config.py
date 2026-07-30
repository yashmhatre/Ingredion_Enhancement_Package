"""
Configuration schema for the bronze JSON ingestion package.

A single IngestionConfig object drives the whole pipeline: where the JSON
comes from, how nested fields should be handled, and where/how the result
is written as a Delta bronze table.
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from .logging_utils import logger
from .sql_utils import validate_identifier, validate_identifiers

try:
    import yaml
except ImportError:  # pyyaml is optional - only needed if you load .yaml configs
    yaml = None


VALID_WRITE_MODES = ("append", "overwrite", "merge")
VALID_INGESTION_MODES = ("batch", "streaming")
VALID_SCHEMA_EVOLUTION_MODES = ("addNewColumns", "rescue", "failOnNewColumns", "none")
VALID_TRIGGER_MODES = ("availableNow", "once", "processingTime")

#: Spark JSON-reader options this package will pass through without
#: complaint (#154). `reader_options` goes straight to `spark.read.option()`,
#: and configs are loaded from a Unity Catalog Volume - anyone with WRITE
#: VOLUME can influence their content, which is a wider set of people than
#: those with CREATE TABLE on the target schema.
#:
#: The specific thing an allowlist buys here: `path` is a reader option, so an
#: unfiltered passthrough lets a config redirect the read at a location the
#: config was never meant to touch, while every log line and audit row still
#: reports `source_path`. Parsing and formatting options cannot do that, so
#: they are allowed.
ALLOWED_READER_OPTIONS = frozenset(
    {
        # Parsing behaviour
        "multiLine",
        "mode",
        "columnNameOfCorruptRecord",
        "primitivesAsString",
        "prefersDecimal",
        "allowComments",
        "allowUnquotedFieldNames",
        "allowSingleQuotes",
        "allowNumericLeadingZeros",
        "allowBackslashEscapingAnyCharacter",
        "allowUnquotedControlChars",
        "dropFieldIfAllNull",
        "ignoreNullFields",
        "samplingRatio",
        "rescuedDataColumn",
        "inferTimestamp",
        "enableDateTimeParsingFallback",
        # Formats, encoding, locale
        "dateFormat",
        "timestampFormat",
        "timestampNTZFormat",
        "timeZone",
        "locale",
        "encoding",
        "charset",
        "lineSep",
        # File selection - these narrow what is read, they cannot redirect it
        "recursiveFileLookup",
        "pathGlobFilter",
        "modifiedBefore",
        "modifiedAfter",
    }
)

#: Prefixes allowed wholesale. Auto Loader's surface is large, versioned and
#: entirely namespaced, so enumerating it would go stale faster than it would
#: protect anything - and every key under it configures incremental discovery
#: for the path already given, rather than choosing a different path.
ALLOWED_READER_OPTION_PREFIXES = ("cloudFiles.",)


@dataclass
class IngestionConfig:
    # --- Source ---
    # any Spark-readable URI: abfss://, s3://, gs://, dbfs:/, /Volumes/..., file:/...
    source_path: str
    multiline: bool = True  # set True if each file is a single JSON document (not JSON-lines)
    # optional DDL string to enforce a read schema instead of inferring it
    schema_hint_ddl: Optional[str] = None
    # extra options passed straight to spark.read.options();
    # keys must be on ALLOWED_READER_OPTIONS
    reader_options: Dict[str, Any] = field(default_factory=dict)
    # opt out of the reader_options allowlist (#154); logs what it lets through
    allow_unsafe_reader_options: bool = False

    # --- Ingestion mode (batch one-off read vs incremental Auto Loader) ---
    ingestion_mode: str = "batch"  # "batch" | "streaming"
    # required for streaming - Auto Loader progress + foreachBatch checkpoint
    checkpoint_location: Optional[str] = None
    # required for streaming - Auto Loader inferred schema store
    schema_location: Optional[str] = None
    schema_evolution_mode: str = "addNewColumns"  # cloudFiles.schemaEvolutionMode for streaming
    # column that captures fields that don't fit the inferred/enforced schema
    rescued_data_column: str = "_rescued_data"
    # column that captures unparseable JSON records (batch mode, PERMISSIVE)
    corrupt_record_column: str = "_corrupt_record"
    max_files_per_trigger: Optional[int] = None
    trigger_mode: str = "availableNow"  # "availableNow" | "once" | "processingTime"
    # e.g. "30 seconds", required if trigger_mode == "processingTime"
    trigger_processing_time: Optional[str] = None

    # --- Data quality ---
    # columns that must be non-null in every row
    required_columns: List[str] = field(default_factory=list)
    # columns whose combination must be unique within a batch; duplicates
    # (all but the first, by dedupe_order_by) are treated as bad rows
    unique_columns: Optional[List[str]] = None
    # if False, bad rows are quarantined instead of failing the run
    fail_on_quality_error: bool = True
    # e.g. "bronze.orders_raw_quarantine" - defaults to f"{table}_quarantine"
    quarantine_table: Optional[str] = None

    # --- Reliability ---
    retry_attempts: int = 3
    retry_delay_seconds: float = 10.0
    # Ceiling on time spent SLEEPING between retries, not on the operation
    # itself (#152). attempts=5 with delay_seconds=30 is otherwise up to 8
    # minutes of driver sleep with no way to bound it. None means unbounded,
    # which is the previous behaviour.
    retry_max_total_seconds: Optional[float] = 120.0
    # txnAppId/txnVersion for batch append/overwrite when batch_id is explicit (see #63)
    idempotent_batch_writes: bool = True

    # --- Target table ---
    catalog: Optional[str] = None  # Unity Catalog catalog name, omit for hive_metastore
    schema_name: str = "bronze"  # target schema/database
    table: str = ""  # target table name (required)
    write_mode: str = "append"  # "append" | "overwrite" | "merge"
    merge_keys: Optional[List[str]] = None  # required when write_mode == "merge"
    # hive-style partitioning - discouraged for new tables, see cluster_by
    partition_by: Optional[List[str]] = None
    merge_schema: bool = True  # allow schema evolution on write (mergeSchema)
    # Keep one row per merge key before MERGE (else raise on duplicates).
    #
    # None (the default) behaves as True on the merge path. It is not simply
    # `True` so that config load can tell "the user asked for this" from "the
    # user never mentioned it" and only warn about the former - see
    # _warn_on_ignored_settings. Read it through
    # `resolved_dedupe_before_merge`, never directly: None is falsy, so a bare
    # truth test would silently disable deduplication by default.
    dedupe_before_merge: Optional[bool] = None
    # column to break ties by, highest wins. Defaults to audit_ingest_ts_col
    # for merge dedupe, or to an arbitrary-but-deterministic order for the
    # unique_columns quality check (which runs before audit columns exist).
    dedupe_order_by: Optional[str] = None

    # --- Table layout: liquid clustering (recommended) vs. partition_by (legacy) ---
    # explicit liquid-clustering columns; mutually exclusive with
    # partition_by / cluster_by_auto
    cluster_by: Optional[List[str]] = None
    # CLUSTER BY AUTO - Databricks Runtime only, not supported by OSS/local Delta
    cluster_by_auto: bool = False
    # e.g. {"delta.enableChangeDataFeed": "true"}
    table_properties: Dict[str, str] = field(default_factory=dict)

    # --- Catalog documentation (see catalog_metadata.py, #64) ---
    # COMMENT ON TABLE - catalog documentation for the bronze table
    table_comment: Optional[str] = None
    # {column_name: comment}; top-level columns only
    column_comments: Dict[str, str] = field(default_factory=dict)

    # --- Audit / lineage columns added automatically ---
    add_audit_columns: bool = True
    audit_ingest_ts_col: str = "_ingested_at"
    audit_source_file_col: str = "_source_file"
    audit_batch_id_col: str = "_batch_id"
    batch_id: Optional[str] = None  # if None, an ISO timestamp is generated at run time
    # --- Run-level audit trail (separate from the per-row audit columns
    # above) — one record per pipeline execution, independent of any
    # single bronze table. See audit.py.
    enable_run_audit: bool = True
    audit_catalog: Optional[str] = None  # defaults to `catalog` if not set
    # None means "use schema_name" - see the note on resolved_audit_table for
    # why this is not defaulted to a literal schema (#54).
    audit_schema_name: Optional[str] = None
    audit_table: str = "_ingestion_audit"  # dedicated table name
    run_id: Optional[str] = None  # if None, generated at run time (like batch_id)
    enable_schema_registry: bool = True
    registry_catalog: Optional[str] = None  # defaults to `catalog` if not set
    registry_schema_name: Optional[str] = None  # None means "use schema_name", as above
    registry_table: str = "_schema_registry"

    def __post_init__(self):
        if not self.source_path:
            raise ValueError("source_path is required")
        if not self.table:
            raise ValueError("table is required")
        if self.write_mode not in VALID_WRITE_MODES:
            raise ValueError(
                f"write_mode must be one of {VALID_WRITE_MODES}, got {self.write_mode!r}"
            )
        if self.write_mode == "merge" and not self.merge_keys:
            raise ValueError("merge_keys must be provided when write_mode='merge'")
        if self.write_mode == "merge" and self.merge_keys:
            unguarded = [k for k in self.merge_keys if k not in self.required_columns]
            if unguarded:
                raise ValueError(
                    f"merge_keys {unguarded} must also be listed in required_columns when "
                    "write_mode='merge' - NULL = NULL is NULL in a SQL MERGE condition, so a "
                    "NULL merge key never matches the target and gets inserted as a duplicate "
                    "row on every run. Add these columns to required_columns so the quality "
                    "gate guarantees non-null values before the write."
                )
        if self.unique_columns is not None and len(self.unique_columns) == 0:
            raise ValueError(
                "unique_columns, if provided, must be a non-empty list of column names."
            )
        if self.column_comments:
            blank = [k for k in self.column_comments if not str(k).strip()]
            if blank:
                raise ValueError("column_comments keys must be non-empty column names.")
        if self.cluster_by is not None and len(self.cluster_by) == 0:
            raise ValueError("cluster_by, if provided, must be a non-empty list of column names.")
        if self.cluster_by and self.cluster_by_auto:
            raise ValueError(
                "cluster_by and cluster_by_auto are mutually exclusive - specify either "
                "explicit clustering columns or cluster_by_auto=True, not both."
            )
        if self.partition_by and (self.cluster_by or self.cluster_by_auto):
            raise ValueError(
                "partition_by cannot be combined with cluster_by/cluster_by_auto - liquid "
                "clustering replaces hive-style partitioning as the table's layout strategy, "
                "they're mutually exclusive."
            )
        if self.ingestion_mode not in VALID_INGESTION_MODES:
            raise ValueError(
                f"ingestion_mode must be one of {VALID_INGESTION_MODES}, "
                f"got {self.ingestion_mode!r}"
            )
        if self.schema_evolution_mode not in VALID_SCHEMA_EVOLUTION_MODES:
            raise ValueError(
                f"schema_evolution_mode must be one of {VALID_SCHEMA_EVOLUTION_MODES}, "
                f"got {self.schema_evolution_mode!r}"
            )
        if self.trigger_mode not in VALID_TRIGGER_MODES:
            raise ValueError(
                f"trigger_mode must be one of {VALID_TRIGGER_MODES}, got {self.trigger_mode!r}"
            )
        if self.trigger_mode == "processingTime" and not self.trigger_processing_time:
            raise ValueError(
                "trigger_processing_time is required when trigger_mode='processingTime'"
            )
        if self.ingestion_mode == "streaming":
            if not self.checkpoint_location:
                raise ValueError("checkpoint_location is required when ingestion_mode='streaming'")
            if not self.schema_location:
                raise ValueError("schema_location is required when ingestion_mode='streaming'")

        self._validate_numeric_ranges()
        self._validate_identifiers()
        self._validate_reader_options()
        self._warn_on_ignored_settings()

    # ---- validation, split out of __post_init__ so each concern is
    # ---- readable on its own and testable by name ----

    def _validate_numeric_ranges(self):
        """
        Numeric fields whose out-of-range values fail LATER, confusingly (#54).

        retry_attempts < 1 is the sharp one: `with_retry` loops
        `range(1, attempts + 1)`, so 0 or -1 makes the loop body never
        execute and it falls through to `raise last_exc` - which is still
        None. The user sees "TypeError: exceptions must derive from
        BaseException", which says nothing about the real failure it was
        supposed to be retrying, or about the config value that caused it.
        """
        if not isinstance(self.retry_attempts, int) or isinstance(self.retry_attempts, bool):
            raise ValueError(f"retry_attempts must be an int, got {self.retry_attempts!r}")
        if self.retry_attempts < 1:
            raise ValueError(
                f"retry_attempts must be >= 1, got {self.retry_attempts}. 1 means "
                "'try once, do not retry'. 0 or negative makes with_retry's loop body "
                "never run, and it then raises a None exception instead of the real "
                "failure - set 1 to disable retries."
            )
        if self.retry_delay_seconds < 0:
            raise ValueError(
                f"retry_delay_seconds must be >= 0, got {self.retry_delay_seconds}. "
                "A negative delay reaches time.sleep() and raises mid-run, on a "
                "cluster, after work has already been paid for."
            )
        if self.retry_max_total_seconds is not None and self.retry_max_total_seconds < 0:
            raise ValueError(
                f"retry_max_total_seconds must be >= 0 or None, got "
                f"{self.retry_max_total_seconds}. 0 means 'never sleep between "
                f"attempts'; None means unbounded."
            )
        if self.max_files_per_trigger is not None and self.max_files_per_trigger < 1:
            raise ValueError(
                f"max_files_per_trigger must be >= 1 when set, got "
                f"{self.max_files_per_trigger}. Leave it None for no limit."
            )

    def _validate_identifiers(self):
        """
        Every config value that reaches a SQL identifier position (#154).

        Validated here rather than at the call sites: it fails before a
        cluster starts, there is one place to audit, and the message names the
        config field rather than surfacing a Spark parse error from inside a
        generated statement.

        `table` and `schema_name` are the required ones; the rest are checked
        only when set. Note `quarantine_table` is validated per dot-separated
        part, because it is documented as accepting a fully-qualified name.
        """
        validate_identifier(self.table, "table")
        validate_identifier(self.schema_name, "schema_name")

        for name in ("catalog", "audit_catalog", "registry_catalog"):
            value = getattr(self, name)
            if value is not None:
                validate_identifier(value, name)

        for name in (
            "audit_schema_name",
            "audit_table",
            "registry_schema_name",
            "registry_table",
            "audit_ingest_ts_col",
            "audit_source_file_col",
            "audit_batch_id_col",
            "rescued_data_column",
            "corrupt_record_column",
        ):
            value = getattr(self, name)
            if value is not None:
                validate_identifier(value, name)

        if self.quarantine_table:
            for i, part in enumerate(self.quarantine_table.split(".")):
                validate_identifier(part, f"quarantine_table part {i + 1}")

        validate_identifiers(self.required_columns, "required_columns")
        validate_identifiers(self.unique_columns, "unique_columns")
        validate_identifiers(self.merge_keys, "merge_keys")
        validate_identifiers(self.partition_by, "partition_by")
        validate_identifiers(self.cluster_by, "cluster_by")
        if self.dedupe_order_by is not None:
            validate_identifier(self.dedupe_order_by, "dedupe_order_by")

        # Keys land in identifier position; the VALUES are free text and are
        # escaped at the call site instead (see catalog_metadata,
        # bronze_writer._ensure_liquid_clustering_and_properties).
        #
        # column_comments keys are validated PER DOT-SEPARATED PART, so
        # "customer.name" passes here. That is deliberate, not laxness:
        # catalog_metadata documents nested paths as unsupported and skips
        # them with a warning, because catalog documentation must never fail
        # an ingestion run. Rejecting them at config load would override that
        # decision and turn a documentation typo into a failed run. The dotted
        # name never reaches SQL anyway - it is checked against the table's
        # real columns first. Per-part validation still blocks the shapes that
        # would matter, like "customer-name" or a quote.
        for key in self.column_comments or {}:
            for i, part in enumerate(str(key).split(".")):
                validate_identifier(part, f"column_comments key {key!r} part {i + 1}")
        for key in self.table_properties or {}:
            # Table property keys are dotted by convention
            # (delta.enableChangeDataFeed), so validate per part.
            for i, part in enumerate(str(key).split(".")):
                validate_identifier(part, f"table_properties key {key!r} part {i + 1}")

    def _validate_reader_options(self):
        """
        reader_options is passed straight to spark.read.option() (#154).

        Any Spark reader option can therefore be set from a config file that
        lives on a Volume - a materially wider set of writers than those with
        CREATE TABLE on the target schema. Unknown keys are rejected rather
        than silently applied; `allow_unsafe_reader_options` is the documented
        way out, and it logs what it let through.
        """
        options = self.reader_options or {}
        if not options:
            return

        unknown = sorted(
            k
            for k in options
            if k not in ALLOWED_READER_OPTIONS and not k.startswith(ALLOWED_READER_OPTION_PREFIXES)
        )
        if not unknown:
            return

        if self.allow_unsafe_reader_options:
            logger.warning(
                "allow_unsafe_reader_options=True - passing non-allowlisted reader "
                "option(s) %s straight to the Spark reader. These are not validated.",
                unknown,
            )
            return

        raise ValueError(
            f"reader_options contains key(s) not on the allowlist: {unknown}. "
            f"Allowed: {sorted(ALLOWED_READER_OPTIONS)}. reader_options is applied "
            "verbatim to the Spark reader and configs are loaded from a Volume, so "
            "unrecognised keys are refused rather than applied silently. Set "
            "allow_unsafe_reader_options=True to override deliberately."
        )

    def _warn_on_ignored_settings(self):
        """
        Combinations that are accepted today and silently do nothing, or are
        never what the user meant (#54).

        Split by severity on one test: would proceeding destroy data or
        produce a result the user cannot detect is wrong? Those raise.
        A setting that is merely ignored warns - raising there would break
        working configs that carry a harmless leftover.
        """
        if self.ingestion_mode == "streaming" and self.write_mode == "overwrite":
            raise ValueError(
                "ingestion_mode='streaming' with write_mode='overwrite' replaces the "
                "ENTIRE table on every micro-batch, so only the last micro-batch "
                "survives and every record before it is discarded. There is no case "
                "where this is intended - use 'append', or 'merge' with merge_keys."
            )

        if (
            self.write_mode == "merge"
            and self.resolved_dedupe_before_merge
            and not self.add_audit_columns
            and not self.dedupe_order_by
        ):
            raise ValueError(
                "write_mode='merge' with dedupe_before_merge=True and "
                "add_audit_columns=False needs an explicit dedupe_order_by. The "
                "default order column is audit_ingest_ts_col, which only exists "
                "because add_audit_columns creates it - without both, the write "
                "fails at MERGE time after the read has already been paid for."
            )

        # `is True`, not truthiness: this must fire only when the user
        # EXPLICITLY set it. The effective default is also True, so warning on
        # the resolved value would emit this on every append pipeline in
        # existence - which is how a codebase teaches people to ignore its
        # warnings.
        if self.dedupe_before_merge is True and self.write_mode != "merge":
            logger.warning(
                "dedupe_before_merge=True is ignored when write_mode=%r - it only "
                "applies to MERGE. Rows will NOT be deduplicated. Set "
                "unique_columns to quarantine duplicates on the %s path.",
                self.write_mode,
                self.write_mode,
            )

        if self.enable_schema_registry and not self.enable_run_audit:
            logger.warning(
                "enable_schema_registry=True with enable_run_audit=False - the "
                "registry will still record fingerprints, but schema-drift "
                "visibility (#51) works by writing the fingerprint onto the audit "
                "row, so drift will not be visible in the audit trail."
            )

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

    @property
    def resolved_dedupe_before_merge(self) -> bool:
        """
        Whether to dedupe before MERGE. Unset means yes.

        Always read this rather than the raw field: the raw default is None so
        that validation can distinguish an explicit choice from silence, and
        None is falsy - `if config.dedupe_before_merge:` would turn the
        default off.
        """
        return True if self.dedupe_before_merge is None else self.dedupe_before_merge

    @property
    def resolved_audit_schema(self) -> str:
        """
        Audit schema, defaulting to the TARGET schema rather than a literal.

        This used to default to the string "bronze", and under the
        one-catalog/three-schema model (#136) that was a live trap (#54):
        every environment that did not override it wrote its audit trail to
        the same `<catalog>.bronze._ingestion_audit`. Dev, staging and
        production run histories mixed together, and each service principal
        gained read access to the others'.

        It failed silently rather than loudly, because `_write_audit_row`
        issues `CREATE SCHEMA IF NOT EXISTS` first - so it created the shared
        schema and carried on. The bundle pins both values per target, but the
        bundle is not the only caller: the notebook widgets default to blank,
        and blank fell through to "bronze".

        Co-locating audit with the data it describes makes the default
        correct, and needs no second file to remember an override.
        """
        return self.audit_schema_name or self.schema_name

    @property
    def resolved_registry_schema(self) -> str:
        """Registry schema - same reasoning as resolved_audit_schema."""
        return self.registry_schema_name or self.schema_name

    @property
    def resolved_audit_table(self) -> str:
        parts = [
            p
            for p in (
                self.audit_catalog or self.catalog,
                self.resolved_audit_schema,
                self.audit_table,
            )
            if p
        ]
        return ".".join(parts)

    @property
    def resolved_registry_table(self) -> str:
        parts = [
            p
            for p in (
                self.registry_catalog or self.catalog,
                self.resolved_registry_schema,
                self.registry_table,
            )
            if p
        ]
        return ".".join(parts)

    # ---- constructors ----
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "IngestionConfig":
        known = {f for f in cls.__dataclass_fields__}
        clean = {k: v for k, v in d.items() if k in known}
        return cls(**clean)

    @classmethod
    def from_json(cls, path: str) -> "IngestionConfig":
        with open(path) as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_yaml(cls, path: str) -> "IngestionConfig":
        if yaml is None:
            raise ImportError("pyyaml is required to load YAML configs: pip install pyyaml")
        with open(path) as fh:
            return cls.from_dict(yaml.safe_load(fh))

    @classmethod
    def resolve(
        cls,
        config: Optional[Dict[str, Any]] = None,
        config_path: Optional[str] = None,
        **overrides: Any,
    ) -> "IngestionConfig":
        """
        Builds a config from any combination of a dict, a file, and keyword
        overrides. Overrides win.

        This lived in `ingest_json_to_bronze` as three branches (#150). It
        belongs here: this class already owns from_dict / load / to_dict, and
        putting the merge beside them gives the unknown-key check one place
        to live instead of one per entry point.

        Two behaviours differ from the version this replaces, both
        deliberate:

        1. Passing BOTH `config` and `config_path` now raises. The old code
           silently used config_path and discarded `config` entirely - the
           caller got a config they did not ask for, with no signal.
        2. Unknown keys in **overrides** raise, rather than being dropped.
           `ingest_json_to_bronze(spark, tabel="orders")` previously
           discarded the typo and then failed with "table is required",
           which points at the wrong thing. Note this applies to overrides
           only: `from_dict` stays lenient, because config FILES are
           versioned artifacts that may legitimately carry keys a given
           package version does not know, and #166 already made the
           directory-ingestion entry point strict on the same reasoning.
        """
        if config is not None and config_path is not None:
            raise ValueError(
                "Pass either config= or config_path=, not both. The previous "
                "behaviour silently ignored config= when both were given."
            )

        if overrides:
            unknown = sorted(set(overrides) - set(cls.__dataclass_fields__))
            if unknown:
                raise ValueError(
                    f"Unknown IngestionConfig field(s): {unknown}. Check for a typo - "
                    f"these were previously dropped silently, which surfaced later as a "
                    f"confusing error about a different field. Valid fields: "
                    f"{sorted(cls.__dataclass_fields__)}"
                )

        if config_path:
            merged = cls.load(config_path).to_dict()
        elif config is not None:
            merged = dict(config)
        else:
            merged = {}

        merged.update(overrides)
        return cls.from_dict(merged)

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
