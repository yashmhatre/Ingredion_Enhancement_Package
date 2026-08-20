"""
Configuration schema for the multi-format bronze ingestion package.

A single IngestionConfig object drives the whole pipeline: where source data
comes from, how it is parsed, and where/how the result is written as a Delta
bronze table.
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from . import formats
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

#: Spark reader options this package will pass through without complaint
#: (#154). `reader_options` goes straight to `spark.read.option()`, and
#: configs are loaded from a Unity Catalog Volume - anyone with WRITE VOLUME
#: can influence their content, which is a wider set of people than those
#: with CREATE TABLE on the target schema.
#:
#: The specific thing an allowlist buys here: `path` is a reader option, so an
#: unfiltered passthrough lets a config redirect the read at a location the
#: config was never meant to touch, while every log line and audit row still
#: reports `source_path`. Parsing and formatting options cannot do that, so
#: they are allowed.
#:
#: The allowlist is now looked up PER-FORMAT - `formats.allowed_reader_options`
#: is the registry that decides what each `source_format` accepts, since not
#: every reader option means the same thing (or exists at all) for every
#: format (see `formats.py`'s module docstring). This name survives as a true
#: alias to the "json" entry of that registry, not a second copy of the set,
#: so existing importers of `config.ALLOWED_READER_OPTIONS` keep working
#: unchanged - `tests/test_formats.py` asserts the two stay equal.
ALLOWED_READER_OPTIONS = formats.allowed_reader_options("json")

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
    # Drives both what discovery lists (formats.extensions_for) and which
    # reader_options are valid (formats.allowed_reader_options) - see
    # formats.py's module docstring for why one field controls both. Must be
    # one of formats.supported_formats(); checked in __post_init__.
    source_format: str = "json"
    multiline: bool = True  # set True if each file is a single JSON document (not JSON-lines)
    # CSV-only; ignored for every other source_format. Spark's CSV reader
    # defaults this to False, which yields positional column names
    # ("_c0".."_cN") instead of the real header row - see the __post_init__
    # check below for why that default would be unsafe here.
    csv_header: bool = True
    # CSV-only; ignored for every other source_format. Spark's CSV reader
    # defaults this to False, which yields all-string columns - the same
    # format-dependent-schema trap documented for batch JSON inference in
    # bronze_layer/config/contracts/orders.yaml.
    csv_infer_schema: bool = True
    # XML-only. Spark's XML data source cannot infer the repeated record
    # element safely; there is deliberately no second rowTag source in
    # reader_options.
    xml_row_tag: Optional[str] = None
    # optional DDL string to enforce a read schema instead of inferring it.
    # XML v1 rejects this because Spark binds raw prefixed names before this
    # package canonicalizes them; accepting canonical hints can yield nulls.
    schema_hint_ddl: Optional[str] = None
    # extra options passed straight to spark.read.options();
    # keys must be on the allowlist for this source_format
    # (formats.allowed_reader_options(source_format))
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
    # Replay stops offering a row after this many failed attempts (#159 item
    # 4). None means "no limit", which is the pre-existing behaviour and stays
    # the default: turning this on changes which rows a recovery tool
    # considers, and that should be a choice rather than something that starts
    # happening on upgrade.
    #
    # Exhausted rows are SKIPPED, never deleted. A row that has failed five
    # times may still pass after a genuine source fix, and deletion is
    # irreversible - the counter exists to stop rescanning hopeless rows, not
    # to throw them away. `reprocess_quarantine(max_replay_attempts=...)` can
    # override this per call to sweep them back in.
    max_replay_attempts: Optional[int] = None

    # --- Reliability ---
    retry_attempts: int = 3
    retry_delay_seconds: float = 10.0
    # Ceiling on time spent SLEEPING between retries, not on the operation
    # itself (#152). attempts=5 with delay_seconds=30 is otherwise up to 8
    # minutes of driver sleep with no way to bound it. None means unbounded,
    # which is the previous behaviour.
    retry_max_total_seconds: Optional[float] = 120.0
    # txnAppId/txnVersion for batch append/overwrite (#63). Gates the feature;
    # it does nothing on its own until idempotent_txn_version supplies a
    # version, which is deliberate - see that field.
    idempotent_batch_writes: bool = True
    # The Delta txnVersion to write under. Opt-in, and there is no default
    # that can be derived safely (#366).
    #
    # This used to be derived from batch_id, and every deployed job sets
    # batch_id to the Databricks job run ID. Delta SILENTLY DISCARDS a write
    # whose txnVersion is at or below one already committed for the same
    # txnAppId - that is the whole mechanism - and job run IDs are globally
    # unique but NOT increasing. So any run drawing an ID below one already
    # used for that table wrote nothing, reported success, and had its
    # source file archived. Stable across retries and increasing across
    # batches are different properties; Delta needs both and a run ID has
    # only the first.
    #
    # Supply this only with a counter you control and know to increase per
    # table, and hold it constant across retries of the same logical batch.
    # Left unset, writes are not idempotent-protected: a retried batch can
    # duplicate rows, which is recoverable, where a discarded write is not.
    idempotent_txn_version: Optional[int] = None

    # --- Target table ---
    catalog: Optional[str] = None  # Unity Catalog catalog name, omit for hive_metastore
    schema_name: str = "bronze"  # target schema/database
    table: str = ""  # target table name (required)
    write_mode: str = "append"  # "append" | "overwrite" | "merge"
    merge_keys: Optional[List[str]] = None  # required when write_mode == "merge"
    # --- Content-addressed merge key (#84) - an alternative to merge_keys,
    # not a variant of it. write_mode == "merge" requires exactly one of the
    # two; they are mutually exclusive.
    #
    # merge_keys answers "is this the same business entity?" - on an
    # upstream UPDATE the key still matches, so the row updates in place
    # (true upsert). content_hash_columns answers "are these the same
    # bytes?" - on an upstream UPDATE the hash no longer matches, so the row
    # INSERTS, leaving both versions in the table. That is correct for an
    # append-only bronze layer capturing every version of a record, and it
    # is NOT upsert semantics: content_hash_columns gives idempotent
    # re-ingestion of identical payloads, nothing more. Pick it when a
    # source has no reliable non-null natural key - the #47 nullable-key
    # guard below has nothing to check for this strategy, so it's skipped.
    #
    # Must be an explicit, non-empty column list - never "hash every
    # column". Hashing a column bronze adds itself (audit_ingest_ts_col,
    # audit_batch_id_col, audit_source_file_col, rescued_data_column,
    # corrupt_record_column) would make the hash depend on ingest-time
    # metadata rather than row content, so identical payloads ingested on
    # different runs would never match; those names are rejected if listed
    # here (see _validate_content_hash_columns). SHA-256 collisions are not
    # a practical concern and are not guarded against separately.
    content_hash_columns: Optional[List[str]] = None
    # Generated column that carries the computed hash and is used as the
    # sole merge key when content_hash_columns is set. Computed on the
    # DataFrame immediately before the write and excluded from the MERGE's
    # matched-row update in bronze_writer._write_core rather than included
    # in a blanket updateAll - a genuine match implies source and target
    # hash are already equal, so excluding it documents that the column is
    # never meant to move rather than relying on that being incidentally
    # true.
    content_hash_key_col: str = "_content_hash_key"
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
    # Free-form Delta properties. Merged OVER the CDF defaults below, so an
    # explicit entry here always wins - see resolved_table_properties.
    table_properties: Dict[str, str] = field(default_factory=dict)

    # --- Change Data Feed (#58) ---
    # ON by default, and that default is the point of the issue: CDF only
    # captures changes from the moment it is enabled, so every run before it
    # is switched on is history no Silver consumer can ever read
    # incrementally. `docs/bronze_silver_contract.md` §1 decides Silver reads
    # Bronze via CDF, which makes this an obligation on Bronze rather than a
    # nice-to-have. Cost is negligible on an append-heavy layer.
    #
    # Applies to bronze AND quarantine tables. Not to the audit or schema
    # registry tables: they are this package's own metadata, nothing consumes
    # them incrementally, and #159 is separately concerned with their growth.
    #
    # None, not True, and for the reason #54 established elsewhere in this
    # class: config load must be able to tell an explicit choice from silence.
    #   None  -> decide from write_mode: on for append/merge, OFF for
    #            overwrite, because `docs/bronze_silver_contract.md` §2 puts
    #            overwrite-mode tables out of contract for incremental reads
    #            (CDF there emits the whole table as deletes then inserts, so
    #            Silver would do strictly more work than a full rescan while
    #            carrying all of CDC's complexity).
    #   True  -> explicit opt-in. REFUSED with write_mode="overwrite", which
    #            is the enforcement §2 asked for when #58 landed - without it
    #            the restriction is violated by someone configuring a table
    #            reasonably and having no way to know.
    #   False -> explicit opt-out, always honoured.
    # A plain default of True would have made the refusal unreachable: every
    # overwrite config in existence would have started failing at load.
    enable_change_data_feed: Optional[bool] = None
    # The CDF window, and the reason this field exists rather than leaving
    # Delta's defaults in place: enabling a feed whose data is then deleted by
    # an unconfigured maintenance operation is a guarantee that looks real and
    # is not (#58, #159). Delta bounds readable change data by BOTH the commit
    # log (delta.logRetentionDuration, default 30 days) and the underlying
    # files (delta.deletedFileRetentionDuration, default 7 days), so the real
    # window is the SHORTER of the two - 7 days by default, which is not what
    # a reader of "log retention 30 days" would assume. Both are set from this
    # one number so they cannot silently disagree.
    #
    # 30 days is a floor chosen to be written down, not derived from a
    # measured consumer lag: Silver does not exist yet (#205), so there is no
    # lag to measure. Revisit when there is a real consumer. A VACUUM with an
    # explicit RETAIN shorter than this overrides the property and silently
    # shortens the window - that interaction belongs to #159.
    change_data_feed_retention_days: int = 30

    # --- Catalog documentation (see catalog_metadata.py, #64) ---
    # COMMENT ON TABLE - catalog documentation for the bronze table
    table_comment: Optional[str] = None
    # {column_name: comment}; top-level columns only
    column_comments: Dict[str, str] = field(default_factory=dict)

    # --- Unity Catalog tags (#64) ---
    # FREE-FORM tags only. Governed keys - the `class.*` PII taxonomy and
    # anything else with a tag policy attached - must NEVER be applied from
    # config, and `apply_catalog_tags` refuses them.
    #
    # The reason is access control, not tidiness: a governed tag can carry an
    # ABAC policy, so writing one can silently change who can read the data
    # (verified live 2026-08-12 - 63 `class.*` policies, ABAC endpoint
    # answering). Config loads from a Volume, which #154 established is a
    # wider trust boundary than the repo, so a YAML file that could opt a
    # table into a policy-bearing key is a privilege-escalation path.
    # Governed keys go through `apply_reviewed_tags` and a review step only.
    # See docs/decisions/2026-08_uc_tag_mechanism.md.
    table_tags: Dict[str, str] = field(default_factory=dict)
    # {column_name: {tag_key: tag_value}}; top-level columns only, same as
    # column_comments.
    column_tags: Dict[str, Dict[str, str]] = field(default_factory=dict)

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
        if self.source_format not in formats.supported_formats():
            raise ValueError(
                f"source_format must be one of {formats.supported_formats()}, "
                f"got {self.source_format!r}"
            )
        # Refused at config load, before a cluster starts, for the same reason
        # every other config defect is: this repo's 96%-compute-cost finding
        # makes a late failure the expensive one. The format is registered and
        # its reader is tested - what is missing is the Tier 2 sign-off on the
        # decision records that govern it, so the block belongs at the point an
        # operator selects it rather than anywhere downstream.
        blocker = formats.signoff_blocker(self.source_format)
        if blocker:
            raise ValueError(blocker)
        if self.source_format == "xml" and "rowTag" in (self.reader_options or {}):
            raise ValueError(
                "reader_options must not contain 'rowTag'; xml_row_tag is the sole "
                "source for the XML record element, including when "
                "allow_unsafe_reader_options=True."
            )
        if self.source_format == "xml":
            if "mode" in (self.reader_options or {}):
                raise ValueError(
                    "reader_options must not contain 'mode' for XML; XML ingestion "
                    "always uses FAILFAST, including when allow_unsafe_reader_options=True."
                )
            if "ignoreNamespace" in (self.reader_options or {}):
                raise ValueError(
                    "reader_options must not contain 'ignoreNamespace' for XML; namespace "
                    "prefixes are preserved and canonicalized deterministically."
                )
            if self.xml_row_tag is None:
                raise ValueError("xml_row_tag is required when source_format='xml'")
            if not isinstance(self.xml_row_tag, str) or not self.xml_row_tag.strip():
                raise ValueError("xml_row_tag must be non-empty when source_format='xml'")
            if self.schema_hint_ddl:
                raise ValueError(
                    "schema_hint_ddl is not supported for source_format='xml': Spark "
                    "binds schemas before namespace-prefix canonicalization, which can "
                    "silently return nulls. Use inference until the XML name-mapping "
                    "contract is signed off."
                )
        # Type-checked before the guard below reads them. Both are plain
        # `bool` with no meaningful third state, so - unlike the
        # `Optional[bool]` fields that use `is True` to tell "user said so"
        # from "unset" - an identity comparison here would buy nothing and
        # cost correctness: `csv_header: "false"` from YAML is the truthy
        # string 'false', which Spark's CSV reader then reads as false. The
        # deployed job writes booleans as quoted strings as a matter of habit
        # (`resources/bronze_ingest_jobs.yml` ships `multiline: "true"`,
        # `fail_on_quality_error: "false"`), and `per_file_config_json`
        # reaches IngestionConfig with only its KEYS validated - so a string
        # here is the realistic input, not a hypothetical one. Fail loudly
        # rather than let the guard below silently not fire.
        for _name, _value in (
            ("csv_header", self.csv_header),
            ("csv_infer_schema", self.csv_infer_schema),
        ):
            if not isinstance(_value, bool):
                raise ValueError(
                    f"{_name} must be a bool, got {_value!r}. A quoted YAML value like "
                    f"`{_name}: \"false\"` is the string 'false', which is truthy in Python "
                    "but false to Spark - the two would disagree silently. Write it "
                    f"unquoted: `{_name}: false`."
                )

        if self.source_format == "csv" and not self.csv_header and not self.schema_hint_ddl:
            raise ValueError(
                "source_format='csv' with csv_header=False and no schema_hint_ddl. Headerless "
                "CSV produces positional column names ('_c0', '_c1', ...) instead of the real "
                "ones - required_columns, merge_keys, unique_columns, cluster_by, "
                "column_comments and the data contract all become unusable without hand-mapping "
                "positions. Worse, those names are positional: an upstream column insertion "
                "silently shifts data under the same stable '_cN' name and the write still "
                "succeeds, because '_c0' is a perfectly legal Delta column name. Set "
                "schema_hint_ddl to name the columns explicitly, or set csv_header=True if "
                "the file does have a header row."
            )
        if self.write_mode not in VALID_WRITE_MODES:
            raise ValueError(
                f"write_mode must be one of {VALID_WRITE_MODES}, got {self.write_mode!r}"
            )
        if self.merge_keys and self.content_hash_columns:
            raise ValueError(
                "merge_keys and content_hash_columns are mutually exclusive (#84) - "
                "merge_keys answers 'is this the same business entity?', "
                "content_hash_columns answers 'are these the same bytes?'. Setting both "
                "leaves it ambiguous which guarantee the write is making. Pick one merge "
                "key strategy."
            )
        if self.write_mode == "merge" and not self.merge_keys and self.content_hash_columns is None:
            raise ValueError(
                "write_mode='merge' requires either merge_keys (a natural business key) "
                "or content_hash_columns (a content-addressed dedup key, #84) - see "
                "content_hash_columns' field comment in config.py for how the two differ."
            )
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
        if self.enable_change_data_feed is True and self.write_mode == "overwrite":
            # The enforcement docs/bronze_silver_contract.md §2 asked for when
            # #58 landed. Under overwrite, CDF emits the entire table as
            # deletes then inserts on every run, so a Silver consumer does
            # strictly more work than a full rescan while carrying all of
            # CDC's complexity. Only an EXPLICIT True lands here - the default
            # (None) resolves to off for overwrite rather than failing, so
            # existing overwrite configs keep loading.
            raise ValueError(
                "enable_change_data_feed=True is not supported with write_mode='overwrite'. "
                "Overwrite emits the entire table as deletes then inserts on every run, so "
                "the feed carries no incremental information - docs/bronze_silver_contract.md "
                "§2 puts overwrite-mode tables out of contract for incremental Silver "
                "reads. Leave enable_change_data_feed unset (it defaults to off for "
                "overwrite), or change write_mode."
            )
        if self.resolved_enable_change_data_feed and self.change_data_feed_retention_days < 1:
            # Zero or negative would render as "interval 0 days", which either
            # errors or sets a window that discards change data immediately -
            # a CDF guarantee that is enabled and empty is the exact
            # looks-real-and-is-not failure #58 set out to avoid.
            raise ValueError(
                "change_data_feed_retention_days must be >= 1 when "
                f"the change data feed is on, got {self.change_data_feed_retention_days}. "
                "Set enable_change_data_feed=False to turn the feed off instead of "
                "configuring a zero-length retention window."
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
        if self.ingestion_mode == "streaming" and self.source_format != "json":
            raise ValueError(
                "streaming ingestion currently supports JSON only; batch multi-format "
                "support does not enable CSV, Parquet, or XML Auto Loader reads."
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
        self._validate_content_hash_columns()
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
            "content_hash_key_col",
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
        validate_identifiers(self.content_hash_columns, "content_hash_columns")
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

    def _validate_content_hash_columns(self):
        """
        content_hash_columns is the source list for a content-addressed
        merge key (#84) - deliberately separate from merge_keys and
        validated separately, because the two answer different questions
        (see the field comment above). Identifier syntax is already checked
        by `_validate_identifiers`; this covers the parts specific to the
        hash strategy.
        """
        if self.content_hash_columns is None:
            return
        if len(self.content_hash_columns) == 0:
            raise ValueError(
                "content_hash_columns, if provided, must be a non-empty list of column names."
            )

        # Columns bronze adds itself, AFTER the quality gate runs (or that
        # exist purely to carry ingest-time metadata). Hashing any of these
        # would make the hash depend on when/how a row was ingested rather
        # than on its content, so two runs ingesting byte-identical source
        # data would never produce a matching hash - defeating the point of
        # a content-addressed key. Rejected explicitly rather than silently
        # dropped: the #84 design note is specific that "the next added
        # column silently breaks it" is the failure mode to avoid.
        reserved = {
            self.audit_ingest_ts_col,
            self.audit_batch_id_col,
            self.audit_source_file_col,
            self.rescued_data_column,
            self.corrupt_record_column,
            self.content_hash_key_col,
        }
        blocked = [c for c in self.content_hash_columns if c in reserved]
        if blocked:
            raise ValueError(
                f"content_hash_columns contains column(s) {blocked} that bronze adds "
                "itself (an audit/rescued-data/corrupt-record column, or the hash key "
                "column's own name). These are either absent at hash time or differ on "
                "every run regardless of row content, so hashing them defeats "
                "content-addressed dedup - remove them from content_hash_columns."
            )

    def _validate_reader_options(self):
        """
        reader_options is passed straight to spark.read.option() (#154).

        Any Spark reader option can therefore be set from a config file that
        lives on a Volume - a materially wider set of writers than those with
        CREATE TABLE on the target schema. Unknown keys are rejected rather
        than silently applied; `allow_unsafe_reader_options` is the documented
        way out, and it logs what it let through.

        The allowlist is looked up PER `source_format` (formats.py) - what is
        valid for one format is not necessarily valid for another. This runs
        after the source_format check in __post_init__, so self.source_format
        is always a registered format by the time it gets here.
        """
        options = self.reader_options or {}
        if not options:
            return

        allowed = formats.allowed_reader_options(self.source_format)
        unknown = sorted(
            k
            for k in options
            if k not in allowed and not k.startswith(ALLOWED_READER_OPTION_PREFIXES)
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
            f"reader_options contains key(s) not on the allowlist for "
            f"source_format={self.source_format!r}: {unknown}. "
            f"Allowed: {sorted(allowed)}. reader_options is applied "
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

    @property
    def resolved_enable_change_data_feed(self) -> bool:
        """
        Whether CDF is actually applied, once silence is resolved (#58).

        `enable_change_data_feed=None` means "nobody said", and the answer
        then comes from `write_mode`: on for append and merge, off for
        overwrite. See the field comment and
        `docs/bronze_silver_contract.md` §2 — an overwrite-mode feed is not
        wrong so much as useless, emitting the entire table as deletes then
        inserts every run.

        `resolved_`, not the raw field, for the #54 reason: the raw one is
        `None` by default so an explicit choice is distinguishable from
        silence, and `None` is falsy, so reading it directly would quietly
        disable the feed everywhere.
        """
        if self.enable_change_data_feed is None:
            return self.write_mode != "overwrite"
        return self.enable_change_data_feed

    @property
    def resolved_table_properties(self) -> Dict[str, str]:
        """
        The Delta properties actually applied to bronze and quarantine tables:
        the CDF defaults (#58) with `table_properties` merged OVER them.

        **Precedence is explicit-dict-wins, and it is decided here rather than
        left to whichever code path runs last.** Someone who writes
        `table_properties: {"delta.enableChangeDataFeed": "false"}` has said
        something more specific than the boolean default, and silently
        overriding it would be the worse surprise of the two. The same holds
        for the retention keys - a caller pinning
        `delta.deletedFileRetentionDuration` by hand keeps their value.

        Returning a fresh dict each call, never the stored one: the writer
        diffs this against DESCRIBE DETAIL and callers have historically
        treated config fields as inert, so handing out a mutable reference to
        `table_properties` would let a caller edit config by accident.
        """
        props: Dict[str, str] = {}
        if self.resolved_enable_change_data_feed:
            props["delta.enableChangeDataFeed"] = "true"
            # Both keys from one number - see change_data_feed_retention_days
            # for why the shorter of the two is what actually bounds the feed.
            interval = f"interval {self.change_data_feed_retention_days} days"
            props["delta.logRetentionDuration"] = interval
            props["delta.deletedFileRetentionDuration"] = interval
        props.update(self.table_properties or {})
        return props

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
