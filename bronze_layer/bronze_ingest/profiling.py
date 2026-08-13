"""
Per-column data profiling for Bronze tables (#299, phase 1 of #298).

Nothing in this package computed column statistics before this module. Every
inference the modeling engine needs - primary-key candidates, grain, measures,
relationships - is a function of null rates and cardinality, so this is the
foundation the rest of #298 sits on.

Three tables, three different grains
------------------------------------

  table_metadata    one row per table          (structure, refreshed in place)
  column_metadata   one row per table+column   (structure, refreshed in place)
  column_profile    one row per table+column+profile run   (append-only history)

The first two describe shape and are replaced when shape changes. The third is
a time series: how a column's null rate moves is itself evidence, and throwing
it away would make drift invisible.

Cost is a hard requirement, not an optimisation
-----------------------------------------------

#149 removed a `final_df.count()` from the ingestion path with the note that it
*"re-read the source and re-ran the quality gate to produce a number Delta
already had."* Profiling can reintroduce exactly that cost, multiplied by every
column of every table, so three rules are structural here:

  1. **One pass.** Every statistic for every column is computed in a single
     `agg`, including the row count (`count(lit(1))`), so a 40-column table
     costs one job and not forty.
  2. **Approximate distinct.** `approx_count_distinct`, never `countDistinct`.
     Cardinality drives inference; exactness does not.
  3. **Skip when nothing changed.** The skip test reads the schema fingerprint
     and the Delta table version - both metadata-only - and scans no data at
     all when neither moved.

Why this module raises and `schema_registry` does not
-----------------------------------------------------

`schema_registry` and `catalog_metadata` never raise, because they run as a
side effect of an ingestion run and must not fail it. Profiling is the
opposite: it is an explicitly-invoked job whose entire output is the metadata.
A profiler that swallows an error and writes nothing is indistinguishable from
one that found nothing - the failure mode this repo keeps finding. So it fails
loudly.

Nested data
-----------

Struct fields are profiled, addressed by dotted path (`customer.name`); they
are ordinary projections and cost nothing extra in the same pass.

**Array and map interiors are deliberately not profiled here.** Reaching inside
them needs an `explode`, which is a different query and a different cost
profile, and the row count it produces answers a different question. They are
recorded in `column_metadata` with their type so the structure is known, and
descending into them belongs to `NestedSchemaAnalyzer` (phase 3 of #298).
"""

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .logging_utils import logger
from .metadata_store import MetadataStore, validate_rows
from .sql_utils import quote_ident

#: Column "kinds" - what profiling can do with a column, not what Spark calls
#: it. Only SCALAR columns get statistics; the rest are recorded for structure.
SCALAR = "SCALAR"
STRUCT = "STRUCT"
ARRAY = "ARRAY"
MAP = "MAP"

_NUMERIC_TYPES = (
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
)
#: Types `min`/`max` are meaningful on. Binary is excluded deliberately - Spark
#: will order it, but a min/max over bytes is not evidence anyone can read.
_ORDERABLE_TYPES = _NUMERIC_TYPES + (StringType, DateType, TimestampType, BooleanType)


TABLE_METADATA_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), nullable=False),
        StructField("catalog_name", StringType(), nullable=True),
        StructField("schema_name", StringType(), nullable=True),
        StructField("table_only_name", StringType(), nullable=True),
        StructField("table_comment", StringType(), nullable=True),
        StructField("partition_columns", StringType(), nullable=True),
        StructField("cluster_columns", StringType(), nullable=True),
        StructField("row_count", LongType(), nullable=True),
        StructField("column_count", IntegerType(), nullable=True),
        StructField("columns_profiled", IntegerType(), nullable=True),
        # True when max_columns capped the run. Recorded rather than logged so
        # a consumer can tell "no statistics for this column" from "this column
        # was never looked at".
        StructField("profile_truncated", BooleanType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("schema_fingerprint", StringType(), nullable=False),
        StructField("table_version", LongType(), nullable=True),
        StructField("profile_run_id", StringType(), nullable=False),
        StructField("profiled_at", TimestampType(), nullable=False),
    ]
)

COLUMN_METADATA_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), nullable=False),
        StructField("column_name", StringType(), nullable=False),
        StructField("ordinal_position", IntegerType(), nullable=False),
        StructField("data_type", StringType(), nullable=False),
        StructField("column_kind", StringType(), nullable=False),
        StructField("nullable", BooleanType(), nullable=True),
        StructField("column_comment", StringType(), nullable=True),
        # Dotted path for a struct leaf ("customer.name"); equal to
        # column_name for a top-level column.
        StructField("nested_path", StringType(), nullable=False),
        StructField("parent_column", StringType(), nullable=True),
        StructField("nesting_depth", IntegerType(), nullable=False),
        StructField("schema_fingerprint", StringType(), nullable=False),
        StructField("profile_run_id", StringType(), nullable=False),
        StructField("recorded_at", TimestampType(), nullable=False),
    ]
)

COLUMN_PROFILE_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), nullable=False),
        StructField("column_name", StringType(), nullable=False),
        StructField("data_type", StringType(), nullable=False),
        StructField("row_count", LongType(), nullable=True),
        StructField("null_count", LongType(), nullable=True),
        # NULL, not 0, when row_count is 0 - see _ratio.
        StructField("null_pct", DoubleType(), nullable=True),
        StructField("distinct_approx", LongType(), nullable=True),
        StructField("distinct_pct", DoubleType(), nullable=True),
        StructField("duplicate_count", LongType(), nullable=True),
        StructField("min_value", StringType(), nullable=True),
        StructField("max_value", StringType(), nullable=True),
        StructField("avg_value", DoubleType(), nullable=True),
        StructField("min_length", IntegerType(), nullable=True),
        StructField("max_length", IntegerType(), nullable=True),
        StructField("sampled", BooleanType(), nullable=False),
        StructField("sample_size", LongType(), nullable=True),
        # Off by default. Stored for HUMAN review only - see
        # ai_safe_profile_payload() for the boundary this must never cross.
        StructField("sample_values_json", StringType(), nullable=True),
        StructField("schema_fingerprint", StringType(), nullable=False),
        StructField("table_version", LongType(), nullable=True),
        StructField("profile_run_id", StringType(), nullable=False),
        StructField("profiled_at", TimestampType(), nullable=False),
    ]
)


@dataclass
class ProfileConfig:
    """Where profiling output goes, and the bounds it runs within."""

    metadata_schema: str
    metadata_catalog: Optional[str] = None
    table_metadata_table: str = "_table_metadata"
    column_metadata_table: str = "_column_metadata"
    column_profile_table: str = "_column_profile"

    #: Fraction passed to DataFrame.sample. None profiles everything. Recorded
    #: on every row, because a statistic from a sample and one from the whole
    #: table are not the same claim.
    sample_fraction: Optional[float] = None

    #: Sample values are OFF by default. They are the one profiling output that
    #: contains business data rather than facts about it.
    collect_sample_values: bool = False
    sample_values_limit: int = 5

    #: Wide-table guard. 200 columns x ~6 aggregates is already 1,200
    #: expressions in one projection; past that, Spark's codegen suffers and
    #: the value of profiling column 500 is low. Capped runs are recorded, not
    #: silently truncated.
    max_columns: int = 200

    #: How far into nested structs to descend. Arrays and maps stop the walk
    #: regardless.
    max_struct_depth: int = 5

    #: Profile even when the fingerprint and table version are unchanged.
    force: bool = False

    source_system: Optional[str] = None

    def _qualified(self, table: str) -> str:
        parts = [p for p in (self.metadata_catalog, self.metadata_schema, table) if p]
        return ".".join(parts)

    @property
    def resolved_table_metadata(self) -> str:
        return self._qualified(self.table_metadata_table)

    @property
    def resolved_column_metadata(self) -> str:
        return self._qualified(self.column_metadata_table)

    @property
    def resolved_column_profile(self) -> str:
        return self._qualified(self.column_profile_table)

    @property
    def schema_ref(self) -> str:
        return ".".join(p for p in (self.metadata_catalog, self.metadata_schema) if p)

    def store(self, spark) -> MetadataStore:
        """The single writer for every framework metadata table (#324)."""
        return MetadataStore(spark, self.metadata_schema, self.metadata_catalog)


@dataclass(frozen=True)
class ColumnNode:
    """One column in a table's schema, flattened out of any enclosing structs."""

    path: str
    parts: Tuple[str, ...]
    data_type: str
    kind: str
    nullable: bool
    #: Document order across the whole flattened schema, parents before their
    #: own fields - not the position within the immediate parent.
    ordinal: int
    parent: Optional[str]
    depth: int
    comment: Optional[str] = None
    #: Classified from the real DataType at walk time, never by matching on the
    #: type STRING. `simpleString()` prefixes overlap - "int" is also the start
    #: of "interval" - so string matching silently mistypes columns, and a
    #: column wrongly judged numeric gets an avg() that fails the whole pass.
    is_numeric: bool = False
    is_string: bool = False
    is_orderable: bool = False

    @property
    def accessor(self) -> str:
        """Backtick-quoted dotted path, safe to hand to `F.col`.

        Quoted per part rather than whole: `customer.name` must reach the
        struct field, while a top-level column literally named `a.b` must not
        be mistaken for one.
        """
        return ".".join(quote_ident(p) for p in self.parts)


def _kind_of(dtype) -> str:
    if isinstance(dtype, StructType):
        return STRUCT
    if isinstance(dtype, ArrayType):
        return ARRAY
    if isinstance(dtype, MapType):
        return MAP
    return SCALAR


def describe_columns(schema: StructType, max_depth: int = 5) -> List[ColumnNode]:
    """
    Flattens `schema` into one node per column, descending into structs and
    stopping at arrays and maps.

    Arrays and maps are *recorded* (so the structure is known and phase 3 can
    pick them up) but not descended into: reaching their contents needs an
    explode, which is a different query with a different row count.
    """
    nodes: List[ColumnNode] = []
    counter = [0]

    def walk(fields, parent: Optional[str], parts: Tuple[str, ...], depth: int) -> None:
        for f in fields:
            path = f"{parent}.{f.name}" if parent else f.name
            kind = _kind_of(f.dataType)
            counter[0] += 1
            is_numeric = isinstance(f.dataType, _NUMERIC_TYPES)
            is_string = isinstance(f.dataType, StringType)
            nodes.append(
                ColumnNode(
                    path=path,
                    parts=parts + (f.name,),
                    data_type=f.dataType.simpleString(),
                    kind=kind,
                    nullable=bool(f.nullable),
                    ordinal=counter[0],
                    parent=parent,
                    depth=depth,
                    comment=(f.metadata or {}).get("comment"),
                    is_numeric=is_numeric,
                    is_string=is_string,
                    is_orderable=isinstance(f.dataType, _ORDERABLE_TYPES),
                )
            )
            if kind == STRUCT and depth < max_depth:
                walk(f.dataType.fields, path, parts + (f.name,), depth + 1)

    walk(schema.fields, None, (), 0)
    return nodes


def schema_fingerprint(schema: StructType) -> str:
    """
    Stable hash of a schema's top-level (name, type) pairs, sorted.

    Deliberately the same algorithm as `schema_registry._fingerprint`, so the
    two agree on whether a schema changed. `test_profiling_fingerprint_matches_
    the_registry` pins that agreement against the real producer rather than
    against a copy of the rule - if the registry's definition moves, that test
    fails instead of the two silently diverging.
    """
    pairs = sorted((f.name, f.dataType.simpleString()) for f in schema.fields)
    payload = json.dumps(pairs, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _delta_version(spark, table_name: str) -> Optional[int]:
    """
    Current Delta version of `table_name`, or None when unavailable.

    Metadata only - no data is read. None means "cannot tell", which callers
    must treat as "cannot skip": profiling something unnecessarily is wasteful,
    but skipping something that changed is wrong.
    """
    try:
        rows = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()
        return int(rows[0]["version"]) if rows else None
    except Exception as exc:  # noqa: BLE001 - non-Delta or no history; see docstring
        logger.debug("No Delta history for %s (%s); cannot skip on version.", table_name, exc)
        return None


def _table_description(spark, table_name: str) -> Dict[str, Any]:
    """Comment, partition and clustering columns from DESCRIBE DETAIL."""
    try:
        row = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        as_dict = row.asDict()
        return {
            "table_comment": as_dict.get("description"),
            "partition_columns": ",".join(as_dict.get("partitionColumns") or []) or None,
            "cluster_columns": ",".join(as_dict.get("clusteringColumns") or []) or None,
        }
    except Exception as exc:  # noqa: BLE001 - advisory detail; absence is usable
        logger.debug("DESCRIBE DETAIL unavailable for %s (%s).", table_name, exc)
        return {"table_comment": None, "partition_columns": None, "cluster_columns": None}


def _last_profile_state(spark, table_name: str, config: ProfileConfig):
    """(schema_fingerprint, table_version) of the most recent profile, or None."""
    try:
        row = config.store(spark).latest_for(
            config.table_metadata_table, table_name, order_by="profiled_at"
        )
        if row is None:
            return None
        return row["schema_fingerprint"], row["table_version"]
    except Exception as exc:  # noqa: BLE001 - unreadable state means "profile it"
        logger.debug("Could not read previous profile state for %s (%s).", table_name, exc)
        return None


def _ratio(numerator, denominator):
    """
    numerator/denominator as a float, or None when the denominator is 0.

    None rather than 0.0 on an empty table, deliberately. "0% null" and "there
    were no rows to be null" are different facts, and a downstream key detector
    reading 0.0 would score an empty column as a perfect primary key.
    """
    if not denominator:
        return None
    return float(numerator or 0) / float(denominator)


def _aggregates_for(node: ColumnNode, index: int) -> List:
    """Every statistic for one column, as aliased aggregate expressions."""
    c = F.col(node.accessor)
    exprs = [
        F.count(c).alias(f"c{index}_nonnull"),
        F.approx_count_distinct(c).alias(f"c{index}_distinct"),
    ]

    if node.is_orderable:
        exprs.append(F.min(c).cast("string").alias(f"c{index}_min"))
        exprs.append(F.max(c).cast("string").alias(f"c{index}_max"))
    if node.is_numeric:
        exprs.append(F.avg(c).cast("double").alias(f"c{index}_avg"))
    if node.is_string:
        exprs.append(F.min(F.length(c)).alias(f"c{index}_minlen"))
        exprs.append(F.max(F.length(c)).alias(f"c{index}_maxlen"))

    return exprs


def profile_table(spark, table_name: str, config: ProfileConfig) -> Dict[str, Any]:
    """
    Profiles one table and writes `table_metadata`, `column_metadata` and
    `column_profile`.

    Returns a summary dict; raises on failure (see module docstring for why
    this differs from the never-raise metadata modules).
    """
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    df = spark.read.table(table_name)
    fingerprint = schema_fingerprint(df.schema)
    version = _delta_version(spark, table_name)

    previous = _last_profile_state(spark, table_name, config)
    if (
        not config.force
        and previous is not None
        and version is not None
        and previous == (fingerprint, version)
    ):
        logger.info(
            "Skipping %s: schema fingerprint and Delta version unchanged since the last "
            "profile. No data was read.",
            table_name,
        )
        return {
            "table_name": table_name,
            "status": "skipped",
            "reason": "unchanged",
            "schema_fingerprint": fingerprint,
            "table_version": version,
            "columns_profiled": 0,
        }

    nodes = describe_columns(df.schema, max_depth=config.max_struct_depth)
    scalars = [n for n in nodes if n.kind == SCALAR]
    truncated = len(scalars) > config.max_columns
    if truncated:
        logger.warning(
            "%s has %d profileable columns, above max_columns=%d. Profiling the first %d; "
            "the rest are recorded in column_metadata but carry no statistics.",
            table_name,
            len(scalars),
            config.max_columns,
            config.max_columns,
        )
        scalars = scalars[: config.max_columns]

    source = df.sample(config.sample_fraction) if config.sample_fraction else df

    # THE single pass. Row count rides along in the same aggregate rather than
    # costing its own scan (#149).
    # Unconditional, even with zero profileable columns: the earlier version
    # fell back to source.count() in that case, which is a SECOND scan for a
    # number this aggregate already produces. That is the exact shape #149
    # removed from the ingestion path.
    exprs = [F.count(F.lit(1)).alias("row_count")]
    for i, node in enumerate(scalars):
        exprs.extend(_aggregates_for(node, i))
    stats = source.agg(*exprs).collect()[0].asDict()

    row_count = int(stats.get("row_count") or 0)
    samples = _collect_samples(source, scalars, config) if config.collect_sample_values else {}

    profile_rows = []
    for i, node in enumerate(scalars):
        non_null = stats.get(f"c{i}_nonnull")
        distinct = stats.get(f"c{i}_distinct")
        null_count = row_count - int(non_null or 0)
        # approx_count_distinct can overshoot the true count; a negative
        # duplicate count would be nonsense, so floor it.
        duplicates = max(int(non_null or 0) - int(distinct or 0), 0)
        profile_rows.append(
            {
                "table_name": table_name,
                "column_name": node.path,
                "data_type": node.data_type,
                "row_count": row_count,
                "null_count": null_count,
                "null_pct": _ratio(null_count, row_count),
                "distinct_approx": int(distinct) if distinct is not None else None,
                "distinct_pct": _ratio(distinct, row_count),
                "duplicate_count": duplicates,
                "min_value": stats.get(f"c{i}_min"),
                "max_value": stats.get(f"c{i}_max"),
                "avg_value": stats.get(f"c{i}_avg"),
                "min_length": stats.get(f"c{i}_minlen"),
                "max_length": stats.get(f"c{i}_maxlen"),
                "sampled": config.sample_fraction is not None,
                "sample_size": row_count if config.sample_fraction is not None else None,
                "sample_values_json": samples.get(node.path),
                "schema_fingerprint": fingerprint,
                "table_version": version,
                "profile_run_id": run_id,
                "profiled_at": now,
            }
        )

    detail = _table_description(spark, table_name)
    catalog, schema_only, table_only = _split_name(table_name)
    table_row = {
        "table_name": table_name,
        "catalog_name": catalog,
        "schema_name": schema_only,
        "table_only_name": table_only,
        "table_comment": detail["table_comment"],
        "partition_columns": detail["partition_columns"],
        "cluster_columns": detail["cluster_columns"],
        "row_count": row_count,
        "column_count": len(nodes),
        "columns_profiled": len(scalars),
        "profile_truncated": truncated,
        "source_system": config.source_system,
        "schema_fingerprint": fingerprint,
        "table_version": version,
        "profile_run_id": run_id,
        "profiled_at": now,
    }

    column_rows = [
        {
            "table_name": table_name,
            "column_name": node.path,
            "ordinal_position": node.ordinal,
            "data_type": node.data_type,
            "column_kind": node.kind,
            "nullable": node.nullable,
            "column_comment": node.comment,
            "nested_path": node.path,
            "parent_column": node.parent,
            "nesting_depth": node.depth,
            "schema_fingerprint": fingerprint,
            "profile_run_id": run_id,
            "recorded_at": now,
        }
        for node in nodes
    ]

    store = config.store(spark)
    # Structure is REPLACED (it describes the current shape, and two rows both
    # claiming to describe one table say nothing); profiles are APPENDED,
    # because how a null rate moves is itself evidence (#324).
    store.replace_for(config.table_metadata_table, TABLE_METADATA_SCHEMA, [table_row])
    store.replace_for(
        config.column_metadata_table,
        COLUMN_METADATA_SCHEMA,
        validate_rows(column_rows, {"column_kind": "column_kind"}),
    )
    store.append(config.column_profile_table, COLUMN_PROFILE_SCHEMA, profile_rows)

    logger.info(
        "Profiled %s: %d row(s), %d of %d column(s) profiled%s.",
        table_name,
        row_count,
        len(scalars),
        len(nodes),
        " (truncated)" if truncated else "",
    )
    return {
        "table_name": table_name,
        "status": "profiled",
        "row_count": row_count,
        "columns_profiled": len(scalars),
        "column_count": len(nodes),
        "profile_truncated": truncated,
        "schema_fingerprint": fingerprint,
        "table_version": version,
        "profile_run_id": run_id,
    }


def profile_tables(spark, table_names, config: ProfileConfig) -> List[Dict[str, Any]]:
    """profile_table over several tables. One failure does not stop the rest."""
    results = []
    for name in table_names:
        try:
            results.append(profile_table(spark, name, config))
        except Exception as exc:  # noqa: BLE001 - per-table isolation, recorded not swallowed
            logger.error("Failed to profile %s: %s", name, exc)
            results.append({"table_name": name, "status": "failed", "error": str(exc)})
    return results


def _collect_samples(df, nodes: List[ColumnNode], config: ProfileConfig) -> Dict[str, str]:
    """
    Up to `sample_values_limit` distinct values per column, as JSON.

    This is the one profiling output containing business data rather than facts
    about it, which is why it is opt-in and why ai_safe_profile_payload exists.
    """
    if not nodes:
        return {}
    # Aliased by index, not by path. A struct leaf's path contains dots, and a
    # column literally named "customer.name" would then be indistinguishable
    # from the leaf customer->name when reading the Row back.
    aliases = {f"s{i}": node for i, node in enumerate(nodes)}
    limited = df.select(*[F.col(n.accessor).alias(a) for a, n in aliases.items()]).limit(
        config.sample_values_limit
    )
    rows = limited.collect()
    out: Dict[str, str] = {}
    for alias, node in aliases.items():
        values = [r[alias] for r in rows if r[alias] is not None]
        out[node.path] = json.dumps([str(v) for v in values])
    return out


#: Profile fields that describe VALUES rather than facts about them. Stripped
#: from anything an AI sees.
_VALUE_BEARING_FIELDS = ("sample_values_json", "min_value", "max_value")


def ai_safe_profile_payload(profile_rows, pii_columns=()) -> List[Dict[str, Any]]:
    """
    Projects profile rows down to what an AI may see (#299, BR-002).

    BR-002 forbids *"any row, any sample of rows, or any single business-column
    value"* in a metadata payload, and additionally bans mode/top-N because an
    aggregate can leak an identifying value on a low-cardinality column. So:

      - `sample_values_json` is removed for every column, always;
      - `min_value` / `max_value` are removed for columns named in
        `pii_columns` (from `_ai_metadata.pii_flags_json`), since a min or max
        IS a real value from the data.

    Null rates, cardinality, lengths and averages survive: they are facts about
    the column, not values from it.

    This is a function with a test rather than a convention, because a
    convention is exactly what leaks.
    """
    pii = set(pii_columns or ())
    safe = []
    for row in profile_rows:
        d = dict(row)
        d.pop("sample_values_json", None)
        if d.get("column_name") in pii:
            d.pop("min_value", None)
            d.pop("max_value", None)
        safe.append(d)
    return safe


def _split_name(table_name: str):
    parts = table_name.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, table_name
