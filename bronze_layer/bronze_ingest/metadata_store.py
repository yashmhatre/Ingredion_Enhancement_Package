"""
One writer for the framework's metadata tables (#324, phase 2 of #298).

#299 shipped three metadata tables and a private writer inside `profiling.py`.
Every later phase of #298 - key candidates, relationships, entity models,
column mappings - needs the same four operations, and copying them per module
is how two writers end up disagreeing about schema creation, replace
semantics, or which column identifies a table.

Two grains, and the distinction is deliberate
---------------------------------------------

`append` is for facts that accumulate: a profile run, a detection run, a status
transition. How a null rate MOVES is evidence, so overwriting it destroys the
only record of the change.

`replace_for` is for descriptions of current shape: a table's columns, its
current inferred model. Appending those would leave two rows both claiming to
describe the same thing, and nothing to say which is true.

Why a row missing a field raises here
-------------------------------------

`createDataFrame` with an explicit schema already fails on a missing key, but
it fails with a Py4J error naming neither the column nor the row. This repo
has now found the same defect four times in one week - `schema_drift_json`
(#276), `tags_failed` and `tag_outcome_json` (#64) were each present in
`AUDIT_SCHEMA`, set faithfully by the caller, and NULL in every row ever
written, because the write path silently dropped them. Both features were
closed as shipped.

So the write path here says which field nothing filled, and `unfilled_columns`
gives every later phase's tests the same check in the "always NULL" direction.

Vocabularies are data, not branches
-----------------------------------

#298 requires the modeling metadata to be model-agnostic: `entity_type`,
`role` and `model_type` are values, never `if model == "star"`. Values that
are not in their vocabulary must fail on the way in, or the table accumulates
free text that no consumer can switch on and no reader can trust.
"""

from typing import Any, Dict, Iterable, List, Optional, Sequence

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .logging_utils import logger
from .sql_utils import quote_literal

#: Controlled vocabularies for the modeling metadata (#298 §6, §15, §18).
#: Held here rather than beside each producer so a value and its meaning cannot
#: drift apart across phases.
VOCABULARIES: Dict[str, frozenset] = {
    "entity_type": frozenset(
        {
            "MASTER_ENTITY",
            "TRANSACTION",
            "REFERENCE",
            "CHILD_ENTITY",
            "BRIDGE",
            "EVENT",
            "SNAPSHOT",
            "HIERARCHY",
            "UNKNOWN",
        }
    ),
    "processing_strategy": frozenset(
        {"FULL_LOAD", "APPEND", "MERGE", "CDC", "SCD_TYPE_1", "SCD_TYPE_2"}
    ),
    "semantic_role": frozenset(
        {"KEY", "MEASURE", "ATTRIBUTE", "TIMESTAMP", "STATUS", "CODE", "DESCRIPTOR"}
    ),
    "additivity": frozenset({"ADDITIVE", "SEMI_ADDITIVE", "NON_ADDITIVE"}),
    "evolution_class": frozenset({"NON_BREAKING", "BREAKING", "REQUIRES_REVIEW"}),
    #: Produced by profiling.describe_columns.
    "column_kind": frozenset({"SCALAR", "STRUCT", "ARRAY", "MAP"}),
    "model_status": frozenset(
        {
            "DISCOVERED",
            "PROFILED",
            "DETECTED",
            "CANDIDATES_RANKED",
            "MODEL_RECOMMENDED",
            "REVIEW_REQUIRED",
            "AUTO_APPROVED",
            "APPROVED",
            "GENERATED",
            "VALIDATED",
            "PUBLISHED",
        }
    ),
}


class VocabularyError(ValueError):
    """A value outside its controlled vocabulary."""


class MetadataRowError(ValueError):
    """A row that does not match the table's schema."""


def validate_vocabulary(value: Optional[str], vocabulary: str, field_name: str) -> Optional[str]:
    """
    Returns `value` if it belongs to `vocabulary`, else raises naming both the
    field and the allowed set.

    None passes: "not yet classified" is a legitimate state, and forcing a
    caller to invent UNKNOWN when it means "not looked at" would destroy the
    distinction. UNKNOWN is a classification; None is the absence of one.
    """
    allowed = VOCABULARIES.get(vocabulary)
    if allowed is None:
        raise VocabularyError(f"no vocabulary named {vocabulary!r}. Known: {sorted(VOCABULARIES)}.")
    if value is None:
        return None
    if value not in allowed:
        raise VocabularyError(
            f"{field_name}={value!r} is not a valid {vocabulary}. "
            f"Allowed: {sorted(allowed)}. Free text here cannot be switched on by any "
            "consumer and cannot be trusted by any reader."
        )
    return value


def missing_fields(row: Dict[str, Any], schema: StructType) -> List[str]:
    """Non-nullable schema fields absent from `row`, or present but None."""
    return sorted(
        f.name
        for f in schema.fields
        if not f.nullable and (f.name not in row or row[f.name] is None)
    )


def unexpected_fields(row: Dict[str, Any], schema: StructType) -> List[str]:
    """Keys in `row` that the schema has no column for."""
    names = {f.name for f in schema.fields}
    return sorted(k for k in row if k not in names)


def unfilled_columns(rows: Sequence[Dict[str, Any]], schema: StructType) -> List[str]:
    """
    Schema columns that are NULL in every one of `rows`.

    The fillability check in the "always NULL" direction, for later phases'
    tests. A column no producer ever fills is indistinguishable from one that
    is legitimately empty, which is exactly how #276's and #64's output went
    missing while both were reported as delivered.

    Returns [] for an empty `rows` - with nothing written there is nothing to
    conclude, and claiming every column is unfilled would be a false alarm.
    """
    if not rows:
        return []
    names = [f.name for f in schema.fields]
    return sorted(n for n in names if all(r.get(n) is None for r in rows))


class MetadataStore:
    """Reads and writes the framework's metadata tables."""

    def __init__(self, spark, schema_name: str, catalog: Optional[str] = None):
        self.spark = spark
        self.schema_name = schema_name
        self.catalog = catalog

    @property
    def schema_ref(self) -> str:
        return ".".join(p for p in (self.catalog, self.schema_name) if p)

    def qualified(self, table: str) -> str:
        return ".".join(p for p in (self.catalog, self.schema_name, table) if p)

    def ensure_schema(self) -> None:
        if self.schema_ref:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_ref}")

    @staticmethod
    def validate(schema: StructType, rows: Sequence[Dict[str, Any]]) -> None:
        """
        Checks every row against `schema`, raising on the first problem.

        Separate from `_frame`, and called BEFORE `ensure_schema`, so a bad row
        is rejected without touching Spark at all. Creating a schema for a
        write that is about to fail is a side effect nobody asked for, and it
        makes the failure look like a Spark problem rather than a caller one.
        """
        for row in rows:
            missing = missing_fields(row, schema)
            if missing:
                raise MetadataRowError(
                    f"row is missing required field(s) {missing}. A field the writer drops "
                    "reads as a column nothing fills, which is how #276 and #64 both shipped "
                    "as delivered while writing NULL to every row."
                )
            unexpected = unexpected_fields(row, schema)
            if unexpected:
                raise MetadataRowError(
                    f"row has field(s) {unexpected} with no column to hold them. They would "
                    "be silently discarded."
                )

    def _frame(self, schema: StructType, rows: Sequence[Dict[str, Any]]):
        self.validate(schema, rows)
        return self.spark.createDataFrame([Row(**r) for r in rows], schema=schema)

    def append(self, table: str, schema: StructType, rows: Sequence[Dict[str, Any]]) -> None:
        """Adds rows, keeping everything already there. For accumulating facts."""
        if not rows:
            return
        self.validate(schema, rows)
        self.ensure_schema()
        target = self.qualified(table)
        self._frame(schema, rows).write.format("delta").mode("append").saveAsTable(target)
        logger.debug("Appended %d row(s) to %s.", len(rows), target)

    def replace_for(
        self,
        table: str,
        schema: StructType,
        rows: Sequence[Dict[str, Any]],
        key_column: str = "table_name",
    ) -> None:
        """
        Replaces the rows whose `key_column` matches `rows`', leaving every
        other key's rows untouched. For descriptions of current shape.

        `replaceWhere` rather than read-modify-write: the latter races two runs
        against each other, and this metadata is small enough that correctness
        beats cleverness.
        """
        if not rows:
            return
        keys = {r.get(key_column) for r in rows}
        if len(keys) != 1 or None in keys:
            raise MetadataRowError(
                f"replace_for needs every row to carry the same non-null {key_column!r}; "
                f"got {sorted(str(k) for k in keys)}. Replacing on a mixed key would delete "
                "rows this call was never given replacements for."
            )
        key = keys.pop()

        self.validate(schema, rows)
        self.ensure_schema()
        target = self.qualified(table)
        frame = self._frame(schema, rows)
        if not self.spark.catalog.tableExists(target):
            frame.write.format("delta").saveAsTable(target)
            return
        (
            frame.write.format("delta")
            .mode("overwrite")
            # quote_literal, not raw interpolation (#154). The key is DATA - it
            # arrives from whatever was scanned, not from a config field that
            # passed validate_identifier - and an apostrophe in it would either
            # break the predicate or widen it. A widened predicate here deletes
            # another table's metadata.
            .option("replaceWhere", f"{key_column} = '{quote_literal(key)}'")
            .saveAsTable(target)
        )

    def read_for(self, table: str, key_value: str, key_column: str = "table_name"):
        """Rows for one key, or None when the table does not exist yet."""
        target = self.qualified(table)
        if not self.spark.catalog.tableExists(target):
            return None
        # A Column expression, never an f-string predicate (#154): the value is
        # data, and there is no string for anything to escape out of.
        return self.spark.read.table(target).filter(F.col(key_column) == key_value)

    def latest_for(self, table: str, key_value: str, order_by: str, key_column: str = "table_name"):
        """The single most recent row for one key, or None."""
        frame = self.read_for(table, key_value, key_column=key_column)
        if frame is None:
            return None
        rows = frame.orderBy(F.col(order_by).desc()).limit(1).collect()
        return rows[0] if rows else None


def validate_rows(
    rows: Iterable[Dict[str, Any]], vocab_fields: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Validates every vocabulary-controlled field on every row, returning the
    rows unchanged so this can wrap a write.

    `vocab_fields` maps field name -> vocabulary name, e.g.
    {"entity_type": "entity_type", "role": "entity_type"}.
    """
    materialised = list(rows)
    for row in materialised:
        for field_name, vocabulary in vocab_fields.items():
            if field_name in row:
                validate_vocabulary(row[field_name], vocabulary, field_name)
    return materialised
