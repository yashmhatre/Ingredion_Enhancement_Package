"""
Candidate-key detection from profile evidence (#327, phase 3 of #298).

#299 measures columns and #324 stores the measurements. This is the first
module that reads that evidence and proposes something, and everything after
it - grain, relationships, entity classification - is downstream of what a
table's keys are.

Shortlist from statistics, decide from data
-------------------------------------------

Profiling uses `approx_count_distinct` on purpose: cardinality drives
inference and exactness does not, and an exact distinct count per column would
reintroduce the per-column scan the whole module exists to avoid. HLL carries
a few percent of error, so

    distinct_approx == row_count

is a reason to LOOK at a column, never a reason to call it a key. Every
shortlisted candidate is verified with an exact `count_distinct` before a row
is written. #298 is explicit that a proposed grain must be verified by a real
uniqueness check and never accepted on inference; a key wrongly declared
becomes a wrong grain, and a wrong grain deduplicates real rows away - data
loss that looks like success.

The cost rule from #299 still applies: `verify()` computes every candidate's
exact counts in ONE aggregate, so twenty candidates cost one job. The common
case - a table with a single-column primary key - costs exactly one pass,
because finding one makes the composite search unnecessary and it is skipped
with that recorded as the reason.

Uniqueness on thin data is vacuous
-----------------------------------

In one row every column is unique, and every table in this repo currently
holds one row. So confidence is a function of row count with a hard floor:
below `min_rows_for_confidence` nothing exceeds the review threshold, and
below two rows nothing is claimed at all. Those candidates are still written,
carrying the row count as a stated blocker - "we looked, and here is why we
cannot say" is information, while writing nothing is indistinguishable from
having found nothing.

What is deliberately not a candidate
-------------------------------------

Columns bronze adds itself. `_quarantine_id` is the sharpest case: it is a
SHA-256 of the row's own content (`quality.py`), so it is unique BY
CONSTRUCTION and would otherwise be the highest-confidence primary key in
every quarantine table while telling nobody anything about the business data.
`_ingested_at` and `_batch_id` are ingest-time facts, not identity. #84 draws
the same line for `content_hash_columns` and rejects the same names.

Foreign keys are not here. A foreign key is a claim about two tables, needs a
join to verify, and belongs to `RelationshipDetector` in phase 4;
`FOREIGN_KEY` exists in the vocabulary so that phase has somewhere to write.
"""

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .logging_utils import logger
from .metadata_store import MetadataStore, validate_rows
from .profiling import SCALAR, ColumnNode, describe_columns, schema_fingerprint

#: Values of `candidate_type`. FOREIGN_KEY is declared but not produced here -
#: see the module docstring.
PRIMARY_KEY = "PRIMARY_KEY"
UNIQUE = "UNIQUE"

#: Columns bronze writes itself. Named literally rather than read from an
#: IngestionConfig because detection runs against a table, not against the
#: config that produced it - by the time anything is profiled, the run that
#: wrote it is long over and its config may not be reachable. Defaults are
#: `config.py`'s (`audit_ingest_ts_col` and friends, `content_hash_key_col`)
#: and `quality.py`'s quarantine columns.
DEFAULT_EXCLUDED_COLUMNS: Tuple[str, ...] = (
    "_ingested_at",
    "_batch_id",
    "_source_file",
    "_rescued_data",
    "_corrupt_record",
    "_content_hash_key",
    "_quarantine_id",
    "_quarantine_reason",
    "_occurrence_count",
    "_first_quarantined_at",
)

KEY_CANDIDATE_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), nullable=False),
        #: Comma-joined column paths in the order they were tested. A list
        #: would need a nested type for something every consumer reads as one
        #: unit; the ordering is stable so two runs produce the same string.
        StructField("column_names", StringType(), nullable=False),
        StructField("column_count", IntegerType(), nullable=False),
        StructField("candidate_type", StringType(), nullable=False),
        StructField("row_count", LongType(), nullable=False),
        #: EXACT, from count_distinct - not the profile's approximation. The
        #: two are kept side by side precisely so a disagreement is visible.
        StructField("distinct_count", LongType(), nullable=False),
        StructField("distinct_approx", LongType(), nullable=True),
        #: Rows where ANY component of the candidate is null.
        StructField("null_count", LongType(), nullable=False),
        StructField("is_unique", BooleanType(), nullable=False),
        StructField("confidence", DoubleType(), nullable=False),
        StructField("reason", StringType(), nullable=False),
        #: JSON list, "[]" when there are none - never NULL. A nullable
        #: "why not" column reads the same whether nothing blocked it or
        #: nothing filled it, which is the #276/#64 failure shape.
        StructField("blockers_json", StringType(), nullable=False),
        StructField("evidence_json", StringType(), nullable=False),
        #: True when the composite search hit its cap, so "no composite key
        #: found" can be told apart from "we stopped looking".
        StructField("search_bounded", BooleanType(), nullable=False),
        StructField("schema_fingerprint", StringType(), nullable=False),
        StructField("detection_run_id", StringType(), nullable=False),
        StructField("detected_at", TimestampType(), nullable=False),
    ]
)


@dataclass
class KeyDetectionConfig:
    """Where candidates go, and the bounds the search runs within."""

    metadata_schema: str
    metadata_catalog: Optional[str] = None
    key_candidate_table: str = "_key_candidate"
    column_profile_table: str = "_column_profile"

    #: A column shortlists when its APPROXIMATE distinct count is at least
    #: this share of the row count. Below 1.0 deliberately: HLL undercounts as
    #: readily as it overcounts, so an exact key can approximate to slightly
    #: under the row count and a strict == would miss it. Verification is what
    #: decides; this only controls what gets looked at.
    shortlist_ratio: float = 0.95

    #: Wide-table guard (#298 risk 16). Verification is one aggregate, but its
    #: width is 2 expressions per candidate, and Spark's codegen degrades on
    #: very wide projections.
    max_single_candidates: int = 50

    #: Composite search is 2^n over all subsets, so it is capped at pairs and
    #: triples and at a fixed number of combinations, ordered by cardinality
    #: (#298 risk 6). A capped run is RECORDED as capped.
    max_composite_width: int = 3
    max_composite_candidates: int = 40

    #: Below this, uniqueness is not evidence. Nothing found on fewer rows can
    #: exceed `review_threshold`, whatever else is true of it.
    min_rows_for_confidence: int = 1000
    thin_data_confidence_cap: float = 0.5

    #: Candidates at or above this may be auto-approved downstream. Held here
    #: so the cap above and the bar it must not clear are one decision.
    review_threshold: float = 0.7

    excluded_columns: Tuple[str, ...] = DEFAULT_EXCLUDED_COLUMNS

    def store(self, spark) -> MetadataStore:
        return MetadataStore(spark, self.metadata_schema, self.metadata_catalog)


@dataclass(frozen=True)
class Candidate:
    """A column set being tested, before anything is known about it."""

    nodes: Tuple[ColumnNode, ...]

    @property
    def paths(self) -> Tuple[str, ...]:
        return tuple(n.path for n in self.nodes)

    @property
    def label(self) -> str:
        return ",".join(self.paths)

    @property
    def width(self) -> int:
        return len(self.nodes)

    @property
    def accessors(self) -> Tuple[str, ...]:
        """Quoted accessors, taken from the schema walk rather than built by
        splitting the path on '.': a struct field `customer.name` and a
        top-level column literally named `customer.name` produce the same
        string and must not resolve to the same thing."""
        return tuple(n.accessor for n in self.nodes)


@dataclass(frozen=True)
class Verified:
    """A candidate plus the exact counts it was measured at."""

    candidate: Candidate
    row_count: int
    distinct_count: int
    null_count: int

    @property
    def non_null_rows(self) -> int:
        return self.row_count - self.null_count

    @property
    def is_unique(self) -> bool:
        """Unique over the rows where every component is present.

        `count_distinct` ignores rows with a null component, so the count is
        compared against the non-null rows, not the row count. A column that
        is unique where present but sometimes absent is a UNIQUE candidate,
        never a primary key - see `classify`.
        """
        return self.non_null_rows > 0 and self.distinct_count == self.non_null_rows


def is_excluded(path: str, excluded: Sequence[str]) -> bool:
    """True when `path` is a column bronze writes itself.

    Matches the LAST path segment, so a struct field named `_batch_id` is
    excluded on the same grounds as a top-level one: the reason it is not a
    business key is what it holds, not where it sits.
    """
    return path.split(".")[-1] in set(excluded)


def shortlist(
    profile_rows: Sequence[Dict[str, Any]],
    nodes: Sequence[ColumnNode],
    config: KeyDetectionConfig,
) -> Tuple[List[Candidate], List[str]]:
    """
    Single-column candidates worth verifying, most promising first, plus the
    reasons anything was dropped.

    Pure: takes profile rows and schema nodes, touches no Spark. Everything
    that decides WHAT to test is testable without a session; only the test
    itself needs data.
    """
    by_path = {n.path: n for n in nodes if n.kind == SCALAR}
    dropped: List[str] = []
    scored: List[Tuple[float, int, Candidate]] = []

    for row in profile_rows:
        path = row.get("column_name")
        if not isinstance(path, str):
            dropped.append(f"{path!r}: profile row carries no column name")
            continue
        node = by_path.get(path)
        if node is None:
            # Either a non-scalar (a struct is not a key; its fields might be)
            # or a column that has since left the schema. Both are worth
            # saying out loud - a profile that no longer matches the table is
            # how stale evidence turns into a confident wrong answer.
            dropped.append(f"{path}: not a scalar column in the current schema")
            continue
        if is_excluded(path, config.excluded_columns):
            dropped.append(f"{path}: excluded (a column bronze writes itself)")
            continue

        row_count = row.get("row_count") or 0
        distinct_approx = row.get("distinct_approx")
        if distinct_approx is None:
            dropped.append(f"{path}: no distinct count in the profile")
            continue
        if row_count <= 0:
            dropped.append(f"{path}: profile records no rows")
            continue
        if distinct_approx < row_count * config.shortlist_ratio:
            continue

        null_count = row.get("null_count") or 0
        # Fewest nulls first, then highest cardinality: a column with no nulls
        # can be a primary key and one with nulls cannot, so it is the more
        # useful thing to spend the candidate budget on.
        scored.append((-float(null_count), int(distinct_approx), Candidate((node,))))

    scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
    kept = [c for _, _, c in scored[: config.max_single_candidates]]
    if len(scored) > config.max_single_candidates:
        dropped.append(
            f"{len(scored) - config.max_single_candidates} further column(s) shortlisted "
            f"but not verified: max_single_candidates={config.max_single_candidates}"
        )
    return kept, dropped


def composite_candidates(
    profile_rows: Sequence[Dict[str, Any]],
    nodes: Sequence[ColumnNode],
    config: KeyDetectionConfig,
) -> Tuple[List[Candidate], bool]:
    """
    Multi-column candidates, ordered by cardinality, capped.

    Returns the candidates and whether the cap was hit. All column subsets is
    2^n; this takes pairs and triples of the highest-cardinality columns only,
    and says so when it stops early. A bounded search reporting "no composite
    key found" would be a false negative dressed as a result.
    """
    by_path = {n.path: n for n in nodes if n.kind == SCALAR}
    ranked: List[Tuple[int, ColumnNode]] = []
    for row in profile_rows:
        path = row.get("column_name")
        if not isinstance(path, str):
            continue
        node = by_path.get(path)
        if node is None or is_excluded(path, config.excluded_columns):
            continue
        distinct_approx = row.get("distinct_approx")
        if distinct_approx is None or distinct_approx <= 1:
            # A constant cannot narrow anything: adding it to a column set
            # leaves the set's distinct count exactly where it was.
            continue
        ranked.append((int(distinct_approx), node))

    ranked.sort(key=lambda t: (t[0], t[1].ordinal), reverse=True)
    ordered = [n for _, n in ranked]

    out: List[Candidate] = []
    bounded = False
    for width in range(2, max(2, config.max_composite_width) + 1):
        for combo in combinations(ordered, width):
            if len(out) >= config.max_composite_candidates:
                bounded = True
                return out, bounded
            out.append(Candidate(tuple(combo)))
    return out, bounded


def verify(df, candidates: Sequence[Candidate]) -> List[Verified]:
    """
    Exact distinct and null counts for every candidate, in ONE pass.

    The whole cost argument of this module is in the shape of this function:
    2 expressions per candidate in a single `agg`, never one job per
    candidate. #149 removed a `count()` from the ingestion path for
    reintroducing a scan; a per-candidate distinct count is that mistake
    multiplied by the size of the search.

    One pass is not the same as cheap, and overstating it here would be the
    repo's own recurring failure in the other direction. Spark evaluates
    several `COUNT(DISTINCT ...)` in one aggregate by EXPANDING each input row
    once per distinct group, so the shuffle grows with the number of
    candidates even though the source is read once. What this buys is one
    read and one job instead of N of each; what it does not buy is a shuffle
    independent of N. That is why `max_single_candidates` and
    `max_composite_candidates` are bounds and not suggestions.
    """
    if not candidates:
        return []

    exprs = [F.count(F.lit(1)).alias("row_count")]
    for i, cand in enumerate(candidates):
        cols = [F.col(a) for a in cand.accessors]
        # count_distinct, not approx: this is the step that DECIDES, and an
        # approximate answer here would make a key claim out of a guess.
        exprs.append(F.count_distinct(*cols).alias(f"d_{i}"))
        any_null = cols[0].isNull()
        for col in cols[1:]:
            any_null = any_null | col.isNull()
        exprs.append(F.sum(F.when(any_null, 1).otherwise(0)).alias(f"n_{i}"))

    stats = df.agg(*exprs).collect()[0].asDict()
    row_count = int(stats["row_count"] or 0)
    return [
        Verified(
            candidate=cand,
            row_count=row_count,
            distinct_count=int(stats[f"d_{i}"] or 0),
            null_count=int(stats[f"n_{i}"] or 0),
        )
        for i, cand in enumerate(candidates)
    ]


def classify(verified: Verified) -> str:
    """PRIMARY_KEY only when unique AND never null. A nullable key is not a
    key: it cannot identify the rows where it is absent."""
    if verified.is_unique and verified.null_count == 0:
        return PRIMARY_KEY
    return UNIQUE


def score(verified: Verified, config: KeyDetectionConfig) -> Tuple[float, List[str], str]:
    """
    Confidence, blockers and a human reason for one verified candidate.

    Confidence answers "how much does this uniqueness tell us", which is a
    different question from "is this unique" - the second is measured exactly
    and the first is a judgement about how much evidence there was. Splitting
    them is what keeps `is_unique` honest on one row of data.
    """
    blockers: List[str] = []

    if not verified.is_unique:
        return (
            0.0,
            [
                f"not unique: {verified.distinct_count} distinct value(s) over "
                f"{verified.non_null_rows} non-null row(s)"
            ],
            "duplicate values present",
        )

    if verified.row_count <= 1:
        # Vacuous, not strong. Every column of a one-row table is unique, and
        # every table in this repo currently holds one row.
        return (
            0.0,
            [
                f"row_count={verified.row_count}: uniqueness over a single row is vacuous - "
                "every column of a one-row table is unique"
            ],
            f"unique over {verified.row_count} row(s), which is too few to mean anything",
        )

    base = {1: 0.95, 2: 0.80}.get(verified.candidate.width, 0.65)
    reason = f"unique over {verified.non_null_rows} non-null row(s)"

    if verified.null_count > 0:
        # Unique where present, absent elsewhere. Real, and not a primary key.
        base -= 0.15
        blockers.append(
            f"{verified.null_count} row(s) have a null component: unique, but cannot "
            "identify every row"
        )
        reason += f"; {verified.null_count} row(s) null"

    confidence = base
    if verified.row_count < config.min_rows_for_confidence:
        confidence = min(base, config.thin_data_confidence_cap)
        blockers.append(
            f"row_count={verified.row_count} is below min_rows_for_confidence="
            f"{config.min_rows_for_confidence}: uniqueness on this few rows is not evidence"
        )

    return round(confidence, 4), blockers, reason


def _row(
    table_name: str,
    verified: Verified,
    config: KeyDetectionConfig,
    distinct_approx: Optional[int],
    fingerprint: str,
    run_id: str,
    now: datetime,
    search_bounded: bool,
) -> Dict[str, Any]:
    confidence, blockers, reason = score(verified, config)
    return {
        "table_name": table_name,
        "column_names": verified.candidate.label,
        "column_count": verified.candidate.width,
        "candidate_type": classify(verified),
        "row_count": verified.row_count,
        "distinct_count": verified.distinct_count,
        "distinct_approx": distinct_approx,
        "null_count": verified.null_count,
        "is_unique": verified.is_unique,
        "confidence": confidence,
        "reason": reason,
        "blockers_json": json.dumps(blockers),
        # The counts the decision was made on, not a restatement of it. #298
        # requires every recommendation to persist the evidence behind it.
        "evidence_json": json.dumps(
            {
                "columns": list(verified.candidate.paths),
                "row_count": verified.row_count,
                "non_null_rows": verified.non_null_rows,
                "distinct_exact": verified.distinct_count,
                "distinct_approx": distinct_approx,
                "null_count": verified.null_count,
                "review_threshold": config.review_threshold,
                "meets_review_threshold": confidence >= config.review_threshold,
            },
            sort_keys=True,
        ),
        "search_bounded": search_bounded,
        "schema_fingerprint": fingerprint,
        "detection_run_id": run_id,
        "detected_at": now,
    }


def _profile_rows(spark, table_name: str, config: KeyDetectionConfig) -> List[Dict[str, Any]]:
    """The latest profile run's rows for one table, as dicts."""
    frame = config.store(spark).read_for(config.column_profile_table, table_name)
    if frame is None:
        raise ValueError(
            f"No profile found for {table_name}: {config.column_profile_table} does not "
            "exist. Key detection reads profile evidence - run profile_table first (#299)."
        )
    latest = frame.orderBy(F.col("profiled_at").desc()).limit(1).collect()
    if not latest:
        raise ValueError(
            f"No profile rows for {table_name}. Key detection reads profile evidence - "
            "run profile_table first (#299)."
        )
    run_id = latest[0]["profile_run_id"]
    return [r.asDict() for r in frame.filter(F.col("profile_run_id") == run_id).collect()]


def detect_keys(
    spark,
    table_name: str,
    config: KeyDetectionConfig,
    profile_rows: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Finds and verifies candidate keys for one table, writing `_key_candidate`.

    `profile_rows` defaults to the most recent profile run's rows. Passing
    them explicitly is what lets the decision logic be tested against evidence
    that was never measured - including evidence that is WRONG, which is the
    case that proves shortlisting is not claiming.

    Raises rather than swallowing, for the same reason `profiling` does: this
    is an explicitly-invoked job whose entire output is the metadata, so a
    silent failure is indistinguishable from having found nothing.
    """
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    df = spark.read.table(table_name)
    fingerprint = schema_fingerprint(df.schema)
    nodes = describe_columns(df.schema)

    if profile_rows is None:
        rows: List[Dict[str, Any]] = _profile_rows(spark, table_name, config)
    else:
        rows = list(profile_rows)

    # Row count comes from the profile, so an empty table costs no pass at
    # all. Zero rows makes every ratio undefined and "100% unique" vacuous
    # (#298 risk 16); it is reported, not divided by.
    profiled_row_count = max((r.get("row_count") or 0) for r in rows) if rows else 0
    if profiled_row_count <= 0:
        logger.info("No key candidates for %s: the table has no rows.", table_name)
        return {
            "table_name": table_name,
            "status": "skipped",
            "reason": "table has no rows; uniqueness is undefined over zero rows",
            "candidates_written": 0,
            "detection_run_id": run_id,
        }

    singles, dropped = shortlist(rows, nodes, config)
    verified = verify(df, singles)

    approx_by_path = {r.get("column_name"): r.get("distinct_approx") for r in rows}
    search_bounded = False
    results = list(verified)

    has_primary_key = any(v.is_unique and v.null_count == 0 for v in verified)
    if has_primary_key:
        # One pass, and the common case. A composite key is only interesting
        # when no single column identifies a row; searching for one anyway
        # would double the cost to produce a longer key nobody would use.
        dropped.append(
            "composite search skipped: a single-column primary key was verified, so a "
            "composite key would not be minimal"
        )
    elif config.max_composite_width >= 2:
        composites, search_bounded = composite_candidates(rows, nodes, config)
        results.extend(verify(df, composites))
        if search_bounded:
            logger.warning(
                "Composite key search for %s stopped at max_composite_candidates=%d. "
                "'No composite key found' would be a false negative here.",
                table_name,
                config.max_composite_candidates,
            )

    candidate_rows = [
        _row(
            table_name,
            v,
            config,
            approx_by_path.get(v.candidate.paths[0]) if v.candidate.width == 1 else None,
            fingerprint,
            run_id,
            now,
            search_bounded,
        )
        for v in results
        if v.is_unique
    ]

    config.store(spark).append(
        config.key_candidate_table,
        KEY_CANDIDATE_SCHEMA,
        validate_rows(candidate_rows, {"candidate_type": "key_candidate_type"}),
    )

    accepted = [r for r in candidate_rows if r["confidence"] >= config.review_threshold]
    logger.info(
        "Key detection on %s: %d candidate(s) tested, %d unique, %d at or above the review "
        "threshold%s.",
        table_name,
        len(results),
        len(candidate_rows),
        len(accepted),
        " (search was bounded)" if search_bounded else "",
    )
    return {
        "table_name": table_name,
        "status": "detected",
        "row_count": profiled_row_count,
        "candidates_tested": len(results),
        "candidates_written": len(candidate_rows),
        "candidates_above_threshold": len(accepted),
        "primary_key_found": has_primary_key,
        "search_bounded": search_bounded,
        "notes": dropped,
        "schema_fingerprint": fingerprint,
        "detection_run_id": run_id,
    }
