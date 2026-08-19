"""
Table maintenance for the objects this package creates: OPTIMIZE and VACUUM
(#159, item 3).

Scoped deliberately. This compacts and expires files; it does not decide table
layout (that is `cluster_by`/`partition_by`), does not delete rows, and does
not touch the quarantine table's contents (that is #159 item 4, and it is a
retention *policy* question, not a maintenance one).

The load-bearing rule
---------------------
**A VACUUM must never expire files a Change Data Feed consumer still needs.**
#58 enabled CDF by default and wrote a retention floor onto every table this
package creates, as `delta.deletedFileRetentionDuration`. A
`VACUUM ... RETAIN n HOURS` shorter than that property overrides it silently -
no error, just missing change history the next time Silver reads the feed.

So the floor is not re-declared here. Each table is asked what its own floor
is and this refuses to go below it. That is stronger than a second configured
number, because there is no second number to drift: the value maintenance
honours is the value #58 wrote, read back at run time.
"""

import re
from typing import Any, Dict, List, Optional

from .logging_utils import logger
from .sql_utils import describe_table_properties

#: Delta's own default when `delta.deletedFileRetentionDuration` is unset -
#: 7 days. Used as the floor for a table that predates #58 or opted out, so
#: this can never be the thing that introduces a shorter retention than Delta
#: would have applied on its own.
DEFAULT_RETENTION_HOURS = 168.0

_INTERVAL = re.compile(r"interval\s+(?:(\d+)\s+days?)?\s*(?:(\d+)\s+hours?)?", re.IGNORECASE)


def parse_retention_hours(value: Optional[str]) -> Optional[float]:
    """
    Delta writes retention as an interval string (`"interval 30 days"`). This
    converts one to hours, or returns None if it is absent or unparseable.

    None means "no opinion from the table", NOT "zero" - a caller that treated
    an unparseable interval as 0 would compute a floor of nothing and vacuum
    away exactly the history the floor exists to protect.
    """
    if not value:
        return None
    match = _INTERVAL.search(value)
    if not match or not any(match.groups()):
        return None
    days = int(match.group(1) or 0)
    hours = int(match.group(2) or 0)
    total = days * 24 + hours
    return float(total) if total > 0 else None


def retention_floor_hours(spark, table: str) -> float:
    """
    The shortest VACUUM retention that is safe for `table`, in hours.

    Read from the table's own `delta.deletedFileRetentionDuration` (#58's
    floor), falling back to Delta's 7-day default when the table does not
    carry one - so a table that predates #58 is still never vacuumed more
    aggressively than Delta itself would.
    """
    props = describe_table_properties(spark, table)
    parsed = parse_retention_hours(props.get("delta.deletedFileRetentionDuration"))
    return parsed if parsed is not None else DEFAULT_RETENTION_HOURS


def optimize_table(spark, table: str) -> Dict[str, Any]:
    """
    Compacts `table`. Never raises - maintenance is housekeeping, and one
    table failing to compact must not stop the rest of the run or fail the
    job. The result dict carries the outcome so the caller can report it.
    """
    try:
        spark.sql(f"OPTIMIZE {table}")
        return {"table": table, "optimize": "ok"}
    except Exception as exc:  # noqa: BLE001 - see docstring
        logger.warning("OPTIMIZE failed for %s: %s", table, exc)
        return {"table": table, "optimize": "failed", "optimize_error": str(exc)}


def vacuum_table(spark, table: str, requested_hours: Optional[float] = None) -> Dict[str, Any]:
    """
    Expires files for `table`, never below its own retention floor (#58/#159).

    `requested_hours=None` means "use the table's floor", which is the normal
    case and the safe default. A request SHORTER than the floor is raised to
    the floor and reported as `clamped` - deliberately not an error, because
    the sane response to "you asked for 24h but this table promises 720h" is
    to honour the promise and say so, not to skip maintenance entirely and let
    small files accumulate instead.

    Returns the outcome rather than raising, for the same reason as
    `optimize_table`.
    """
    floor = retention_floor_hours(spark, table)
    effective = floor if requested_hours is None else max(float(requested_hours), floor)
    clamped = requested_hours is not None and effective > float(requested_hours)

    if clamped:
        logger.warning(
            "VACUUM retention for %s raised from %sh to %sh: the table's "
            "delta.deletedFileRetentionDuration is the floor a CDF consumer relies on "
            "(#58). Vacuuming below it would silently expire change history.",
            table,
            requested_hours,
            effective,
        )

    try:
        spark.sql(f"VACUUM {table} RETAIN {effective} HOURS")
        return {
            "table": table,
            "vacuum": "ok",
            "retain_hours": effective,
            "floor_hours": floor,
            "clamped": clamped,
        }
    except Exception as exc:  # noqa: BLE001 - see optimize_table
        logger.warning("VACUUM failed for %s: %s", table, exc)
        return {
            "table": table,
            "vacuum": "failed",
            "vacuum_error": str(exc),
            "retain_hours": effective,
            "floor_hours": floor,
            "clamped": clamped,
        }


def run_maintenance(
    spark,
    tables: List[str],
    *,
    optimize: bool = True,
    vacuum: bool = True,
    vacuum_retention_hours: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    Runs the configured maintenance over an explicit list of tables.

    **An explicit list, not a schema scan.** Discovering tables would make this
    job's blast radius depend on whatever else happens to live in the schema -
    including tables this package did not create and has no business
    compacting. The caller names what it owns.

    Per-table failures are collected, not raised: a run that could not compact
    one table has still usefully compacted the others, and the caller decides
    whether the failures warrant failing the task.
    """
    results: List[Dict[str, Any]] = []
    for table in tables:
        entry: Dict[str, Any] = {"table": table}
        if optimize:
            entry.update(optimize_table(spark, table))
        if vacuum:
            entry.update(vacuum_table(spark, table, vacuum_retention_hours))
        results.append(entry)

    failed = [r for r in results if r.get("optimize") == "failed" or r.get("vacuum") == "failed"]
    logger.info(
        "Maintenance finished over %d table(s): %d with at least one failure.",
        len(results),
        len(failed),
    )
    return results
