"""
Catalog documentation — applies config-driven table and column COMMENTs to
the bronze table after a successful write (#64).

Diff-and-apply, not blind-apply. Comment DDL creates a **new Delta table
version on every execution, including when the comment is unchanged**
(measured: applying the same `COMMENT ON TABLE` twice takes the table from
version 1 to 2 to 3). Re-stamping unchanged comments on every ingestion run
would therefore append junk versions to the table's history indefinitely,
so this module reads the current state first and issues DDL only for what
actually differs.

Scope note: this module intentionally covers COMMENTs only, not Unity
Catalog tags. `ALTER TABLE ... SET TAGS` and the `information_schema`
tag views are Databricks Runtime features that raise ParseException on
OSS/local Delta, so a tagging implementation cannot be executed - let
alone verified - by this package's test suite. Since tag failures are
non-fatal by design, an unverified implementation would silently report
success while applying nothing, which is a worse outcome for a governance
feature than not shipping it. Tags remain tracked on #64 pending
validation against a real UC workspace.

Never raises: catalog documentation failing must never fail an ingestion
run, matching audit.py and schema_registry.py.
"""

from typing import Dict, Optional

from .config import IngestionConfig
from .logging_utils import logger
from .sql_utils import quote_ident, quote_literal

#: Kept as a module-level alias so existing references and tests keep
#: working; the implementation now lives in sql_utils so every module escapes
#: identically (#154). This module is the reason that centralisation
#: happened: it escaped the comment BODY correctly here and interpolated the
#: table name and column name raw, two lines apart.
_quote = quote_literal


def _current_table_comment(spark, full_name: str) -> Optional[str]:
    """Current table COMMENT, or None if unset/unreadable."""
    try:
        for row in spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}").collect():
            if row[0] == "Comment":
                return row[1]
    except Exception:  # noqa: BLE001 - no readable comment is indistinguishable from no comment set
        pass
    return None


def _current_column_comments(spark, full_name: str) -> Dict[str, str]:
    """
    Current per-column COMMENTs keyed by column name. Uses
    spark.catalog.listColumns(), whose `description` field carries the
    comment directly - no DESCRIBE output parsing needed.

    Returns {} if the table can't be read; callers treat that as "nothing
    known", which at worst re-applies a comment that was already correct.
    """
    try:
        return {c.name: c.description for c in spark.catalog.listColumns(full_name)}
    except Exception:  # noqa: BLE001 - listColumns is unavailable outside UC; an empty map means 'nothing to diff'
        return {}


def apply_catalog_metadata(spark, config: IngestionConfig) -> Dict[str, object]:
    """
    Applies config.table_comment and config.column_comments to the target
    table, issuing DDL only for values that differ from what's already in
    the catalog.

    Columns named in `column_comments` that don't exist on the table are
    logged as warnings and skipped rather than raising - this includes
    nested paths like "customer.name", which are deliberately not
    supported: bronze preserves nested JSON structures rather than
    flattening them, so there is no top-level column by that name.

    No-ops entirely (zero catalog reads, zero DDL) when neither
    table_comment nor column_comments is configured.

    Returns a summary dict {"table_comment_applied", "columns_applied",
    "columns_skipped"} for logging/testing. Never raises.
    """
    result = {"table_comment_applied": False, "columns_applied": [], "columns_skipped": []}

    if not config.table_comment and not config.column_comments:
        return result

    full_name = config.full_table_name

    try:
        if not spark.catalog.tableExists(full_name):
            logger.warning(
                "Skipping catalog metadata for %s: table does not exist.",
                full_name,
            )
            return result

        if config.table_comment:
            if _current_table_comment(spark, full_name) != config.table_comment:
                spark.sql(f"COMMENT ON TABLE {full_name} IS '{_quote(config.table_comment)}'")
                result["table_comment_applied"] = True
                logger.info("Applied table comment to %s.", full_name)

        if config.column_comments:
            current = _current_column_comments(spark, full_name)
            for column, comment in config.column_comments.items():
                if column not in current:
                    result["columns_skipped"].append(column)
                    continue
                if current.get(column) == comment:
                    continue
                # quote_ident, not a bare backtick pair: a column name
                # containing a backtick would otherwise close the quoting
                # early and the rest of the name would be parsed as SQL
                # (#154). Column names here are checked against the table's
                # actual columns just above, so this is defence in depth
                # rather than the only guard - but the names come from the
                # DATA's schema, which never passed through config
                # validation.
                spark.sql(
                    f"ALTER TABLE {full_name} ALTER COLUMN {quote_ident(column)} "
                    f"COMMENT '{quote_literal(comment)}'"
                )
                result["columns_applied"].append(column)

            if result["columns_skipped"]:
                logger.warning(
                    "Skipped column comments for %s: column(s) %s not present on the table "
                    "(nested paths like 'a.b' are not supported - bronze preserves nested "
                    "structures rather than flattening them). Available columns: %s",
                    full_name,
                    result["columns_skipped"],
                    sorted(current),
                )
            if result["columns_applied"]:
                logger.info(
                    "Applied column comments to %s: %s",
                    full_name,
                    result["columns_applied"],
                )
    except Exception as exc:  # noqa: BLE001 - documentation must never fail a successful write
        logger.warning("Failed to apply catalog metadata for %s: %s", full_name, exc)

    return result
