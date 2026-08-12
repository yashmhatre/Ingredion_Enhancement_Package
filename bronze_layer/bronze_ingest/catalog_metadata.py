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

from typing import Any, Dict, Optional

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
    # nosec B110 - the try/except/pass is the intended control flow, not a
    # swallowed error. DESCRIBE TABLE EXTENDED is unavailable on some
    # engines and fails on a table that does not exist yet; both mean "no
    # comment is currently set", which is what the caller needs to diff
    # against. Falling through to `return None` states that.
    except Exception:  # noqa: BLE001 - no readable comment means no comment set  # nosec B110
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
    result: Dict[str, Any] = {
        "table_comment_applied": False,
        "columns_applied": [],
        "columns_skipped": [],
    }

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


# ---------------------------------------------------------------------------
# Unity Catalog tags (#64)
# ---------------------------------------------------------------------------
#
# Same diff-then-apply shape as the COMMENT logic above, different transport -
# the wire call lives in tag_client.py, and the reasoning for REST over
# `ALTER TABLE ... SET TAGS` is in docs/decisions/2026-08_uc_tag_mechanism.md.


#: Prefixes reserved for GOVERNED tag keys - ones a Unity Catalog tag policy
#: can be attached to. Applying one of these is not labelling: it can bind an
#: ABAC policy and change who can read the data. Verified live 2026-08-12:
#: 63 `class.*` policies exist in this workspace and the ABAC policy endpoint
#: answers, so both halves of that hazard are real today, not prospective.
#:
#: Config-declared tags are refused if they start with one of these. That is
#: the "no auto-apply" gate in its cheapest form - see apply_reviewed_tags for
#: the path governed keys DO travel.
GOVERNED_TAG_PREFIXES = ("class.", "system.", "ai.", "sap.")


def is_governed_tag(key: str) -> bool:
    """True if `key` belongs to a policy-bearing namespace."""
    return any(key.startswith(prefix) for prefix in GOVERNED_TAG_PREFIXES)


def _diff_tags(current: Dict[str, str], desired: Dict[str, str]) -> Dict[str, str]:
    """
    The subset of `desired` that differs from `current`.

    Deliberately does NOT return removals. Config declaring fewer tags than
    the catalog holds means "these are the ones I manage", not "delete
    everything else" - a table may legitimately carry tags applied by a
    governance process this pipeline knows nothing about, and silently
    stripping them on the next ingestion run would be the worst kind of
    surprise. Removal is an explicit operation, not a side effect of a diff.
    """
    return {k: v for k, v in desired.items() if current.get(k) != v}


def apply_catalog_tags(spark, config: IngestionConfig, client=None) -> Dict[str, Any]:
    """
    Applies `config.table_tags` and `config.column_tags` to the target table,
    issuing writes only for values that differ from what the catalog already
    holds (#64).

    **Refuses governed keys.** Anything under `class.*`, `system.*`, `ai.*` or
    `sap.*` is skipped with a warning, because config comes from a Volume and a
    governed tag can carry an ABAC policy - see `GOVERNED_TAG_PREFIXES` and
    `config.table_tags`' field comment. Those keys reach the catalog only
    through `apply_reviewed_tags`.

    Never raises, matching `apply_catalog_metadata` and the rest of the
    advisory lane: a governance write failing must not fail the ingestion run
    that produced the data.

    No-ops entirely - zero reads, zero writes - when neither field is set.
    """
    result: Dict[str, Any] = {"tags_applied": 0, "tags_skipped_governed": []}
    if not config.table_tags and not config.column_tags:
        return result

    if client is None:
        from .tag_client import WorkspaceTagClient

        client = WorkspaceTagClient()

    from .tag_client import ENTITY_COLUMN, ENTITY_TABLE

    table = config.full_table_name
    targets = [(ENTITY_TABLE, table, dict(config.table_tags or {}))]
    for column, tags in (config.column_tags or {}).items():
        targets.append((ENTITY_COLUMN, f"{table}.{column}", dict(tags or {})))

    for entity_type, entity_name, desired in targets:
        governed = sorted(k for k in desired if is_governed_tag(k))
        if governed:
            # Not an exception. A config that names a governed key is a
            # mistake to correct, not a reason to fail an ingestion run - and
            # failing here would make the safe path the one that stops data
            # flowing, which is how gates get disabled.
            logger.warning(
                "Refusing to apply governed tag key(s) %s to %s from config: these "
                "namespaces can carry an ABAC policy, so applying one from a "
                "Volume-loaded config could silently change data access (#64). Use "
                "apply_reviewed_tags for governed keys.",
                governed,
                entity_name,
            )
            result["tags_skipped_governed"].extend(f"{entity_name}:{k}" for k in governed)
            for key in governed:
                desired.pop(key, None)

        if not desired:
            continue

        try:
            changed = _diff_tags(client.list_tags(entity_type, entity_name), desired)
            for key, value in changed.items():
                client.set_tag(entity_type, entity_name, key, value)
            if changed:
                logger.info(
                    "Applied %d tag(s) to %s: %s", len(changed), entity_name, sorted(changed)
                )
            result["tags_applied"] += len(changed)
        except Exception as exc:  # noqa: BLE001 - see docstring
            logger.warning("Tagging failed for %s: %s", entity_name, exc)

    return result


def apply_reviewed_tags(
    spark, config: IngestionConfig, reviewed: Dict[str, Dict[str, str]], client=None
) -> Dict[str, Any]:
    """
    The AI layer's publish target (#64), and the ONLY path governed keys take.

    `reviewed` is `{entity_name: {tag_key: tag_value}}`, where `entity_name` is
    a table or four-part column FQN.

    **The name is the contract.** This takes tags a human or an explicit
    promotion step has approved - never `_ai_metadata.pii_flags_json` read
    directly, and never "drafts above a confidence threshold". A confidence
    score is not a review. `architecture.md` keeps `_ai_metadata` out of the
    write path and #208 verified that by grep; this function is where that
    separation would be lost if it were wired to drafts.

    The failure chain it exists to prevent is short, and every link in it is
    live today: a model guesses a column holds email addresses -> a governed
    tag lands -> an ABAC policy masks the column -> the data disappears from a
    consumer three systems away from the cause.

    Never raises. Returns the same shape as `apply_catalog_tags`.
    """
    result: Dict[str, Any] = {"tags_applied": 0, "tags_skipped_governed": []}
    if not reviewed:
        return result

    if client is None:
        from .tag_client import WorkspaceTagClient

        client = WorkspaceTagClient()

    from .tag_client import ENTITY_COLUMN, ENTITY_TABLE

    table = config.full_table_name
    for entity_name, desired in reviewed.items():
        if not desired:
            continue
        # A column FQN has one more dot-separated part than its table.
        entity_type = (
            ENTITY_COLUMN
            if entity_name.startswith(f"{table}.") and entity_name != table
            else ENTITY_TABLE
        )
        try:
            changed = _diff_tags(client.list_tags(entity_type, entity_name), dict(desired))
            for key, value in changed.items():
                client.set_tag(entity_type, entity_name, key, value)
            if changed:
                logger.info(
                    "Applied %d reviewed tag(s) to %s: %s",
                    len(changed),
                    entity_name,
                    sorted(changed),
                )
            result["tags_applied"] += len(changed)
        except Exception as exc:  # noqa: BLE001 - see docstring
            logger.warning("Reviewed tagging failed for %s: %s", entity_name, exc)

    return result
