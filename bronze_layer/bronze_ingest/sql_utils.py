"""
Small SQL/Column helpers shared by more than one module.

This exists so modules at the same layer can share helpers without one
importing the other - `bronze_writer` and `quality` both need
`row_content_hash`, and every module that builds a SQL string needs the
quoting helpers.

The quoting helpers are deliberately centralised rather than reimplemented
per module. Before #154 the escaping was done ad hoc, and the giveaway was
`catalog_metadata`: it escaped the comment BODY correctly and dropped the
table name and column name in raw, one line apart. When each site decides
for itself, some sites decide wrong.
"""

import re

from pyspark.sql.functions import col, sha2, struct, to_json

#: Unity Catalog object names: letters, digits, underscores, not leading with
#: a digit. Deliberately NARROWER than what UC will actually accept (it
#: permits spaces and most punctuation in backtick-delimited names). The point
#: is not to model UC's grammar - it is to reject anything that could change
#: the meaning of a SQL string this package builds, while still accepting
#: every name a sane bronze pipeline uses. A user who genuinely needs an
#: exotic name gets a clear error at config load instead of a mangled
#: statement at write time.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value, field_name: str) -> str:
    """
    Returns `value` if it is safe to interpolate into a SQL identifier
    position, otherwise raises ValueError naming the field.

    Called from `IngestionConfig.__post_init__`, not from the SQL call sites,
    and that is the whole design (#154):

      - it fails before a cluster starts, rather than 40 minutes into a run
      - there is exactly one place to audit
      - the error names the config field, so the fix is obvious

    The realistic case this catches is not an attacker. It is a legitimate
    config author writing `table: "orders-2024"` or a name with an
    apostrophe, and getting an opaque Spark parse error from deep inside a
    generated statement. Same fix either way.
    """
    if value is None or not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string identifier, got {value!r}.")
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(
            f"{field_name}={value!r} is not a valid identifier. Expected letters, "
            "digits and underscores, not starting with a digit. This is enforced "
            "because the name is interpolated into SQL statements this package "
            "builds - a hyphen, space or quote would change what those statements "
            "mean, or fail with an opaque parse error mid-run."
        )
    return value


def validate_identifiers(values, field_name: str):
    """validate_identifier over a list, naming the offending index."""
    if values is None:
        return values
    for i, value in enumerate(values):
        validate_identifier(value, f"{field_name}[{i}]")
    return values


def quote_literal(value) -> str:
    """
    Escapes a value for use inside a single-quoted SQL string literal, by
    doubling single quotes. Verified against Spark SQL: `'it''s fine'`
    round-trips to `it's fine`.

    Returns the INNER text, without the surrounding quotes, so callers keep
    control of the quoting: `f"COMMENT '{quote_literal(text)}'"`.
    """
    return str(value).replace("'", "''")


def quote_ident(name) -> str:
    """
    Backtick-delimits an identifier, doubling any embedded backtick.

    Belt and braces alongside `validate_identifier`: config-supplied names
    are validated at load, but some identifiers reaching SQL come from the
    DATA rather than from config - DataFrame column names discovered by
    schema inference, for instance - and those never passed through
    __post_init__.
    """
    return "`" + str(name).replace("`", "``") + "`"


def row_content_hash(df, columns=None):
    """
    A deterministic SHA-256 over the content of each row.

    `to_json(struct(*))` rather than `concat_ws`: concat_ws SKIPS nulls
    instead of encoding them, so `(1, 'a', None)` and `(1, None, 'a')` both
    render as `1|a` and collide. Verified directly against a local Spark
    session - those two rows produce identical concat_ws hashes and
    different to_json hashes. That distinction matters because null-bearing
    rows are exactly the rows the quality gate sends to quarantine, so a
    collision there would merge two genuinely different bad rows.

    to_json also encodes the field NAME alongside the value, so it is not
    fooled by two columns swapping values, and it is stable across
    recomputation and across partitionings - the property
    `monotonically_increasing_id()` does not have, and the reason this
    function exists (#147).

    It is a function of content only. Two byte-identical rows hash
    identically by design; callers that need to tell such rows apart need
    something other than content.

    columns: defaults to every column on `df`, in DataFrame order. Pass an
    explicit list to hash a subset - note the hash then depends on that
    list's order, so callers must keep it stable if they compare hashes
    across runs.
    """
    cols = list(df.columns) if columns is None else list(columns)
    return sha2(to_json(struct(*[col(f"`{c}`") for c in cols])), 256)


def describe_table_properties(spark, full_name: str) -> dict:
    """
    Current Delta TBLPROPERTIES for `full_name`, or {} if the table does not
    exist or DESCRIBE DETAIL is unavailable on this engine.

    Never raises: property introspection failing must not fail an ingestion
    run, and "absent" is a usable answer - the caller then applies everything,
    which is correct for a table that does not exist yet.
    """
    try:
        row = spark.sql(f"DESCRIBE DETAIL {full_name}").select("properties").collect()[0]
        return row["properties"] or {}
    except Exception:  # noqa: BLE001 - see docstring; absent state is the answer
        return {}


def apply_table_properties(spark, full_name: str, desired, current=None) -> dict:
    """
    Sets only the TBLPROPERTIES that differ from what the table already has,
    and returns the ones it changed ({} when there was nothing to do).

    Lives here rather than in `bronze_writer` because `quality.write_quarantine`
    needs it too, and this module exists precisely so those two do not import
    each other.

    Diffing rather than unconditionally issuing the ALTER is what makes a
    second run against an already-configured table a no-op at the DDL level
    (#58's acceptance criterion). `current` is a parameter rather than always
    re-read so `_ensure_liquid_clustering_and_properties`, which has already
    paid for a DESCRIBE DETAIL, does not pay for a second one.

    Both key and value are escaped (#154): `table_properties` is free-form
    Dict[str, str] straight from YAML, and unescaped values broke the
    statement on an apostrophe and could append arbitrary DDL to it. Keys are
    additionally validated per dot-separated part at config load, since they
    are dotted by convention (delta.enableChangeDataFeed).
    """
    if not desired:
        return {}
    if current is None:
        current = describe_table_properties(spark, full_name)

    changed = {k: v for k, v in desired.items() if current.get(k) != v}
    if not changed:
        return {}

    clause = ", ".join(f"'{quote_literal(k)}' = '{quote_literal(v)}'" for k, v in changed.items())
    spark.sql(f"ALTER TABLE {full_name} SET TBLPROPERTIES ({clause})")
    return changed
