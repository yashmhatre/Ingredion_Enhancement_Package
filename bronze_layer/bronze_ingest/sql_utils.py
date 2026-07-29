"""
Small SQL/Column helpers shared by more than one module.

This exists so `bronze_writer` and `quality` can share `row_content_hash`
without one importing the other - they sit at the same layer and neither
owns the other's concerns.
"""

from pyspark.sql.functions import col, sha2, to_json, struct


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
