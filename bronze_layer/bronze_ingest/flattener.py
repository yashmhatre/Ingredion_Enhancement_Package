"""
Handles nested struct/array fields in a DataFrame according to the
configured flatten_mode:

  - "raw"      -> leave the DataFrame untouched (structs/arrays stay nested).
                  This is the typical medallion-architecture bronze pattern:
                  store data as close to source shape as possible.
  - "flatten"  -> recursively flatten structs into top-level columns
                  (col_subcol_subsubcol), optionally exploding arrays.
  - "auto"     -> inspect nesting depth; flatten if depth <= threshold,
                  otherwise fall back to raw (protects against exploding a
                  huge/variable schema into thousands of columns).
"""

from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer

from .config import IngestionConfig


def _max_struct_depth(dtype, current=0):
    if isinstance(dtype, StructType):
        if not dtype.fields:
            return current
        return max(_max_struct_depth(f.dataType, current + 1) for f in dtype.fields)
    if isinstance(dtype, ArrayType):
        return _max_struct_depth(dtype.elementType, current)
    return current


def _flatten_once(df, separator: str, explode_arrays: bool):
    """One pass: expand every top-level struct column, optionally explode arrays."""
    changed = False
    for field in df.schema.fields:
        dtype = field.dataType
        if isinstance(dtype, StructType):
            changed = True
            expanded = [
                col(f"`{field.name}`.`{sub.name}`").alias(f"{field.name}{separator}{sub.name}")
                for sub in dtype.fields
            ]
            other_cols = [col(f"`{c}`") for c in df.columns if c != field.name]
            df = df.select(*other_cols, *expanded)
        elif isinstance(dtype, ArrayType) and explode_arrays:
            changed = True
            df = df.withColumn(field.name, explode_outer(col(f"`{field.name}`")))
    return df, changed


def flatten_dataframe(df, separator: str = "_", explode_arrays: bool = False, max_depth=None):
    """
    Repeatedly flattens struct columns (and optionally explodes arrays) until
    no top-level structs remain, or max_depth passes have been made.
    """
    depth = 0
    while True:
        if max_depth is not None and depth >= max_depth:
            break
        df, changed = _flatten_once(df, separator, explode_arrays)
        depth += 1
        if not changed:
            break
    return df


def apply_flatten_mode(df, config: IngestionConfig):
    """Entry point used by the pipeline: dispatches based on config.flatten_mode."""
    if config.flatten_mode == "raw":
        return df

    if config.flatten_mode == "flatten":
        return flatten_dataframe(
            df,
            separator=config.flatten_separator,
            explode_arrays=config.explode_arrays,
            max_depth=config.max_flatten_depth,
        )

    if config.flatten_mode == "auto":
        depth = max(_max_struct_depth(f.dataType) for f in df.schema.fields) if df.schema.fields else 0
        if depth <= config.auto_flatten_threshold:
            return flatten_dataframe(
                df,
                separator=config.flatten_separator,
                explode_arrays=config.explode_arrays,
                max_depth=config.max_flatten_depth,
            )
        return df  # too deep/variable - keep raw for safety

    raise ValueError(f"Unknown flatten_mode: {config.flatten_mode}")
