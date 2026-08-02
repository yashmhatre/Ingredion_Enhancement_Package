"""
Table naming: filename -> Unity Catalog table name.

Split out of `directory_ingestion` (#151). Depends on nothing - not even
`databricks_fs` - which is the clearest illustration of why that module was
four modules in one.

`sanitize_table_name` and `build_table_name` are part of the package's
public API (`__init__.__all__`), so they are re-exported from
`directory_ingestion` as well; no caller needs to change its import.
"""

import os
import re


def sanitize_table_name(filename: str) -> str:
    """
    Converts a filename into a valid Databricks/Unity Catalog table name:
      orders-2026 Jan.json -> orders_2026_jan
    Rules: strip extension, lowercase, replace non [a-z0-9_] with '_',
    collapse repeats, prefix 't_' if it starts with a digit.
    """
    name = os.path.splitext(os.path.basename(filename))[0]
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name).lower()
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        raise ValueError(f"Filename {filename!r} produced an empty table name")
    if name[0].isdigit():
        name = f"t_{name}"
    return name


def build_table_name(filename: str, template: str = "{filename}_bronze") -> str:
    """
    Applies the naming template. The template must contain '{filename}'.
      template="{filename}_bronze"  -> orders_bronze
      template="bronze_{filename}"  -> bronze_orders
    """
    if "{filename}" not in template:
        raise ValueError("table_name_template must contain '{filename}'")
    return template.replace("{filename}", sanitize_table_name(filename))
