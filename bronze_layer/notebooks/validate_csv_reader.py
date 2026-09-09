# Databricks notebook source
# MAGIC %md
# MAGIC # CSV reader validation
# MAGIC Manual DBR validation for #311. Set `scratch_path` to a writable
# MAGIC `/Volumes/<catalog>/<schema>/<volume>/...` directory. The notebook
# MAGIC creates replaceable fixtures below that path and performs no table writes.

# COMMAND ----------

# This manual notebook has no bundle job environment of its own. Install the
# wheel built by the bundle into the attached serverless session first:
#
#   %pip install /Volumes/<catalog>/<schema>/<volume>/bronze_ingest-<version>-py3-none-any.whl
#   dbutils.library.restartPython()
#
# Keeping the path operator-supplied avoids binding validation to a user's
# workspace home or to one deployment target.

# COMMAND ----------

from bronze_ingest.config import IngestionConfig
from bronze_ingest.readers import read_source

dbutils.widgets.text("scratch_path", "", "Writable /Volumes scratch directory")
SCRATCH = dbutils.widgets.get("scratch_path").strip().rstrip("/")
if not SCRATCH.startswith("/Volumes/"):
    raise ValueError("scratch_path must name a writable /Volumes/... directory")

dbutils.fs.mkdirs(SCRATCH)
dbutils.fs.put(f"{SCRATCH}/header.csv", "id,name,amount\n1,Ana,10.5\n2,Bob,20.0\n", True)
dbutils.fs.put(f"{SCRATCH}/headerless.csv", "1,Ana\n2,Bob\n", True)
dbutils.fs.put(f"{SCRATCH}/illegal_header.csv", "order id,total(amount)\n1,10.5\n", True)
dbutils.fs.put(f"{SCRATCH}/malformed.csv", "id,amount\n1,10.5\n2,not-a-number\n", True)
dbutils.fs.put(
    f"{SCRATCH}/rfc4180.csv",
    '"KUNNR","NAME1"\n'
    '"0000100001","He said ""premium grade"""\n'
    '"0000100002","Line one\nLine two"\n',
    True,
)

results: list[tuple[str, str, str]] = []


def check(name, fn):
    try:
        fn()
        results.append((name, "PASS", ""))
    except Exception as exc:  # noqa: BLE001 - validation must report every case
        results.append((name, "FAIL", f"{type(exc).__name__}: {exc}"))


def config(filename, **overrides):
    return IngestionConfig(
        source_path=f"{SCRATCH}/{filename}",
        table="_csv_validation",
        source_format="csv",
        **overrides,
    )


def header_and_lineage():
    df = read_source(spark, config("header.csv"))
    assert df.count() == 2
    assert {"id", "name", "amount", "_input_file_name"}.issubset(df.columns)
    assert df.select("_input_file_name").first()[0].endswith("header.csv")


def headerless():
    df = read_source(
        spark,
        config(
            "headerless.csv",
            csv_header=False,
            schema_hint_ddl="id INT, name STRING",
        ),
    )
    assert df.count() == 2
    assert {"id", "name"}.issubset(df.columns)


def illegal_headers_are_visible():
    df = read_source(spark, config("illegal_header.csv"))
    assert {"order id", "total(amount)"}.issubset(df.columns)
    assert df.count() == 1


def malformed_value_is_preserved():
    ddl = "id INT, amount DOUBLE, _corrupt_record STRING, _rescued_data STRING"
    df = read_source(spark, config("malformed.csv", schema_hint_ddl=ddl))
    assert df.count() == 2
    bad = df.filter("id = 2").first()
    assert bad["amount"] is None
    assert bad["_corrupt_record"] is not None or bad["_rescued_data"] is not None


def rfc4180_quotes_are_unescaped():
    """#375: escape defaults to the doubled quote, not Spark's backslash."""
    df = read_source(spark, config("rfc4180.csv", csv_multiline=True))
    names = {r["KUNNR"]: r["NAME1"] for r in df.collect()}
    assert names["0000100001"] == 'He said "premium grade"', names


def csv_multiline_keeps_a_quoted_newline_in_one_row():
    """#375: left off (the default) this file reads as 3 rows, the third
    one values shifted under the wrong column names."""
    df = read_source(spark, config("rfc4180.csv", csv_multiline=True))
    assert df.count() == 2
    row = df.filter("KUNNR = '0000100002'").first()
    assert row["NAME1"] == "Line one\nLine two", row["NAME1"]


check("header_and_metadata_file_path", header_and_lineage)
check("headerless_with_explicit_schema", headerless)
check("illegal_header_characters", illegal_headers_are_visible)
check("malformed_value_rescue", malformed_value_is_preserved)
check("rfc4180_escaped_quotes", rfc4180_quotes_are_unescaped)
check("csv_multiline_quoted_newline", csv_multiline_keeps_a_quoted_newline_in_one_row)

display(spark.createDataFrame(results, "case STRING, status STRING, detail STRING"))
failed = [row for row in results if row[1] != "PASS"]
if failed:
    raise RuntimeError(f"FAILED: {len(failed)}/{len(results)} CSV cases failed")
dbutils.notebook.exit(f"SUCCESS: all {len(results)} CSV cases passed")
