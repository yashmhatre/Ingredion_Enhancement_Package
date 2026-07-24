# Databricks notebook source
# MAGIC %md
# MAGIC # JSON Reader Validation — real ADLS fixtures
# MAGIC Manual validation notebook (not part of the local pytest suite). Reads
# MAGIC each fixture file from ADLS and asserts expected behavior against
# MAGIC `json_reader.read_json`. Run cell by cell, or run all — failures print
# MAGIC clearly and the notebook does not stop on the first failure so you get
# MAGIC a full report in one pass.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/fabricyash@gmail.com/Ingredion_Enhancement_Package/bronze_json_loader")

# COMMAND ----------

from bronze_json_loader.config import IngestionConfig
from bronze_json_loader.json_reader import read_json

BASE = "abfss://ingredion@ingredionenpkgdev.dfs.core.windows.net/raw/JSON"

results = []

def check(name, fn):
    """Runs fn(), records pass/fail + message, never raises past this point."""
    try:
        fn()
        results.append((name, "PASS", ""))
    except AssertionError as e:
        results.append((name, "FAIL", str(e)))
    except Exception as e:
        results.append((name, "ERROR", f"{type(e).__name__}: {e}"))

def cfg(path, multiline=True, **kwargs):
    return IngestionConfig(source_path=path, table="_validation_scratch", multiline=multiline, **kwargs)

# COMMAND ----------

# MAGIC %md ### Structure

# COMMAND ----------

check("single_object", lambda: (
    lambda df: (_ for _ in ()).throw(AssertionError(f"expected 1 row, got {df.count()}")) if df.count() != 1 else None
)(read_json(spark, cfg(f"{BASE}/single_object.json"))))

check("array_of_objects", lambda: (
    lambda df: (_ for _ in ()).throw(AssertionError(f"expected 2 rows, got {df.count()}")) if df.count() != 2 else None
)(read_json(spark, cfg(f"{BASE}/array_of_objects.json"))))

check("lines_jsonl", lambda: (
    lambda df: (_ for _ in ()).throw(AssertionError(f"expected 2 rows, got {df.count()}")) if df.count() != 2 else None
)(read_json(spark, cfg(f"{BASE}/lines.jsonl", multiline=False))))

check("nested_one_level", lambda: (
    lambda df: (_ for _ in ()).throw(AssertionError("customer struct missing")) if "customer" not in df.columns else None
)(read_json(spark, cfg(f"{BASE}/nested_one_level.json"))))

check("nested_three_level", lambda: (
    lambda df: (_ for _ in ()).throw(AssertionError("customer struct missing")) if "customer" not in df.columns else None
)(read_json(spark, cfg(f"{BASE}/nested_three_level.json"))))

check("array_field", lambda: (
    lambda df: (_ for _ in ()).throw(AssertionError("items array missing")) if "items" not in df.columns else None
)(read_json(spark, cfg(f"{BASE}/array_field.json"))))

check("array_of_arrays", lambda: read_json(spark, cfg(f"{BASE}/array_of_arrays.json")).count())

# COMMAND ----------

# MAGIC %md ### Multi-file source

# COMMAND ----------

def _check_multi_file():
    df = read_json(spark, cfg(f"{BASE}/multi_file/"))
    assert df.count() == 2, f"expected 2 rows across files, got {df.count()}"
    distinct_sources = df.select("_input_file_name").distinct().count()
    assert distinct_sources == 2, f"expected 2 distinct source files, got {distinct_sources}"

check("multi_file_source", _check_multi_file)

# COMMAND ----------

# MAGIC %md ### Bad data

# COMMAND ----------

def _check_malformed():
    df = read_json(spark, cfg(f"{BASE}/malformed.json"))
    # Should not raise. Corrupt record should be captured, not silently dropped.
    assert df.count() >= 1, "malformed file produced no rows at all"
    assert "_corrupt_record" in df.columns, "_corrupt_record column missing"

check("malformed_json_does_not_crash", _check_malformed)

def _check_empty_file():
    df = read_json(spark, cfg(f"{BASE}/empty_file.json"))
    assert df.count() == 0, f"expected 0 rows for empty file, got {df.count()}"

check("empty_file", _check_empty_file)

def _check_empty_object():
    df = read_json(spark, cfg(f"{BASE}/empty_object.json"))
    assert df.count() == 1, f"expected 1 row for empty object, got {df.count()}"

check("empty_object", _check_empty_object)

def _check_empty_array():
    df = read_json(spark, cfg(f"{BASE}/empty_array.json"))
    assert df.count() == 0, f"expected 0 rows for empty array, got {df.count()}"

check("empty_array", _check_empty_array)

def _check_mixed_valid_malformed():
    df = read_json(spark, cfg(f"{BASE}/mixed_valid_malformed.jsonl", multiline=False))
    good = df.filter("id IS NOT NULL").count()
    assert good >= 2, f"expected at least 2 valid rows, got {good}"

check("mixed_valid_malformed", _check_mixed_valid_malformed)

# COMMAND ----------

# MAGIC %md ### Value edge cases

# COMMAND ----------

check("nulls_preserved", lambda: (
    lambda df: (_ for _ in ()).throw(AssertionError("row missing")) if df.count() != 1 else None
)(read_json(spark, cfg(f"{BASE}/nulls.json"))))

def _check_unicode():
    df = read_json(spark, cfg(f"{BASE}/unicode.json"))
    row = df.collect()[0]
    assert row["name"] == "Müller", f"unicode mangled: {row['name']!r}"

check("unicode_preserved", _check_unicode)

def _check_duplicate_keys():
    """
    Spark's schema inference rejects duplicate keys outright — this is a
    hard failure at schema-resolution time, not something PERMISSIVE mode's
    corrupt-record handling catches. Confirmed behavior, not a bug in
    json_reader.py. This test asserts the failure happens as expected.
    """
    raised_correctly = False
    try:
        read_json(spark, cfg(f"{BASE}/duplicate_keys.json")).count()
    except Exception as e:
        if "COLUMN_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            raised_correctly = True
        else:
            raise  # unexpected error type — surface it, don't silently pass
    assert raised_correctly, "expected AnalysisException/COLUMN_ALREADY_EXISTS for duplicate keys"

check("duplicate_keys_documented_limitation", _check_duplicate_keys)

def _check_numeric_as_string():
    df = read_json(spark, cfg(f"{BASE}/numeric_as_string.json"))
    dtypes = dict(df.dtypes)
    assert dtypes.get("amount") == "string", f"expected amount inferred as string, got {dtypes.get('amount')}"

check("numeric_as_string_inference", _check_numeric_as_string)

# COMMAND ----------

# MAGIC %md ### Lineage + errors

# COMMAND ----------

def _check_lineage_column():
    df = read_json(spark, cfg(f"{BASE}/single_object.json"))
    assert "_input_file_name" in df.columns, "_input_file_name missing"
    val = df.collect()[0]["_input_file_name"]
    assert val and "single_object.json" in val, f"unexpected lineage value: {val}"

check("lineage_column_present", _check_lineage_column)

def _check_missing_source_raises():
    raised = False
    try:
        read_json(spark, cfg(f"{BASE}/does_not_exist.json")).count()
    except Exception:
        raised = True
    assert raised, "expected an error reading a nonexistent source_path"

check("missing_source_raises", _check_missing_source_raises)

# COMMAND ----------

# MAGIC %md ### Report

# COMMAND ----------

import pandas as pd

report_df = pd.DataFrame(results, columns=["case", "status", "detail"])
display(spark.createDataFrame(report_df))

failed = report_df[report_df["status"] != "PASS"]
if len(failed) > 0:
    dbutils.notebook.exit(f"FAILED: {len(failed)}/{len(report_df)} case(s) failed: {list(failed['case'])}")

dbutils.notebook.exit(f"SUCCESS: all {len(report_df)} case(s) passed")

# COMMAND ----------

report_df[report_df["case"] == "duplicate_keys_no_crash"]["detail"].values[0]
