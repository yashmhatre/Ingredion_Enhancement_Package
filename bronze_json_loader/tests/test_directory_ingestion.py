import json
import os

import pytest

from bronze_json_loader.directory_ingestion import (
    sanitize_table_name,
    build_table_name,
    list_json_files,
)


def _write(write_dir, name, content):
    with open(os.path.join(write_dir, name), "w") as f:
        f.write(content)


# ---- pure-python naming tests (no Spark) - unchanged ----

def test_sanitize_basic():
    assert sanitize_table_name("orders.json") == "orders"
    assert sanitize_table_name("Customer Orders-Jan 2026.json") == "customer_orders_jan_2026"


def test_sanitize_leading_digit_prefixed():
    assert sanitize_table_name("2026_sales.json") == "t_2026_sales"


def test_sanitize_collapses_repeats_and_strips():
    assert sanitize_table_name("--weird__name--.json") == "weird_name"


def test_sanitize_empty_raises():
    with pytest.raises(ValueError):
        sanitize_table_name("---.json")


def test_build_table_name_suffix_and_prefix():
    assert build_table_name("orders.json", "{filename}_bronze") == "orders_bronze"
    assert build_table_name("orders.json", "bronze_{filename}") == "bronze_orders"


def test_build_table_name_requires_placeholder():
    with pytest.raises(ValueError):
        build_table_name("orders.json", "no_placeholder_here")


# ---- file discovery tests (real filesystem, local or Databricks Volume) ----

def test_list_json_files_finds_only_json(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "a.json", json.dumps({"x": 1}))
    _write(write_dir, "b.JSON", json.dumps({"x": 2}))
    _write(write_dir, "notes.txt", "ignore me")
    os.makedirs(os.path.join(write_dir, "subdir"), exist_ok=True)

    files = list_json_files(spark, source_dir)
    names = sorted(f.split("/")[-1] for f in files)
    assert names == ["a.json", "b.JSON"]


def test_list_json_files_includes_jsonl(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "a.json", json.dumps({"x": 1}))
    _write(write_dir, "b.jsonl", json.dumps({"x": 2}))
    _write(write_dir, "c.JSONL", json.dumps({"x": 3}))
    _write(write_dir, "notes.txt", "ignore me")

    files = list_json_files(spark, source_dir)
    names = sorted(f.split("/")[-1] for f in files)
    assert names == ["a.json", "b.jsonl", "c.JSONL"]


def test_list_json_files_max_files(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    for i in range(5):
        _write(write_dir, f"f{i}.json", json.dumps({"i": i}))
    files = list_json_files(spark, source_dir, max_files=2)
    assert len(files) == 2


def test_list_json_files_missing_dir_raises(spark, json_test_dir):
    _, source_dir = json_test_dir
    with pytest.raises(FileNotFoundError):
        list_json_files(spark, f"{source_dir}/does_not_exist")