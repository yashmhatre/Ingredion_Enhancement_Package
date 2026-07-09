import json
import os

import pytest

from bronze_json_loader.directory_ingestion import (
    sanitize_table_name,
    build_table_name,
    list_json_files,
)


# ---- pure-python naming tests (no Spark) ----

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


# ---- file discovery test (local Spark, local filesystem) ----

def test_list_json_files_finds_only_json(spark, tmp_path):
    (tmp_path / "a.json").write_text(json.dumps({"x": 1}))
    (tmp_path / "b.JSON").write_text(json.dumps({"x": 2}))
    (tmp_path / "notes.txt").write_text("ignore me")
    os.makedirs(tmp_path / "subdir", exist_ok=True)

    files = list_json_files(spark, f"file://{tmp_path}")
    names = [f.split("/")[-1] for f in files]
    assert names == ["a.json", "b.JSON"]


def test_list_json_files_max_files(spark, tmp_path):
    for i in range(5):
        (tmp_path / f"f{i}.json").write_text(json.dumps({"i": i}))
    files = list_json_files(spark, f"file://{tmp_path}", max_files=2)
    assert len(files) == 2


def test_list_json_files_missing_dir_raises(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        list_json_files(spark, f"file://{tmp_path}/does_not_exist")
