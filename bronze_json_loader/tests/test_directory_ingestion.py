import json
import os
import pytest
from bronze_json_loader.directory_ingestion import (
    sanitize_table_name,
    build_table_name,
    list_json_files,
)
from bronze_json_loader.directory_ingestion import (
    _move_file,
    _archive_ingested_file,
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

# ---- file archival tests (real filesystem, local or Databricks Volume) ----


import bronze_json_loader.directory_ingestion as di


def test_move_file_relocates_to_subfolder(json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "a.json", json.dumps({"x": 1}))
    src_path = f"{source_dir}/a.json"

    dest = di._move_file(source_dir, src_path, "processed/2026-07-24")

    assert dest == f"{source_dir}/processed/2026-07-24/a.json"
    assert os.path.exists(os.path.join(write_dir, "processed", "2026-07-24", "a.json"))
    assert not os.path.exists(os.path.join(write_dir, "a.json"))


def test_archive_ingested_file_moves_to_processed_dated_folder(json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))
    src_path = f"{source_dir}/orders.json"

    result = di._archive_ingested_file(source_dir, src_path)

    assert result["move_status"] == "moved"
    assert "processed/" in result["move_detail"]
    assert not os.path.exists(os.path.join(write_dir, "orders.json"))


def test_archive_ingested_file_falls_back_to_quarantine_on_move_failure(json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))
    src_path = f"{source_dir}/orders.json"

    real_move_file = di._move_file

    def flaky_move(source_dir, file_path, dest_subfolder):
        if dest_subfolder.startswith("processed/"):
            raise OSError("simulated failure archiving to processed/")
        return real_move_file(source_dir, file_path, dest_subfolder)

    monkeypatch.setattr(di, "_move_file", flaky_move)

    result = di._archive_ingested_file(source_dir, src_path)

    assert result["move_status"] == "quarantined"
    assert "quarantine_files" in result["move_detail"]
    assert not os.path.exists(os.path.join(write_dir, "orders.json"))


def test_archive_ingested_file_leaves_file_in_place_when_all_moves_fail(json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))
    src_path = f"{source_dir}/orders.json"

    def always_fails(source_dir, file_path, dest_subfolder):
        raise OSError(f"simulated total failure for {dest_subfolder}")

    monkeypatch.setattr(di, "_move_file", always_fails)

    result = di._archive_ingested_file(source_dir, src_path)

    assert result["move_status"] == "failed_left_in_place"
    assert os.path.exists(os.path.join(write_dir, "orders.json"))  # untouched, not lost

# ---- retry-limit before quarantine tests ----

from bronze_json_loader.directory_ingestion import ingest_directory_to_bronze


def _make_failing_config_class(fail_on_filenames):
    """Returns a fake BronzeIngestion-like run() that raises for specific
    filenames and succeeds otherwise - lets us simulate repeated ingestion
    failures across multiple calls without needing real bad JSON content
    (which would also need real Spark schema behavior to fail correctly)."""
    def fake_run(self):
        if any(name in self.config.source_path for name in fail_on_filenames):
            raise ValueError("simulated ingestion failure")
        return {"table": self.config.full_table_name, "row_count": 1, "quarantined_row_count": 0}
    return fake_run


def test_retry_state_persists_across_calls_and_quarantines_at_limit(spark, json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "bad.json", json.dumps({"x": 1}))
    _write(write_dir, "good.json", json.dumps({"x": 2}))

    import bronze_json_loader.directory_ingestion as di
    from bronze_json_loader.pipeline import BronzeIngestion

    monkeypatch.setattr(BronzeIngestion, "run", _make_failing_config_class(["bad.json"]))

    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )
    bad_result = next(r for r in results if "bad.json" in r["file"])
    actual_file_path = bad_result["file"]  # use this everywhere below
    assert bad_result["status"] == "failed"
    assert bad_result["attempts"] == 1
    assert "move_status" not in bad_result

    state = di._read_retry_state(source_dir)
    assert state.get(actual_file_path) == 1

    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )
    bad_result = next(r for r in results if "bad.json" in r["file"])
    assert bad_result["attempts"] == 2
    assert "move_status" not in bad_result

    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )
    bad_result = next(r for r in results if "bad.json" in r["file"])
    assert bad_result["attempts"] == 3
    assert bad_result["move_status"] == "quarantined"
    assert not os.path.exists(os.path.join(write_dir, "bad.json"))

    state = di._read_retry_state(source_dir)
    assert actual_file_path not in state


def test_retry_state_cleared_on_eventual_success(spark, json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "flaky.json", json.dumps({"x": 1}))

    import bronze_json_loader.directory_ingestion as di
    from bronze_json_loader.pipeline import BronzeIngestion

    monkeypatch.setattr(BronzeIngestion, "run", _make_failing_config_class(["flaky.json"]))
    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )
    flaky_result = next(r for r in results if "flaky.json" in r["file"])
    assert flaky_result["attempts"] == 1
    actual_file_path = flaky_result["file"]  # use the real path, don't reconstruct it

    state = di._read_retry_state(source_dir)
    assert state.get(actual_file_path) == 1

    monkeypatch.setattr(BronzeIngestion, "run", _make_failing_config_class([]))
    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )
    good_result = next(r for r in results if "flaky.json" in r["file"])
    assert good_result["status"] == "success"

    state = di._read_retry_state(source_dir)
    assert actual_file_path not in state


def test_retry_state_file_never_treated_as_data(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "a.json", json.dumps({"x": 1}))

    di_module = __import__("bronze_json_loader.directory_ingestion", fromlist=["_write_retry_state"])
    di_module._write_retry_state(source_dir, {"some/file.json": 1})

    files = di_module.list_json_files(spark, source_dir)
    names = [f.split("/")[-1] for f in files]
    assert "retry_state.json" not in names
    assert names == ["a.json"]