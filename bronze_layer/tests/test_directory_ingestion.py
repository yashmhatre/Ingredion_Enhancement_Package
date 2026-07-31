import json
import os

import pytest

from bronze_ingest.directory_ingestion import (
    build_table_name,
    list_json_files,
    sanitize_table_name,
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
    names = sorted(os.path.basename(f) for f in files)
    assert names == ["a.json", "b.JSON"]


def test_list_json_files_includes_jsonl(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "a.json", json.dumps({"x": 1}))
    _write(write_dir, "b.jsonl", json.dumps({"x": 2}))
    _write(write_dir, "c.JSONL", json.dumps({"x": 3}))
    _write(write_dir, "notes.txt", "ignore me")

    files = list_json_files(spark, source_dir)
    names = sorted(os.path.basename(f) for f in files)
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


import bronze_ingest.directory_ingestion as di
from bronze_ingest.fs import RetryState, archival


def test_move_file_relocates_to_subfolder(json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "a.json", json.dumps({"x": 1}))
    src_path = f"{source_dir}/a.json"

    dest = archival.move_file(source_dir, src_path, "processed/2026-07-24")

    assert dest == f"{source_dir}/processed/2026-07-24/a.json"
    assert os.path.exists(os.path.join(write_dir, "processed", "2026-07-24", "a.json"))
    assert not os.path.exists(os.path.join(write_dir, "a.json"))


def test_archive_ingested_file_moves_to_processed_dated_folder(json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))
    src_path = f"{source_dir}/orders.json"

    result = archival.archive_ingested_file(source_dir, src_path)

    assert result["move_status"] == "moved"
    assert "processed/" in result["move_detail"]
    assert not os.path.exists(os.path.join(write_dir, "orders.json"))


def test_archive_ingested_file_falls_back_to_quarantine_on_move_failure(json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))
    src_path = f"{source_dir}/orders.json"

    real_move_file = archival.move_file

    def flaky_move(source_dir, file_path, dest_subfolder, relative_subpath=""):
        if dest_subfolder.startswith("processed/"):
            raise OSError("simulated failure archiving to processed/")
        return real_move_file(
            source_dir, file_path, dest_subfolder, relative_subpath=relative_subpath
        )

    monkeypatch.setattr(archival, "move_file", flaky_move)

    result = archival.archive_ingested_file(source_dir, src_path)

    assert result["move_status"] == "quarantined"
    assert "quarantine_files" in result["move_detail"]
    assert not os.path.exists(os.path.join(write_dir, "orders.json"))


def test_archive_ingested_file_leaves_file_in_place_when_all_moves_fail(json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))
    src_path = f"{source_dir}/orders.json"

    def always_fails(source_dir, file_path, dest_subfolder, relative_subpath=""):
        raise OSError(f"simulated total failure for {dest_subfolder}")

    monkeypatch.setattr(archival, "move_file", always_fails)

    result = archival.archive_ingested_file(source_dir, src_path)

    assert result["move_status"] == "failed_left_in_place"
    assert os.path.exists(os.path.join(write_dir, "orders.json"))  # untouched, not lost


# ---- retry-limit before quarantine tests ----


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


def test_retry_state_persists_across_calls_and_quarantines_at_limit(
    spark, json_test_dir, monkeypatch
):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "bad.json", json.dumps({"x": 1}))
    _write(write_dir, "good.json", json.dumps({"x": 2}))

    import bronze_ingest.directory_ingestion as di
    from bronze_ingest.pipeline import BronzeIngestion

    monkeypatch.setattr(BronzeIngestion, "run", _make_failing_config_class(["bad.json"]))

    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )
    bad_result = next(r for r in results if "bad.json" in r["file"])
    actual_file_path = bad_result["file"]  # use this everywhere below
    assert bad_result["status"] == "failed"
    assert bad_result["attempts"] == 1
    assert "move_status" not in bad_result

    state = RetryState.load(source_dir).as_dict()
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

    state = RetryState.load(source_dir).as_dict()
    assert actual_file_path not in state


def test_retry_state_cleared_on_eventual_success(spark, json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "flaky.json", json.dumps({"x": 1}))

    import bronze_ingest.directory_ingestion as di
    from bronze_ingest.pipeline import BronzeIngestion

    monkeypatch.setattr(BronzeIngestion, "run", _make_failing_config_class(["flaky.json"]))
    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )
    flaky_result = next(r for r in results if "flaky.json" in r["file"])
    assert flaky_result["attempts"] == 1
    actual_file_path = flaky_result["file"]  # use the real path, don't reconstruct it

    state = RetryState.load(source_dir).as_dict()
    assert state.get(actual_file_path) == 1

    monkeypatch.setattr(BronzeIngestion, "run", _make_failing_config_class([]))
    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )
    good_result = next(r for r in results if "flaky.json" in r["file"])
    assert good_result["status"] == "success"

    state = RetryState.load(source_dir).as_dict()
    assert actual_file_path not in state


def test_retry_state_file_never_treated_as_data(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "a.json", json.dumps({"x": 1}))

    seeded = RetryState.load(source_dir)
    seeded.increment("some/file.json")
    seeded.flush()

    files = di.list_json_files(spark, source_dir)
    names = [os.path.basename(f) for f in files]
    assert "retry_state.json" not in names
    assert names == ["a.json"]


# ---- folder-as-table tests ----


def test_folder_as_table_merges_files_into_one_table(spark, json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    os.makedirs(os.path.join(write_dir, "orders"), exist_ok=True)
    _write(write_dir, "orders/order1.json", json.dumps({"id": 1}))
    _write(write_dir, "orders/order2.json", json.dumps({"id": 2}))

    from bronze_ingest.pipeline import BronzeIngestion

    def fake_run_on_dataframe(self, df):
        return {
            "table": self.config.full_table_name,
            "row_count": df.count(),
            "quarantined_row_count": 0,
        }

    monkeypatch.setattr(BronzeIngestion, "run_on_dataframe", fake_run_on_dataframe)

    results = di.ingest_directory_to_bronze(spark, source_dir, catalog=None, schema_name="default")

    folder_result = next(r for r in results if r["table"].endswith("orders_bronze"))
    assert folder_result["status"] == "success"
    assert folder_result["rows"] == 2
    assert len(folder_result["file_results"]) == 2
    assert all(fr["status"] == "success" for fr in folder_result["file_results"])


def test_folder_with_no_json_is_skipped_not_failed(spark, json_test_dir):
    """A folder with nothing to ingest is not an error - there is no bad
    data and nothing for a human to fix. Reporting it as "failed" made the
    job task exit non-zero and fire alerting for a non-event, and buried
    genuine failures in the same run."""
    write_dir, source_dir = json_test_dir
    os.makedirs(os.path.join(write_dir, "multi_file"), exist_ok=True)
    _write(write_dir, "multi_file/readme.txt", "not json")

    results = di.ingest_directory_to_bronze(spark, source_dir, catalog=None, schema_name="default")

    assert len(results) == 1
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "no JSON files in folder"
    assert "error" not in results[0]
    # The job task keys off "failed" specifically - a skip must not appear there.
    assert [r for r in results if r["status"] == "failed"] == []


def test_folder_with_no_json_does_not_mask_a_real_failure(spark, json_test_dir, monkeypatch):
    """An empty folder alongside a genuinely failing one: the skip is
    reported separately and the real failure still surfaces."""
    write_dir, source_dir = json_test_dir
    os.makedirs(os.path.join(write_dir, "empty_folder"), exist_ok=True)
    _write(write_dir, "empty_folder/notes.txt", "not json")
    os.makedirs(os.path.join(write_dir, "orders"), exist_ok=True)
    _write(write_dir, "orders/order1.json", json.dumps({"id": 1}))

    from bronze_ingest.pipeline import BronzeIngestion

    def boom(self, df):
        raise RuntimeError("write blew up")

    monkeypatch.setattr(BronzeIngestion, "run_on_dataframe", boom)

    results = di.ingest_directory_to_bronze(spark, source_dir, catalog=None, schema_name="default")

    by_status = {r["status"] for r in results}
    assert by_status == {"skipped", "failed"}
    assert len(results) == 2


def test_folder_as_table_one_bad_file_does_not_block_the_rest(spark, json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    os.makedirs(os.path.join(write_dir, "orders"), exist_ok=True)
    _write(write_dir, "orders/good1.json", json.dumps({"id": 1}))
    _write(write_dir, "orders/good2.json", json.dumps({"id": 2}))

    from bronze_ingest import json_reader as jr
    from bronze_ingest.pipeline import BronzeIngestion

    real_read_json = jr.read_json

    def flaky_read_json(spark, config):
        if "good2" in config.source_path:
            raise ValueError("simulated bad file")
        return real_read_json(spark, config)

    monkeypatch.setattr(di, "read_json", flaky_read_json)

    def fake_run_on_dataframe(self, df):
        return {
            "table": self.config.full_table_name,
            "row_count": df.count(),
            "quarantined_row_count": 0,
        }

    monkeypatch.setattr(BronzeIngestion, "run_on_dataframe", fake_run_on_dataframe)

    results = di.ingest_directory_to_bronze(
        spark, source_dir, max_ingestion_retries=3, catalog=None, schema_name="default"
    )

    folder_result = next(r for r in results if r["table"].endswith("orders_bronze"))
    assert folder_result["status"] == "success"
    assert folder_result["rows"] == 1  # only good1.json contributed
    good_result = next(fr for fr in folder_result["file_results"] if "good1" in fr["file"])
    bad_result = next(fr for fr in folder_result["file_results"] if "good2" in fr["file"])
    assert good_result["status"] == "success"
    assert bad_result["status"] == "failed"
    assert bad_result["attempts"] == 1


def test_folder_as_table_archives_files_with_folder_name_preserved(
    spark, json_test_dir, monkeypatch
):
    write_dir, source_dir = json_test_dir
    os.makedirs(os.path.join(write_dir, "orders"), exist_ok=True)
    _write(write_dir, "orders/order1.json", json.dumps({"id": 1}))

    from bronze_ingest.pipeline import BronzeIngestion

    def fake_run_on_dataframe(self, df):
        return {
            "table": self.config.full_table_name,
            "row_count": df.count(),
            "quarantined_row_count": 0,
        }

    monkeypatch.setattr(BronzeIngestion, "run_on_dataframe", fake_run_on_dataframe)

    di.ingest_directory_to_bronze(spark, source_dir, catalog=None, schema_name="default")

    # file should be archived under processed/{date}/orders/order1.json,
    # not processed/{date}/order1.json - folder context preserved
    today = (
        __import__("datetime")
        .datetime.now(__import__("datetime").timezone.utc)
        .strftime("%Y-%m-%d")
    )
    expected_path = os.path.join(write_dir, "processed", today, "orders", "order1.json")
    assert os.path.exists(expected_path)
    assert not os.path.exists(os.path.join(write_dir, "orders", "order1.json"))


# ---- parallel archival tests ----


def test_archive_files_parallel_preserves_input_order(json_test_dir):
    write_dir, source_dir = json_test_dir
    names = [f"file_{i}.json" for i in range(5)]
    for n in names:
        _write(write_dir, n, json.dumps({"x": 1}))
    paths = [f"{source_dir}/{n}" for n in names]

    results = archival.archive_files_parallel(source_dir, paths)

    # Order must match input despite concurrent execution - this is what
    # keeps per-file error attribution correct downstream.
    assert [fp for fp, _ in results] == paths
    assert all(r["move_status"] == "moved" for _, r in results)


def test_archive_files_parallel_empty_list(json_test_dir):
    _, source_dir = json_test_dir
    assert archival.archive_files_parallel(source_dir, []) == []


def test_archive_files_parallel_attributes_failures_correctly(json_test_dir, monkeypatch):
    write_dir, source_dir = json_test_dir
    names = [f"file_{i}.json" for i in range(4)]
    for n in names:
        _write(write_dir, n, json.dumps({"x": 1}))
    paths = [f"{source_dir}/{n}" for n in names]

    real_move = archival.move_file

    def selective_fail(source_dir, file_path, dest_subfolder, relative_subpath=""):
        if "file_2.json" in file_path:
            raise OSError("simulated move failure")
        return real_move(source_dir, file_path, dest_subfolder, relative_subpath=relative_subpath)

    monkeypatch.setattr(archival, "move_file", selective_fail)

    results = archival.archive_files_parallel(source_dir, paths)
    by_path = dict(results)

    # The failure must land on file_2 specifically, not bleed onto a
    # neighbour - the real risk when results come back out of order.
    assert by_path[f"{source_dir}/file_2.json"]["move_status"] == "failed_left_in_place"
    for i in (0, 1, 3):
        assert by_path[f"{source_dir}/file_{i}.json"]["move_status"] == "moved"


def test_archive_files_parallel_preserves_folder_subpath(json_test_dir):
    write_dir, source_dir = json_test_dir
    os.makedirs(os.path.join(write_dir, "orders"), exist_ok=True)
    _write(write_dir, "orders/a.json", json.dumps({"x": 1}))
    _write(write_dir, "orders/b.json", json.dumps({"x": 2}))
    paths = [f"{source_dir}/orders/a.json", f"{source_dir}/orders/b.json"]

    results = archival.archive_files_parallel(source_dir, paths, relative_subpath="orders")

    for _, r in results:
        assert "/orders/" in r["move_detail"]


def test_directory_ingestion_rejects_overwrite_by_default(spark, json_test_dir):
    """
    #55: write_mode='overwrite' silently loses history in directory
    ingestion - each freshly-discovered, same-named file replaces the
    whole table on every run since prior files are archived out of
    source_dir. Must fail fast before touching any file, for both
    per-file and folder-as-table modes.
    """
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))

    with pytest.raises(ValueError, match="write_mode='overwrite'"):
        di.ingest_directory_to_bronze(
            spark,
            source_dir,
            catalog=None,
            schema_name="default",
            write_mode="overwrite",
        )

    # Nothing should have been touched - the file must still be sitting
    # untouched in source_dir, not archived/moved.
    assert os.path.exists(os.path.join(write_dir, "orders.json"))


def test_directory_ingestion_rejects_overwrite_for_folder_as_table(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    os.makedirs(os.path.join(write_dir, "orders"), exist_ok=True)
    _write(write_dir, "orders/a.json", json.dumps({"id": 1}))

    with pytest.raises(ValueError, match="write_mode='overwrite'"):
        di.ingest_directory_to_bronze(
            spark,
            source_dir,
            catalog=None,
            schema_name="default",
            write_mode="overwrite",
        )


def test_directory_ingestion_allows_overwrite_with_explicit_opt_in(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))

    results = di.ingest_directory_to_bronze(
        spark,
        source_dir,
        catalog=None,
        schema_name="default",
        write_mode="overwrite",
        allow_overwrite_in_directory_mode=True,
    )

    assert results[0]["status"] == "success"


def test_directory_ingestion_default_write_mode_is_unaffected(spark, json_test_dir):
    """Sanity check the guard only triggers on an explicit write_mode='overwrite' -
    the default (append) must keep working with no extra flag."""
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))

    results = di.ingest_directory_to_bronze(spark, source_dir, catalog=None, schema_name="default")

    assert results[0]["status"] == "success"


# ---- per_file_config (#145) ----


def test_per_file_config_is_applied_to_the_named_file(spark, json_test_dir, monkeypatch):
    """The deployed job sets a per-file required_columns rule. Before this was
    a real parameter it was absorbed by **config_overrides and dropped by
    IngestionConfig.from_dict's unknown-key filter - the rule never ran and
    the run reported success."""
    write_dir, source_dir = json_test_dir
    _write(write_dir, "strict.json", json.dumps({"id": 1}))  # missing order_id
    _write(write_dir, "loose.json", json.dumps({"id": 2}))

    seen = {}

    from bronze_ingest.pipeline import BronzeIngestion

    def fake_run(self):
        seen[self.config.table] = list(self.config.required_columns)
        return {"table": self.config.full_table_name, "row_count": 1, "quarantined_row_count": 0}

    monkeypatch.setattr(BronzeIngestion, "run", fake_run)

    di.ingest_directory_to_bronze(
        spark,
        source_dir,
        catalog=None,
        schema_name="default",
        per_file_config={"strict.json": {"required_columns": ["order_id"]}},
    )

    assert seen["strict_bronze"] == ["order_id"]  # override applied
    assert seen["loose_bronze"] == []  # others untouched


def test_unknown_config_override_raises_instead_of_being_dropped(spark, json_test_dir):
    """The root cause of #145: unknown keys vanished silently. A misspelled
    or unsupported field must fail loudly, not produce a successful-looking
    run with the setting ignored."""
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))

    with pytest.raises(ValueError, match="Unknown IngestionConfig field"):
        di.ingest_directory_to_bronze(
            spark,
            source_dir,
            catalog=None,
            schema_name="default",
            not_a_real_field=123,
        )


def test_per_file_config_rejects_unknown_fields(spark, json_test_dir):
    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))

    with pytest.raises(ValueError, match="unknown IngestionConfig field"):
        di.ingest_directory_to_bronze(
            spark,
            source_dir,
            catalog=None,
            schema_name="default",
            per_file_config={"orders.json": {"nope": 1}},
        )


def test_per_file_config_warns_when_it_matches_nothing(spark, json_test_dir, monkeypatch, caplog):
    """An override naming a file that was never discovered is a configured
    rule that will never run - the same silent-no-op #145 was about."""
    import logging

    write_dir, source_dir = json_test_dir
    _write(write_dir, "orders.json", json.dumps({"id": 1}))

    from bronze_ingest.pipeline import BronzeIngestion

    monkeypatch.setattr(
        BronzeIngestion,
        "run",
        lambda self: {
            "table": self.config.full_table_name,
            "row_count": 1,
            "quarantined_row_count": 0,
        },
    )

    with caplog.at_level(logging.WARNING):
        di.ingest_directory_to_bronze(
            spark,
            source_dir,
            catalog=None,
            schema_name="default",
            per_file_config={"typo_in_name.json": {"required_columns": ["x"]}},
        )

    assert "matched no discovered file" in caplog.text
    assert "typo_in_name.json" in caplog.text
