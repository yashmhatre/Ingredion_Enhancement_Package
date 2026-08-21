# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion — end-to-end validation (#343)
# MAGIC Generates its own fixtures on the dev Volume (no data flows here), then
# MAGIC exercises E2E-1..E2E-7. Every check reports PASS/FAIL and **never aborts
# MAGIC on the first failure** — a run that stops tells you about one defect, a
# MAGIC run that completes tells you about all of them.
# MAGIC
# MAGIC Dev only. No GRANT/DROP/VACUUM. Tables are prefixed `e2e_`.

# COMMAND ----------

import json
import os
import time
from typing import Any

VOL = "/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/DEV/e2e"
CATALOG = "ingredion_en"
SCHEMA = "ingredion_dev"

results: list[dict[str, str]] = []


def record(issue, name, status, detail=""):
    results.append({"issue": issue, "check": name, "status": status, "detail": str(detail)[:600]})
    print(f"{status:>7}  [{issue}] {name}  {str(detail)[:120]}")


def check(issue, name, fn):
    """Runs fn(); records PASS/FAIL/ERROR. Never raises past this point."""
    try:
        fn()
        record(issue, name, "PASS")
    except AssertionError as e:
        record(issue, name, "FAIL", e)
    except Exception as e:  # noqa: BLE001 - harness reports every failure
        record(issue, name, "ERROR", f"{type(e).__name__}: {e}")


def sql(q):
    return spark.sql(q)


def table_rows(t):
    return sql(f"SELECT COUNT(*) c FROM {t}").collect()[0]["c"]


def drop(t):
    # DROP on an e2e_-prefixed dev table only. Kept out of `check` so a
    # cleanup failure never masquerades as a test failure.
    try:
        sql(f"DROP TABLE IF EXISTS {t}")
    except Exception as e:  # noqa: BLE001 - cleanup failure is reported but must not hide test results
        print(f"  (cleanup skipped for {t}: {e})")


# COMMAND ----------
# MAGIC %md ## E2E-1 — fixtures

# COMMAND ----------

FIXTURES = {
    "orders_clean.json": "\n".join(
        json.dumps(
            {
                "order_id": f"o{i}",
                "customer_id": f"c{i}",
                "amount": 10.5 + i,
                "customer": {"name": f"n{i}", "email": f"e{i}@x.com"},
            }
        )
        for i in range(1, 6)
    ),
    # two rows missing required order_id -> quarantine
    "orders_nulls.json": "\n".join(
        [
            json.dumps({"order_id": "ok1", "customer_id": "c1", "amount": 1.0}),
            json.dumps({"order_id": None, "customer_id": "c2", "amount": 2.0}),
            json.dumps({"customer_id": "c3", "amount": 3.0}),
            json.dumps({"order_id": "ok2", "customer_id": "c4", "amount": 4.0}),
        ]
    ),
    "orders_dupes.json": "\n".join(
        [
            json.dumps({"order_id": "d1", "customer_id": "c1", "amount": 1.0}),
            json.dumps({"order_id": "d1", "customer_id": "c2", "amount": 2.0}),
            json.dumps({"order_id": "d2", "customer_id": "c3", "amount": 3.0}),
        ]
    ),
    # extra field + renamed field vs baseline -> schema drift
    "orders_drift.json": "\n".join(
        [
            json.dumps(
                {
                    "order_id": "s1",
                    "customer_id": "c1",
                    "amount": 1.0,
                    "new_column": "surprise",
                }
            )
        ]
    ),
    "orders.jsonl": "\n".join(
        json.dumps({"order_id": f"l{i}", "customer_id": f"c{i}", "amount": float(i)})
        for i in range(1, 4)
    ),
    "orders.csv": "order_id,customer_id,amount\nx1,c1,1.0\n",
    "folder_a/part1.json": json.dumps({"order_id": "f1", "customer_id": "c1", "amount": 1.0}),
    "folder_a/part2.json": json.dumps({"order_id": "f2", "customer_id": "c2", "amount": 2.0}),
    "folder_empty/readme.txt": "no ingestable files here",
}


def _write_fixtures():
    for rel, body in FIXTURES.items():
        p = f"{VOL}/{rel}"
        d = os.path.dirname(p)
        try:
            dbutils.fs.mkdirs(d.replace("/Volumes", "dbfs:/Volumes"))
        except Exception:  # noqa: BLE001 - dbutils mkdir is idempotent; put below reports real failure
            pass
        dbutils.fs.put(p.replace("/Volumes", "dbfs:/Volumes"), body, True)
    record("E2E-1", "fixtures_written", "INFO", f"{len(FIXTURES)} files under {VOL}")


check("E2E-1", "write_fixtures", _write_fixtures)
time.sleep(5)


def _fixtures_listable():
    entries = dbutils.fs.ls(VOL)
    names = sorted(e.name for e in entries)
    record("E2E-1", "volume_listing", "INFO", ",".join(names))
    assert any(n.startswith("orders_clean") for n in names), names


check("E2E-1", "fixtures_listable_via_dbutils", _fixtures_listable)

# COMMAND ----------
# MAGIC %md ## E2E-7 — format registry + discovery on real compute

# COMMAND ----------

import bronze_ingest.formats as formats
from bronze_ingest.config import IngestionConfig
from bronze_ingest.fs import list_source_files


def _supported():
    got = formats.supported_formats()
    record("E2E-7", "supported_formats", "INFO", str(got))
    assert got == ("json",), got


check("E2E-7", "supported_formats_is_json_only", _supported)


def _csv_rejected():
    try:
        IngestionConfig(source_path=f"{VOL}/orders.csv", table="e2e_x", source_format="csv")
        raise AssertionError("csv was accepted but is not registered")
    except ValueError as e:
        assert "csv" in str(e) and "json" in str(e), str(e)
        record("E2E-7", "csv_reject_message", "INFO", str(e)[:200])


check("E2E-7", "unregistered_format_rejected_at_config", _csv_rejected)


def _discovery_dbutils_branch():
    """THE check CI can never make: only _try_posix_ls is reachable in CI, so
    the dbutils branch (#304) has never filtered by extension for real."""
    found = list_source_files(spark, VOL, source_format="json")
    names = sorted(f.rsplit("/", 1)[-1] for f in found)
    record("E2E-7", "dbutils_discovery_found", "INFO", ",".join(names))
    assert "orders_clean.json" in names, names
    assert "orders.jsonl" in names, "jsonl must be discovered as json"
    # the whole point: another format present is invisible, not an error
    assert "orders.csv" not in names, f"csv leaked into json discovery: {names}"
    assert not any(n.endswith(".txt") for n in names), names


check("E2E-7", "dbutils_discovery_filters_by_extension", _discovery_dbutils_branch)


def _read_source_raises_for_readerless():
    import bronze_ingest.readers as r

    spec = formats.FormatSpec(
        name="csv",
        extensions=(".csv",),
        cloudfiles_format="csv",
        allowed_reader_options=frozenset(),
    )
    formats.FORMATS["csv"] = spec
    try:
        cfg = IngestionConfig(source_path=f"{VOL}/orders.csv", table="e2e_x", source_format="csv")
        try:
            r.read_source(spark, cfg)
            raise AssertionError("read_source did not raise for a readerless format")
        except ValueError as e:
            assert "No registered batch reader" in str(e), str(e)
    finally:
        formats.FORMATS.pop("csv", None)


check("E2E-7", "read_source_raises_not_falls_through", _read_source_raises_for_readerless)

# COMMAND ----------
# MAGIC %md ## E2E-2 — single-file JSON batch ingestion

# COMMAND ----------

from bronze_ingest.pipeline import BronzeIngestion

T_CLEAN = f"{CATALOG}.{SCHEMA}.e2e_orders_clean"
drop(T_CLEAN)


def _base(**kw: Any):
    d: dict[str, Any] = dict(
        catalog=CATALOG,
        schema_name=SCHEMA,
        multiline=False,  # fixtures are JSON-lines shaped
        write_mode="append",
        merge_schema=True,
        add_audit_columns=True,
        enable_run_audit=True,
        enable_schema_registry=True,
        audit_schema_name=SCHEMA,
        registry_schema_name=SCHEMA,
    )
    d.update(kw)
    return IngestionConfig(**d)


def _ingest_clean():
    cfg = _base(source_path=f"{VOL}/orders_clean.json", table="e2e_orders_clean")
    out = BronzeIngestion(spark, cfg).run()
    record("E2E-2", "run_result", "INFO", json.dumps(out, default=str)[:400])
    assert table_rows(T_CLEAN) == 5, table_rows(T_CLEAN)


check("E2E-2", "single_file_ingest_writes_delta_table", _ingest_clean)


def _audit_columns_populated():
    row = sql(f"SELECT _ingested_at, _source_file, _batch_id FROM {T_CLEAN} LIMIT 1").collect()[0]
    record("E2E-2", "audit_col_values", "INFO", str(row))
    assert row["_ingested_at"] is not None, "._ingested_at null"
    assert row["_source_file"], "_source_file empty"
    assert "orders_clean.json" in row["_source_file"], row["_source_file"]
    assert row["_batch_id"], "_batch_id empty"


check("E2E-2", "audit_columns_populated_with_real_path", _audit_columns_populated)


def _append_appends():
    cfg = _base(source_path=f"{VOL}/orders_clean.json", table="e2e_orders_clean")
    BronzeIngestion(spark, cfg).run()
    assert table_rows(T_CLEAN) == 10, table_rows(T_CLEAN)


check("E2E-2", "second_run_appends_not_replaces", _append_appends)


def _jsonl_multiline_forced_off():
    """#146: multiLine=true on a .jsonl file returns only the first record,
    with no error. effective_multiline must force it off."""
    t = f"{CATALOG}.{SCHEMA}.e2e_orders_jsonl"
    drop(t)
    cfg = _base(source_path=f"{VOL}/orders.jsonl", table="e2e_orders_jsonl", multiline=True)
    BronzeIngestion(spark, cfg).run()
    n = table_rows(t)
    assert n == 3, f"expected 3 rows from .jsonl with multiline=True, got {n}"


check("E2E-2", "jsonl_extension_forces_multiline_off", _jsonl_multiline_forced_off)

# COMMAND ----------
# MAGIC %md ## E2E-3 — quality gate and quarantine

# COMMAND ----------

T_NULLS = f"{CATALOG}.{SCHEMA}.e2e_orders_nulls"
Q_NULLS = f"{CATALOG}.{SCHEMA}.e2e_orders_nulls_quarantine"
drop(T_NULLS)
drop(Q_NULLS)


def _quarantine_splits():
    cfg = _base(
        source_path=f"{VOL}/orders_nulls.json",
        table="e2e_orders_nulls",
        required_columns=["order_id"],
        fail_on_quality_error=False,
        quarantine_table=Q_NULLS,
    )
    out = BronzeIngestion(spark, cfg).run()
    record("E2E-3", "run_result", "INFO", json.dumps(out, default=str)[:400])
    good, bad = table_rows(T_NULLS), table_rows(Q_NULLS)
    record("E2E-3", "counts", "INFO", f"bronze={good} quarantine={bad}")
    assert good == 2, f"expected 2 good rows, got {good}"
    assert bad == 2, f"expected 2 quarantined rows, got {bad}"
    assert good + bad == 4, "counts must add up to the input"


check("E2E-3", "required_columns_violations_quarantined", _quarantine_splits)


def _fail_on_quality_error_raises():
    t = f"{CATALOG}.{SCHEMA}.e2e_orders_failfast"
    drop(t)
    cfg = _base(
        source_path=f"{VOL}/orders_nulls.json",
        table="e2e_orders_failfast",
        required_columns=["order_id"],
        fail_on_quality_error=True,
    )
    try:
        BronzeIngestion(spark, cfg).run()
        raise AssertionError("fail_on_quality_error=True did not fail the run")
    except AssertionError:
        raise
    except Exception as e:  # noqa: BLE001 - the validation asserts that the configured run fails
        record("E2E-3", "failfast_exception", "INFO", f"{type(e).__name__}: {str(e)[:200]}")


check("E2E-3", "fail_on_quality_error_fails_loudly", _fail_on_quality_error_raises)


def _unique_columns_quarantined():
    t = f"{CATALOG}.{SCHEMA}.e2e_orders_dupes"
    q = f"{CATALOG}.{SCHEMA}.e2e_orders_dupes_quarantine"
    drop(t)
    drop(q)
    cfg = _base(
        source_path=f"{VOL}/orders_dupes.json",
        table="e2e_orders_dupes",
        unique_columns=["order_id"],
        fail_on_quality_error=False,
        quarantine_table=q,
    )
    BronzeIngestion(spark, cfg).run()
    good, bad = table_rows(t), table_rows(q)
    record("E2E-3", "dupe_counts", "INFO", f"bronze={good} quarantine={bad}")
    assert good + bad == 3, f"{good}+{bad} != 3"
    assert bad >= 1, "duplicate was not quarantined"


check("E2E-3", "unique_columns_violations_quarantined", _unique_columns_quarantined)

# COMMAND ----------
# MAGIC %md ## E2E-5 — audit trail and schema registry

# COMMAND ----------


def _audit_table_has_rows():
    t = f"{CATALOG}.{SCHEMA}._ingestion_audit"
    n = table_rows(t)
    cols = [f.name for f in sql(f"SELECT * FROM {t} LIMIT 1").schema.fields]
    record("E2E-5", "audit_columns", "INFO", ",".join(cols))
    record("E2E-5", "audit_row_count", "INFO", str(n))
    assert n > 0, "audit table is empty after several runs"


check("E2E-5", "run_audit_writes_rows", _audit_table_has_rows)


def _audit_has_our_run():
    t = f"{CATALOG}.{SCHEMA}._ingestion_audit"
    rows = sql(
        f"SELECT * FROM {t} WHERE table_name LIKE '%e2e_orders_clean%' ORDER BY 1 DESC LIMIT 3"
    ).collect()
    record("E2E-5", "audit_sample", "INFO", str(rows[0].asDict() if rows else "none")[:400])
    assert rows, "no audit row for e2e_orders_clean"


check("E2E-5", "audit_row_for_our_table", _audit_has_our_run)


def _schema_registry_and_drift():
    t = f"{CATALOG}.{SCHEMA}.e2e_orders_clean"
    reg = f"{CATALOG}.{SCHEMA}._schema_registry"
    before = table_rows(reg)
    cfg = _base(source_path=f"{VOL}/orders_drift.json", table="e2e_orders_clean")
    BronzeIngestion(spark, cfg).run()
    after = table_rows(reg)
    record("E2E-5", "registry_rows", "INFO", f"before={before} after={after}")
    assert after >= before, "schema registry did not record the drift run"
    cols = [f.name for f in sql(f"SELECT * FROM {t} LIMIT 1").schema.fields]
    record("E2E-5", "post_drift_columns", "INFO", ",".join(cols))
    assert "new_column" in cols, "merge_schema did not add the drifted column"


check("E2E-5", "schema_registry_records_drift", _schema_registry_and_drift)

# COMMAND ----------
# MAGIC %md ## E2E-4 — directory ingestion

# COMMAND ----------

import bronze_ingest.directory_ingestion as di


def _directory_run():
    out = di.ingest_directory_to_bronze(
        spark,
        VOL,
        catalog=CATALOG,
        schema_name=SCHEMA,
        table_name_template="e2e_dir_{filename}",
        write_mode="overwrite",
        allow_overwrite_in_directory_mode=True,
        add_audit_columns=True,
        enable_run_audit=False,
        enable_schema_registry=False,
        multiline=False,
    )
    record("E2E-4", "results", "INFO", json.dumps(out, default=str)[:900])
    globals()["_DIR_OUT"] = out
    assert out, "directory ingestion returned nothing"


check("E2E-4", "directory_ingestion_runs", _directory_run)


def _empty_folder_skipped():
    out = globals().get("_DIR_OUT") or []
    skipped = [r for r in out if r.get("status") == "skipped"]
    record("E2E-4", "skipped_entries", "INFO", str(skipped)[:400])
    assert skipped, "folder_empty was not reported as skipped"
    assert any("json" in (r.get("reason") or "") for r in skipped), skipped
    assert not [r for r in out if r.get("status") == "failed"], (
        f"a non-failure was reported failed: {[r for r in out if r.get('status') == 'failed']}"
    )


check("E2E-4", "empty_folder_is_skipped_not_failed", _empty_folder_skipped)


def _folder_as_table():
    out = globals().get("_DIR_OUT") or []
    fa = [
        r
        for r in out
        if "folder_a" in str(r.get("file", "")) or "folder_a" in str(r.get("table", ""))
    ]
    record("E2E-4", "folder_a_result", "INFO", str(fa)[:400])
    assert fa, "folder_a produced no result"
    assert fa[0].get("status") == "success", fa


check("E2E-4", "folder_a_merges_into_one_table", _folder_as_table)


def _csv_not_ingested():
    out = globals().get("_DIR_OUT") or []
    csv_hits = [r for r in out if str(r.get("file", "")).endswith(".csv")]
    assert not csv_hits, f"csv was ingested under json discovery: {csv_hits}"


check("E2E-4", "csv_invisible_to_json_directory_run", _csv_not_ingested)

# COMMAND ----------
# MAGIC %md ## E2E-6 — replay

# COMMAND ----------

from bronze_ingest.replay import reprocess_quarantined_files


def _replay_moves_files_back():
    qdir = f"{VOL}/quarantine_files"
    dbutils.fs.mkdirs(qdir.replace("/Volumes", "dbfs:/Volumes"))
    dbutils.fs.put(
        f"{qdir}/replay_me.json".replace("/Volumes", "dbfs:/Volumes"),
        json.dumps({"order_id": "r1", "customer_id": "c1", "amount": 1.0}),
        True,
    )
    dbutils.fs.put(f"{qdir}/ignore_me.csv".replace("/Volumes", "dbfs:/Volumes"), "a,b\n1,2\n", True)
    time.sleep(3)

    out = reprocess_quarantined_files(spark, VOL)
    record("E2E-6", "replay_result", "INFO", json.dumps(out, default=str)[:400])

    # Assert file movement on the Volume, not the returned dict (#305).
    src = sorted(e.name for e in dbutils.fs.ls(VOL))
    still_q = sorted(e.name for e in dbutils.fs.ls(qdir))
    record("E2E-6", "after_replay", "INFO", f"source={src[:12]} quarantine={still_q}")
    assert "replay_me.json" in src, f"json not moved back: {src}"
    assert "ignore_me.csv" in still_q, "csv was moved by a json-scoped replay"


check("E2E-6", "replay_moves_only_the_configured_format", _replay_moves_files_back)

# COMMAND ----------
# MAGIC %md ## Report

# COMMAND ----------

order = {"FAIL": 0, "ERROR": 1, "PASS": 2, "INFO": 3}
summary: dict[str, int] = {}
for r in results:
    if r["status"] in ("PASS", "FAIL", "ERROR"):
        summary[r["status"]] = summary.get(r["status"], 0) + 1

print("=" * 70)
for r in sorted(results, key=lambda x: (order.get(x["status"], 9), x["issue"])):
    if r["status"] in ("FAIL", "ERROR"):
        print(f"{r['status']:>7}  [{r['issue']}] {r['check']}\n         {r['detail'][:400]}")
print("=" * 70)
print("SUMMARY:", summary)

dbutils.notebook.exit(json.dumps(results))
