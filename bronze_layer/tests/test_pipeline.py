import json
import uuid

import pytest

from bronze_ingest.bronze_writer import write_bronze
from bronze_ingest.config import IngestionConfig
from bronze_ingest.pipeline import BronzeIngestion
from bronze_ingest.quality import DataQualityError
from tests.conftest import file_uri


def test_json_entry_point_is_a_true_alias_for_format_neutral_entry_point():
    import bronze_ingest
    from bronze_ingest.pipeline import ingest_json_to_bronze, ingest_to_bronze

    assert ingest_json_to_bronze is ingest_to_bronze
    assert bronze_ingest.ingest_json_to_bronze is bronze_ingest.ingest_to_bronze


def _write_json(path, rows):
    with open(path, "w") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _cfg(tmp_path, table, **overrides):
    return IngestionConfig(
        source_path=file_uri(tmp_path, "data.json"),
        multiline=False,
        table=table,
        schema_name="default",
        catalog=None,
        audit_schema_name="default",
        audit_table=f"_ingestion_audit_test_{uuid.uuid4().hex[:8]}",
        registry_schema_name="default",
        registry_table=f"_schema_registry_test_{uuid.uuid4().hex[:8]}",
        **overrides,
    )


def test_run_failure_records_bad_count_and_quality_stage_in_audit(spark, tmp_path):
    """
    End-to-end regression test for #50: a quality-gate failure used to
    leave quarantined_row_count and failure_stage None on the failed
    audit row, even though bad_count was known moments before the raise.
    """
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}, {"id": 2, "name": None}])
    cfg = _cfg(
        tmp_path,
        f"pipeline_fail_{uuid.uuid4().hex[:8]}",
        required_columns=["name"],
        fail_on_quality_error=True,
    )
    job = BronzeIngestion(spark, cfg)

    with pytest.raises(DataQualityError):
        job.run()

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "failed"
    assert row["quarantined_row_count"] == 1
    assert row["failure_stage"] == "quality"
    assert not spark.catalog.tableExists(cfg.full_table_name), (
        "bronze table should never have been written"
    )


def test_run_success_records_schema_fingerprint_in_audit(spark, tmp_path):
    """
    End-to-end regression test for #51: a successful run should surface
    the schema fingerprint (and whether it changed) on the run-level
    audit row, not just in the separate schema registry table.
    """
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    table = f"pipeline_success_{uuid.uuid4().hex[:8]}"
    cfg = _cfg(tmp_path, table)
    job = BronzeIngestion(spark, cfg)

    job.run()

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "success"
    assert row["schema_fingerprint"] is not None
    assert row["schema_changed"] is False  # first-ever registration, nothing to have drifted from


# ---- one orchestration body, three entry points (#150) ----
#
# The refactor's risk is concentrated in one place: run() performs its read
# INSIDE the audited_run block so a read failure is tagged and still produces
# an audit row. Extracting the shared body naively - by materialising the
# DataFrame at the call site and passing it in - moves the read outside the
# block, and a failing read stops being recorded at all. Hence read_fn is a
# callable, and hence this test.


def test_failed_read_still_records_a_read_stage_audit_row(spark, tmp_path):
    """The regression the extraction could silently introduce."""
    cfg = _cfg(tmp_path, f"pipeline_read_fail_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    boom = RuntimeError("storage unreachable")

    def _explode():
        raise boom

    job.read = _explode

    with pytest.raises(RuntimeError, match="storage unreachable"):
        job.run()

    rows = spark.read.table(cfg.resolved_audit_table).collect()
    assert len(rows) == 1, "a failed read must still produce exactly one audit row"
    assert rows[0]["status"] == "failed"
    assert rows[0]["failure_stage"] == "read"


def test_read_is_not_invoked_until_inside_the_audited_run(spark, tmp_path):
    """Stronger than the above: proves laziness directly rather than
    inferring it from the audit row. If read_fn were called eagerly at the
    call site, the read would happen before audited_run opened."""
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}])
    cfg = _cfg(tmp_path, f"pipeline_lazy_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    calls = []
    original_read = job.read
    job.read = lambda: (calls.append("read"), original_read())[1]

    # Nothing read at construction or at call-argument evaluation time.
    assert calls == []
    job.run()
    assert calls == ["read"]


def test_write_failure_is_still_tagged_write_on_the_shared_body(spark, tmp_path, monkeypatch):
    """failure_stage for the write stage must survive the consolidation."""
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}])
    cfg = _cfg(tmp_path, f"pipeline_write_fail_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    import bronze_ingest.pipeline as pipeline_module

    def _explode(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(pipeline_module, "write_bronze", _explode)

    with pytest.raises(RuntimeError, match="disk full"):
        job.run()

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "failed"
    assert row["failure_stage"] == "write"


def test_run_and_run_on_dataframe_return_identical_summary_shapes(spark, tmp_path):
    """Both entry points now share one body, so their summaries must agree
    field for field - the acceptance criterion for a no-behaviour-change
    refactor."""
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    _write_json(tmp_path / "data.json", rows)

    cfg_run = _cfg(tmp_path, f"pipeline_shape_a_{uuid.uuid4().hex[:8]}")
    summary_run = BronzeIngestion(spark, cfg_run).run()

    cfg_df = _cfg(tmp_path, f"pipeline_shape_b_{uuid.uuid4().hex[:8]}")
    raw = spark.read.option("multiLine", False).json(cfg_df.source_path)
    summary_df = BronzeIngestion(spark, cfg_df).run_on_dataframe(raw)

    assert (
        set(summary_run)
        == set(summary_df)
        == {
            "table",
            "row_count",
            "quarantined_row_count",
            "quarantine_table",
            "columns",
            "write_mode",
        }
    )
    assert summary_run["row_count"] == summary_df["row_count"] == 2
    assert summary_run["quarantined_row_count"] == summary_df["quarantined_row_count"] == 0
    assert summary_run["quarantine_table"] is None and summary_df["quarantine_table"] is None
    assert summary_run["write_mode"] == summary_df["write_mode"] == "append"


def test_quarantine_table_is_reported_only_when_rows_were_quarantined(spark, tmp_path):
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}, {"id": 2, "name": None}])
    cfg = _cfg(
        tmp_path,
        f"pipeline_quarantine_{uuid.uuid4().hex[:8]}",
        required_columns=["name"],
        fail_on_quality_error=False,
    )

    summary = BronzeIngestion(spark, cfg).run()

    assert summary["row_count"] == 1
    assert summary["quarantined_row_count"] == 1
    assert summary["quarantine_table"] == cfg.resolved_quarantine_table


def test_a_bad_row_is_absent_from_bronze_and_present_in_quarantine(spark, tmp_path):
    """#250's second acceptance criterion: "a deliberately malformed row is
    caught and quarantined, NOT passed through".

    The test above asserts summary["quarantined_row_count"] - a number
    reported by the same code path that decides the split. It is the
    reported value, not the persisted one, and it cannot distinguish "the
    bad row was held back" from "the bad row was counted and written
    anyway". "Not passed through" is the half that was never checked, and
    it is the half the acceptance criterion is actually about.

    So this reads both tables back and asserts where the rows ended up.
    """
    _write_json(
        tmp_path / "data.json",
        [{"order_id": "A-1", "amount": 10}, {"order_id": None, "amount": 20}],
    )
    cfg = _cfg(
        tmp_path,
        f"pipeline_quarantine_split_{uuid.uuid4().hex[:8]}",
        required_columns=["order_id"],
        fail_on_quality_error=False,
    )

    BronzeIngestion(spark, cfg).run()

    bronze = spark.read.table(cfg.full_table_name).collect()
    assert [r["order_id"] for r in bronze] == ["A-1"]
    assert [r["amount"] for r in bronze] == [10]

    quarantined = spark.read.table(cfg.resolved_quarantine_table).collect()
    assert len(quarantined) == 1
    assert quarantined[0]["order_id"] is None
    assert quarantined[0]["amount"] == 20
    assert quarantined[0]["_quarantine_reason"] == "null:order_id"


def test_truncation_guard_failure_is_tagged_read_through_the_shared_body(spark, tmp_path):
    """#146's guard used to sit in its own try/except at the top of
    _process_batch, tagged failure_stage="read". #150 folded that method into
    _execute, and the guard now runs inside the read_fn closure - so its tag
    comes from _execute's read try/except instead of its own.

    That equivalence is the whole basis of the conflict resolution between
    the two, so it gets a test rather than an argument.
    """
    from bronze_ingest.streaming_reader import JsonLinesTruncationError

    cfg = _cfg(tmp_path, f"pipeline_trunc_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    def _guard_then_read():
        raise JsonLinesTruncationError("events.jsonl read with multiLine=true")

    with pytest.raises(JsonLinesTruncationError):
        job._execute(
            _guard_then_read,
            lambda df: None,
            "should never get this far -> %s",
            build_summary=False,
        )

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "failed"
    assert row["failure_stage"] == "read"


# ---- streaming schema drift reaches the audit row (#256) ----


def _drift_json():
    return '{"added":["email"],"removed":[],"type_changed":[]}'


def test_execute_stamps_caller_supplied_schema_audit_onto_the_audit_row(spark, tmp_path):
    """#256. Streaming resolves the schema ONCE per stream (#156) and runs
    every micro-batch with record_metadata=False, so `_execute` resolves
    nothing itself. run_streaming used to simply discard record_schema's
    return value, which meant `schema_changed` and `schema_drift_json` were
    NULL on every streaming audit row ever written - a bronze table could
    gain a column, from a run reporting SUCCESS, with nothing in the audit
    trail marking it. Confirmed against dev under #248.

    This is the seam that carries it: fields resolved by the caller must
    reach the persisted audit row.
    """
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}])
    cfg = _cfg(tmp_path, f"pipeline_schema_audit_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    job._execute(
        lambda: spark.read.json(file_uri(tmp_path, "data.json")),
        lambda df: write_bronze(spark, df, cfg),
        "stamping schema audit -> %s",
        build_summary=False,
        record_metadata=False,
        schema_audit={
            "schema_fingerprint": "deadbeef",
            "schema_changed": True,
            "schema_drift_json": _drift_json(),
        },
    )

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["schema_fingerprint"] == "deadbeef"
    assert row["schema_changed"] is True
    assert row["schema_drift_json"] == _drift_json()


def test_execute_stamps_schema_audit_even_when_the_batch_fails(spark, tmp_path):
    """A run that drifted and then broke is precisely the one worth being
    able to find, so the stamp happens before the read rather than after a
    successful write."""
    cfg = _cfg(tmp_path, f"pipeline_schema_audit_fail_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    def _boom():
        raise RuntimeError("read exploded")

    with pytest.raises(RuntimeError):
        job._execute(
            _boom,
            lambda df: None,
            "stamping then failing -> %s",
            build_summary=False,
            record_metadata=False,
            schema_audit={
                "schema_fingerprint": "deadbeef",
                "schema_changed": True,
                "schema_drift_json": _drift_json(),
            },
        )

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["status"] == "failed"
    assert row["failure_stage"] == "read"
    assert row["schema_changed"] is True
    assert row["schema_drift_json"] == _drift_json()


def test_execute_records_no_schema_fields_when_the_caller_supplies_none(spark, tmp_path):
    """The other direction: absent a caller-supplied value, `_execute` must
    not invent one. Streaming batches after the first pass None, and they
    must stay NULL so one drift is not counted N times."""
    _write_json(tmp_path / "data.json", [{"id": 1, "name": "a"}])
    cfg = _cfg(tmp_path, f"pipeline_schema_audit_none_{uuid.uuid4().hex[:8]}")
    job = BronzeIngestion(spark, cfg)

    job._execute(
        lambda: spark.read.json(file_uri(tmp_path, "data.json")),
        lambda df: write_bronze(spark, df, cfg),
        "no schema audit -> %s",
        build_summary=False,
        record_metadata=False,
        schema_audit=None,
    )

    row = spark.read.table(cfg.resolved_audit_table).collect()[0]
    assert row["schema_changed"] is None
    assert row["schema_drift_json"] is None


class _FakeQuery:
    def awaitTermination(self):  # noqa: N802 - mirrors the PySpark API
        return None


class _FakeWriteStream:
    """Captures the foreachBatch handler instead of running a stream."""

    def __init__(self, sink):
        self._sink = sink

    def foreachBatch(self, fn):  # noqa: N802 - mirrors the PySpark API
        self._sink["handler"] = fn
        return self

    def option(self, *_a, **_k):
        return self

    def trigger(self, **_k):
        return self

    def start(self):
        return _FakeQuery()


class _FakeStreamDataFrame:
    def __init__(self, sink):
        self.writeStream = _FakeWriteStream(sink)


def test_run_streaming_gives_the_schema_audit_to_exactly_one_micro_batch(monkeypatch, tmp_path):
    """#256's actual bug and its actual fix.

    run_streaming discarded record_schema's return value, so no streaming
    audit row could ever carry schema_changed or schema_drift_json. It must
    now reach a micro-batch - and exactly ONE of them, because the schema is
    resolved once per stream (#156) and `schema_changed` exists so a
    dashboard can COUNT drift. N batches each claiming the same drift would
    report one event as N.

    No cloudFiles and no Spark here: the stream is faked down to the
    foreachBatch handler, which is the only part this behaviour lives in.
    """
    import bronze_ingest.pipeline as pl

    sink = {}
    drift = _drift_json()

    monkeypatch.setattr(pl, "read_json_stream", lambda spark, cfg: _FakeStreamDataFrame(sink))
    monkeypatch.setattr(pl, "record_schema", lambda *a, **k: ("fp-1", True, drift))
    monkeypatch.setattr(pl, "apply_catalog_metadata", lambda *a, **k: None)
    monkeypatch.setattr(pl, "apply_catalog_tags", lambda *a, **k: None)
    monkeypatch.setattr(pl, "get_trigger_kwargs", lambda cfg: {"availableNow": True})

    calls = []
    monkeypatch.setattr(
        pl.BronzeIngestion,
        "_execute",
        lambda self, *a, **kw: calls.append(kw.get("schema_audit")),
    )

    cfg = _cfg(
        tmp_path,
        "streaming_oneshot",
        ingestion_mode="streaming",
        checkpoint_location=str(tmp_path / "_cp"),
        schema_location=str(tmp_path / "_schema"),
    )
    BronzeIngestion(None, cfg).run_streaming()

    handler = sink["handler"]
    handler(object(), 0)
    handler(object(), 1)
    handler(object(), 2)

    assert calls[0] == {
        "schema_fingerprint": "fp-1",
        "schema_changed": True,
        "schema_drift_json": drift,
    }
    # Once, not once per batch.
    assert calls[1] is None
    assert calls[2] is None
