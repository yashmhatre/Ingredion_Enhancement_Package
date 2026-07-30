import uuid

import pytest

from bronze_ingest.config import IngestionConfig
from bronze_ingest.schema_registry import record_schema, _fingerprint, REGISTRY_SCHEMA


def _cfg(tmp_path, table, **overrides):
    return IngestionConfig(
        source_path=f"file://{tmp_path}/src",
        table=table,
        schema_name="default",
        catalog=None,
        registry_schema_name="default",
        registry_table=f"_schema_registry_test_{uuid.uuid4().hex[:8]}",
        **overrides,
    )


def _df(spark, cols):
    """Builds a one-row DataFrame with the given column names."""
    return spark.createDataFrame([tuple(range(len(cols)))], schema=list(cols))


def test_first_ingestion_registers_one_row(spark, tmp_path):
    cfg = _cfg(tmp_path, "reg_first")
    record_schema(spark, cfg, _df(spark, ["id", "name"]))

    rows = spark.read.table(cfg.resolved_registry_table).collect()
    assert len(rows) == 1
    assert rows[0]["table_name"] == cfg.full_table_name
    assert rows[0]["schema_fingerprint"]
    assert "id" in rows[0]["schema_json"]
    assert rows[0]["first_seen_at"] == rows[0]["last_updated_at"]


def test_unchanged_schema_writes_nothing(spark, tmp_path):
    cfg = _cfg(tmp_path, "reg_unchanged")
    df = _df(spark, ["id", "name"])

    record_schema(spark, cfg, df)
    first = spark.read.table(cfg.resolved_registry_table).collect()[0]

    record_schema(spark, cfg, df)
    rows = spark.read.table(cfg.resolved_registry_table).collect()

    assert len(rows) == 1
    # No write occurred, so the timestamp must be untouched.
    assert rows[0]["last_updated_at"] == first["last_updated_at"]


def test_changed_schema_upserts_in_place(spark, tmp_path):
    cfg = _cfg(tmp_path, "reg_drift")

    record_schema(spark, cfg, _df(spark, ["id", "name"]))
    before = spark.read.table(cfg.resolved_registry_table).collect()[0]

    record_schema(spark, cfg, _df(spark, ["id", "name", "email"]))
    rows = spark.read.table(cfg.resolved_registry_table).collect()

    assert len(rows) == 1, "drift must upsert, not append a second row"
    after = rows[0]
    assert after["schema_fingerprint"] != before["schema_fingerprint"]
    assert after["last_updated_at"] > before["last_updated_at"]
    assert after["first_seen_at"] == before["first_seen_at"]
    assert "email" in after["schema_json"]


def test_fingerprint_stable_across_column_reordering(spark, tmp_path):
    a = _df(spark, ["id", "name", "email"])
    b = _df(spark, ["email", "id", "name"])
    assert _fingerprint(a) == _fingerprint(b)


def test_fingerprint_changes_on_type_change(spark, tmp_path):
    a = spark.createDataFrame([(1,)], schema="id INT")
    b = spark.createDataFrame([("1",)], schema="id STRING")
    assert _fingerprint(a) != _fingerprint(b)


def test_disabled_registry_writes_nothing(spark, tmp_path):
    cfg = _cfg(tmp_path, "reg_disabled", enable_schema_registry=False)
    record_schema(spark, cfg, _df(spark, ["id"]))
    assert not spark.catalog.tableExists(cfg.resolved_registry_table)


def test_registry_failure_never_raises(spark, tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, "reg_failure")

    import bronze_ingest.schema_registry as sr

    def boom(*args, **kwargs):
        raise RuntimeError("simulated registry failure")

    monkeypatch.setattr(sr, "_write_row", boom)

    # Must not propagate - a registry failure can never fail an ingestion run.
    sr.record_schema(spark, cfg, _df(spark, ["id"]))


def test_registry_schema_matches_documented_fields():
    assert {f.name for f in REGISTRY_SCHEMA.fields} == {
        "table_name",
        "source_path",
        "schema_fingerprint",
        "schema_json",
        "first_seen_at",
        "last_updated_at",
    }


def test_record_schema_returns_fingerprint_and_unchanged_on_first_registration(spark, tmp_path):
    """#51: callers (e.g. the run-level audit trail) need (fingerprint,
    changed) back - changed=False on first-ever registration, since
    there's nothing to have drifted from."""
    cfg = _cfg(tmp_path, "reg_return_first")
    fingerprint, changed = record_schema(spark, cfg, _df(spark, ["id", "name"]))
    assert fingerprint == _fingerprint(_df(spark, ["id", "name"]))
    assert changed is False


def test_record_schema_returns_unchanged_false_on_stable_schema(spark, tmp_path):
    cfg = _cfg(tmp_path, "reg_return_stable")
    df = _df(spark, ["id", "name"])
    record_schema(spark, cfg, df)

    fingerprint, changed = record_schema(spark, cfg, df)
    assert fingerprint == _fingerprint(df)
    assert changed is False


def test_record_schema_returns_changed_true_on_drift(spark, tmp_path):
    cfg = _cfg(tmp_path, "reg_return_drift")
    record_schema(spark, cfg, _df(spark, ["id", "name"]))

    fingerprint, changed = record_schema(spark, cfg, _df(spark, ["id", "name", "email"]))
    assert fingerprint == _fingerprint(_df(spark, ["id", "name", "email"]))
    assert changed is True


def test_record_schema_disabled_returns_none_false(spark, tmp_path):
    cfg = _cfg(tmp_path, "reg_return_disabled", enable_schema_registry=False)
    fingerprint, changed = record_schema(spark, cfg, _df(spark, ["id"]))
    assert fingerprint is None
    assert changed is False


def test_record_schema_failure_returns_none_false(spark, tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, "reg_return_failure")

    import bronze_ingest.schema_registry as sr

    def boom(*args, **kwargs):
        raise RuntimeError("simulated registry failure")

    monkeypatch.setattr(sr, "_write_row", boom)

    fingerprint, changed = sr.record_schema(spark, cfg, _df(spark, ["id"]))
    assert fingerprint is None
    assert changed is False
