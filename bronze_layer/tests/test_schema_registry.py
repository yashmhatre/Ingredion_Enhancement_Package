import json
import uuid

from bronze_ingest.config import IngestionConfig
from bronze_ingest.schema_registry import (
    REGISTRY_SCHEMA,
    _fingerprint,
    describe_drift,
    record_schema,
)
from tests.conftest import file_uri


def _cfg(tmp_path, table, **overrides):
    return IngestionConfig(
        source_path=file_uri(tmp_path, "src"),
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


# ---------------------------------------------------------------------------
# Structured drift detail (#256)
# ---------------------------------------------------------------------------


def _schema_json(*fields):
    return json.dumps({"fields": [{"name": n, "type": t} for n, t in fields]})


def test_describe_drift_reports_added_removed_and_retyped_columns():
    old = _schema_json(("id", "long"), ("name", "string"), ("dropped", "string"))
    new = _schema_json(("id", "string"), ("name", "string"), ("added", "double"))

    drift = json.loads(describe_drift(old, new))

    assert drift["added"] == ["added"]
    assert drift["removed"] == ["dropped"]
    assert drift["type_changed"] == [{"column": "id", "from": "long", "to": "string"}]


def test_describe_drift_returns_none_when_there_is_no_previous_schema():
    """A first registration is not drift. None rather than an empty object, so
    a consumer can tell 'no drift recorded' from 'drift recorded, and it was
    nothing' - and so the column stays NULL for rows predating it."""
    assert describe_drift(None, _schema_json(("id", "long"))) is None
    assert describe_drift("", _schema_json(("id", "long"))) is None


def test_describe_drift_returns_none_for_a_change_that_is_not_add_remove_or_retype():
    """Reordering columns changes the fingerprint, so schema_changed is True
    while this is None. That is the honest answer: something changed, and it
    was not the set of columns or their types."""
    old = _schema_json(("id", "long"), ("name", "string"))
    new = _schema_json(("name", "string"), ("id", "long"))

    assert describe_drift(old, new) is None


def test_describe_drift_never_raises_on_unparseable_input():
    """Advisory metadata must never fail the run that produced it - the same
    contract the rest of this module keeps."""
    assert describe_drift("not json", _schema_json(("id", "long"))) is None
    assert describe_drift(_schema_json(("id", "long")), "not json") is None
    assert describe_drift('{"no_fields_key": 1}', _schema_json(("id", "long"))) is None


def test_record_schema_returns_structured_drift_on_a_real_change(spark, tmp_path):
    cfg = _cfg(tmp_path, "drift_detail")

    fp1, changed1, drift1 = record_schema(spark, cfg, _df(spark, ["id", "name"]))
    assert changed1 is False and drift1 is None, "first registration is not drift"

    fp2, changed2, drift2 = record_schema(spark, cfg, _df(spark, ["id", "name", "email"]))

    assert changed2 is True
    assert json.loads(drift2)["added"] == ["email"]
    assert fp1 != fp2


def test_record_schema_reports_no_drift_when_the_schema_is_unchanged(spark, tmp_path):
    cfg = _cfg(tmp_path, "drift_unchanged")
    record_schema(spark, cfg, _df(spark, ["id", "name"]))

    fingerprint, changed, drift = record_schema(spark, cfg, _df(spark, ["id", "name"]))

    assert changed is False
    assert drift is None
    assert fingerprint is not None
