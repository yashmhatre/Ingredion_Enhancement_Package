"""
Tests for `csv_reader.read_csv` (#309).

Split deliberately into two halves:

- **Option-shape tests** run against a recording fake in place of
  `spark.read`, so they need no SparkSession at all. That matters here: the
  Spark suite reports NOT RUN on Windows (no winutils.exe), so a rule
  expressed only as a Spark call is unverifiable on a contributor's machine
  until CI runs. The single most important rule in this module - that
  `config.multiline` never reaches the CSV reader - is one of those.
- **Behaviour tests** use the real `spark` fixture for what a fake cannot
  prove: that inference actually yields a numeric type, and that a
  transient load failure is really retried.
"""

import time

import pytest

from bronze_ingest import csv_reader as cr
from bronze_ingest import formats as formats_module
from bronze_ingest.config import IngestionConfig
from tests.conftest import file_uri


@pytest.fixture
def csv_format(monkeypatch):
    """csv is not in the registry until #310, so `source_format="csv"` is
    rejected at config validation. Install a throwaway spec for the test -
    the pattern #304 established and #305/#307/#308 reuse. This does not
    land real csv support."""
    monkeypatch.setitem(
        formats_module.FORMATS,
        "csv",
        formats_module.FormatSpec(
            name="csv",
            extensions=(".csv",),
            cloudfiles_format="csv",
            allowed_reader_options=frozenset({"multiLine", "sep", "quote", "escape"}),
        ),
    )


class _RecordingReader:
    """Stands in for the object `spark.read.format("csv")` returns, recording
    the exact option/schema calls read_csv makes. Chainable like the real one."""

    def __init__(self, sink):
        self.sink = sink

    def option(self, key, value):
        self.sink["options"].append((key, value))
        return self

    def schema(self, ddl):
        self.sink["schema"] = ddl
        return self

    def load(self, path):
        self.sink["loaded"] = path
        return _RecordingDF(self.sink)


class _RecordingDF:
    def __init__(self, sink):
        self.sink = sink

    def select(self, *cols):
        self.sink["selected"] = [str(c) for c in cols]
        return self


class _RecordingRead:
    def __init__(self, sink):
        self.sink = sink

    def format(self, fmt):
        self.sink["format"] = fmt
        return _RecordingReader(self.sink)


class _FakeSpark:
    def __init__(self, sink):
        self.read = _RecordingRead(sink)


def _capture(config) -> dict:
    sink = {"options": [], "schema": None, "loaded": None, "selected": None}
    cr.read_csv(_FakeSpark(sink), config)
    sink["option_dict"] = dict(sink["options"])
    return sink


def _cfg(csv_format_installed=True, **kw):
    base = {"source_path": "/tmp/x.csv", "table": "t", "source_format": "csv"}
    base.update(kw)
    return IngestionConfig(**base)


# ---- the rule this module exists to enforce -----------------------------


def test_config_multiline_never_reaches_the_csv_reader(csv_format):
    """
    CSV's `multiLine` means "a quoted field may contain a newline" - an
    unrelated meaning to JSON's "this file is one document". The deployed
    job hardcodes `multiline: "true"` for JSON's benefit
    (resources/bronze_ingest_jobs.yml), so a reader that took that value
    would silently set multiLine=true on EVERY csv run.

    Asserted with multiline explicitly True, which is also the default.
    """
    sink = _capture(_cfg(multiline=True))
    assert "multiLine" not in sink["option_dict"], sink["options"]


def test_multiline_still_reachable_explicitly_through_reader_options(csv_format):
    """The escape hatch must remain: a source whose quoted fields genuinely
    contain newlines sets it deliberately, and that value is what applies."""
    sink = _capture(_cfg(multiline=False, reader_options={"multiLine": "true"}))
    assert sink["option_dict"]["multiLine"] == "true"


# ---- option shape --------------------------------------------------------


def test_fixed_options(csv_format):
    sink = _capture(_cfg())
    assert sink["format"] == "csv"
    assert sink["option_dict"]["header"] is True
    assert sink["option_dict"]["inferSchema"] is True
    assert sink["option_dict"]["mode"] == "PERMISSIVE"


def test_header_and_infer_schema_follow_their_config_fields(csv_format):
    sink = _capture(_cfg(csv_header=False, csv_infer_schema=False, schema_hint_ddl="a STRING"))
    assert sink["option_dict"]["header"] is False
    assert sink["option_dict"]["inferSchema"] is False


def test_corrupt_record_options_only_with_a_schema_hint(csv_format):
    """Not style: Spark only materialises columnNameOfCorruptRecord for CSV
    when the column is declared in the read schema. Setting it against an
    inferred schema would imply a capture that does not happen."""
    without = _capture(_cfg())
    assert "columnNameOfCorruptRecord" not in without["option_dict"]
    assert "rescuedDataColumn" not in without["option_dict"]

    cfg = _cfg(schema_hint_ddl="id INT, name STRING")
    with_hint = _capture(cfg)
    assert with_hint["option_dict"]["columnNameOfCorruptRecord"] == cfg.corrupt_record_column
    assert with_hint["option_dict"]["rescuedDataColumn"] == cfg.rescued_data_column


def test_reader_options_are_applied_last_so_an_override_wins(csv_format):
    """Same precedence as read_json - config.reader_options runs after the
    fixed options, so an explicit value beats the default."""
    sink = _capture(_cfg(reader_options={"mode": "FAILFAST"}))
    assert sink["option_dict"]["mode"] == "FAILFAST"
    keys = [k for k, _ in sink["options"]]
    assert keys.index("mode") < len(keys) - 1 or keys.count("mode") == 2


def test_schema_applied_only_when_ddl_is_set(csv_format):
    assert _capture(_cfg())["schema"] is None
    assert _capture(_cfg(schema_hint_ddl="id INT"))["schema"] == "id INT"


def test_lineage_column_is_selected(csv_format):
    sink = _capture(_cfg())
    assert sink["loaded"] == "/tmp/x.csv"
    assert any("_input_file_name" in s for s in sink["selected"]), sink["selected"]


# ---- behaviour a fake cannot prove --------------------------------------


def test_read_csv_infers_numeric_types_not_strings(spark, tmp_path, csv_format):
    """#308's whole reason for csv_infer_schema defaulting True: an
    all-string `amount` is the format-dependent-schema trap already
    documented in config/contracts/orders.yaml."""
    p = tmp_path / "orders.csv"
    p.write_text("order_id,amount\n1,10\n2,20\n", encoding="utf-8")

    cfg = _cfg(source_path=file_uri(p))
    df = cr.read_csv(spark, cfg)

    types = dict(df.dtypes)
    assert "order_id" in types and "amount" in types, types
    assert types["amount"] != "string", f"inferSchema did not apply: {types}"
    assert "_input_file_name" in df.columns
    assert df.count() == 2


def test_read_csv_honours_csv_header_false_with_a_schema(spark, tmp_path, csv_format):
    p = tmp_path / "noheader.csv"
    p.write_text("1,alpha\n2,beta\n", encoding="utf-8")

    cfg = _cfg(
        source_path=file_uri(p),
        csv_header=False,
        csv_infer_schema=False,
        schema_hint_ddl="id INT, name STRING",
    )
    df = cr.read_csv(spark, cfg)

    assert df.count() == 2
    assert "id" in df.columns and "name" in df.columns


def test_read_csv_retries_transient_load_failure(spark, tmp_path, monkeypatch, csv_format):
    """The read path retries transient failures with backoff, same as the
    write path (#81). Mirrors test_json_reader's equivalent."""
    p = tmp_path / "orders.csv"
    p.write_text("order_id,amount\n1,10\n", encoding="utf-8")

    cfg = _cfg(source_path=file_uri(p), retry_attempts=3, retry_delay_seconds=0.01)

    calls = {"count": 0}
    real_load = type(spark.read).load

    def flaky_load(self, *args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 2:
            raise RuntimeError("simulated transient read failure")
        return real_load(self, *args, **kwargs)

    monkeypatch.setattr(type(spark.read), "load", flaky_load)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    df = cr.read_csv(spark, cfg)

    assert calls["count"] == 2
    assert df.count() == 1
