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

from bronze_ingest import csv_reader as cr
from bronze_ingest.config import IngestionConfig
from tests.conftest import file_uri


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


class _FakeCol:
    """`col()` requires an active SparkContext, which is the one thing these
    tests exist to avoid needing. We are asserting which columns read_csv
    asks for, not what Spark builds from them."""

    def __init__(self, name):
        self.name = name

    def alias(self, other):
        return _FakeCol(f"{self.name} AS {other}")

    def __str__(self):
        return self.name


def _capture(config, monkeypatch) -> dict:
    import pyspark.sql.functions as F

    monkeypatch.setattr(F, "col", lambda name: _FakeCol(name))
    sink = {"options": [], "schema": None, "loaded": None, "selected": None}
    cr.read_csv(_FakeSpark(sink), config)
    sink["option_dict"] = dict(sink["options"])
    return sink


def _cfg(**kw):
    base = {"source_path": "/tmp/x.csv", "table": "t", "source_format": "csv"}
    base.update(kw)
    return IngestionConfig(**base)


# ---- the rule this module exists to enforce -----------------------------


def test_config_multiline_never_reaches_the_csv_reader(monkeypatch):
    """
    CSV's `multiLine` means "a quoted field may contain a newline" - an
    unrelated meaning to JSON's "this file is one document". The deployed
    job hardcodes `multiline: "true"` for JSON's benefit
    (resources/bronze_ingest_jobs.yml), so a reader that took that value
    would silently set multiLine=true on EVERY csv run.

    Asserted with multiline explicitly True, which is also the default.
    Since #375 the reader does set `multiLine` - from `csv_multiline`, CSV's
    own field - so the rule is now "the value is csv_multiline's, never
    config.multiline's", which is what this asserts.
    """
    sink = _capture(_cfg(multiline=True), monkeypatch)
    assert sink["option_dict"]["multiLine"] is False, sink["options"]


def test_csv_multiline_drives_the_option(monkeypatch):
    """A source whose quoted fields genuinely contain newlines turns it on
    through CSV's own field, with config.multiline left off (#375)."""
    sink = _capture(_cfg(multiline=False, csv_multiline=True), monkeypatch)
    assert sink["option_dict"]["multiLine"] is True


def test_multiline_still_reachable_explicitly_through_reader_options(monkeypatch):
    """The escape hatch must remain: reader_options are applied last, so an
    explicit value wins over csv_multiline."""
    sink = _capture(_cfg(multiline=False, reader_options={"multiLine": "true"}), monkeypatch)
    assert sink["option_dict"]["multiLine"] == "true"


# ---- RFC4180 quoting (#375) ---------------------------------------------


def test_escape_defaults_to_the_rfc4180_doubled_quote(monkeypatch):
    """Spark defaults `escape` to a backslash; RFC4180 doubles the quote."""
    sink = _capture(_cfg(), monkeypatch)
    assert sink["option_dict"]["escape"] == '"'


def test_escape_is_overridable_for_a_backslash_dialect(monkeypatch):
    sink = _capture(_cfg(reader_options={"escape": "\\"}), monkeypatch)
    assert sink["option_dict"]["escape"] == "\\"


# ---- option shape --------------------------------------------------------


def test_fixed_options(monkeypatch):
    sink = _capture(_cfg(), monkeypatch)
    assert sink["format"] == "csv"
    assert sink["option_dict"]["header"] is True
    assert sink["option_dict"]["inferSchema"] is False
    assert sink["option_dict"]["mode"] == "PERMISSIVE"


def test_header_and_infer_schema_follow_their_config_fields(monkeypatch):
    off = _capture(
        _cfg(csv_header=False, csv_infer_schema=False, schema_hint_ddl="a STRING"), monkeypatch
    )
    assert off["option_dict"]["header"] is False
    assert off["option_dict"]["inferSchema"] is False

    # Both directions, since each field now has a different default.
    on = _capture(_cfg(csv_header=True, csv_infer_schema=True), monkeypatch)
    assert on["option_dict"]["header"] is True
    assert on["option_dict"]["inferSchema"] is True


def test_corrupt_record_options_only_with_a_schema_hint(monkeypatch):
    """Not style: Spark only materialises columnNameOfCorruptRecord for CSV
    when the column is declared in the read schema. Setting it against an
    inferred schema would imply a capture that does not happen."""
    without = _capture(_cfg(), monkeypatch)
    assert "columnNameOfCorruptRecord" not in without["option_dict"]
    assert "rescuedDataColumn" not in without["option_dict"]

    cfg = _cfg(schema_hint_ddl="id INT, name STRING")
    with_hint = _capture(cfg, monkeypatch)
    assert with_hint["option_dict"]["columnNameOfCorruptRecord"] == cfg.corrupt_record_column
    assert with_hint["option_dict"]["rescuedDataColumn"] == cfg.rescued_data_column


def test_reader_options_are_applied_last_so_an_override_wins(monkeypatch):
    """Same precedence as read_json - config.reader_options runs after the
    fixed options, so an explicit value beats the default."""
    sink = _capture(_cfg(reader_options={"mode": "FAILFAST"}), monkeypatch)
    assert sink["option_dict"]["mode"] == "FAILFAST"
    keys = [k for k, _ in sink["options"]]
    assert keys.index("mode") < len(keys) - 1 or keys.count("mode") == 2


def test_schema_applied_only_when_ddl_is_set(monkeypatch):
    assert _capture(_cfg(), monkeypatch)["schema"] is None
    assert _capture(_cfg(schema_hint_ddl="id INT"), monkeypatch)["schema"] == "id INT"


def test_lineage_column_is_selected(monkeypatch):
    sink = _capture(_cfg(), monkeypatch)
    assert sink["loaded"] == "/tmp/x.csv"
    assert any("_input_file_name" in s for s in sink["selected"]), sink["selected"]


# ---- behaviour a fake cannot prove --------------------------------------


def test_read_csv_preserves_zero_padded_keys_by_default(spark, tmp_path):
    """#371, on real Spark because a fake cannot prove it. With inference on,
    an 18-character MATNR came back as the integer 1000001 and the padding
    was gone - silently, with the row count still reconciling. The default
    read must return the source text byte for byte."""
    p = tmp_path / "makt.csv"
    p.write_text(
        "MANDT,MATNR,MAKTX\n100,000000000001000001,Dextrose\n",
        encoding="utf-8",
    )

    df = cr.read_csv(spark, _cfg(source_path=file_uri(p)))

    types = dict(df.dtypes)
    assert types["MATNR"] == "string", types
    assert types["MANDT"] == "string", types
    row = df.first()
    assert row["MATNR"] == "000000000001000001"
    assert row["MANDT"] == "100"
    assert "_input_file_name" in df.columns
    assert df.count() == 1


def test_read_csv_infers_numeric_types_when_opted_in(spark, tmp_path):
    """The knob still works when a caller asks for it explicitly (#308) -
    #371 changed the default, not the capability."""
    p = tmp_path / "orders.csv"
    p.write_text("order_id,amount\n1,10\n2,20\n", encoding="utf-8")

    cfg = _cfg(source_path=file_uri(p), csv_infer_schema=True)
    df = cr.read_csv(spark, cfg)

    types = dict(df.dtypes)
    assert "order_id" in types and "amount" in types, types
    assert types["amount"] != "string", f"inferSchema did not apply: {types}"
    assert df.count() == 2


def test_read_csv_honours_csv_header_false_with_a_schema(spark, tmp_path):
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


def test_read_csv_retries_transient_load_failure(spark, tmp_path, monkeypatch):
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


# ---- RFC4180 behaviour, against real Spark (#375) ------------------------

#: The EC-20 fixture row from `fixtures/generate_sap_migration.py`: an
#: embedded comma, RFC4180 doubled quotes, and a newline inside a quoted
#: field. Under Spark's own defaults this file parses as 5 rows, the last of
#: them values shifted under the wrong column names.
_RFC4180_CSV = (
    '"KUNNR","NAME1","STRAS"\n'
    '"0000100001","Müller, Meier & Co. KG","Hauptstr. 1"\n'
    '"0000100002","He said ""premium grade""","Main St 2"\n'
    '"0000100003","Line one\nLine two","Av. Central 3"\n'
    '"0000100004","Trailing comma test,","Rua 4"\n'
)


def test_read_csv_parses_rfc4180_doubled_quotes(spark, tmp_path):
    """The corruption #375 reports: with escape left at Spark's backslash
    default the value keeps its outer quotes and doubled inner quotes."""
    p = tmp_path / "kna1.csv"
    # newline="\n" - the default would translate the quoted newline inside
    # NAME1 to CRLF on Windows, changing what is being asserted.
    p.write_text(_RFC4180_CSV, encoding="utf-8", newline="\n")

    df = cr.read_csv(spark, _cfg(source_path=file_uri(p), csv_multiline=True))
    names = {r["KUNNR"]: r["NAME1"] for r in df.collect()}

    assert names["0000100002"] == 'He said "premium grade"', names


def test_read_csv_with_csv_multiline_keeps_a_quoted_newline_in_one_row(spark, tmp_path):
    """Without multiLine the quoted newline splits the record and the tail
    lands as a 5th row with KUNNR='Av. Central 3' and NAME1 null."""
    p = tmp_path / "kna1.csv"
    # newline="\n" - the default would translate the quoted newline inside
    # NAME1 to CRLF on Windows, changing what is being asserted.
    p.write_text(_RFC4180_CSV, encoding="utf-8", newline="\n")

    df = cr.read_csv(spark, _cfg(source_path=file_uri(p), csv_multiline=True))
    rows = {r["KUNNR"]: r for r in df.collect()}

    assert df.count() == 4, sorted(rows)
    assert rows["0000100003"]["NAME1"] == "Line one\nLine two"
    assert rows["0000100001"]["NAME1"] == "Müller, Meier & Co. KG"
    assert rows["0000100004"]["NAME1"] == "Trailing comma test,"


def test_read_csv_defaults_still_split_a_quoted_newline(spark, tmp_path):
    """csv_multiline stays off by default (multiLine CSV cannot be split
    across tasks), so the row-splitting is documented, not fixed, here."""
    p = tmp_path / "kna1.csv"
    # newline="\n" - the default would translate the quoted newline inside
    # NAME1 to CRLF on Windows, changing what is being asserted.
    p.write_text(_RFC4180_CSV, encoding="utf-8", newline="\n")

    df = cr.read_csv(spark, _cfg(source_path=file_uri(p)))

    assert df.count() == 5
    # The quote fix is independent of multiLine and applies either way.
    names = [r["NAME1"] for r in df.collect()]
    assert 'He said "premium grade"' in names, names
