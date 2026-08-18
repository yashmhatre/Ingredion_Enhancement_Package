"""
Tests for the batch-read dispatcher (#306).

Every test here runs without a SparkSession. That is the point of
`batch_reader_options` existing as a pure function, and it matters
concretely in this repo: the Spark suite reports NOT RUN on Windows (no
winutils.exe), so anything expressed only as Spark reader calls is
untestable on a contributor's machine until CI runs.
"""

import pytest

from bronze_ingest import formats as formats_module
from bronze_ingest import readers
from bronze_ingest.config import IngestionConfig
from bronze_ingest.csv_reader import read_csv
from bronze_ingest.json_reader import read_json
from bronze_ingest.parquet_reader import read_parquet
from bronze_ingest.xml_reader import read_xml


def _cfg(**kwargs) -> IngestionConfig:
    base = {"source_path": "/tmp/x.json", "table": "_scratch"}
    base.update(kwargs)
    return IngestionConfig(**base)


def _fake_avro_spec():
    return formats_module.FormatSpec(
        name="avro",
        extensions=(".avro",),
        cloudfiles_format="avro",
        allowed_reader_options=frozenset(),
    )


# ---- the invariant that makes formats.py's docstring true ----------------


def test_every_registered_format_has_a_batch_reader():
    """
    `formats.py`'s module docstring promises that adding a format is "two
    edits, in two issues: an entry here, and a line in
    `readers._BATCH_READERS`", and that "neither alone does anything".

    That was aspirational until #306: before `readers.py` existed, `config.py`
    was the registry's only consumer, so a FormatSpec added with no reader
    would validate clean, discover files, and read nothing - the half-wired
    state the docstring says is impossible. This test is what makes the claim
    enforced rather than merely written down: register a format without a
    reader and CI goes red here, immediately, instead of a pipeline reporting
    a successful run over data it never parsed.

    If this fails, do not widen the exception - add the reader.
    """
    missing = sorted(set(formats_module.FORMATS) - set(readers._BATCH_READERS))
    assert not missing, (
        f"formats registered with no batch reader: {missing}. "
        f"Add an entry to readers._BATCH_READERS, or drop it from formats.FORMATS."
    )


def test_every_registered_format_has_reader_options():
    """Same invariant for the parallel options table - a format that can be
    read but whose options cannot be described is still half-wired."""
    missing = sorted(set(formats_module.FORMATS) - set(readers._BATCH_READER_OPTIONS))
    assert not missing, f"formats registered with no reader-options builder: {missing}"


def test_batch_formats_have_registered_readers():
    assert sorted(readers._BATCH_READERS) == ["csv", "json", "parquet", "xml"]
    assert readers._BATCH_READERS["csv"] is read_csv
    assert readers._BATCH_READERS["json"] is read_json
    assert readers._BATCH_READERS["parquet"] is read_parquet
    assert readers._BATCH_READERS["xml"] is read_xml


# ---- a half-wired format must fail loudly, not fall through --------------


def test_read_source_raises_for_a_registered_format_with_no_reader(monkeypatch):
    """
    The exact half-wired state #333's review flagged: `formats.FORMATS` gains
    an entry, `_BATCH_READERS` does not. Config accepts the format (its
    validation asks the registry), so the failure surfaces here or nowhere.

    It must raise. Returning None or silently falling through to the JSON
    reader would make a format that reads nothing look like it works, for
    exactly as long as nobody checks the columns it produced.
    """
    monkeypatch.setitem(formats_module.FORMATS, "avro", _fake_avro_spec())
    cfg = _cfg(source_path="/tmp/x.avro", source_format="avro")

    with pytest.raises(ValueError, match="No registered batch reader"):
        readers.read_source(object(), cfg)


def test_batch_reader_options_raises_for_a_registered_format_with_no_builder(monkeypatch):
    monkeypatch.setitem(formats_module.FORMATS, "avro", _fake_avro_spec())
    cfg = _cfg(source_path="/tmp/x.avro", source_format="avro")

    with pytest.raises(ValueError, match="No registered reader options"):
        readers.batch_reader_options(cfg)


def test_read_source_error_names_what_is_registered(monkeypatch):
    """The message has to be actionable - naming the bad format alone doesn't
    tell you whether you typo'd or hit an unimplemented format."""
    monkeypatch.setitem(formats_module.FORMATS, "avro", _fake_avro_spec())
    cfg = _cfg(source_path="/tmp/x.avro", source_format="avro")

    with pytest.raises(ValueError) as exc:
        readers.read_source(object(), cfg)

    assert "avro" in str(exc.value)
    assert "json" in str(exc.value)


# ---- dispatch actually routes -------------------------------------------


def test_read_source_dispatches_to_the_registered_reader(monkeypatch):
    """Proves routing without a SparkSession: swap the json entry for a spy
    and assert read_source called it with what it was handed."""
    seen = {}

    def spy(spark, config):
        seen["spark"] = spark
        seen["config"] = config
        return "dataframe-sentinel"

    monkeypatch.setitem(readers._BATCH_READERS, "json", spy)
    sentinel_spark = object()
    cfg = _cfg()

    result = readers.read_source(sentinel_spark, cfg)

    assert result == "dataframe-sentinel"
    assert seen["spark"] is sentinel_spark
    assert seen["config"] is cfg


# ---- the pure option dict mirrors read_json ------------------------------


def test_batch_reader_options_defaults():
    opts = readers.batch_reader_options(_cfg())
    assert opts["mode"] == "PERMISSIVE"
    assert opts["columnNameOfCorruptRecord"] == _cfg().corrupt_record_column
    # rescuedDataColumn only applies with an explicit schema - read_json
    # sets it under the same condition, because Spark otherwise infers a
    # schema that already includes every field.
    assert "rescuedDataColumn" not in opts


def test_batch_reader_options_adds_rescued_column_only_with_a_schema_hint():
    cfg = _cfg(schema_hint_ddl="id STRING")
    opts = readers.batch_reader_options(cfg)
    assert opts["rescuedDataColumn"] == cfg.rescued_data_column


def test_batch_reader_options_excludes_the_read_schema():
    """`read_json` sets the schema via `.schema(...)`, a different call than
    `.option(...)`. It is not part of "the option dict"."""
    opts = readers.batch_reader_options(_cfg(schema_hint_ddl="id STRING"))
    assert "schema" not in opts
    assert "id STRING" not in opts.values()


def test_batch_reader_options_lets_reader_options_override():
    """read_json applies config.reader_options last, so an explicit override
    wins. If that precedence ever flips, this catches it without Spark."""
    cfg = _cfg(reader_options={"mode": "FAILFAST"})
    assert readers.batch_reader_options(cfg)["mode"] == "FAILFAST"


def test_batch_reader_options_multiline_follows_effective_multiline():
    """Not config.multiline directly: a .jsonl path forces it off, because
    multiLine=true on a JSON-lines file silently returns only its first
    record (#146). The dispatcher must not reintroduce that bug by reading
    the raw config field."""
    assert readers.batch_reader_options(_cfg(source_path="/tmp/x.json", multiline=True))[
        "multiLine"
    ]
    assert not readers.batch_reader_options(_cfg(source_path="/tmp/x.jsonl", multiline=True))[
        "multiLine"
    ]


def test_csv_batch_reader_options_match_the_public_config_contract():
    cfg = _cfg(source_path="/tmp/x.csv", source_format="csv", csv_infer_schema=False)
    assert readers.batch_reader_options(cfg) == {
        "header": True,
        "inferSchema": False,
        "mode": "PERMISSIVE",
    }


def test_parquet_batch_reader_options_are_only_explicit_overrides():
    cfg = _cfg(
        source_path="/tmp/x.parquet",
        source_format="parquet",
        reader_options={"mergeSchema": "true"},
    )
    assert readers.batch_reader_options(cfg) == {"mergeSchema": "true"}


def test_xml_batch_reader_options_pin_row_tag_and_failfast():
    cfg = _cfg(source_path="/tmp/x.xml", source_format="xml", xml_row_tag="record")
    assert readers.batch_reader_options(cfg) == {"rowTag": "record", "mode": "FAILFAST"}
