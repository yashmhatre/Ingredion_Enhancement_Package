"""
Tests for the streaming reader, and specifically for the JSON-lines
truncation guard (#146).

A note on what is and is not covered here, because the split is deliberate
rather than an oversight:

Auto Loader (`cloudFiles`) is a Databricks Runtime feature. It does not
exist in OSS Spark, so `read_json_stream` cannot be executed by this suite
at all - constructing the reader raises before any assertion could run.
That is the same constraint `catalog_metadata` documents for UC tag DDL.

So the logic that decides whether data was silently truncated is written as
pure functions over paths and config, which ARE fully testable here, and
`assert_no_silent_truncation` is a thin layer that pulls the paths out of a
DataFrame. The DataFrame layer is covered with an ordinary batch DataFrame
carrying the same `_input_file_name` column Auto Loader attaches - which
exercises every branch of the guard without needing a stream.

What remains genuinely unverified locally is only the wiring: that
`read_json_stream` passes the resolved `multiLine` to cloudFiles. That is
asserted against a recording fake below, which proves the option is set and
proves nothing about whether Auto Loader honours it. Confirming the latter
needs a run on Databricks.
"""

import pytest

from bronze_ingest import streaming_reader as sr
from bronze_ingest.config import IngestionConfig
from bronze_ingest.json_reader import is_json_lines_path


def _cfg(**overrides):
    base = dict(
        source_path="/Volumes/x/incoming/",
        table="t",
        ingestion_mode="streaming",
        checkpoint_location="/Volumes/x/_ckpt",
        schema_location="/Volumes/x/_schema",
    )
    base.update(overrides)
    return IngestionConfig(**base)


# ---- path classification (pure) ----


def test_is_json_lines_path_by_extension():
    assert is_json_lines_path("/Volumes/x/events.jsonl") is True
    assert is_json_lines_path("/Volumes/x/events.ndjson") is True
    assert is_json_lines_path("/Volumes/x/events.JSONL") is True

    # .json is ambiguous by design - it may be either format.
    assert is_json_lines_path("/Volumes/x/order.json") is False
    # Directories and empties classify as "unknown", never as JSON-lines.
    assert is_json_lines_path("/Volumes/x/incoming/") is False
    assert is_json_lines_path("") is False
    assert is_json_lines_path(None) is False


def test_is_json_lines_path_ignores_query_string():
    """A signed URL still names a .jsonl file; the signature is not part of
    the extension."""
    assert is_json_lines_path("https://acct.blob.core.windows.net/e.jsonl?sig=abc%3D") is True
    assert is_json_lines_path("https://acct.blob.core.windows.net/e.json?sig=abc%3D") is False


def test_json_lines_files_dedupes_and_sorts():
    got = sr.json_lines_files(
        [
            "/x/b.jsonl",
            "/x/a.json",
            "/x/b.jsonl",
            "/x/a.ndjson",
            "",
            None,
        ]
    )
    assert got == ["/x/a.ndjson", "/x/b.jsonl"]


# ---- whether the guard applies at all (pure) ----


def test_should_guard_follows_multiline_when_no_override():
    assert sr.should_guard_truncation(_cfg(multiline=True)) is True
    assert sr.should_guard_truncation(_cfg(multiline=False)) is False


def test_any_explicit_reader_options_multiline_suppresses_the_guard():
    """An explicit override is the operator overriding the package on this
    exact point, so the guard steps aside whatever the value.

    "true" is the documented escape hatch for .jsonl files that really are
    single JSON documents - firing on it would turn the way out of this
    failure into another instance of it. "false" means the read is correct
    and there is nothing to guard.
    """
    for value in ["true", "True", True, "false", "False", False, "", "anything"]:
        for configured in (True, False):
            cfg = _cfg(multiline=configured, reader_options={"multiLine": value})
            assert sr.should_guard_truncation(cfg) is False, (value, configured)


def test_should_guard_ignores_unrelated_reader_options():
    """Only a multiLine entry suppresses it - not reader_options generally."""
    cfg = _cfg(multiline=True, reader_options={"encoding": "UTF-8"})
    assert sr.should_guard_truncation(cfg) is True


def test_should_guard_off_for_a_single_jsonl_file_source():
    """A streaming source pointed straight at a .jsonl file is unambiguous,
    so effective_multiline forces multiLine off and nothing can truncate."""
    assert (
        sr.should_guard_truncation(_cfg(source_path="/Volumes/x/events.jsonl", multiline=True))
        is False
    )


# ---- the guard itself (needs a DataFrame, not a stream) ----

_BATCH_SCHEMA = "id INT, _input_file_name STRING"


def _batch(spark, file_names):
    """A DataFrame shaped like an Auto Loader micro-batch: real rows plus the
    `_input_file_name` lineage column read_json_stream attaches.

    The schema is explicit so an empty batch is still a valid DataFrame -
    inference on an empty list raises CANNOT_INFER_EMPTY_SCHEMA, which is
    the same defect #144 hit in the notebook layer.
    """
    return spark.createDataFrame(
        [(i, name) for i, name in enumerate(file_names)],
        schema=_BATCH_SCHEMA,
    )


def test_guard_raises_when_multiline_true_meets_jsonl(spark):
    df = _batch(spark, ["/Volumes/x/incoming/a.json", "/Volumes/x/incoming/b.jsonl"])

    with pytest.raises(sr.JsonLinesTruncationError) as excinfo:
        sr.assert_no_silent_truncation(df, _cfg(multiline=True))

    msg = str(excinfo.value)
    assert "b.jsonl" in msg
    # The message must tell an operator the data is still recoverable and
    # name the checkpoint - that is the actionable part.
    assert "has NOT advanced" in msg
    assert "/Volumes/x/_ckpt" in msg
    assert "multiline: false" in msg


def test_guard_silent_when_multiline_false(spark):
    """multiLine=false reads JSON-lines correctly - nothing to guard."""
    df = _batch(spark, ["/Volumes/x/incoming/b.jsonl"])
    sr.assert_no_silent_truncation(df, _cfg(multiline=False))


def test_guard_silent_on_json_only_batch(spark):
    """The healthy path: multiLine=true over .json documents is exactly what
    this package was built for and must not be disturbed."""
    df = _batch(spark, ["/Volumes/x/incoming/a.json", "/Volumes/x/incoming/b.JSON"])
    sr.assert_no_silent_truncation(df, _cfg(multiline=True))


def test_guard_silent_when_override_deliberately_forces_multiline(spark):
    """The documented escape hatch: an operator who set reader_options
    multiLine=true on .jsonl files asked for this and must not be blocked."""
    df = _batch(spark, ["/Volumes/x/incoming/b.jsonl"])
    cfg = _cfg(multiline=False, reader_options={"multiLine": "true"})
    sr.assert_no_silent_truncation(df, cfg)


def test_guard_does_not_fire_on_override_to_false(spark):
    """The false-positive case: config says multiline=true, reader_options
    corrects it to false. The read is fine, so the guard must stay quiet."""
    df = _batch(spark, ["/Volumes/x/incoming/b.jsonl"])
    cfg = _cfg(multiline=True, reader_options={"multiLine": "false"})
    sr.assert_no_silent_truncation(df, cfg)


def test_guard_reports_a_bounded_number_of_files(spark):
    """A fully-offending batch must not build an unbounded message, and must
    not collect every path to the driver (the #155 mistake)."""
    df = _batch(spark, [f"/Volumes/x/incoming/f{i}.jsonl" for i in range(200)])

    with pytest.raises(sr.JsonLinesTruncationError) as excinfo:
        sr.assert_no_silent_truncation(df, _cfg(multiline=True))

    msg = str(excinfo.value)
    assert "and possibly others" in msg
    assert msg.count(".jsonl") <= sr._MAX_REPORTED_FILES + 1


def test_guard_warns_but_does_not_fail_without_lineage_column(spark, caplog):
    """A hand-built DataFrame gets no protection - but this guard must never
    be the reason an otherwise-working stream fails."""
    df = spark.createDataFrame([(1,)], ["id"])

    sr.assert_no_silent_truncation(df, _cfg(multiline=True))

    assert "truncation guard" in caplog.text


def test_guard_is_empty_batch_safe(spark):
    """availableNow with no new files produces an empty micro-batch."""
    df = _batch(spark, [])
    assert df.count() == 0
    sr.assert_no_silent_truncation(df, _cfg(multiline=True))


# ---- reader wiring (fake, not Spark) ----


class _RecordingReader:
    """Records .option()/.schema() calls the way DataStreamReader would."""

    def __init__(self):
        self.options = {}

    def format(self, _):
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def schema(self, _):
        return self

    def load(self, path):
        raise AssertionError(f"load({path!r}) should not run in this test")


class _FakeSpark:
    def __init__(self, reader):
        self.readStream = reader


def test_read_json_stream_passes_resolved_multiline_to_cloudfiles():
    """The regression this issue is about: streaming used to pass
    config.multiline straight through, so a .jsonl source truncated."""
    reader = _RecordingReader()
    cfg = _cfg(source_path="/Volumes/x/events.jsonl", multiline=True)

    with pytest.raises(AssertionError):  # stops at .load, options already recorded
        sr.read_json_stream(_FakeSpark(reader), cfg)

    assert reader.options["multiLine"] is False


def test_read_json_stream_keeps_config_multiline_for_a_directory():
    reader = _RecordingReader()
    cfg = _cfg(source_path="/Volumes/x/incoming/", multiline=True)

    with pytest.raises(AssertionError):
        sr.read_json_stream(_FakeSpark(reader), cfg)

    assert reader.options["multiLine"] is True
