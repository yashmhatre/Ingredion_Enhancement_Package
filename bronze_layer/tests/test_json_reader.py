import json
import time

from bronze_ingest.config import IngestionConfig
from bronze_ingest import json_reader as jr


def _write(path, obj):
    with open(path, "w") as fh:
        fh.write(json.dumps(obj))


def test_read_json_retries_transient_load_failure(spark, tmp_path, monkeypatch):
    """The read path should retry transient failures with backoff, same as
    the write path - see issue #81."""
    file_path = tmp_path / "orders.json"
    _write(file_path, {"id": 1})

    cfg = IngestionConfig(
        source_path=f"file://{file_path}",
        multiline=True,
        table="t",
        retry_attempts=3,
        retry_delay_seconds=0.01,
    )

    calls = {"count": 0}
    real_reader_load = type(spark.read).load

    def flaky_load(self, *args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 2:
            raise RuntimeError("simulated transient read failure")
        return real_reader_load(self, *args, **kwargs)

    monkeypatch.setattr(type(spark.read), "load", flaky_load)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    df = jr.read_json(spark, cfg)

    assert calls["count"] == 2
    assert df.count() == 1


def test_read_json_gives_up_after_retry_attempts_exhausted(spark, tmp_path, monkeypatch):
    file_path = tmp_path / "orders.json"
    _write(file_path, {"id": 1})

    cfg = IngestionConfig(
        source_path=f"file://{file_path}",
        multiline=True,
        table="t",
        retry_attempts=2,
        retry_delay_seconds=0.01,
    )

    calls = {"count": 0}

    def always_fails(self, *args, **kwargs):
        calls["count"] += 1
        raise RuntimeError("simulated permanent read failure")

    monkeypatch.setattr(type(spark.read), "load", always_fails)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    try:
        jr.read_json(spark, cfg)
        assert False, "expected RuntimeError to propagate"
    except RuntimeError:
        pass

    assert calls["count"] == 2


# ---- JSON-lines files must not be read with multiLine=true (#146) ----
#
# Discovery accepts .json and .jsonl, but `multiline` is one config flag
# shared across every file in a directory run, and the deployed job sets it
# to true. On a .jsonl file that combination returns ONLY the first record -
# no error, no warning, nothing in _corrupt_record. The run reports success
# and the rest of the file is gone.


def _write_lines(path, objs):
    with open(path, "w") as fh:
        for obj in objs:
            fh.write(json.dumps(obj) + "\n")


def test_jsonl_reads_every_record_despite_multiline_true(spark, tmp_path):
    """The regression: 3 records in, 3 records out, with multiline=True set."""
    file_path = tmp_path / "events.jsonl"
    _write_lines(file_path, [{"id": 1}, {"id": 2}, {"id": 3}])

    cfg = IngestionConfig(source_path=f"file://{file_path}", table="t", multiline=True)
    df = jr.read_json(spark, cfg)

    assert sorted(r["id"] for r in df.collect()) == [1, 2, 3]


def test_multiline_true_on_jsonl_would_have_lost_records(spark, tmp_path):
    """Pins the underlying Spark behaviour this fix exists to avoid.

    If this ever stops holding - if Spark starts reading JSON-lines
    correctly under multiLine=true, or starts raising - the override above
    becomes unnecessary and should be reconsidered rather than left as
    unexplained defensive code.
    """
    file_path = tmp_path / "events.jsonl"
    _write_lines(file_path, [{"id": 1}, {"id": 2}, {"id": 3}])

    raw = spark.read.option("multiLine", True).json(f"file://{file_path}")

    assert raw.count() == 1


def test_json_extension_still_honours_multiline_config(spark, tmp_path):
    """.json is ambiguous, so the config decides - only .jsonl is overridden."""
    file_path = tmp_path / "order.json"
    with open(file_path, "w") as fh:
        fh.write('{\n  "id": 1,\n  "customer": "acme"\n}\n')

    cfg = IngestionConfig(source_path=f"file://{file_path}", table="t", multiline=True)
    df = jr.read_json(spark, cfg)

    assert df.count() == 1
    assert df.collect()[0]["customer"] == "acme"


def test_effective_multiline_decides_by_extension():
    """Unit-level: the decision itself, without touching Spark."""

    def eff(path, configured):
        cfg = IngestionConfig(source_path=path, table="t", multiline=configured)
        return jr.effective_multiline(cfg)

    # JSON-lines extensions are forced off whatever the config says.
    assert eff("/Volumes/x/events.jsonl", True) is False
    assert eff("/Volumes/x/events.NDJSON", True) is False
    assert eff("/Volumes/x/events.jsonl", False) is False

    # .json and directories keep the configured value - neither is
    # unambiguous enough to override.
    assert eff("/Volumes/x/order.json", True) is True
    assert eff("/Volumes/x/order.json", False) is False
    assert eff("/Volumes/x/incoming/", True) is True
    assert eff("/Volumes/x/incoming", True) is True


def test_reader_options_can_override_the_extension_rule(spark, tmp_path):
    """Documented escape hatch: reader_options is applied last and wins.

    A .jsonl file that really is one pretty-printed document is misnamed,
    but the package should not make it unreadable.
    """
    file_path = tmp_path / "single.jsonl"
    with open(file_path, "w") as fh:
        fh.write('{\n  "id": 1\n}\n')

    cfg = IngestionConfig(
        source_path=f"file://{file_path}",
        table="t",
        multiline=False,
        reader_options={"multiLine": "true"},
    )
    df = jr.read_json(spark, cfg)

    assert df.count() == 1
    assert df.collect()[0]["id"] == 1
