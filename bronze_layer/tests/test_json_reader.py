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
