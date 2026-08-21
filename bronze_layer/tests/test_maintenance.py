"""Tests for table maintenance (#159 item 3).

Most of these are pure Python: the interval parsing and the retention-floor
arithmetic are where the correctness lives, and neither needs Spark. The rule
being protected is that a VACUUM must never expire files a Change Data Feed
consumer still needs (#58).
"""

import pytest

from bronze_ingest.maintenance import (
    DEFAULT_RETENTION_HOURS,
    parse_retention_hours,
    retention_floor_hours,
    run_maintenance,
    vacuum_table,
)


class _FakeSpark:
    """Records SQL instead of running it, and serves table properties."""

    def __init__(self, properties=None, fail_on=None):
        self.sql_log = []
        self._properties = properties or {}
        self._fail_on = fail_on or ()

    def sql(self, statement):
        self.sql_log.append(statement)
        if any(token in statement for token in self._fail_on):
            raise RuntimeError("simulated engine failure")
        if statement.startswith("DESCRIBE DETAIL"):
            table = statement.split()[-1]
            return _FakeResult(self._properties.get(table, {}))
        return _FakeResult({})


class _FakeResult:
    def __init__(self, properties):
        self._properties = properties

    def select(self, *_):
        return self

    def collect(self):
        return [{"properties": self._properties}]


@pytest.mark.parametrize(
    "value,expected",
    [
        ("interval 30 days", 720.0),
        ("interval 7 days", 168.0),
        ("interval 1 days", 24.0),
        ("interval 36 hours", 36.0),
        ("INTERVAL 2 DAYS", 48.0),
        (None, None),
        ("", None),
        ("not an interval", None),
        ("interval 0 days", None),
    ],
)
def test_parse_retention_hours(value, expected):
    assert parse_retention_hours(value) == expected


def test_unparseable_retention_is_none_not_zero():
    """None means 'the table has no opinion', never 'zero hours'. Treating an
    unparseable interval as 0 would compute a floor of nothing and vacuum away
    exactly the history the floor exists to protect."""
    assert parse_retention_hours("interval banana days") is None


def test_floor_comes_from_the_table_not_from_config():
    """#58 writes delta.deletedFileRetentionDuration onto every table this
    package creates. Maintenance reads it back rather than being told the
    number separately - so there is no second value to drift out of step."""
    spark = _FakeSpark({"cat.sch.t": {"delta.deletedFileRetentionDuration": "interval 30 days"}})

    assert retention_floor_hours(spark, "cat.sch.t") == 720.0


def test_floor_falls_back_to_deltas_own_default_for_a_table_without_the_property():
    """A table predating #58, or one that opted out, must still never be
    vacuumed more aggressively than Delta itself would have."""
    spark = _FakeSpark({"cat.sch.t": {}})

    assert retention_floor_hours(spark, "cat.sch.t") == DEFAULT_RETENTION_HOURS == 168.0


def test_a_request_below_the_floor_is_raised_to_it_and_reported(caplog):
    """The rule this module exists for. Asking for 24h against a table
    promising 720h honours the promise and says so - rather than silently
    expiring change history a Silver consumer still needs (#58)."""
    spark = _FakeSpark({"t": {"delta.deletedFileRetentionDuration": "interval 30 days"}})

    result = vacuum_table(spark, "t", requested_hours=24)

    assert result["retain_hours"] == 720.0
    assert result["floor_hours"] == 720.0
    assert result["clamped"] is True
    assert "VACUUM t RETAIN 720.0 HOURS" in spark.sql_log
    assert "RETAIN 24" not in " ".join(spark.sql_log)


def test_a_request_above_the_floor_is_honoured_unchanged():
    spark = _FakeSpark({"t": {"delta.deletedFileRetentionDuration": "interval 7 days"}})

    result = vacuum_table(spark, "t", requested_hours=1000)

    assert result["retain_hours"] == 1000.0
    assert result["clamped"] is False


def test_no_request_uses_the_tables_own_floor():
    """The normal case, and the safe default: maintenance with nothing
    configured vacuums exactly to what the table promises."""
    spark = _FakeSpark({"t": {"delta.deletedFileRetentionDuration": "interval 30 days"}})

    result = vacuum_table(spark, "t")

    assert result["retain_hours"] == 720.0
    assert result["clamped"] is False


def test_one_table_failing_does_not_stop_the_others():
    """Maintenance is housekeeping. A table that cannot be compacted must not
    fail the job or skip the remaining tables - the run has still usefully
    compacted everything else."""
    spark = _FakeSpark(fail_on=("OPTIMIZE bad",))

    results = run_maintenance(spark, ["good", "bad", "alsogood"], vacuum=False)

    assert [r["table"] for r in results] == ["good", "bad", "alsogood"]
    assert results[0]["optimize"] == "ok"
    assert results[1]["optimize"] == "failed"
    assert "simulated engine failure" in results[1]["optimize_error"]
    assert results[2]["optimize"] == "ok"


def test_run_maintenance_only_touches_the_tables_it_is_given():
    """An explicit list, never a schema scan: discovering tables would make
    this job's blast radius depend on whatever else lives in the schema,
    including tables this package did not create."""
    spark = _FakeSpark()

    run_maintenance(spark, ["a", "b"], vacuum=False)

    optimized = [s for s in spark.sql_log if s.startswith("OPTIMIZE")]
    assert optimized == ["OPTIMIZE a", "OPTIMIZE b"]
