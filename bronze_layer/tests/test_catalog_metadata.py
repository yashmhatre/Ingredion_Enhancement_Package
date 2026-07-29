"""
Tests for catalog documentation (#64) - table/column COMMENT support.

These exercise real DDL against the local Delta session rather than
asserting on generated SQL strings, so idempotency in particular is proven
by counting actual Delta table versions.
"""

import uuid

import pytest

from bronze_ingest.config import IngestionConfig
from bronze_ingest.catalog_metadata import (
    apply_catalog_metadata, _current_table_comment, _current_column_comments, _quote,
)
from bronze_ingest.pipeline import BronzeIngestion


def _cfg(table, **overrides):
    return IngestionConfig(
        source_path="file:///dev/null",
        table=table,
        schema_name="default",
        catalog=None,
        enable_run_audit=False,
        enable_schema_registry=False,
        **overrides,
    )


def _make_table(spark, table):
    spark.createDataFrame(
        [(1, "a@b.com", "Alice")], "id INT, email STRING, name STRING"
    ).write.format("delta").mode("overwrite").saveAsTable(f"default.{table}")


def _versions(spark, table):
    return spark.sql(f"DESCRIBE HISTORY default.{table}").count()


# ---- unit ----

def test_quote_doubles_single_quotes():
    assert _quote("it's fine") == "it''s fine"
    assert _quote("plain") == "plain"


# ---- table comment ----

def test_applies_table_comment(spark):
    table = f"cm_tbl_{uuid.uuid4().hex[:8]}"
    _make_table(spark, table)
    cfg = _cfg(table, table_comment="Raw orders from SAP")

    result = apply_catalog_metadata(spark, cfg)

    assert result["table_comment_applied"] is True
    assert _current_table_comment(spark, cfg.full_table_name) == "Raw orders from SAP"


def test_table_comment_with_apostrophe_round_trips(spark):
    """A comment containing a single quote must not break the DDL - the
    escaping is what makes free-text config values safe here."""
    table = f"cm_quote_{uuid.uuid4().hex[:8]}"
    _make_table(spark, table)
    cfg = _cfg(table, table_comment="Customer's raw orders")

    apply_catalog_metadata(spark, cfg)

    assert _current_table_comment(spark, cfg.full_table_name) == "Customer's raw orders"


# ---- column comments ----

def test_applies_column_comments(spark):
    table = f"cm_col_{uuid.uuid4().hex[:8]}"
    _make_table(spark, table)
    cfg = _cfg(table, column_comments={"email": "Customer email", "name": "Full name"})

    result = apply_catalog_metadata(spark, cfg)

    assert sorted(result["columns_applied"]) == ["email", "name"]
    current = _current_column_comments(spark, cfg.full_table_name)
    assert current["email"] == "Customer email"
    assert current["name"] == "Full name"
    assert current["id"] is None  # untouched


def test_unknown_column_is_skipped_not_raised(spark):
    """Nested paths like 'customer.name' land here too - bronze preserves
    nested structures, so there is no top-level column by that name."""
    table = f"cm_unknown_{uuid.uuid4().hex[:8]}"
    _make_table(spark, table)
    cfg = _cfg(table, column_comments={"email": "ok", "customer.name": "nested", "nope": "x"})

    result = apply_catalog_metadata(spark, cfg)

    assert result["columns_applied"] == ["email"]
    assert sorted(result["columns_skipped"]) == ["customer.name", "nope"]


# ---- idempotency: the reason diff-and-apply exists ----

def test_reapplying_identical_comments_issues_no_ddl(spark):
    """
    Comment DDL bumps the Delta table version even when the comment is
    unchanged, so a blind re-apply would add junk versions on every
    ingestion run. Proven by version count, not by spying on SQL calls.
    """
    table = f"cm_idem_{uuid.uuid4().hex[:8]}"
    _make_table(spark, table)
    cfg = _cfg(table, table_comment="Orders", column_comments={"email": "Customer email"})

    apply_catalog_metadata(spark, cfg)
    after_first = _versions(spark, table)

    result = apply_catalog_metadata(spark, cfg)

    assert _versions(spark, table) == after_first  # no new versions
    assert result["table_comment_applied"] is False
    assert result["columns_applied"] == []


def test_changed_comment_is_reapplied(spark):
    table = f"cm_change_{uuid.uuid4().hex[:8]}"
    _make_table(spark, table)

    apply_catalog_metadata(spark, _cfg(table, table_comment="old", column_comments={"email": "old"}))
    changed = _cfg(table, table_comment="new", column_comments={"email": "new"})
    result = apply_catalog_metadata(spark, changed)

    assert result["table_comment_applied"] is True
    assert result["columns_applied"] == ["email"]
    assert _current_table_comment(spark, changed.full_table_name) == "new"
    assert _current_column_comments(spark, changed.full_table_name)["email"] == "new"


# ---- no-op / failure paths ----

def test_noop_when_nothing_configured(spark):
    table = f"cm_noop_{uuid.uuid4().hex[:8]}"
    _make_table(spark, table)
    before = _versions(spark, table)

    result = apply_catalog_metadata(spark, _cfg(table))

    assert result == {"table_comment_applied": False, "columns_applied": [], "columns_skipped": []}
    assert _versions(spark, table) == before


def test_missing_table_does_not_raise(spark):
    cfg = _cfg(f"cm_absent_{uuid.uuid4().hex[:8]}", table_comment="x")
    result = apply_catalog_metadata(spark, cfg)
    assert result["table_comment_applied"] is False


def test_never_raises_on_broken_spark(spark):
    """Catalog documentation failing must never fail the ingestion run."""
    class Boom:
        @property
        def catalog(self):
            raise RuntimeError("catalog unavailable")

    result = apply_catalog_metadata(Boom(), _cfg("t", table_comment="x"))
    assert result["table_comment_applied"] is False


# ---- config validation ----

def test_blank_column_comment_key_rejected():
    with pytest.raises(ValueError):
        IngestionConfig(source_path="s3://x", table="t", column_comments={"  ": "c"})


# ---- pipeline integration ----

def test_pipeline_applies_comments_after_write(spark, tmp_path):
    src = tmp_path / "orders.json"
    src.write_text('{"id": 1, "email": "a@b.com"}\n')

    table = f"cm_pipe_{uuid.uuid4().hex[:8]}"
    cfg = IngestionConfig(
        source_path=f"file://{src}",
        table=table,
        schema_name="default",
        catalog=None,
        multiline=False,
        enable_run_audit=False,
        enable_schema_registry=False,
        table_comment="Bronze orders",
        column_comments={"email": "Customer email"},
    )

    BronzeIngestion(spark, cfg).run()

    assert _current_table_comment(spark, cfg.full_table_name) == "Bronze orders"
    assert _current_column_comments(spark, cfg.full_table_name)["email"] == "Customer email"
