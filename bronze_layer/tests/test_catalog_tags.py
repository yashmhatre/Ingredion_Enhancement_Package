"""
Unity Catalog tag application (#64).

Every test here runs with no Spark, no SDK, no workspace and no credentials -
which is the entire argument for choosing the REST transport over
`ALTER TABLE ... SET TAGS` in `docs/decisions/2026-08_uc_tag_mechanism.md`. A
DDL implementation could not be covered this way at all.
"""

import pytest

from bronze_ingest.catalog_metadata import (
    GOVERNED_TAG_PREFIXES,
    apply_catalog_tags,
    apply_reviewed_tags,
    is_governed_tag,
)
from bronze_ingest.config import IngestionConfig


class FakeTagClient:
    """A dict with a TagClient shape. Records calls so a test can assert that
    an unchanged tag issues no write at all."""

    def __init__(self, existing=None, fail_on=None):
        self.tags = dict(existing or {})
        self.sets = []
        self.deletes = []
        self._fail_on = fail_on or ()

    def list_tags(self, entity_type, entity_name):
        return dict(self.tags.get((entity_type, entity_name), {}))

    def set_tag(self, entity_type, entity_name, key, value):
        if key in self._fail_on:
            raise RuntimeError(f"simulated tag failure for {key}")
        self.sets.append((entity_type, entity_name, key, value))
        self.tags.setdefault((entity_type, entity_name), {})[key] = value

    def delete_tag(self, entity_type, entity_name, key):
        self.deletes.append((entity_type, entity_name, key))
        self.tags.get((entity_type, entity_name), {}).pop(key, None)


def _cfg(**overrides):
    return IngestionConfig(
        source_path="x", table="t", schema_name="sch", catalog="cat", **overrides
    )


# ---------------------------------------------------------------------------
# The governed-key gate — the part that guards data access
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key,governed",
    [
        ("class.email_address", True),
        ("class.credit_card", True),
        ("system.certification_status", True),
        ("ai.model_family", True),
        ("sap.PersonalData.entitySemantics", True),
        ("team", False),
        ("cost_center", False),
        ("classification_note", False),  # 'class' without the dot is not governed
    ],
)
def test_is_governed_tag(key, governed):
    assert is_governed_tag(key) is governed


def test_config_declared_governed_tags_are_refused_not_applied(caplog):
    """The privilege-escalation path this gate closes: config loads from a
    Volume (#154's wider trust boundary), and a governed key can carry an ABAC
    policy - so a YAML file could otherwise silently change who can read the
    data."""
    client = FakeTagClient()
    cfg = _cfg(table_tags={"class.email_address": "true", "team": "bronze"})

    result = apply_catalog_tags(None, cfg, client=client)

    applied = {key for _, _, key, _ in client.sets}
    assert applied == {"team"}, "the free-form tag applies, the governed one does not"
    assert result["tags_skipped_governed"] == ["cat.sch.t:class.email_address"]
    assert "Refusing to apply governed tag key(s)" in caplog.text


def test_a_governed_key_in_config_does_not_stop_the_run(caplog):
    """Refused, not raised. Failing here would make the safe path the one that
    stops data flowing, which is how gates get disabled."""
    client = FakeTagClient()
    cfg = _cfg(table_tags={"class.name": "true"})

    result = apply_catalog_tags(None, cfg, client=client)

    assert result["tags_applied"] == 0
    assert client.sets == []


def test_governed_prefixes_cover_the_taxonomy_that_exists():
    """Verified live 2026-08-12: 63 of the workspace's 70 tag policies are
    `class.*`, the rest are `system.*`, `ai.*` and `sap.*`."""
    assert set(GOVERNED_TAG_PREFIXES) == {"class.", "system.", "ai.", "sap."}


# ---------------------------------------------------------------------------
# Diff-then-apply
# ---------------------------------------------------------------------------


def test_unchanged_tags_issue_no_write():
    """#64's acceptance criterion: re-running with unchanged tags does
    nothing. The COMMENT machinery this mirrors exists because re-stamping
    unchanged values appended junk versions to table history."""
    client = FakeTagClient(existing={("tables", "cat.sch.t"): {"team": "bronze"}})
    cfg = _cfg(table_tags={"team": "bronze"})

    result = apply_catalog_tags(None, cfg, client=client)

    assert client.sets == []
    assert result["tags_applied"] == 0


def test_a_changed_value_is_written():
    client = FakeTagClient(existing={("tables", "cat.sch.t"): {"team": "silver"}})
    cfg = _cfg(table_tags={"team": "bronze"})

    result = apply_catalog_tags(None, cfg, client=client)

    assert client.sets == [("tables", "cat.sch.t", "team", "bronze")]
    assert result["tags_applied"] == 1


def test_tags_the_config_does_not_mention_are_left_alone():
    """Config declaring fewer tags than the catalog holds means 'these are the
    ones I manage', not 'delete everything else'. A table may carry tags from a
    governance process this pipeline knows nothing about."""
    client = FakeTagClient(
        existing={("tables", "cat.sch.t"): {"team": "bronze", "owner": "governance"}}
    )
    cfg = _cfg(table_tags={"team": "bronze"})

    apply_catalog_tags(None, cfg, client=client)

    assert client.deletes == []
    assert client.tags[("tables", "cat.sch.t")]["owner"] == "governance"


def test_column_tags_use_a_four_part_fqn():
    """Verified against the workspace: a column is addressed as
    catalog.schema.table.column, not a table plus a column argument."""
    client = FakeTagClient()
    cfg = _cfg(column_tags={"order_id": {"pii": "false"}})

    apply_catalog_tags(None, cfg, client=client)

    assert client.sets == [("columns", "cat.sch.t.order_id", "pii", "false")]


def test_no_tags_configured_touches_nothing():
    client = FakeTagClient()

    result = apply_catalog_tags(None, _cfg(), client=client)

    assert result == {"tags_applied": 0, "tags_skipped_governed": []}
    assert client.sets == []


def test_a_failing_write_never_raises(caplog):
    """Governance metadata failing must not fail the ingestion run that
    produced the data - the same contract audit.py and schema_registry.py
    keep."""
    client = FakeTagClient(fail_on=("team",))
    cfg = _cfg(table_tags={"team": "bronze"})

    result = apply_catalog_tags(None, cfg, client=client)

    assert result["tags_applied"] == 0
    assert "Tagging failed" in caplog.text


# ---------------------------------------------------------------------------
# apply_reviewed_tags — the only path governed keys travel
# ---------------------------------------------------------------------------


def test_reviewed_tags_may_carry_governed_keys():
    """The distinction the whole design rests on: the same key config refuses
    is allowed here, because 'reviewed' means a human or an explicit promotion
    step approved it."""
    client = FakeTagClient()
    cfg = _cfg()

    result = apply_reviewed_tags(
        None, cfg, {"cat.sch.t.email": {"class.email_address": "true"}}, client=client
    )

    assert client.sets == [("columns", "cat.sch.t.email", "class.email_address", "true")]
    assert result["tags_applied"] == 1


def test_reviewed_tags_distinguish_table_from_column_entities():
    client = FakeTagClient()

    apply_reviewed_tags(
        None,
        _cfg(),
        {"cat.sch.t": {"class.name": "true"}, "cat.sch.t.email": {"class.email_address": "true"}},
        client=client,
    )

    entity_types = {entity_type for entity_type, _, _, _ in client.sets}
    assert entity_types == {"tables", "columns"}


def test_reviewed_tags_are_also_diffed():
    client = FakeTagClient(
        existing={("columns", "cat.sch.t.email"): {"class.email_address": "true"}}
    )

    result = apply_reviewed_tags(
        None, _cfg(), {"cat.sch.t.email": {"class.email_address": "true"}}, client=client
    )

    assert client.sets == []
    assert result["tags_applied"] == 0


def test_empty_review_is_a_noop():
    client = FakeTagClient()

    assert apply_reviewed_tags(None, _cfg(), {}, client=client)["tags_applied"] == 0
    assert client.sets == []
