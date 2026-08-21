"""
Behavioral contract for Delta-safe identifiers on every read path (#374).

The pure tests come first and need no SparkSession, which matters here:
the Spark suite reports NOT RUN on Windows (see AGENTS.md), so the rule
itself has to be assertable without Java. The Spark tests below cover what
a pure function cannot - that the projection actually renames nested
fields and preserves their values.
"""

import pytest

from bronze_ingest.identifiers import (
    ON_COLLISION_DISAMBIGUATE,
    ON_COLLISION_ERROR,
    IdentifierCollisionError,
    _resolve_names,
    canonical_name,
    canonicalize_identifiers,
    needs_canonicalization,
)


class _Field:
    """The only thing `_resolve_names` reads off a StructField is `.name`."""

    def __init__(self, name):
        self.name = name


def _resolve(names, on_collision=ON_COLLISION_DISAMBIGUATE):
    return _resolve_names([_Field(name) for name in names], on_collision, ())


# --- the rule -------------------------------------------------------------


@pytest.mark.parametrize(
    "source, expected",
    [
        # The characters Delta names in DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES,
        # which is the whole reason this exists.
        ("Material Number", "Material_Number"),
        ("Gross Weight (KG)", "Gross_Weight__KG_"),
        ("a,b", "a_b"),
        ("a;b", "a_b"),
        ("a{b}", "a_b_"),
        ("a=b", "a_b"),
        ("a\tb", "a_b"),
        ("a\nb", "a_b"),
        # Not Delta-invalid, but awkward enough in a column reference to be
        # worth normalising, and required for EC-12 to collide at all.
        ("Plant/Site", "Plant_Site"),
        ("unit.of.measure", "unit_of_measure"),
        ("50%_moisture", "50__moisture"),
        ("column-with-dash", "column_with_dash"),
    ],
)
def test_ec11_field_names_become_delta_safe(source, expected):
    assert canonical_name(source) == expected


def test_a_name_that_is_already_safe_is_returned_unchanged():
    for name in ("MATNR", "order_id", "_input_file_name", "_rescued_data", "col2"):
        assert canonical_name(name) == name


def test_unicode_letters_survive_because_delta_accepts_them():
    # EC-11's umlaut. `Ä` is a letter, so mangling it would lose
    # information Delta was never going to object to.
    assert canonical_name("Änderungsdatum") == "Änderungsdatum"
    assert canonical_name("Änderungs datum") == "Änderungs_datum"


def test_the_xml_namespace_separator_is_still_special_cased_first():
    # `:` -> `__`, not `_`. If it collapsed to a single underscore, `a:id`
    # and a sibling literally named `a_id` would silently become one column.
    assert canonical_name("a:id") == "a__id"
    assert canonical_name("a:id") != canonical_name("a_id")


def test_the_rule_is_idempotent():
    for name in ("Gross Weight (KG)", "a:id", "unit.of.measure", "50%_moisture", "Ä b"):
        once = canonical_name(name)
        assert canonical_name(once) == once


def test_underscore_runs_are_not_collapsed_and_edges_are_not_stripped():
    # Collapsing would undo the `:` -> `__` rule; stripping would rename
    # the lineage columns this package adds itself.
    assert canonical_name("a  b") == "a__b"
    assert canonical_name("_input_file_name") == "_input_file_name"
    assert canonical_name("trailing ") == "trailing_"


def test_needs_canonicalization_only_fires_on_names_that_change():
    assert not needs_canonicalization([_Field("MATNR"), _Field("_input_file_name")])
    assert needs_canonicalization([_Field("MATNR"), _Field("Gross Weight (KG)")])


# --- collisions -----------------------------------------------------------


def test_ec12_collisions_are_disambiguated_rather_than_last_wins():
    # The EC-12 fixture: three source names, two of which collapse.
    assert _resolve(["Order Id", "Order.Id", "order_id"]) == [
        "Order_Id",
        "Order_Id_2",
        "order_id",
    ]


def test_disambiguation_keeps_every_field():
    names = ["a b", "a.b", "a-b", "a/b"]
    resolved = _resolve(names)
    assert len(resolved) == len(names)
    assert len(set(resolved)) == len(names)
    assert resolved == ["a_b", "a_b_2", "a_b_3", "a_b_4"]


def test_a_generated_suffix_never_steals_a_name_a_real_field_already_has():
    # `Order.Id` would naturally become `Order_Id_2`, which the third field
    # is genuinely called. It must skip to `_3` instead of colliding.
    assert _resolve(["Order Id", "Order.Id", "Order_Id_2"]) == [
        "Order_Id",
        "Order_Id_3",
        "Order_Id_2",
    ]


def test_disambiguation_is_deterministic_in_source_field_order():
    assert _resolve(["a.b", "a b"]) == ["a_b", "a_b_2"]
    assert _resolve(["a b", "a.b"]) == ["a_b", "a_b_2"]


def test_the_error_policy_raises_instead_of_renaming():
    with pytest.raises(IdentifierCollisionError, match="Order Id.*Order.Id.*Order_Id"):
        _resolve(["Order Id", "Order.Id"], on_collision=ON_COLLISION_ERROR)


def test_names_that_do_not_collide_pass_through_under_both_policies():
    names = ["MATNR", "Gross Weight (KG)"]
    expected = ["MATNR", "Gross_Weight__KG_"]
    assert _resolve(names) == expected
    assert _resolve(names, on_collision=ON_COLLISION_ERROR) == expected


def test_an_unknown_collision_policy_is_refused():
    # Checked before the frame is touched at all, so this needs no Spark -
    # a typo'd policy must not read as "no policy" and quietly last-win.
    with pytest.raises(ValueError, match="on_collision"):
        canonicalize_identifiers(None, on_collision="last_wins")


# --- the projection -------------------------------------------------------


def test_top_level_names_are_renamed_and_values_preserved(spark):
    frame = spark.createDataFrame(
        [("M1", 25.0, "1010")],
        "`Material Number` string, `Gross Weight (KG)` double, `Plant/Site` string",
    )

    result = canonicalize_identifiers(frame)

    assert result.columns == ["Material_Number", "Gross_Weight__KG_", "Plant_Site"]
    row = result.collect()[0]
    assert row["Material_Number"] == "M1"
    assert row["Gross_Weight__KG_"] == 25.0
    assert row["Plant_Site"] == "1010"


def test_nested_struct_field_names_are_renamed_too(spark):
    frame = spark.createDataFrame(
        [(("A1", "B2"),)],
        "`order header` struct<`Order Id`: string, `Plant/Site`: string>",
    )

    result = canonicalize_identifiers(frame)
    row = result.collect()[0]

    assert result.columns == ["order_header"]
    assert row["order_header"]["Order_Id"] == "A1"
    assert row["order_header"]["Plant_Site"] == "B2"


def test_a_null_struct_stays_null_rather_than_a_struct_of_nulls(spark):
    frame = spark.createDataFrame([(None,)], "`order header` struct<`Order Id`: string>")

    assert canonicalize_identifiers(frame).collect()[0]["order_header"] is None


def test_names_inside_an_array_of_structs_are_renamed(spark):
    frame = spark.createDataFrame(
        [([("A1",), ("B2",)],)],
        "`line items` array<struct<`Item No`: string>>",
    )

    row = canonicalize_identifiers(frame).collect()[0]

    assert [item["Item_No"] for item in row["line_items"]] == ["A1", "B2"]


def test_a_collision_inside_a_nested_struct_is_disambiguated(spark):
    frame = spark.createDataFrame(
        [(("A1", "B2"),)],
        "header struct<`Order Id`: string, `Order.Id`: string>",
    )

    row = canonicalize_identifiers(frame).collect()[0]

    assert row["header"]["Order_Id"] == "A1"
    assert row["header"]["Order_Id_2"] == "B2"


def test_a_collision_inside_a_nested_struct_raises_under_the_error_policy(spark):
    frame = spark.createDataFrame(
        [(("A1", "B2"),)],
        "header struct<`Order Id`: string, `Order.Id`: string>",
    )

    with pytest.raises(IdentifierCollisionError, match=r"header.*Order Id.*Order_Id"):
        canonicalize_identifiers(frame, on_collision=ON_COLLISION_ERROR)


def test_a_frame_that_is_already_canonical_is_returned_with_the_same_shape(spark):
    frame = spark.createDataFrame([("M1", 1)], "MATNR string, qty int")

    result = canonicalize_identifiers(frame)

    assert result.columns == frame.columns
    assert result.collect() == frame.collect()
