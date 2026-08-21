"""
Tests for the source-format registry (#302).

Like `test_contract.py`, none of this needs a SparkSession - the registry is
pure data and four lookups over it. That matters in this repo, where the
Spark-backed suite cannot run on the maintainer's machine at all, so a test
that runs anywhere is a test that gets run before CI.
"""

from dataclasses import FrozenInstanceError

import pytest

from bronze_ingest import formats
from bronze_ingest.formats import (
    FORMATS,
    FormatSpec,
    allowed_reader_options,
    extensions_for,
    spec_for,
    supported_formats,
)

#: A verbatim copy of `config.ALLOWED_READER_OPTIONS` as it stood before the
#: registry existed (config.py:38-73 at 9db7fa9).
#:
#: Hardcoded on purpose. Importing the real one would make this test assert
#: that two expressions agree, which they would even if both were wrong - and
#: the failure this guards against is precisely a key going missing during the
#: split. `reader_options` keys outside the allowlist are REFUSED, not ignored,
#: so a dropped key turns a config file that loads today into one that raises,
#: which `CONTRIBUTING.md` forbids ("Config additions to IngestionConfig should
#: be additive with sane defaults - never break existing config files").
#:
#: If a future format needs one of these moved out of json's set, this list
#: does not change: it is a historical record of what json accepted, and json
#: must keep accepting all of it.
PRE_SPLIT_ALLOWED_READER_OPTIONS = frozenset(
    {
        # Parsing behaviour
        "multiLine",
        "mode",
        "columnNameOfCorruptRecord",
        "primitivesAsString",
        "prefersDecimal",
        "allowComments",
        "allowUnquotedFieldNames",
        "allowSingleQuotes",
        "allowNumericLeadingZeros",
        "allowBackslashEscapingAnyCharacter",
        "allowUnquotedControlChars",
        "dropFieldIfAllNull",
        "ignoreNullFields",
        "samplingRatio",
        "rescuedDataColumn",
        "inferTimestamp",
        "enableDateTimeParsingFallback",
        # Formats, encoding, locale
        "dateFormat",
        "timestampFormat",
        "timestampNTZFormat",
        "timeZone",
        "locale",
        "encoding",
        "charset",
        "lineSep",
        # File selection
        "recursiveFileLookup",
        "pathGlobFilter",
        "modifiedBefore",
        "modifiedAfter",
    }
)


# --- the load-bearing one -------------------------------------------------


def test_json_still_accepts_every_reader_option_it_accepted_before_the_split():
    """
    The whole point of the module. Everything else here is a detail.
    """
    assert allowed_reader_options("json") == PRE_SPLIT_ALLOWED_READER_OPTIONS


def test_the_split_sets_do_not_overlap():
    """
    "Which set is this key in?" must have exactly one answer, or a later
    format that wants a common option gets it from the wrong place and the
    union identity above starts passing for the wrong reason.
    """
    assert not (formats._COMMON_READER_OPTIONS & formats._JSON_ONLY_READER_OPTIONS)


def test_config_and_the_registry_agree_on_what_json_accepts():
    """
    `config.ALLOWED_READER_OPTIONS` outlives the split as an alias so existing
    importers keep working. An alias that has quietly stopped aliasing is
    worse than no alias, because every caller still reads as if it does.
    """
    from bronze_ingest.config import ALLOWED_READER_OPTIONS

    assert ALLOWED_READER_OPTIONS == allowed_reader_options("json")


# --- lookups --------------------------------------------------------------


def test_batch_formats_are_registered():
    assert supported_formats() == ("csv", "json", "parquet", "xml")


def test_csv_format_contract():
    spec = spec_for("csv")
    assert spec.extensions == (".csv",)
    assert spec.cloudfiles_format == "csv"
    assert {"sep", "quote", "escape", "multiLine"} <= spec.allowed_reader_options


def test_parquet_format_contract():
    spec = spec_for("parquet")
    assert spec.extensions == (".parquet",)
    assert spec.cloudfiles_format == "parquet"
    assert {"mergeSchema", "datetimeRebaseMode", "int96RebaseMode"} <= (spec.allowed_reader_options)


def test_xml_format_contract_keeps_safety_options_out_of_generic_overrides():
    spec = spec_for("xml")
    assert spec.extensions == (".xml",)
    assert spec.cloudfiles_format == "xml"
    assert "rowTag" not in spec.allowed_reader_options
    assert "mode" not in spec.allowed_reader_options
    assert "ignoreNamespace" not in spec.allowed_reader_options


def test_spec_for_unregistered_format_names_what_is_supported():
    """
    ValueError, not KeyError, and it must say what the caller could have
    written instead - this surfaces from config validation, where the cause is
    a typo or a format that is designed but not built yet.
    """
    with pytest.raises(ValueError) as excinfo:
        spec_for("avro")

    message = str(excinfo.value)
    assert "avro" in message
    assert "json" in message


@pytest.mark.parametrize("lookup", [spec_for, extensions_for, allowed_reader_options])
def test_every_lookup_rejects_an_unregistered_format(lookup):
    """All three go through `spec_for`, so none can quietly return a default."""
    with pytest.raises(ValueError):
        lookup("avro")


def test_json_discovers_the_two_extensions_discovery_has_always_listed():
    """
    Pinned deliberately. `.ndjson` is recognised by
    `json_reader.JSON_LINES_EXTENSIONS` when deciding multiLine, but discovery
    has never listed it - widening that here would silently start ingesting
    files that today sit untouched beside the data.
    """
    assert extensions_for("json") == (".json", ".jsonl")


def test_json_carries_its_auto_loader_format_name():
    """
    Unused today (streaming multi-format is deferred), so nothing else would
    notice if it were wrong until the streaming issue is picked up.
    """
    assert spec_for("json").cloudfiles_format == "json"


# --- invariants that must hold as formats are added -----------------------


def test_every_spec_is_registered_under_its_own_name():
    for key, spec in FORMATS.items():
        assert spec.name == key


def test_every_extension_is_lowercase_with_a_leading_dot():
    """
    Discovery lowercases the filename before comparing, so an uppercase entry
    here would match nothing and the format would appear to work while
    discovering zero files.
    """
    for spec in FORMATS.values():
        assert spec.extensions, f"{spec.name} declares no extensions"
        for extension in spec.extensions:
            assert extension.startswith(".")
            assert extension == extension.lower()


def test_no_two_formats_claim_the_same_extension():
    """
    `architecture.md` rules out mixed-format folders and per-file routing, so
    an extension maps to exactly one format. Two claimants would make
    discovery's result depend on which format the config happened to name.
    """
    seen = {}
    for spec in FORMATS.values():
        for extension in spec.extensions:
            assert extension not in seen, (
                f"{extension} claimed by both {seen.get(extension)} and {spec.name}"
            )
            seen[extension] = spec.name


def test_no_format_may_allowlist_an_option_that_can_redirect_the_read():
    """
    The reason the allowlist exists at all (config.py:29-37): configs load
    from a Volume, and `path` is a reader option, so an unfiltered passthrough
    lets a config redirect a read while every log line and audit row still
    reports `source_path`. Parsing options cannot do that; these can.
    """
    for spec in FORMATS.values():
        assert not (spec.allowed_reader_options & {"path", "paths", "basePath"})


def test_specs_are_immutable():
    """A mutated spec would change what every later read in the process
    discovers, with nothing in the audit trail to show it."""
    with pytest.raises(FrozenInstanceError):
        spec_for("json").extensions = (".txt",)  # type: ignore[misc]


def test_format_spec_is_constructible_directly():
    """Guards the field names the registry entries depend on."""
    spec = FormatSpec(
        name="example",
        extensions=(".example",),
        cloudfiles_format="example",
        allowed_reader_options=frozenset({"mode"}),
    )
    assert spec.name == "example"
    assert spec.allowed_reader_options == frozenset({"mode"})
