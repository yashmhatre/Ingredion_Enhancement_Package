"""
Which source formats exist, and what each one implies.

One table, four consumers. `bronze_layer/docs/architecture.md` fixes the rule
this module encodes: `source_format` decides BOTH what gets discovered and how
it gets read. Those are two different modules (`fs/discovery` and the readers),
and before this existed they each carried their own hardcoded idea of what
"JSON" meant - an extension tuple repeated at three places in `fs/discovery`,
and a format string in `json_reader`. Adding a second format to that shape
means adding a second copy of each, which is how the two drift apart.

So the facts about a format live here, once, and everything else asks:

    config.py          -> allowed_reader_options(fmt)   validate reader_options
    fs/discovery.py    -> extensions_for(fmt)           what to list
    readers.py         -> FORMATS[fmt]                  what exists at all
    streaming_reader   -> spec.cloudfiles_format        (deferred, see below)

**This module must stay a leaf.** It imports nothing from the package, and
nothing here may. `config.py` needs the per-format reader-options allowlist at
validation time, and every reader imports `config.py` - so if the tables lived
beside the reader imports (in `readers.py`, the obvious other home), config
could not read them without a cycle. The registry is data; the dispatch that
maps a format to a callable is `readers.py`. Keeping those apart is what makes
the data importable from anywhere.

Adding a format is therefore two edits, in two issues: an entry here, and a
line in `readers._BATCH_READERS`. Neither alone does anything - config cannot
accept a format with no reader, and no reader is reachable without an entry
here - which is what lets a format land across several independently
mergeable PRs without ever being half-wired.
"""

from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional, Tuple

#: Reader options that mean the same thing regardless of format, and are safe
#: for all of them. Split out of the single JSON-era allowlist that lived in
#: `config.ALLOWED_READER_OPTIONS`; see the note on `_JSON_ONLY_READER_OPTIONS`
#: for why the split is expressed as two sets rather than one per format.
#:
#: `multiLine` is deliberately here rather than JSON-only. Both formats accept
#: the option, but they do not mean the same thing by it: for JSON it is "this
#: file is one document, not one per line", for CSV it is "a quoted field may
#: contain a newline". It is allowlisted for both because the operator may
#: legitimately need either; what must never happen is `config.multiline` (the
#: JSON config field) being plumbed into a CSV read. That is a reader concern,
#: not an allowlist concern.
_COMMON_READER_OPTIONS = frozenset(
    {
        # Parsing behaviour
        "mode",
        "columnNameOfCorruptRecord",
        "rescuedDataColumn",
        "multiLine",
        "samplingRatio",
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
        # File selection - these narrow what is read, they cannot redirect it
        "recursiveFileLookup",
        "pathGlobFilter",
        "modifiedBefore",
        "modifiedAfter",
    }
)

#: Options that only Spark's JSON parser understands.
#:
#: The split is two sets, not one flat set per format, for one reason: the
#: union of these two must equal the pre-split allowlist EXACTLY. Anything
#: dropped in the move silently tightens validation on a config file that
#: loads today, which `CONTRIBUTING.md` forbids ("never break existing config
#: files"). Two sets and a union make that a checkable identity - see
#: `test_formats.py`, which asserts it against a hardcoded copy of the
#: original literal rather than against an import, so the test cannot pass by
#: agreeing with a mistake.
_JSON_ONLY_READER_OPTIONS = frozenset(
    {
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
        "inferTimestamp",
    }
)

_CSV_ONLY_READER_OPTIONS = frozenset(
    {
        "sep",
        "delimiter",
        "quote",
        "escape",
        "comment",
        "ignoreLeadingWhiteSpace",
        "ignoreTrailingWhiteSpace",
        "nullValue",
        "emptyValue",
        "nanValue",
        "positiveInf",
        "negativeInf",
        "maxColumns",
        "maxCharsPerColumn",
        "unescapedQuoteHandling",
    }
)

_PARQUET_READER_OPTIONS = frozenset(
    {
        "mergeSchema",
        "datetimeRebaseMode",
        "int96RebaseMode",
        "recursiveFileLookup",
        "pathGlobFilter",
        "modifiedBefore",
        "modifiedAfter",
    }
)

_XML_READER_OPTIONS = frozenset(
    {
        "samplingRatio",
        "excludeAttribute",
        "attributePrefix",
        "valueTag",
        "wildcardColName",
        "rowValidationXSDPath",
        "dateFormat",
        "timestampFormat",
        "timestampNTZFormat",
        "timeZone",
        "locale",
        "encoding",
        "charset",
        "recursiveFileLookup",
        "pathGlobFilter",
        "modifiedBefore",
        "modifiedAfter",
    }
)


@dataclass(frozen=True)
class FormatSpec:
    """
    Everything the package knows about one source format.

    Frozen because this is a lookup table, not state: a caller that mutated a
    spec would change what every later read discovers, from anywhere in the
    process, with nothing in the audit trail to show it.
    """

    #: The `source_format` config value, and the key under which this is
    #: registered. Stored on the spec too so a spec passed around alone still
    #: knows what it is.
    name: str

    #: Lowercase file extensions discovery will list for this format, leading
    #: dot included. A file that matches no registered format's extensions is
    #: INVISIBLE, not an error - `notes.txt` sitting beside the data today is
    #: already treated that way, and `architecture.md` generalises exactly that
    #: behaviour rather than inventing a new one.
    extensions: Tuple[str, ...]

    #: The value for Auto Loader's `cloudFiles.format`.
    #:
    #: Populated but UNUSED today: streaming multi-format was deliberately
    #: deferred out of the multi-format epic, so `streaming_reader` still
    #: hardcodes "json". It is recorded here rather than left for later
    #: because it is a fact about the format, known now, and splitting the
    #: facts about a format across two modules by which one happens to be
    #: wired yet is how they drift. The deferred issue wires it up.
    cloudfiles_format: str

    #: Reader options accepted for this format, checked by
    #: `config._validate_reader_options`. Options outside this set are refused
    #: rather than passed to Spark - configs load from a Volume, so anyone with
    #: WRITE VOLUME can influence them, and `path` is a reader option, meaning
    #: an unfiltered passthrough could redirect a read while every log line and
    #: audit row still reported `source_path`.
    allowed_reader_options: FrozenSet[str]


#: The registry. Order is not significant; `supported_formats()` sorts.
FORMATS: Dict[str, FormatSpec] = {
    "csv": FormatSpec(
        name="csv",
        extensions=(".csv",),
        cloudfiles_format="csv",
        allowed_reader_options=_COMMON_READER_OPTIONS | _CSV_ONLY_READER_OPTIONS,
    ),
    "json": FormatSpec(
        name="json",
        # `.json` and `.jsonl` only - NOT `.ndjson`, even though
        # `json_reader.JSON_LINES_EXTENSIONS` recognises it when deciding
        # multiLine. That asymmetry is pre-existing behaviour: discovery has
        # never listed `.ndjson`, and widening it here would silently start
        # ingesting files that today sit untouched beside the data. Widening
        # discovery is a change with a blast radius, and it is not this one.
        extensions=(".json", ".jsonl"),
        cloudfiles_format="json",
        allowed_reader_options=_COMMON_READER_OPTIONS | _JSON_ONLY_READER_OPTIONS,
    ),
    "parquet": FormatSpec(
        name="parquet",
        extensions=(".parquet",),
        cloudfiles_format="parquet",
        allowed_reader_options=_PARQUET_READER_OPTIONS,
    ),
    "xml": FormatSpec(
        name="xml",
        extensions=(".xml",),
        cloudfiles_format="xml",
        allowed_reader_options=_XML_READER_OPTIONS,
    ),
}


#: Formats that are registered, implemented and tested, but whose governing
#: decision records have not been signed off yet. `config.py` refuses them; the
#: notebooks do not offer them.
#:
#: Empty here on purpose: this change ships the mechanism, not a gated format.
#: The consumers land next (the notebooks build their dropdown from
#: `approved_formats()`), and the first entry goes in after them, so no
#: intermediate commit leaves a format half-gated - refused by config while a
#: widget still offers it.
#:
#: It exists because "blocked" has been written only in prose. `CHANGELOG.md`
#: and `architecture.md` both say XML is blocked pending Tier 2 sign-off on
#: #336/#337, while `FORMATS` registers it, `readers` dispatches it, and both
#: notebook dropdowns offer it. A block that lives in prose is not a block.
#:
#: The gate is here rather than in `config.py` for the same reason the rest of
#: this module is here: it is a fact about a format, and facts about a format
#: live in one place or they drift. Keeping it beside `FORMATS` also means the
#: reader, its tests and its allowlist stay exactly where they are - lifting
#: the gate is deleting an entry from this dict, not re-landing the format.
#:
#: The value is the operator-facing reason, so the error names what is pending
#: rather than only saying no.
_PENDING_SIGNOFF: Dict[str, str] = {}


def supported_formats() -> Tuple[str, ...]:
    """Registered `source_format` values, sorted, for error messages and for
    pinning the notebooks' format dropdown to the registry."""
    return tuple(sorted(FORMATS))


def approved_formats() -> Tuple[str, ...]:
    """
    Registered formats an operator may actually select, sorted.

    `supported_formats()` answers "what does this package know how to read?"
    and stays the right answer for parity tests and for the reader dispatch.
    This answers the narrower question the notebooks and the bundle need:
    "what may be run today?" The two differ only while a format is awaiting
    sign-off, which is exactly when offering the wider set is wrong.
    """
    return tuple(f for f in supported_formats() if f not in _PENDING_SIGNOFF)


def signoff_blocker(source_format: str) -> Optional[str]:
    """
    Why `source_format` may not be used yet, or `None` when it is approved.

    Returns `None` for an unregistered format too: that is `spec_for`'s error
    to raise, and reporting a sign-off blocker for a typo would name the wrong
    problem.
    """
    return _PENDING_SIGNOFF.get(source_format)


def spec_for(source_format: str) -> FormatSpec:
    """
    The `FormatSpec` for `source_format`.

    Raises `ValueError` naming what IS supported, rather than `KeyError`: this
    is reached from config validation, where the cause is a typo or an
    unbuilt format in a config file, and the fix is a value the caller cannot
    guess from a bare key error.
    """
    try:
        return FORMATS[source_format]
    except KeyError:
        raise ValueError(
            f"source_format must be one of {supported_formats()}, got {source_format!r}"
        ) from None


def extensions_for(source_format: str) -> Tuple[str, ...]:
    """File extensions discovery should list for `source_format`."""
    return spec_for(source_format).extensions


def allowed_reader_options(source_format: str) -> FrozenSet[str]:
    """Reader options accepted for `source_format`."""
    return spec_for(source_format).allowed_reader_options
