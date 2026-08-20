"""
Delta-safe column identifiers, for every read path rather than just XML.

Delta rejects a column whose name contains any of ` ,;{}()\\n\\t=`, and the
error it raises names only the offending columns - not the package, not the
file, not what an operator should change:

    [DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES] Found invalid character(s)
    among ' ,;{}()\\n\\t=' in the column names of your schema.

Field names like `Gross Weight (KG)` are ordinary in real extracts - Excel
exports, BW queries, long-text descriptions - so until #374 every one of
them was an ingestion failure at the WRITE, three retries deep, for a
source that had already been read successfully.

Canonicalization lived in `xml_reader` and nowhere else, so the guarantee
held for XML and for no other format. This module is that logic, lifted
out and generalised; `readers.read_source` applies it to whatever the
dispatched reader returned, so JSON, CSV, Parquet and XML all get the same
identifiers from the same code.

## The rule

1. `:` becomes `__`. This is XML's namespace separator and it is special-
   cased FIRST so `a:id` and a sibling literally named `a__id` still
   collide rather than one silently shadowing the other - the property
   `xml_reader` has always had, preserved exactly.
2. Every remaining character that is not alphanumeric or `_` becomes `_`.
   Alphanumeric is Python's `str.isalnum()`, which is Unicode-aware: an
   umlaut is a letter, so `Änderungsdatum` survives intact. Delta accepts
   Unicode letters; mangling them would lose information for no gain.
3. That is all. Runs of `_` are NOT collapsed and leading/trailing `_` is
   NOT stripped - both would undo step 1 (`a__id` -> `a_id`) and strip the
   leading underscore off the lineage columns (`_input_file_name`) this
   package adds itself.

The rule is idempotent: a name that is already alphanumeric-plus-underscore
passes through untouched, which is the overwhelmingly common case. Only a
name Delta or a namespace would have made trouble with changes at all.

## Collisions

Two distinct source names can canonicalize to one identifier - `Order Id`
and `Order.Id` both become `Order_Id`. Silently keeping the last one is
data loss, so that never happens. What happens instead depends on the
source, and the split is deliberate:

- `on_collision="error"` (XML): raise. An XML collision means two
  differently-namespaced elements, `a:id` and `b:id`-style, which are
  semantically different fields that happen to share a local name. There
  is no defensible way to pick one, so the run fails closed. This is
  `xml_reader`'s long-standing behavior and #374 does not change it.
- `on_collision="disambiguate"` (JSON, CSV, Parquet): keep the first
  occurrence's name and suffix later ones `_2`, `_3`, ... in source field
  order, logging a WARNING that names both source names. A flat extract
  with `Order Id` and `Order.Id` is a naming accident in the exporter, not
  an ambiguity of meaning, and failing the whole file over it would be a
  worse outcome than two clearly-numbered columns. Suffixes skip any name
  already claimed elsewhere in the same struct, so disambiguating can
  never itself create a collision.

Both policies are deterministic and neither drops a field.

## Blast radius

An existing JSON/CSV/Parquet source whose field names contain any of these
characters DID NOT WORK before this change - it failed at the write - so
there is no such caller to break. A source whose names are already
Delta-safe is unaffected: the rule is a no-op on it. The one case that
does change shape is a name containing `:` or `.` that Delta happened to
accept; `sales.amount` becomes `sales_amount`. That is called out in the
0.6.0 changelog.
"""

from collections import Counter
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from .logging_utils import logger

#: The two collision policies `canonicalize_identifiers` accepts. See the
#: module docstring for why the choice is per-format rather than global.
ON_COLLISION_ERROR = "error"
ON_COLLISION_DISAMBIGUATE = "disambiguate"


class IdentifierCollisionError(ValueError):
    """Two sibling source names canonicalize to the same Delta identifier."""


def canonical_name(name: str) -> str:
    """
    One source field name -> its Delta-safe identifier. See the module
    docstring for the rule and why each step is shaped the way it is.
    """
    namespaced = name.replace(":", "__")
    return "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in namespaced)


def _disambiguate(names: Sequence[str], canonicals: Sequence[str], location: str) -> List[str]:
    """
    Canonical names with duplicates numbered, first occurrence unsuffixed.

    `taken` is seeded with EVERY canonical name in the struct before any
    suffix is chosen, so a generated `Order_Id_2` cannot land on a third
    field that is genuinely named `Order_Id_2` at source.
    """
    counts = Counter(canonicals)
    taken = set(canonicals)
    occurrences: Dict[str, int] = {}
    resolved: List[str] = []
    for source_name, canonical in zip(names, canonicals):
        if counts[canonical] == 1:
            resolved.append(canonical)
            continue
        seen = occurrences.get(canonical, 0)
        occurrences[canonical] = seen + 1
        if seen == 0:
            resolved.append(canonical)
            continue
        suffix = seen + 1
        candidate = f"{canonical}_{suffix}"
        while candidate in taken:
            suffix += 1
            candidate = f"{canonical}_{suffix}"
        taken.add(candidate)
        resolved.append(candidate)
        logger.warning(
            "Identifier collision at %s: %r canonicalizes to %r, already used by an "
            "earlier field. Reading it as %r instead. Both columns are present; "
            "neither source field was dropped (#374).",
            location,
            source_name,
            canonical,
            candidate,
        )
    return resolved


def _resolve_names(fields: Sequence[Any], on_collision: str, path: Tuple[str, ...]) -> List[str]:
    names = [field.name for field in fields]
    canonicals = [canonical_name(name) for name in names]
    location = ".".join(path) or "<root>"
    if on_collision == ON_COLLISION_DISAMBIGUATE:
        return _disambiguate(names, canonicals, location)

    by_canonical: Dict[str, str] = {}
    for source_name, canonical in zip(names, canonicals):
        previous = by_canonical.get(canonical)
        if previous is not None and previous != source_name:
            raise IdentifierCollisionError(
                f"Identifier collision at {location}: {previous!r} and "
                f"{source_name!r} both canonicalize to {canonical!r}."
            )
        by_canonical[canonical] = source_name
    return canonicals


def _validate_data_type(data_type: Any, on_collision: str, path: Tuple[str, ...]) -> None:
    """
    Walk a nested type for collisions without building any column
    expressions. `on_collision="disambiguate"` resolves rather than raises,
    so this is a no-op there and only runs to keep the error policy's
    check reachable inside arrays and maps, where the projection below
    cannot raise from a lambda body eagerly.
    """
    from pyspark.sql.types import ArrayType, MapType, StructType

    if isinstance(data_type, StructType):
        _resolve_names(data_type.fields, on_collision, path)
        for field in data_type.fields:
            _validate_data_type(field.dataType, on_collision, path + (canonical_name(field.name),))
    elif isinstance(data_type, ArrayType):
        _validate_data_type(data_type.elementType, on_collision, path + ("[]",))
    elif isinstance(data_type, MapType):
        _validate_data_type(data_type.valueType, on_collision, path + ("{}",))


def _canonicalize_column(
    column: Any, data_type: Any, on_collision: str, path: Tuple[str, ...]
) -> Any:
    from pyspark.sql.functions import lit, struct, transform, transform_values, when
    from pyspark.sql.types import ArrayType, MapType, StructType

    if isinstance(data_type, StructType):
        resolved = _resolve_names(data_type.fields, on_collision, path)
        canonical_struct = struct(
            *(
                _canonicalize_column(
                    column.getField(field.name), field.dataType, on_collision, path + (alias,)
                ).alias(alias)
                for field, alias in zip(data_type.fields, resolved)
            )
        )
        # A null struct must stay null rather than becoming a struct of
        # nulls, which is what `struct(...)` on a null parent would give.
        return when(column.isNull(), lit(None)).otherwise(canonical_struct)
    if isinstance(data_type, ArrayType):
        return transform(
            column,
            lambda item: _canonicalize_column(
                item, data_type.elementType, on_collision, path + ("[]",)
            ),
        )
    if isinstance(data_type, MapType):
        return transform_values(
            column,
            lambda _key, value: _canonicalize_column(
                value, data_type.valueType, on_collision, path + ("{}",)
            ),
        )
    return column


def needs_canonicalization(fields: Iterable[Any]) -> bool:
    """
    True when any top-level field name is not already its own canonical
    form. Nested names are not inspected - a top-level-clean frame can
    still carry a nested name that needs work, so this is only ever used
    to skip the log line, never to skip the projection.
    """
    return any(field.name != canonical_name(field.name) for field in fields)


def canonicalize_identifiers(dataframe: Any, on_collision: str = ON_COLLISION_DISAMBIGUATE):
    """
    Project `dataframe` so every column name, at every nesting depth, is a
    Delta-safe identifier.

    Structs, arrays of structs and map values are all rewritten, so a name
    four levels down gets the same treatment as a top-level one. Values are
    untouched: this renames, it never casts, drops or reorders.

    Raises `IdentifierCollisionError` when two sibling names collapse to
    one and `on_collision="error"`; suffixes the later ones and logs a
    WARNING when `on_collision="disambiguate"`.
    """
    from pyspark.sql.functions import col

    if on_collision not in (ON_COLLISION_ERROR, ON_COLLISION_DISAMBIGUATE):
        raise ValueError(
            f"on_collision must be {ON_COLLISION_ERROR!r} or "
            f"{ON_COLLISION_DISAMBIGUATE!r}, got {on_collision!r}."
        )

    fields = dataframe.schema.fields
    # Under the error policy, walk the whole nested schema up front. The
    # projection below builds array/map bodies through `transform` lambdas
    # that Spark may not evaluate eagerly, so a collision buried inside an
    # array of structs would otherwise escape until much later, or never.
    if on_collision == ON_COLLISION_ERROR:
        for field in fields:
            _validate_data_type(field.dataType, on_collision, (canonical_name(field.name),))

    resolved = _resolve_names(fields, on_collision, ())
    projections = []
    for field, alias in zip(fields, resolved):
        escaped = field.name.replace("`", "``")
        projections.append(
            _canonicalize_column(col(f"`{escaped}`"), field.dataType, on_collision, (alias,)).alias(
                alias
            )
        )
    return dataframe.select(*projections)
