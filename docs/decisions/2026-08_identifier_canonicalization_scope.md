# Identifier canonicalization applies to every read path, not only XML

Delta refuses a column name containing any of ` ,;{}()\n\t=`. A JSON source with
a field called `Gross Weight (KG)` — the ordinary output of an Excel export or a
BW query — was read successfully, retried three times, and failed at the write
with `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`. Canonicalization existed only
in `xml_reader.py`, so the guarantee the fixture README describes held for XML
and no other format.

**Status:** Accepted

Written against `dev` @ `5b33cc7` for #374, after EC-11 and EC-12 both failed in
the #369 staging smoke test on a nine-table SAP migration extract.

---

## Decision

The canonicalization logic moves out of `xml_reader.py` into `identifiers.py`
and `readers.read_source` applies it to whatever the dispatched reader returned.
JSON, CSV, Parquet and XML all get the same identifiers from the same code, and
`streaming_reader.read_json_stream` applies it to its frame for the same reason.

The rule, in order:

1. `:` becomes `__`. This is #337's namespace rule and it runs first so `a:id`
   and a sibling literally named `a__id` still collide instead of one shadowing
   the other.
2. Every remaining character that is not a letter, digit or underscore becomes
   `_`. Letter and digit mean `str.isalnum()`, which is Unicode-aware, so
   `Änderungsdatum` passes through intact — Delta accepts Unicode letters and
   mangling them would lose information for nothing.
3. Nothing else. Underscore runs are not collapsed and edges are not stripped;
   either would undo step 1 and would rename the lineage columns this package
   adds itself, `_input_file_name` and `_rescued_data`.

The rule is idempotent, so a name already made of letters, digits and
underscores is untouched — the overwhelmingly common case — and running it twice
over an XML frame that `read_xml` already canonicalized changes nothing.

## Collisions: two policies, on purpose

Two source names can canonicalize to one identifier. Keeping the last one
silently is data loss and never happens. What happens instead depends on the
format:

- **XML fails closed.** `on_collision="error"`, raising
  `XMLIdentifierCollisionError` exactly as before. #337 is explicit that an
  ordinal must never be appended, and it is right for XML: `a:id` and `b:id` are
  two differently-namespaced elements, semantically distinct fields that share a
  local name, and choosing between them would be a guess.
- **JSON, CSV and Parquet disambiguate.** `on_collision="disambiguate"`. The
  first occurrence keeps the canonical name, later ones are suffixed `_2`, `_3`
  in source field order, and each rename logs a WARNING naming both source
  names. Suffixes skip any name already claimed in the same struct, so
  disambiguation can never itself create a collision.

The split is the point rather than an inconsistency to tidy away later. A flat
extract carrying both `Order Id` and `Order.Id` has a sloppy exporter, not two
meanings; failing the whole file over it would be a worse outcome than two
numbered columns and a warning. An XML collision carries namespace identity, and
#337's fail-closed rule stands unchanged.

## What was rejected

1. **Canonicalize on every read path.** Chosen — this record.
2. **Enable Delta column mapping on bronze tables.** The original names would
   survive verbatim, which is genuinely nicer. Rejected on blast radius: column
   mapping is a table property with its own constraints, it is not trivially
   reversible once set, and it would apply to every table this package has ever
   written to solve a naming problem that a rename solves.
3. **Refuse at config time, naming the offending columns.** The way
   `config.py:396` refuses headerless CSV. It converts a Delta stacktrace into
   an instruction, which is an improvement, but it leaves a routine source
   permanently un-ingestable and the instruction it gives — "rename your
   columns" — is one the caller usually cannot act on, because the file comes
   from a system they do not control.
4. **Apply the XML rule as-is (`:` → `__` only) to the other formats.** It fixes
   nothing: a space is still a space.

## Consequences

**Sources that used to work and now get different column names.** Only names
containing `.` or `:`, which Delta happened to accept: `sales.amount` becomes
`sales_amount`. A source containing a Delta-invalid character did not work
before this change — it failed at the write — so there is no working caller to
break there. Everything already made of letters, digits and underscores is
unaffected. This is called out in the 0.6.0 changelog under "Read this before
deploying".

`main` has not moved since 0.5.0, so no deployed pipeline changes behaviour when
this ships; the renames land with 0.6.0's first deployment rather than under an
already-running one.

**A silver query selecting a renamed column breaks loudly**, at the select, with
a column-not-found error. That is the right failure: it is visible immediately
and the fix is mechanical.

**Config fields that name columns now name canonical ones.** `required_columns`,
`unique_columns`, `merge_keys`, `partition_columns` and a data contract all run
against the frame after the rename, so they take `Material_Number`, not
`Material Number`. A config using the raw name could not have worked before —
the write it fed failed — so this is a new spelling for a case that had none.
`schema_hint_ddl` is the exception and still uses the SOURCE names, because it
is handed to the reader before the rename happens; a DDL naming
`` `Gross Weight (KG)` `` reads correctly and the column arrives as
`Gross_Weight__KG_`.

**Auto Loader's schema location is unaffected.** Canonicalization is a
projection applied after the load, and schema inference and evolution stay keyed
on the source names, so an existing stream does not re-infer or re-process.

**EC-11 and EC-12 should now pass** on re-run. EC-12's real question — whether
colliding names are disambiguated rather than last-wins — becomes answerable for
the first time, since the run previously died before reaching it. Confirming
that needs a staging re-run, which #369 covers and this change does not.
