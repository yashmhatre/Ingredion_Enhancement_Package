# CSV schema inference is off by default

Spark's CSV inference reads an 18-character zero-padded SAP material number as
a number and drops the padding. There is no error, no warning and no quarantine
row; the run reports `SUCCESS` and the audit row records the correct count.
`csv_infer_schema` defaulted to `True`, so this was the default read.

**Status:** Accepted

Written against `fix/366-idempotent-txn-version` @ `381950f` for #371, after the
defect was found during the #369 staging smoke test on a nine-table SAP
migration extract — the first time CSV had been ingested on real compute.

---

## Decision

`IngestionConfig.csv_infer_schema` defaults to `False`. Every CSV column lands
as a string and the caller casts what it wants cast. The field stays settable,
so a caller who wants inference asks for it by name.

`csv_header` keeps its `True` default. The two knobs arrived together in #308
and are easy to conflate, but they fail in opposite directions: a wrong header
default produces `_c0`, `_c1` and breaks loudly at the first `required_columns`
check, while a wrong inference default produces plausible numbers and breaks
nothing until a silver join returns zero rows.

JSON and Parquet are untouched. Both carry their own types, and neither has
this failure mode.

## Why this one is worse than it looks

Three separate checks pass on the corrupted data, which is why the smoke test
went green:

- **Row counts reconcile.** Bronze plus quarantine equals source, for every
  table. Nothing is dropped — the values are simply wrong.
- **The referential-integrity check passes.** `makt.MATNR subset of
  mara.MATNR` returned zero orphans, but only because the join compared an
  `int` against a `string` and Spark coerced the string to a number. It passed
  *because* of the corruption. The same join written the obvious way in silver,
  string to string, matches nothing.
- **Nothing quarantines.** Inference is not a parse failure. Every row is
  perfectly valid CSV.

`MANDT` is the same bug on the client field, which every SAP table carries.

Trailing-space padding was never affected: `MAKTX` and `NAME1` both keep their
padding to width. The defect is inference of the type, not handling of the
value.

## Considered options

1. **Default `csv_infer_schema` to `False`.** Chosen. It makes the failure
   impossible rather than visible, and it matches the shape of a migration
   extract, where everything is text until someone says otherwise.
2. **Keep inference, require `schema_hint_ddl` for CSV.** Also correct, and
   rejected only on cost: it makes every CSV ingestion, including throwaway
   ones, carry a hand-written DDL before it can run once.
3. **Warn when an inferred numeric column's source text had leading zeros.** A
   warning in a job log is not a gate. #250 is already open about a check that
   passes everything quietly, and this would add a second one.

## Consequences

The default read of a CSV is now all-string, which is also Spark's own default.
A pipeline that relied on `amount` arriving as a number gets a string and must
cast. That cast belongs in silver regardless.

`csv_infer_schema: true` remains available and is validated the same way it
always was — as a real bool, not the string `"true"`.

This lands before 0.6.0 ships. `main` has not moved since 0.5.0, so the `True`
default from #308 never reached production and no deployed pipeline changes
behaviour.

Corrupt-record capture is unchanged and still requires `schema_hint_ddl`: Spark
only materialises `columnNameOfCorruptRecord` for CSV when the column is
declared in the read schema, and the new default has no declared schema either.

The tables written during the #369 smoke test are wrong and cannot be repaired
in place — `MATNR` no longer contains the padding, so there is nothing to
recover it from. They need a re-ingest from the fixtures, not a backfill.
