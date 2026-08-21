# Fixtures — SAP-style migration extract

`generate_sap_migration.py` writes a synthetic SAP cutover extract and an edge-case matrix. It exists because every source this package has ingested so far was a hand-built fixture of at most 457 bytes. Fixtures that small prove the code runs; they cannot show how it behaves against data shaped like a real migration.

Nothing here is committed as data. The script is the artefact, the files are generated, and a fixed seed makes two runs byte-identical for JSON and CSV.

## Running it

```bash
python bronze_layer/fixtures/generate_sap_migration.py --out ./out
```

| Flag | Default | Effect |
|---|---|---|
| `--out` | required | Output directory, cleared first unless `--keep` |
| `--rows` | `5000` | Approximate total migration rows; per-table counts scale proportionally |
| `--seed` | `20260820` | Change it for a different dataset, keep it for a reproducible one |
| `--skip-parquet` | off | Write VBAP as `.jsonl` instead |
| `--keep` | off | Do not clear `--out` first |

`pyarrow` is optional. Without it VBAP falls back to `.jsonl` and `MANIFEST.json` records that it did.

Upload to a Volume:

```bash
MSYS_NO_PATHCONV=1 databricks fs cp -r ./out \
  dbfs:/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/STG/Raw \
  --profile bronze-json-loader-dev
```

The `MSYS_NO_PATHCONV=1` prefix is needed on Git Bash. Without it the leading slash in the target is rewritten to `C:/Program Files/Git/Volumes/...` and the CLI reports a confusing `Not Found`.

## What gets written

### `migration/` — roughly 5,000 rows across nine tables

Referential integrity holds: every `MATNR` in VBAP, AUFK and MCHA exists in MARA, every `KUNNR` in VBAK exists in KNA1, every `VBELN` in VBAP exists in VBAK.

| File | SAP table | Rows | Format | Why this format |
|---|---|---|---|---|
| `kna1.json` | Customer master | 400 | JSON array | Nested address struct |
| `mara.json` | Material master | 500 | JSON array | Nested classification struct |
| `vbak.jsonl` | Sales order header | 700 | JSON Lines | Large, line-delimited |
| `vbap.parquet` | Sales order items | 1,800 | Parquet | The big child table |
| `aufk.jsonl` | Production orders | 250 | JSON Lines | |
| `mcha.jsonl` | Batch master | 200 | JSON Lines | |
| `makt.csv` | Material descriptions | 1,000 | CSV | Flat, two languages per material |
| `t001w.csv` | Plants | 12 | CSV | Small reference table |
| `lfa1.csv` | Vendor master | 150 | CSV | Flat |

Mixed formats are deliberate. A real extract is not one format, and `source_format` drives both discovery and interpretation — so a JSON run over this directory must ingest the JSON and ignore the CSV and Parquet without erroring.

### SAP conventions reproduced on purpose

Each of these has broken a real migration somewhere:

- **`MANDT`** (client) on every table, always `"100"`.
- **Zero-padded string keys.** `MATNR` is 18 characters, `KUNNR` and `LIFNR` are 10. Inferred as numbers they lose the padding and stop joining. This is the most common migration defect there is.
- **`YYYYMMDD` strings, not dates**, and the SAP empty date `"00000000"`, which is neither null nor a valid date.
- **`HHMMSS` time strings.**
- **Space-padded CHAR fields.** `NAME1` is padded to 35. Trailing spaces are data.
- **`LVORM`** deletion indicator: `"X"` or a single space. A space, not an empty string, and not null.
- **Non-ASCII text.** `Mogi Guaçu`, `San Juan del Río`, `Müller Milch GmbH`, `上海工厂`.

The data is also internally coherent, which matters more than it sounds: net weight never exceeds gross weight, batch expiry always follows batch creation, production orders finish after they start, and a customer's dialling code matches their country. Independently random fields are what make a fixture read as fake.

`MAKT` carries two rows per material, one per language. `MATNR` alone is therefore **not** unique in that table, which is the trap a dedup keyed on `MATNR` falls into.

### `edge_cases/` — 26 isolated cases

One directory per case, so a failure names itself rather than arriving as one red run. `MANIFEST.json` carries the same table in machine-readable form, including the code path each case targets.

| ID | Case | What it proves |
|---|---|---|
| EC-01 | Null and absent required key | `required_columns` quarantines; bronze + quarantine = source rows |
| EC-02 | Duplicate business key | `unique_columns` keeps one, quarantines the other, deterministically |
| EC-03 | Drift — column added | `schema_drift_json` names it; `merge_schema` adds it |
| EC-04 | Drift — column removed | Reported; existing rows keep their value |
| EC-05 | Drift — type changed | `type_changed` reported, not silently coerced |
| EC-06 | Corrupt JSON record | PERMISSIVE routes it to `_corrupt_record`, file still ingests |
| EC-07 | `.jsonl` with `multiline=true` | 5 rows, not 1 — the #146 regression |
| EC-08 | Empty folder | `skipped` with a reason, never `failed` |
| EC-09 | Folder-as-table | 3 files become one table of 12 rows |
| EC-10 | Data beside control file and log | JSON run ingests only JSON, ignores the rest |
| EC-11 | Identifier canonicalization | Spaces, parentheses, slashes, dots, percent, dash, umlaut |
| EC-12 | Canonicalization collision | `Order Id` and `Order.Id` collapse to one name and are disambiguated, not last-wins |
| EC-13 | Leading zeros | Padded `MATNR` and a `00501` postal code survive CSV inference |
| EC-14 | SAP null dates | `"00000000"`, null and `""` stay distinct in one column |
| EC-15 | Trailing spaces | Padding survives; `ignoreTrailingWhiteSpace` must not strip it |
| EC-16 | Unicode | Accents, en dash and CJK in one column |
| EC-17 | Deep nesting | Four levels, arrays of scalars and of structs, unflattened |
| EC-18 | Zero-byte file | No `CANNOT_INFER_EMPTY_SCHEMA` |
| EC-19 | CSV header, no rows | An empty table is a valid outcome — **this is the shape that crashed staging on 2026-07-29** |
| EC-20 | CSV quoting | Embedded comma, escaped quotes, newline inside a quoted field: RFC4180 quotes unescaped by default; 4 rows not 5 with `csv_multiline: true` (#375) |
| EC-21 | Headerless CSV | Refused unless `schema_hint_ddl` names the columns |
| EC-22 | Numeric extremes | Zero, negative credit amounts, 11 significant digits, float noise |
| EC-23 | Null vs empty vs space | All three distinct; only null counts as missing |
| EC-24 | 122-column wide row | The append-a-Z-field pattern every SAP shop ends up with |
| EC-25 | Two batches sharing a key | `append` gives 4 rows, `merge` on `MATNR` gives 3 with the value updated |
| EC-26 | Numeric-looking strings | `"1.10"` must not become `1.1`; EANs and phone numbers stay strings |

Several cases need configuration to mean anything. EC-01 and EC-02 need `required_columns` and `unique_columns` set — with the gate unconfigured they pass everything, which is what #250 is open about. EC-03 through EC-05 must be ingested **into a table a baseline run already created**, because drift against an empty table is not drift. EC-21 and EC-25 need a config change rather than a second file.

## Known gaps

- **XML is not covered.** The reader is complete but `source_format: xml` is refused until #336 and #337 clear Tier 2 sign-off, so an XML fixture could not be ingested today.
- **No streaming fixtures.** Auto Loader takes JSON only, and #323 defers multi-format streaming.
- **Volume, not scale.** 5,000 rows shows correctness. It does not exercise partitioning, clustering, or the maintenance job the way a production table would; `--rows 100000` is the lever if that is the question.
- **`MANIFEST.json` is descriptive, not executable.** It records what each case should prove. Nothing asserts it yet — the assertions live in whatever harness consumes this.
