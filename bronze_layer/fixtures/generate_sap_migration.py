#!/usr/bin/env python3
"""Generate a realistic SAP-style migration extract, plus an edge-case matrix.

Why this exists
---------------
Every source this package has ever ingested was a hand-built fixture of at
most 457 bytes (the note in `config/contracts/orders.yaml` says so, and #250
is open because of it). Fixtures that small prove the code runs. They cannot
show how it behaves against data that looks like a cutover extract, and two
of the defects found during the 0.6.0 staging smoke test only appeared once
a table already held rows from an earlier batch.

So this writes two things:

`migration/`
    Nine SAP tables with referential integrity between them - materials that
    the order items actually reference, customers the order headers actually
    point at. Roughly 5,000 rows. This is the "looks real" half.

`edge_cases/`
    One directory per case, each isolated so a failure names itself. This is
    the "proves something" half. Every case maps to a specific code path;
    `MANIFEST.json` records which, and README.md explains why each one is
    hard.

Determinism
-----------
Seeded. The same `--seed` produces byte-identical JSON and CSV output, so a
diff between two runs is a real change rather than reshuffled fake data.
Parquet is not byte-stable across pyarrow versions; row content is.

Usage
-----
    python generate_sap_migration.py --out ./out
    python generate_sap_migration.py --out ./out --rows 20000 --seed 7
    python generate_sap_migration.py --out ./out --skip-parquet

Then upload the tree to a Volume:

    databricks fs cp -r ./out \\
      dbfs:/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/STG/Raw

On Git Bash, prefix that with MSYS_NO_PATHCONV=1 or the leading slash in the
target is rewritten into a Windows path.

SAP conventions this reproduces deliberately
--------------------------------------------
These are not decoration. Each one has broken a real migration somewhere:

- `MANDT` (client) on every table, always "100". Three characters, and it is
  part of the key in SAP even though nothing here keys on it.
- Numeric-looking keys are zero-padded strings: MATNR is 18 characters,
  KUNNR and LIFNR are 10. Inferred as numbers they lose the padding and stop
  joining. This is the single most common migration defect.
- Dates are `YYYYMMDD` strings, not dates. The SAP empty date is `00000000`,
  which is not null and is not a valid date.
- Times are `HHMMSS` strings.
- CHAR fields are space-padded to their declared width, so trailing spaces
  are data as far as the extract is concerned.
- `LVORM` is the deletion indicator: "X" or blank. Blank is a space, not an
  empty string, and not null.
- Text carries non-ASCII: German and Spanish plant and customer names are
  normal in a real extract.
"""

import argparse
import datetime
import json
import os
import random
import shutil
import string
from typing import Any, Dict, List

# --------------------------------------------------------------------------
# SAP reference data. Small, closed vocabularies - the kind of thing that in
# a real extract comes from a check table.
# --------------------------------------------------------------------------

MANDT = "100"

PLANTS = [
    ("1010", "Hamburg Werk", "DE", "EUR"),
    ("1020", "Krefeld Werk", "DE", "EUR"),
    ("2010", "Cedar Rapids", "US", "USD"),
    ("2020", "Indianapolis", "US", "USD"),
    ("2030", "Bedford Park", "US", "USD"),
    ("3010", "Guadalajara", "MX", "MXN"),
    ("3020", "San Juan del Río", "MX", "MXN"),
    ("4010", "Mogi Guaçu", "BR", "BRL"),
    ("5010", "Manchester Site", "GB", "GBP"),
    ("6010", "Shanghai Plant", "CN", "CNY"),
    ("7010", "Bangkok Plant", "TH", "THB"),
    ("8010", "Sydney Plant", "AU", "AUD"),
]

# Ingredion makes starches, sweeteners and texturisers. Material groups and
# names below follow that, so the data reads as plausible to someone who
# knows the business rather than as lorem ipsum.
MATERIAL_GROUPS = ["STARCH", "SWEETNR", "TEXTURE", "NUTRI", "PACKG", "RAWCORN"]

MATERIAL_NAMES = [
    "Native Corn Starch",
    "Waxy Maize Starch",
    "Modified Tapioca Starch",
    "Glucose Syrup 42DE",
    "High Fructose Corn Syrup 55",
    "Maltodextrin DE18",
    "Crystalline Fructose",
    "Potato Starch Native",
    "Pea Protein Isolate",
    "Rice Starch Fine",
    "Citric Acid Anhydrous",
    "Dextrose Monohydrate",
    "Polydextrose Powder",
    "Resistant Starch RS4",
    "Corn Fibre Gum",
    "Sorbitol Solution 70",
    "Tapioca Maltodextrin",
    "Oxidised Starch E1404",
]

UNITS = ["KG", "TO", "L", "ST", "PAL"]
LANGUAGES = ["E", "D", "ES"]

CUSTOMER_NAMES = [
    "Nestlé Deutschland AG",
    "Mondelez International",
    "Danone S.A.",
    "Unilever PLC",
    "General Mills Inc",
    "Grupo Bimbo S.A.B.",
    "Kellanova Company",
    "Ferrero Group",
    "Barilla G. e R.",
    "Müller Milch GmbH",
    "PepsiCo Foods",
    "Lactalis Ingredients",
    "Dr. Oetker KG",
    "Campbell Soup Company",
    "Conagra Brands",
    "Hero Group AG",
]

VENDOR_NAMES = [
    "Cargill Trading BV",
    "ADM Europoort",
    "Bunge Ibérica S.A.",
    "Louis Dreyfus Company",
    "Südzucker AG",
    "Tereos Starch & Sweeteners",
    "Roquette Frères",
    "Agrana Beteiligungs-AG",
]

CITIES = {
    "DE": ["Hamburg", "Krefeld", "München", "Düsseldorf"],
    "US": ["Cedar Rapids", "Chicago", "Indianapolis", "Westchester"],
    "MX": ["Guadalajara", "San Juan del Río", "Monterrey"],
    "BR": ["Mogi Guaçu", "São Paulo", "Curitiba"],
    "GB": ["Manchester", "Leeds", "Bristol"],
    "CN": ["Shanghai", "Qingdao"],
    "TH": ["Bangkok", "Rayong"],
    "AU": ["Sydney", "Melbourne"],
}

ORDER_TYPES = ["TA", "ZOR", "RE", "KB"]
ORDER_STATUS = ["A", "B", "C"]

#: International dialling codes, so a German customer does not carry a
#: Peruvian phone number. Small detail, but incoherent reference data is the
#: fastest way to make a fixture read as fake.
DIAL_CODES = {
    "DE": "49",
    "US": "1",
    "MX": "52",
    "BR": "55",
    "GB": "44",
    "CN": "86",
    "TH": "66",
    "AU": "61",
}


# --------------------------------------------------------------------------
# SAP field formatting. The whole point of these helpers is that no caller
# ever writes a bare int into a key field.
# --------------------------------------------------------------------------


def matnr(n: int) -> str:
    """18-character zero-padded material number, as SAP stores it."""
    return str(n).zfill(18)


def kunnr(n: int) -> str:
    """10-character zero-padded customer number."""
    return str(n).zfill(10)


def lifnr(n: int) -> str:
    """10-character zero-padded vendor number."""
    return str(n).zfill(10)


def vbeln(n: int) -> str:
    """10-character zero-padded sales document number."""
    return str(n).zfill(10)


def aufnr(n: int) -> str:
    """12-character zero-padded production order number."""
    return str(n).zfill(12)


def posnr(n: int) -> str:
    """6-character zero-padded item number, counted in tens like SAP does."""
    return str(n * 10).zfill(6)


def sap_date(rng: random.Random, start_year: int = 2023, end_year: int = 2026) -> str:
    """A YYYYMMDD string. Day is capped at 28 so no month is ever invalid."""
    y = rng.randint(start_year, end_year)
    m = rng.randint(1, 12)
    d = rng.randint(1, 28)
    return f"{y:04d}{m:02d}{d:02d}"


def sap_time(rng: random.Random) -> str:
    """An HHMMSS string."""
    return f"{rng.randint(0, 23):02d}{rng.randint(0, 59):02d}{rng.randint(0, 59):02d}"


def sap_date_after(rng: random.Random, base: str, min_days: int, max_days: int) -> str:
    """A YYYYMMDD string a bounded number of days after `base`.

    Independently random dates are how a fixture ends up with batches that
    expire before they were created and production orders that finish before
    they start. Anything with a real ordering goes through this.
    """
    start = datetime.date(int(base[0:4]), int(base[4:6]), int(base[6:8]))
    return (start + datetime.timedelta(days=rng.randint(min_days, max_days))).strftime("%Y%m%d")


def pad(value: str, width: int) -> str:
    """Space-pad to SAP's declared CHAR width. Trailing spaces are data here."""
    return value.ljust(width)


def amount(rng: random.Random, low: float, high: float) -> float:
    """A currency amount at SAP's two-decimal precision."""
    return round(rng.uniform(low, high), 2)


def quantity(rng: random.Random, low: float, high: float) -> float:
    """A quantity at SAP's three-decimal precision."""
    return round(rng.uniform(low, high), 3)


def lvorm(rng: random.Random, rate: float = 0.03) -> str:
    """Deletion indicator: 'X' on a small minority of rows, otherwise a space.

    A space, not an empty string. In the extract these are different, and a
    downstream `WHERE lvorm = ''` that works in a test against empty strings
    silently matches nothing against the real thing.
    """
    return "X" if rng.random() < rate else " "


# --------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------


def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    """One JSON object per line. `ensure_ascii=False` keeps the accents real."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=False) + "\n")


def write_json_array(path: str, rows: List[Dict[str, Any]]) -> None:
    """A single JSON array document - the multiline=true read path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def write_csv(path: str, rows: List[Dict[str, Any]], header: bool = True) -> None:
    """RFC 4180 CSV, quoting every field.

    Quoting unconditionally rather than only when needed is what a real SAP
    extract tool does, and it keeps a trailing space or an embedded comma
    from changing the column count.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        raise ValueError("write_csv needs at least one row to derive a header")
    cols = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        if header:
            fh.write(",".join(f'"{c}"' for c in cols) + "\n")
        for row in rows:
            cells = []
            for c in cols:
                v = row[c]
                s = "" if v is None else str(v)
                cells.append('"{}"'.format(s.replace('"', '""')))
            fh.write(",".join(cells) + "\n")


def write_parquet(path: str, rows: List[Dict[str, Any]]) -> bool:
    """Write Parquet if pyarrow is available. Returns False if it is not.

    Not a hard dependency: the JSON and CSV halves of this generator are
    worth having on a machine without pyarrow, and `--skip-parquet` makes
    that explicit rather than silent.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cols = list(rows[0].keys())
    table = pa.table({c: [r[c] for r in rows] for c in cols})
    pq.write_table(table, path)
    return True


# --------------------------------------------------------------------------
# The migration extract. Master data first, then the documents that
# reference it, so referential integrity is a property of the order things
# are built in rather than something to check afterwards.
# --------------------------------------------------------------------------


def gen_t001w() -> List[Dict[str, Any]]:
    """T001W - plants. Small, closed, and referenced by nearly everything."""
    return [
        {
            "MANDT": MANDT,
            "WERKS": code,
            "NAME1": name,
            "LAND1": country,
            "WAERS": currency,
            "SPRAS": "E",
        }
        for code, name, country, currency in PLANTS
    ]


def gen_kna1(rng: random.Random, n: int) -> List[Dict[str, Any]]:
    """KNA1 - customer master, with the address nested rather than flattened.

    Bronze keeps nested JSON nested by design, so the address is a struct.
    That is the shape Silver will have to deal with, which makes it the
    honest shape to test against.
    """
    rows = []
    for i in range(1, n + 1):
        country = rng.choice(list(CITIES.keys()))
        name = rng.choice(CUSTOMER_NAMES)
        rows.append(
            {
                "MANDT": MANDT,
                "KUNNR": kunnr(100000 + i),
                "NAME1": pad(name, 35),
                "LAND1": country,
                "SPRAS": rng.choice(LANGUAGES),
                "KTOKD": rng.choice(["0001", "0002", "ZCUS"]),
                "ERDAT": sap_date(rng, 2015, 2025),
                "ERNAM": rng.choice(["MIGRATION", "BATCHJOB", "SAPUSER"]),
                "LVORM": lvorm(rng),
                "ADDRESS": {
                    "STRAS": (
                        f"{rng.choice(['Hauptstr.', 'Main St', 'Av. Central'])} "
                        f"{rng.randint(1, 240)}"
                    ),
                    "ORT01": rng.choice(CITIES[country]),
                    "PSTLZ": _postal_code(rng, country),
                    "REGIO": rng.choice(["01", "07", "BY", "NW", ""]),
                    "TELF1": f"+{DIAL_CODES[country]} {rng.randint(1000000, 9999999)}",
                },
            }
        )
    return rows


def _postal_code(rng: random.Random, country: str) -> str:
    """Postal codes are strings. US ones have leading zeros and must keep them."""
    if country == "US":
        return str(rng.randint(1, 99999)).zfill(5)
    if country == "GB":
        return (
            f"{rng.choice(['M', 'LS', 'BS'])}{rng.randint(1, 40)} "
            f"{rng.randint(1, 9)}"
            f"{rng.choice(string.ascii_uppercase)}{rng.choice(string.ascii_uppercase)}"
        )
    return str(rng.randint(10000, 99999))


def gen_lfa1(rng: random.Random, n: int) -> List[Dict[str, Any]]:
    """LFA1 - vendor master."""
    rows = []
    for i in range(1, n + 1):
        country = rng.choice(list(CITIES.keys()))
        rows.append(
            {
                "MANDT": MANDT,
                "LIFNR": lifnr(500000 + i),
                "NAME1": pad(rng.choice(VENDOR_NAMES), 35),
                "LAND1": country,
                "ORT01": rng.choice(CITIES[country]),
                "PSTLZ": _postal_code(rng, country),
                "ERDAT": sap_date(rng, 2012, 2024),
                "LVORM": lvorm(rng),
            }
        )
    return rows


def gen_mara(rng: random.Random, n: int) -> List[Dict[str, Any]]:
    """MARA - material master, general data. The MATNR here is the join key
    every other table's material reference has to match exactly, padding and
    all."""
    rows = []
    for i in range(1, n + 1):
        gross = quantity(rng, 0.5, 1200.0)
        created = sap_date(rng, 2010, 2025)
        rows.append(
            {
                "MANDT": MANDT,
                "MATNR": matnr(1000000 + i),
                "MTART": rng.choice(["FERT", "HALB", "ROH", "VERP"]),
                "MATKL": rng.choice(MATERIAL_GROUPS),
                "MEINS": rng.choice(UNITS),
                "BRGEW": gross,
                # Net is a fraction of gross, never independent of it.
                "NTGEW": round(gross * rng.uniform(0.85, 0.99), 3),
                "GEWEI": "KG",
                "ERSDA": created,
                # Last-changed is after created, or the SAP empty date when the
                # material has never been changed since migration.
                "LAEDA": rng.choice([sap_date_after(rng, created, 1, 900), "00000000"]),
                "LVORM": lvorm(rng),
                "CLASSIFICATION": {
                    "ALLERGEN_FREE": rng.choice([True, False]),
                    "ORGANIC": rng.choice([True, False]),
                    "NON_GMO": rng.choice([True, False]),
                    "CERTIFICATIONS": rng.sample(
                        ["KOSHER", "HALAL", "ISO22000", "FSSC22000", "RSPO"],
                        k=rng.randint(0, 3),
                    ),
                },
            }
        )
    return rows


def gen_makt(rng: random.Random, materials: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """MAKT - material descriptions, one row per material per language.

    Two rows per material means MATNR alone is not unique here. A dedup
    keyed on MATNR would silently drop half this table, which is exactly the
    mistake this table exists to catch.
    """
    rows = []
    for mat in materials:
        for lang in rng.sample(LANGUAGES, k=2):
            base = rng.choice(MATERIAL_NAMES)
            if lang == "D":
                base = base.replace("Starch", "Stärke").replace("Corn", "Mais")
            elif lang == "ES":
                base = base.replace("Starch", "Almidón").replace("Corn", "Maíz")
            rows.append(
                {
                    "MANDT": MANDT,
                    "MATNR": mat["MATNR"],
                    "SPRAS": lang,
                    "MAKTX": pad(base, 40),
                }
            )
    return rows


def gen_vbak(rng: random.Random, n: int, customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """VBAK - sales document header."""
    rows = []
    for i in range(1, n + 1):
        cust = rng.choice(customers)
        plant = rng.choice(PLANTS)
        rows.append(
            {
                "MANDT": MANDT,
                "VBELN": vbeln(2000000 + i),
                "AUART": rng.choice(ORDER_TYPES),
                "KUNNR": cust["KUNNR"],
                "VKORG": plant[0][:2] + "00",
                "WAERK": plant[3],
                "NETWR": amount(rng, 500.0, 480000.0),
                "ERDAT": sap_date(rng),
                "ERZET": sap_time(rng),
                "ERNAM": rng.choice(["MIGRATION", "EDI_IN", "SAPUSER"]),
                "GBSTK": rng.choice(ORDER_STATUS),
                "LVORM": lvorm(rng, 0.01),
            }
        )
    return rows


def gen_vbap(
    rng: random.Random,
    n: int,
    headers: List[Dict[str, Any]],
    materials: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """VBAP - sales document items. Every VBELN here exists in VBAK and every
    MATNR exists in MARA; that is what makes this an extract rather than
    noise."""
    rows = []
    per_header: Dict[str, int] = {}
    for _ in range(n):
        head = rng.choice(headers)
        seq = per_header.get(head["VBELN"], 0) + 1
        per_header[head["VBELN"]] = seq
        mat = rng.choice(materials)
        qty = quantity(rng, 1.0, 4000.0)
        price = amount(rng, 0.4, 95.0)
        rows.append(
            {
                "MANDT": MANDT,
                "VBELN": head["VBELN"],
                "POSNR": posnr(seq),
                "MATNR": mat["MATNR"],
                "WERKS": rng.choice(PLANTS)[0],
                "KWMENG": qty,
                "VRKME": mat["MEINS"],
                "NETWR": round(qty * price, 2),
                "WAERK": head["WAERK"],
                "ABGRU": rng.choice(["", "", "", "01", "02"]),
                "ERDAT": head["ERDAT"],
            }
        )
    return rows


def gen_aufk(rng: random.Random, n: int, materials: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """AUFK - production order master."""
    rows = []
    for i in range(1, n + 1):
        mat = rng.choice(materials)
        created = sap_date(rng)
        start = sap_date_after(rng, created, 1, 60)
        rows.append(
            {
                "MANDT": MANDT,
                "AUFNR": aufnr(60000000 + i),
                "AUART": rng.choice(["PP01", "PP02", "ZP10"]),
                "WERKS": rng.choice(PLANTS)[0],
                "MATNR": mat["MATNR"],
                "GAMNG": quantity(rng, 100.0, 25000.0),
                "GMEIN": mat["MEINS"],
                "GSTRP": start,
                # Scheduled finish is after scheduled start; the order is
                # created before either.
                "GLTRP": sap_date_after(rng, start, 1, 45),
                "ERDAT": created,
                "STTXT": rng.choice(["REL  PRC  MANC", "CRTD", "TECO  DLV", "REL  CNF"]),
            }
        )
    return rows


def gen_mcha(rng: random.Random, n: int, materials: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """MCHA - batch master. Shelf-life dates are where '00000000' turns up in
    real data, because a batch that never expires has no expiry date."""
    rows = []
    for _i in range(1, n + 1):
        mat = rng.choice(materials)
        created = sap_date(rng, 2024, 2026)
        rows.append(
            {
                "MANDT": MANDT,
                "MATNR": mat["MATNR"],
                "WERKS": rng.choice(PLANTS)[0],
                "CHARG": f"B{str(rng.randint(1, 999999)).zfill(9)}",
                "ERSDA": created,
                # Shelf life runs from creation. '00000000' is a batch with no
                # expiry, which is normal for a non-perishable starch.
                "VFDAT": rng.choice([sap_date_after(rng, created, 90, 1095), "00000000"]),
                "ZUSTD": rng.choice(["", "01", "02"]),
                "LVORM": lvorm(rng),
            }
        )
    return rows


# --------------------------------------------------------------------------
# Edge cases. Each returns (relative_path, description, code_path) and writes
# its own files, so adding one is a single function and a single list entry.
# --------------------------------------------------------------------------


def build_edge_cases(
    root: str, rng: random.Random, materials: List[Dict[str, Any]]
) -> List[Dict[str, str]]:
    """Write every edge case under `root` and return the manifest rows."""
    cases: List[Dict[str, str]] = []

    def case(cid: str, path: str, what: str, proves: str, code_path: str) -> None:
        cases.append(
            {"id": cid, "path": path, "what": what, "proves": proves, "code_path": code_path}
        )

    base = materials[0]

    def mara_row(**over: Any) -> Dict[str, Any]:
        row = {
            "MANDT": MANDT,
            "MATNR": base["MATNR"],
            "MTART": "FERT",
            "MATKL": "STARCH",
            "MEINS": "KG",
            "BRGEW": 25.0,
            "ERSDA": "20250114",
            "LVORM": " ",
        }
        row.update(over)
        return row

    # EC-01 - a required column arrives null.
    write_jsonl(
        os.path.join(root, "ec01_null_required_key", "mara.json"),
        [
            mara_row(),
            mara_row(MATNR=None),
            mara_row(MATNR=matnr(1000002)),
            {"MANDT": MANDT, "MTART": "ROH"},  # MATNR absent entirely, not just null
        ],
    )
    case(
        "EC-01",
        "ec01_null_required_key/mara.json",
        "4 rows, 2 with MATNR null or absent",
        "required_columns quarantines the bad rows; bronze + quarantine = 4",
        "quality.split_good_bad",
    )

    # EC-02 - the same business key twice.
    write_jsonl(
        os.path.join(root, "ec02_duplicate_key", "mara.json"),
        [
            mara_row(),
            mara_row(BRGEW=99.9),  # same MATNR, different payload
            mara_row(MATNR=matnr(1000003)),
        ],
    )
    case(
        "EC-02",
        "ec02_duplicate_key/mara.json",
        "3 rows, 2 sharing one MATNR with different payloads",
        "unique_columns keeps one and quarantines the other, deterministically",
        "quality._duplicate_flag_column",
    )

    # EC-03/04/05 - drift, as three separate second-batch files. Each is meant
    # to be ingested into a table the baseline already created.
    write_jsonl(os.path.join(root, "ec03_drift_added", "mara.json"), [mara_row(NEW_FIELD="X")])
    case(
        "EC-03",
        "ec03_drift_added/mara.json",
        "baseline shape plus one new column",
        "schema_changed true, schema_drift_json names the added column, merge_schema adds it",
        "schema_registry + bronze_writer merge_schema",
    )

    dropped = mara_row()
    dropped.pop("MATKL")
    write_jsonl(os.path.join(root, "ec04_drift_removed", "mara.json"), [dropped])
    case(
        "EC-04",
        "ec04_drift_removed/mara.json",
        "baseline shape minus MATKL",
        "schema_drift_json names the removed column; existing rows keep their value",
        "schema_registry drift detection",
    )

    write_jsonl(os.path.join(root, "ec05_drift_type", "mara.json"), [mara_row(BRGEW="25.0")])
    case(
        "EC-05",
        "ec05_drift_type/mara.json",
        "BRGEW arrives as a string where the baseline inferred a double",
        "type_changed is reported rather than silently coerced or dropped",
        "schema_registry type_changed",
    )

    # EC-06 - a record the reader can identify but not parse.
    path = os.path.join(root, "ec06_corrupt_record", "mara.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(mara_row(), ensure_ascii=False) + "\n")
        fh.write('{"MANDT": "100", "MATNR": "00000000000100000, BROKEN\n')
        fh.write(json.dumps(mara_row(MATNR=matnr(1000004)), ensure_ascii=False) + "\n")
    case(
        "EC-06",
        "ec06_corrupt_record/mara.json",
        "3 lines, the middle one unparseable JSON",
        "PERMISSIVE puts it in _corrupt_record rather than failing the file",
        "json_reader mode=PERMISSIVE",
    )

    # EC-07 - the #146 regression, in SAP clothing.
    write_jsonl(
        os.path.join(root, "ec07_jsonl_multiline", "vbak.jsonl"),
        [{"MANDT": MANDT, "VBELN": vbeln(2000001 + i), "NETWR": 100.0 + i} for i in range(5)],
    )
    case(
        "EC-07",
        "ec07_jsonl_multiline/vbak.jsonl",
        "5 records, one per line, .jsonl extension, ingested with multiline=true",
        "effective_multiline forces multiLine off: 5 rows, not 1",
        "json_reader.effective_multiline (#146)",
    )

    # EC-08 - a folder with nothing to ingest.
    empty_dir = os.path.join(root, "ec08_empty_folder", "vbrk")
    os.makedirs(empty_dir, exist_ok=True)
    with open(os.path.join(empty_dir, "README.txt"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("Extract produced no rows for this table in this cutover window.\n")
    case(
        "EC-08",
        "ec08_empty_folder/vbrk/",
        "a folder holding one non-ingestable file",
        "status skipped with a reason, never failed, and the run does not alert",
        "directory_ingestion, the not-inner_files branch",
    )

    # EC-09 - folder-as-table, split the way a real extract splits it.
    for part in (1, 2, 3):
        write_jsonl(
            os.path.join(root, "ec09_folder_as_table", "vbap", f"vbap_part{part}.json"),
            [
                {
                    "MANDT": MANDT,
                    "VBELN": vbeln(2100000 + part),
                    "POSNR": posnr(i + 1),
                    "MATNR": base["MATNR"],
                    "KWMENG": 10.0 * (i + 1),
                }
                for i in range(4)
            ],
        )
    case(
        "EC-09",
        "ec09_folder_as_table/vbap/",
        "one logical table split across 3 files, as a large extract arrives",
        "the folder becomes one table of 12 rows, not three tables",
        "directory_ingestion folder-as-table",
    )

    # EC-10 - the control file sitting beside the data.
    d = os.path.join(root, "ec10_mixed_formats")
    write_jsonl(os.path.join(d, "mara.json"), [mara_row()])
    write_csv(os.path.join(d, "_control.csv"), [{"TABLE": "MARA", "ROWS": "1", "STATUS": "OK"}])
    with open(os.path.join(d, "extract.log"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("2026-08-20 03:00:01 INFO export finished rc=0\n")
    case(
        "EC-10",
        "ec10_mixed_formats/",
        "a JSON data file beside a CSV control file and a log",
        "a json run ingests only the json and ignores the rest without erroring",
        "formats-aware list_source_files",
    )

    # EC-11 - field names an extract tool produces and a catalog will not take.
    write_jsonl(
        os.path.join(root, "ec11_identifier_canonicalization", "zcustom.json"),
        [
            {
                "Material Number": base["MATNR"],
                "Gross Weight (KG)": 25.0,
                "Plant/Site": "1010",
                "Änderungsdatum": "20250114",
                "unit.of.measure": "KG",
                "50%_moisture": False,
                "column-with-dash": "x",
            }
        ],
    )
    case(
        "EC-11",
        "ec11_identifier_canonicalization/zcustom.json",
        "field names with spaces, parentheses, slashes, dots, percent, dash and an umlaut",
        "names are canonicalized deterministically and identity is preserved, not flattened",
        "naming / identifier canonicalization",
    )

    # EC-12 - two source names that canonicalize to the same thing.
    write_jsonl(
        os.path.join(root, "ec12_canonicalization_collision", "zcollide.json"),
        [{"Order Id": "A1", "Order.Id": "B2", "order_id": "C3"}],
    )
    case(
        "EC-12",
        "ec12_canonicalization_collision/zcollide.json",
        "three distinct source names that collapse to one canonical name",
        "the collision is detected and disambiguated, never silently last-wins",
        "naming collision detection",
    )

    # EC-13 - the single most common migration defect.
    write_csv(
        os.path.join(root, "ec13_leading_zeros", "mara.csv"),
        [
            {"MANDT": MANDT, "MATNR": matnr(1000001), "PSTLZ": "01234", "WERKS": "1010"},
            {"MANDT": MANDT, "MATNR": matnr(42), "PSTLZ": "00501", "WERKS": "2010"},
        ],
    )
    case(
        "EC-13",
        "ec13_leading_zeros/mara.csv",
        "18-char zero-padded MATNR and a US postal code starting 00",
        "must land as text; csv_infer_schema on turns them into numbers and the padding is gone (#371)",
        "csv_reader inference vs schema_hint_ddl",
    )

    # EC-14 - the SAP empty date.
    write_jsonl(
        os.path.join(root, "ec14_sap_null_dates", "mcha.json"),
        [
            {"MANDT": MANDT, "CHARG": "B000000001", "VFDAT": "20271231", "LAEDA": "20250101"},
            {"MANDT": MANDT, "CHARG": "B000000002", "VFDAT": "00000000", "LAEDA": "00000000"},
            {"MANDT": MANDT, "CHARG": "B000000003", "VFDAT": None, "LAEDA": ""},
        ],
    )
    case(
        "EC-14",
        "ec14_sap_null_dates/mcha.json",
        "'00000000', null and empty string in the same date column",
        "bronze keeps all three distinct; collapsing them is a Silver decision",
        "bronze source-faithfulness",
    )

    # EC-15 - trailing spaces are data.
    write_csv(
        os.path.join(root, "ec15_trailing_spaces", "kna1.csv"),
        [
            {"MANDT": MANDT, "KUNNR": kunnr(100001), "NAME1": pad("Nestlé Deutschland AG", 35)},
            {"MANDT": MANDT, "KUNNR": kunnr(100002), "NAME1": pad("Danone S.A.", 35)},
        ],
    )
    case(
        "EC-15",
        "ec15_trailing_spaces/kna1.csv",
        "CHAR fields space-padded to width 35, inside quoted CSV fields",
        "padding survives the read; ignoreTrailingWhiteSpace must not silently strip it",
        "csv_reader options",
    )

    # EC-16 - non-ASCII across scripts.
    write_jsonl(
        os.path.join(root, "ec16_unicode", "t001w.json"),
        [
            {"MANDT": MANDT, "WERKS": "3020", "NAME1": "San Juan del Río", "LAND1": "MX"},
            {"MANDT": MANDT, "WERKS": "4010", "NAME1": "Mogi Guaçu", "LAND1": "BR"},
            {"MANDT": MANDT, "WERKS": "1020", "NAME1": "Krefeld Werk – Süd", "LAND1": "DE"},
            {"MANDT": MANDT, "WERKS": "6010", "NAME1": "上海工厂", "LAND1": "CN"},
        ],
    )
    case(
        "EC-16",
        "ec16_unicode/t001w.json",
        "accents, an en dash, and CJK in the same column",
        "UTF-8 survives read, write and the catalog round trip",
        "readers, bronze_writer",
    )

    # EC-17 - depth and arrays.
    write_json_array(
        os.path.join(root, "ec17_deep_nesting", "zspec.json"),
        [
            {
                "MANDT": MANDT,
                "MATNR": base["MATNR"],
                "SPEC": {
                    "PHYSICAL": {
                        "MOISTURE": {"MIN": 0.0, "MAX": 13.5, "UOM": "PCT"},
                        "PARTICLE": {"MESH": [40, 80, 200], "UOM": "MESH"},
                    },
                    "MICRO": {
                        "TESTS": [
                            {"NAME": "TPC", "LIMIT": 10000, "UOM": "CFU/G"},
                            {"NAME": "YEAST", "LIMIT": 100, "UOM": "CFU/G"},
                        ]
                    },
                },
            }
        ],
    )
    case(
        "EC-17",
        "ec17_deep_nesting/zspec.json",
        "four levels of nesting, arrays of scalars and arrays of structs",
        "nesting is preserved, not flattened - the bronze rule this repo will not bend",
        "bronze_writer, no flattening by design",
    )

    # EC-18 - a zero-byte file.
    p = os.path.join(root, "ec18_empty_file", "vbrp.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    open(p, "w", encoding="utf-8").close()
    case(
        "EC-18",
        "ec18_empty_file/vbrp.json",
        "a 0-byte file, which a failed extract leaves behind",
        "handled without CANNOT_INFER_EMPTY_SCHEMA killing the run",
        "readers, the empty-dataset path",
    )

    # EC-19 - header, no rows. This is the shape that failed staging in July.
    p = os.path.join(root, "ec19_header_only", "vbrk.csv")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="\n") as fh:
        fh.write('"MANDT","VBELN","NETWR","WAERK"\n')
    case(
        "EC-19",
        "ec19_header_only/vbrk.csv",
        "a CSV with a header and zero data rows",
        "an empty table is a valid outcome, not a failure - the 2026-07-29 staging crash",
        "csv_reader, the empty-dataset path",
    )

    # EC-20 - CSV that exercises the parser rather than the reader.
    p = os.path.join(root, "ec20_csv_quoting", "kna1.csv")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="\n") as fh:
        fh.write('"MANDT","KUNNR","NAME1","STRAS"\n')
        fh.write('"100","0000100001","Müller, Meier & Co. KG","Hauptstr. 1"\n')
        fh.write('"100","0000100002","He said ""premium grade""","Main St 2"\n')
        fh.write('"100","0000100003","Line one\nLine two","Av. Central 3"\n')
        fh.write('"100","0000100004","Trailing comma test,","Rua 4"\n')
    case(
        "EC-20",
        "ec20_csv_quoting/kna1.csv",
        "embedded comma, escaped double quotes, and a newline inside a quoted field",
        "4 rows, not 5 - the embedded newline must not split a record",
        "csv_reader multiLine and quote handling",
    )

    # EC-21 - headerless, which needs schema_hint_ddl to be ingestable at all.
    write_csv(
        os.path.join(root, "ec21_headerless", "t001w.csv"),
        [
            {"a": MANDT, "b": "1010", "c": "Hamburg Werk", "d": "DE"},
            {"a": MANDT, "b": "2010", "c": "Cedar Rapids", "d": "US"},
        ],
        header=False,
    )
    case(
        "EC-21",
        "ec21_headerless/t001w.csv",
        "no header row; columns would be _c0.._c3",
        "refused unless schema_hint_ddl names the columns - config fails closed",
        "config validation, csv_header false",
    )

    # EC-22 - numbers at the edges of their types.
    write_jsonl(
        os.path.join(root, "ec22_numeric_extremes", "vbap.json"),
        [
            {"VBELN": vbeln(2200001), "NETWR": 0.0, "KWMENG": 0.001},
            {"VBELN": vbeln(2200002), "NETWR": 99999999999.99, "KWMENG": 999999.999},
            {"VBELN": vbeln(2200003), "NETWR": -1250.75, "KWMENG": -5.0},
            {"VBELN": vbeln(2200004), "NETWR": 0.1 + 0.2, "KWMENG": 1e-7},
        ],
    )
    case(
        "EC-22",
        "ec22_numeric_extremes/vbap.json",
        "zero, a credit note's negative amount, 11 significant digits, and float noise",
        "precision is not silently lost and negatives are not treated as invalid",
        "bronze_writer type handling",
    )

    # EC-23 - the three kinds of nothing.
    write_jsonl(
        os.path.join(root, "ec23_null_vs_empty", "kna1.json"),
        [
            {"KUNNR": kunnr(100001), "REGIO": "BY", "TELF1": "+49 40123456"},
            {"KUNNR": kunnr(100002), "REGIO": "", "TELF1": None},
            {"KUNNR": kunnr(100003), "REGIO": " ", "TELF1": ""},
        ],
    )
    case(
        "EC-23",
        "ec23_null_vs_empty/kna1.json",
        "null, empty string and a single space in the same columns",
        "all three stay distinct; required_columns treats only null as missing",
        "quality._missing_columns",
    )

    # EC-24 - a wide table.
    wide = {"MANDT": MANDT, "MATNR": base["MATNR"]}
    for i in range(1, 121):
        wide[f"ZZFIELD{i:03d}"] = rng.choice(["X", " ", "", None, str(rng.randint(0, 999))])
    write_jsonl(os.path.join(root, "ec24_wide_row", "zmara_ext.json"), [wide])
    case(
        "EC-24",
        "ec24_wide_row/zmara_ext.json",
        "122 columns, the append-a-Z-field pattern every SAP shop ends up with",
        "column count alone does not break the write or the catalog comment path",
        "bronze_writer, catalog_metadata",
    )

    # EC-25 - the same keys arriving twice, which is what a re-run looks like.
    first = [
        {"MANDT": MANDT, "MATNR": matnr(1000001), "MTART": "FERT", "BRGEW": 25.0},
        {"MANDT": MANDT, "MATNR": matnr(1000002), "MTART": "HALB", "BRGEW": 30.0},
    ]
    second = [
        {"MANDT": MANDT, "MATNR": matnr(1000001), "MTART": "FERT", "BRGEW": 27.5},
        {"MANDT": MANDT, "MATNR": matnr(1000003), "MTART": "ROH", "BRGEW": 12.0},
    ]
    write_jsonl(os.path.join(root, "ec25_merge_rerun", "batch1", "mara.json"), first)
    write_jsonl(os.path.join(root, "ec25_merge_rerun", "batch2", "mara.json"), second)
    case(
        "EC-25",
        "ec25_merge_rerun/batch1|batch2",
        "two batches sharing one key, with a changed value on the shared row",
        "append gives 4 rows; merge on MATNR gives 3 with BRGEW updated to 27.5",
        "bronze_writer merge_keys, write_mode",
    )

    # EC-26 - strings that look like numbers and must not become them.
    write_jsonl(
        os.path.join(root, "ec26_numeric_looking_strings", "zkeys.json"),
        [
            {
                "MATNR": matnr(1000001),
                "PSTLZ": "01234",
                "TELF1": "0049401234567",
                "EAN11": "04012345678901",
                "BUKRS": "0001",
                "VERSION": "1.10",
            }
        ],
    )
    case(
        "EC-26",
        "ec26_numeric_looking_strings/zkeys.json",
        "keys, postal codes, phone numbers, EANs and a version that are all strings",
        "none are coerced to numbers; '1.10' must not become 1.1",
        "readers type inference",
    )

    return cases


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def generate(out: str, rows: int, seed: int, skip_parquet: bool) -> Dict[str, Any]:
    rng = random.Random(seed)

    # Scale each table off the requested total, holding the proportions that
    # make the result read like an extract: a few plants, many order items.
    scale = rows / 5000.0

    def n(base_count: int) -> int:
        return max(1, int(round(base_count * scale)))

    migration = os.path.join(out, "migration")
    edge = os.path.join(out, "edge_cases")

    plants = gen_t001w()
    customers = gen_kna1(rng, n(400))
    vendors = gen_lfa1(rng, n(150))
    materials = gen_mara(rng, n(500))
    descriptions = gen_makt(rng, materials)
    headers = gen_vbak(rng, n(700), customers)
    items = gen_vbap(rng, n(1800), headers, materials)
    prod_orders = gen_aufk(rng, n(250), materials)
    batches = gen_mcha(rng, n(200), materials)

    # Format per table is chosen the way a real extract chooses it: master
    # data with nested attributes as JSON, flat reference tables as CSV, the
    # big item table as Parquet.
    write_json_array(os.path.join(migration, "kna1.json"), customers)
    write_json_array(os.path.join(migration, "mara.json"), materials)
    write_jsonl(os.path.join(migration, "vbak.jsonl"), headers)
    write_jsonl(os.path.join(migration, "aufk.jsonl"), prod_orders)
    write_jsonl(os.path.join(migration, "mcha.jsonl"), batches)
    write_csv(os.path.join(migration, "t001w.csv"), plants)
    write_csv(os.path.join(migration, "lfa1.csv"), vendors)
    write_csv(os.path.join(migration, "makt.csv"), descriptions)

    parquet_written = False
    if not skip_parquet:
        parquet_written = write_parquet(os.path.join(migration, "vbap.parquet"), items)
    if not parquet_written:
        # Fall back rather than lose the biggest table entirely.
        write_jsonl(os.path.join(migration, "vbap.jsonl"), items)

    cases = build_edge_cases(edge, rng, materials)

    counts = {
        "t001w": len(plants),
        "kna1": len(customers),
        "lfa1": len(vendors),
        "mara": len(materials),
        "makt": len(descriptions),
        "vbak": len(headers),
        "vbap": len(items),
        "aufk": len(prod_orders),
        "mcha": len(batches),
    }

    manifest = {
        "seed": seed,
        "requested_rows": rows,
        "migration_row_counts": counts,
        "migration_total_rows": sum(counts.values()),
        "vbap_format": "parquet" if parquet_written else "jsonl (pyarrow unavailable)",
        "edge_cases": cases,
    }
    with open(os.path.join(out, "MANIFEST.json"), "w", encoding="utf-8", newline="\n") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", required=True, help="output directory (recreated if it exists)")
    ap.add_argument("--rows", type=int, default=5000, help="approx total migration rows")
    ap.add_argument("--seed", type=int, default=20260820, help="random seed")
    ap.add_argument("--skip-parquet", action="store_true", help="write vbap as jsonl instead")
    ap.add_argument("--keep", action="store_true", help="do not clear --out first")
    args = ap.parse_args()

    if os.path.isdir(args.out) and not args.keep:
        shutil.rmtree(args.out)
    os.makedirs(args.out, exist_ok=True)

    m = generate(args.out, args.rows, args.seed, args.skip_parquet)

    print(f"seed              : {m['seed']}")
    print(f"migration rows    : {m['migration_total_rows']}")
    for table, count in sorted(m["migration_row_counts"].items()):
        print(f"  {table:<8} {count:6d}")
    print(f"vbap format       : {m['vbap_format']}")
    print(f"edge cases        : {len(m['edge_cases'])}")
    print(f"manifest          : {os.path.join(args.out, 'MANIFEST.json')}")


if __name__ == "__main__":
    main()
