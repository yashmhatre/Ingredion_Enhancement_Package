"""
Data contracts for bronze ingestion (#264).

A contract is a declarative statement of what a source is *supposed* to look
like: which columns exist, what type each one is, and which of them must
always carry a value. It is spec plus validation only - wiring it into the
live pipeline is #265.

Scope, and why it is narrower than the issue as written
-------------------------------------------------------

#264 asks for "column names, types, nullability, and value ranges". This
module deliberately implements the first three and **not** value ranges.

`docs/bronze_silver_contract.md` records that #109 split five of seven
quality rules - `in_range`, `regex`, `in_set`, `expression`, `freshness` -
*out* of bronze and reserved them for a Silver rule engine, and #76 archived
`flattener.py` on the same reasoning. Adding value ranges here would walk
that decision back silently, and would give this repo two places to express
the same business rule. So the contract describes STRUCTURE, which is what
bronze owns, and points at #109 for VALUE rules, which Silver owns. If that
call is wrong it should be overturned in the decision record, not worked
around here.

One source of truth for non-null
--------------------------------

`nullable: false` and `IngestionConfig.required_columns` say exactly the same
thing. Rather than let them drift apart, the contract is the declaration and
`required_columns()` derives the config value from it (see #250, where an
empty `required_columns` made the quality gate a no-op that looked like a
pass).

The type vocabulary is Spark's, not one of our own
--------------------------------------------------

`type:` is compared against `StructField.dataType.simpleString()`, so it must
use Spark's spelling exactly:

    string  bigint  int  double  boolean  timestamp  date
    decimal(10,2)  array<string>  struct<a:string>

Note **`bigint`, not `long`** - `LongType().simpleString()` returns `bigint`,
and the first draft of this module got that wrong. `test_the_type_vocabulary_is
_sparks_own` pins the mapping against the real Spark types rather than against
a remembered list, in the manner of `test_schema_json_really_is_a_flat_array`
(#276): the point is that the test fails when the assumption is wrong, instead
of confirming it.

Why nullability is not checked against the schema
-------------------------------------------------

It cannot be. Spark's JSON inference marks every field nullable regardless of
the data, so a schema-level nullability check would pass on any input and
mean nothing - the same shape of no-op #250 is about. Non-null is a
*per-row* property, which is what the quality gate already evaluates. So
`validate_schema` never reports a nullability violation; `required_columns()`
routes it to the gate that can actually see it.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

#: Violation kinds, named so callers can branch without matching on prose.
MISSING_COLUMN = "missing_column"
TYPE_MISMATCH = "type_mismatch"
UNDECLARED_COLUMN = "undeclared_column"


class ContractError(Exception):
    """Raised for a malformed contract - not for data that violates one."""


@dataclass(frozen=True)
class ColumnContract:
    name: str
    #: Spark's simple type string, e.g. "string", "long", "double".
    #: Compared against StructField.dataType.simpleString().
    type: str
    #: False means "must carry a value in every row". Enforced by the quality
    #: gate via required_columns(), not by validate_schema - see module docs.
    nullable: bool = True
    description: Optional[str] = None


@dataclass(frozen=True)
class ContractViolation:
    kind: str
    column: str
    expected: Optional[str] = None
    actual: Optional[str] = None

    def __str__(self) -> str:
        if self.kind == MISSING_COLUMN:
            return f"missing column {self.column!r} (contract declares type {self.expected})"
        if self.kind == TYPE_MISMATCH:
            return (
                f"column {self.column!r} has type {self.actual}, contract declares {self.expected}"
            )
        return f"column {self.column!r} is present in the data but not declared by the contract"


@dataclass
class DataContract:
    source: str
    columns: List[ColumnContract]
    version: int = 1
    #: Bronze is deliberately additive - a new upstream column is normal and
    #: is what schema drift detection (#256) exists to report. So an
    #: undeclared column is NOT a violation by default. Set False for a
    #: source that must be exactly what it says it is.
    allow_undeclared_columns: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.source:
            raise ContractError("contract must name the source it describes")
        if not self.columns:
            raise ContractError(
                f"contract for {self.source!r} declares no columns. An empty contract "
                "validates everything and therefore checks nothing (#250)."
            )
        seen = set()
        for col in self.columns:
            if col.name in seen:
                raise ContractError(
                    f"contract for {self.source!r} declares column {col.name!r} twice"
                )
            seen.add(col.name)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "DataContract":
        if not isinstance(raw, dict):
            raise ContractError(f"contract must be a mapping, got {type(raw).__name__}")

        unknown = set(raw) - {
            "source",
            "version",
            "columns",
            "allow_undeclared_columns",
            "metadata",
        }
        if unknown:
            # Same reasoning as ingest_directory_to_bronze's unknown-key guard:
            # a silently dropped key is a rule the author believes is in force
            # and which never runs.
            raise ContractError(f"unknown key(s) in contract: {sorted(unknown)}")

        raw_columns = raw.get("columns")
        if not isinstance(raw_columns, list):
            raise ContractError("contract 'columns' must be a list")

        columns = []
        for entry in raw_columns:
            if not isinstance(entry, dict):
                raise ContractError(f"each column must be a mapping, got {entry!r}")
            bad = set(entry) - {"name", "type", "nullable", "description"}
            if bad:
                raise ContractError(f"unknown key(s) in column entry: {sorted(bad)}")
            if not entry.get("name"):
                raise ContractError(f"column entry is missing 'name': {entry!r}")
            if not entry.get("type"):
                raise ContractError(f"column {entry['name']!r} is missing 'type'")
            columns.append(
                ColumnContract(
                    name=entry["name"],
                    type=entry["type"],
                    nullable=bool(entry.get("nullable", True)),
                    description=entry.get("description"),
                )
            )

        return cls(
            source=raw.get("source", ""),
            columns=columns,
            version=int(raw.get("version", 1)),
            allow_undeclared_columns=bool(raw.get("allow_undeclared_columns", True)),
            metadata=raw.get("metadata") or {},
        )

    @classmethod
    def load(cls, path: str) -> "DataContract":
        """Loads a contract from a .yaml/.yml or .json file."""
        import json

        with open(path, encoding="utf-8") as fh:
            text = fh.read()

        if path.endswith((".yaml", ".yml")):
            import yaml

            raw = yaml.safe_load(text)
        else:
            raw = json.loads(text)
        return cls.from_dict(raw)

    def required_columns(self) -> List[str]:
        """
        The columns this contract says must always carry a value, in the
        shape `IngestionConfig.required_columns` wants.

        This is the single-source-of-truth seam: declare non-null once, in
        the contract, and let the quality gate enforce it (#250, #265).
        """
        return [c.name for c in self.columns if not c.nullable]

    def column(self, name: str) -> Optional[ColumnContract]:
        return next((c for c in self.columns if c.name == name), None)


def validate_schema(schema, contract: DataContract) -> List[ContractViolation]:
    """
    Checks a Spark schema (a StructType) against `contract` and returns every
    violation found - all of them, not just the first, so one run tells you
    everything that is wrong.

    Takes a schema rather than a DataFrame on purpose: it needs no Spark
    session and no action against the data, so it is testable on any machine
    and costs nothing at runtime. Use `validate_dataframe` for the
    convenience wrapper.

    Nullability is never reported here - see the module docstring.
    """
    actual = {f.name: f.dataType.simpleString() for f in schema.fields}
    violations: List[ContractViolation] = []

    for col in contract.columns:
        if col.name not in actual:
            violations.append(
                ContractViolation(MISSING_COLUMN, col.name, expected=col.type, actual=None)
            )
        elif actual[col.name] != col.type:
            violations.append(
                ContractViolation(
                    TYPE_MISMATCH, col.name, expected=col.type, actual=actual[col.name]
                )
            )

    if not contract.allow_undeclared_columns:
        declared = {c.name for c in contract.columns}
        for name in actual:
            if name not in declared:
                violations.append(
                    ContractViolation(UNDECLARED_COLUMN, name, expected=None, actual=actual[name])
                )

    return violations


def validate_dataframe(df, contract: DataContract) -> List[ContractViolation]:
    """`validate_schema` against `df.schema`. No action is taken on the data."""
    return validate_schema(df.schema, contract)


def format_violations(violations: List[ContractViolation]) -> str:
    """One violation per line, for a log message or an exception body."""
    return "\n".join(f"  - {v}" for v in violations)
