# XML document integrity is a precondition for Bronze ingestion

XML readers can recover rows from a truncated document without reporting the
document as corrupt. That behavior conflicts with Bronze source fidelity: a
partial parse can look successful while silently omitting source content.

**Status:** Proposed — awaiting Tier 2 sign-off

Written against `dev` @ `9e0dbea` for #336. This record must be signed off
before #316 implements the XML reader.

---

## Decision

Every physical XML document must pass a complete, serverless-compatible
well-formedness check before any of its rows may be written. The check runs on
the executors; the driver receives validation results and error summaries, not
document contents.

The Spark XML read then uses `FAILFAST` for corruption Spark itself detects.
XML configuration cannot override that mode through generic reader options,
including the unsafe-options escape hatch. The preflight remains necessary
because parser recovery can accept a truncated document without producing a
corrupt-record signal.

The physical document is the integrity boundary:

- a malformed standalone file fails its ingestion unit and enters the existing
  file retry/quarantine policy;
- in folder-as-table ingestion, any malformed member fails the whole folder
  ingestion unit; no sibling rows are written or archived from that attempt;
- a failed document is never archived as successfully ingested;
- no partial rows from a failed document reach Bronze or row quarantine.

This policy establishes structural completeness only. Schema conformance,
business quality, and optional XSD validation are separate concerns.

## Considered options

| Option | Disposition |
| --- | --- |
| Trust Spark `PERMISSIVE` and `_corrupt_record` | Rejected. The live #318 probe showed a truncated document returning a row without a corrupt-record failure. |
| Use Spark `FAILFAST` alone | Rejected. It catches errors Spark classifies as malformed, not every incomplete document its parser can recover. |
| Require an XSD for every source | Rejected. Well-formed XML does not imply a source has or should have a business schema, and requiring one would block schema-on-read ingestion. |
| Validate each complete document, then read with `FAILFAST` | Proposed. It fails closed without collecting source data to the driver or turning business validation into a Bronze concern. |

## Consequences

- XML costs one structural validation pass before the Spark read.
- Validation must work with Spark Connect/serverless execution and with Unity
  Catalog Volume paths; driver-local filesystem APIs are outside the contract.
- Tests must prove truncated and malformed documents produce zero Bronze writes,
  and folder-as-table tests must prove one bad member makes the folder attempt
  atomic: zero writes and zero successful archives.
- Streaming XML remains outside this decision and stays deferred under #323.
