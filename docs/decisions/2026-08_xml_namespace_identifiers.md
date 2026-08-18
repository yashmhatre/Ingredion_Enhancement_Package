# Preserve XML namespace-prefix spelling through identifier canonicalization

Spark's XML reader can expose namespaced elements as fields such as `a:id`, but
the Bronze identifier contract rejects `:`. Dropping the prefix would make
distinct prefixed names indistinguishable and could silently merge data. A
prefix is not namespace identity: its URI binding must also be stable.

**Status:** Proposed — awaiting Tier 2 sign-off

Written against `dev` @ `9e0dbea` for #337. This record must be signed off
before #316 implements the XML reader.

---

## Decision

Canonicalize namespace separators by replacing each `:` with `__`, preserving
the namespace prefix: `a:id` becomes `a__id`, while `b:id` becomes `b__id`.
Apply the rule recursively to fields in structs and arrays or maps containing
structs. Map keys are values, not field identifiers, and are unchanged.

Before writing, compare sibling names at every nesting level after
canonicalization. If two source names resolve to the same canonical name, fail
the containing ingestion unit with an error that identifies the conflicting
source paths. Never choose one value, append an ordinal, or overwrite a field.

This is identifier canonicalization at the reader boundary. It changes names
only as required to make the source structure addressable; it does not flatten,
explode, or otherwise reshape nested XML.

Require every XML ingestion unit to use one stable prefix-to-namespace-URI
mapping. Preflight must fail the entire unit when the same prefix maps to
different URIs, or the same URI is represented by different prefixes. This is
an explicit source contract for v1; prefix spelling alone is not XML namespace
identity.

Do not accept `schema_hint_ddl` for XML v1. Spark binds an explicit schema to
raw prefixed names before canonicalization, so a canonical hint such as
`a__id STRING` can silently yield nulls. Inference remains mandatory until a
separate signed decision defines a lossless raw-to-canonical hint mapping.

## Considered options

| Option | Disposition |
| --- | --- |
| Set Spark `ignoreNamespace=true` | Rejected. It discards the distinction between prefixes and makes same-local-name collisions unavoidable. |
| Strip the prefix | Rejected for the same reason: `a:id` and `b:id` would both become `id`. |
| Replace `:` with `_` | Rejected. A single underscore is visually ambiguous with ordinary source names and makes collisions less apparent. |
| Make the replacement configurable | Rejected for the first implementation. Different configurations would give the same source incompatible Bronze schemas. |
| Replace `:` with `__`, enforce stable prefix/URI bindings, and fail on collisions | Proposed. It preserves prefix spelling without pretending the prefix alone is namespace identity. |

## Consequences

- A source field already named `a__id` conflicts with `a:id`; the ingestion
  unit fails rather than silently changing either field.
- Quality-rule paths, metadata profiles, and downstream consumers refer to
  canonical names after the XML reader boundary. XML schema hints are rejected.
- Preflight tests must cover prefix-to-URI drift across files in one ingestion
  unit before this decision can be signed or the implementation merged.
- Tests must cover top-level and nested names, attributes, arrays of structs,
  map values containing structs, and deterministic collision errors.
- A future change to the separator or namespace policy requires a superseding
  decision and a schema migration plan.
