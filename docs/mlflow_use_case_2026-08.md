# MLflow — is there a use case here? August 2026

The deliverable for **#212**, which asks for a concrete MLflow use case in this
platform *or* a recorded finding that none exists, in the style of
`docs/buy_vs_build_2026-08.md`: evaluated against what the code actually does,
not against a README.

Written against `dev` @ `b7fbdb1`.

---

## Verdict

**Defer. There is no MLflow use case in this platform today, and the one
question that looks like it needs a model registry is already answered by a
column.**

This is not "MLflow is bad" or "we might need it later, who knows". It is a
specific claim with a specific trigger for revisiting, in §4.

## 1. What MLflow is for, and what this platform has

MLflow's four surfaces are experiment tracking, a model registry, model
packaging/serving, and (more recently) LLM evaluation. Each presupposes
something this codebase does not contain.

**There is nothing that trains, fits, or versions a model.** Checked rather
than assumed — across `bronze_layer/bronze_ingest/`, there is no `mlflow`, no
`sklearn`, no `.fit(`, no training loop, and no model artifact. The only
matches for "model" are `model_id` (an endpoint *name*) and log messages about
a model call failing.

Every AI surface in this platform is a **call to a hosted endpoint**:

| Surface | What it actually does |
| --- | --- |
| `AIFunctionsMetadataDrafter` (the default) | `ai_query` against a Databricks-served foundation model, pinned by endpoint name |
| `AnthropicMetadataDrafter` | An HTTP call to the Anthropic API |

Neither produces a model. Both consume one that somebody else versions, serves
and retires.

## 2. The two candidates #212 names, examined

**#61's volume-anomaly baseline is not a model.** It is a rolling aggregate — a
median, or the ±2σ band #262 proposed before being folded into #61 — computed
over `_ingestion_audit` rows. There is no training step, no held-out data, no
hyperparameter, and no artifact to version. Tracking it in MLflow would record
a SQL query's output as an experiment run. If the number is wrong, the fix is
to change the query, not to compare two model versions.

**#208's AI metadata job does not classify with a model of ours.** PII flags
and descriptions are *drafted by prompting a foundation model*. The prompt is
in source control; the model is pinned by endpoint name
(`databricks-claude-opus-4-8`) and that pin is already recorded, with its
reasoning, in `docs/decisions/2026-08_ai_genie_architecture.md` Amendment 1.
The version boundary is the endpoint's, not ours.

## 3. The one thing that genuinely looks like it needs a registry — and does not

The real question underneath "should we use MLflow" here is usually
**provenance**: *given a draft in `_ai_metadata`, which model produced it, and
when?* That is a legitimate need, and it is what a registry would be reached
for.

It is already answered, by columns on `_ai_metadata`:

```python
StructField("schema_fingerprint", StringType(), nullable=True),  # what it described
StructField("model_id",           StringType(), nullable=True),  # which model
StructField("generated_at",       TimestampType(), nullable=False),  # when
StructField("source_run_id",      StringType(), nullable=True),  # from which run
```

A draft can be traced to a model, a moment, a schema version and an ingestion
run, by querying one table. **A model registry would add a second place to look
without answering anything the first place does not.** This is the same shape as
the `_schema_registry`/`_ingestion_audit` split the architecture already uses:
facts in a table, queryable, no service required.

## 4. What would change this verdict

Stated concretely so this can be reopened on evidence rather than re-argued:

1. **#61's baseline becomes learned rather than fixed.** A seasonal or
   multivariate model that must be retrained, compared across versions and
   rolled back is a genuine registry case. A ±2σ band is not. This is the most
   likely trigger of the three.
2. **PII classification becomes a trained classifier rather than a prompt.**
   Note this is now *less* likely, not more: `docs/buy_vs_build_2026-08.md`'s
   2026-08-11 amendment records that DQX ships 80+ checks including PII
   validation, and `discoverx` does semantic classification — so the realistic
   path is adopting one, not training one.
3. **Prompt/model changes need systematic evaluation across versions.** If the
   drafter's output quality has to be compared between endpoints or prompt
   revisions on a fixed sample, that is MLflow's LLM-evaluation surface — a
   different product area from the registry, and it should be evaluated as its
   own question rather than inherited from this one.

## 5. Cost of being wrong

Low, in both directions.

Adopting MLflow now would add a dependency and a service to a package whose
`install_requires` is one entry, to track things that are not models — and
`docs/buy_vs_build_2026-08.md`'s DQX amendment shows how quickly a dependency
argument compounds here (a Python floor and a transitively-pinned
`databricks-sdk` that this package excludes on purpose).

Deferring costs nothing recoverable. MLflow is available in the workspace
whenever a real model appears, and none of the columns above would have to
change to start using it — `model_id` is a string, and a registry URI is a
string.

---

**Recorded so the question is not silently re-asked.** BR-001-shaped requests
tend to name MLflow because it is on every Databricks platform diagram. The
answer is not "no", it is **"not until something here is actually a model"**,
and §4 says what that would look like.
