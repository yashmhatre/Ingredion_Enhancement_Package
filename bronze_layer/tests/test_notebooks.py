"""
Tests for the notebook layer - the deployed job entrypoints (#157).

Why this file exists, since its absence was the finding: `bronze_layer/
notebooks/` contains the code the Databricks job actually runs. Before this,
142 tests covered the library and zero covered the four files that call it,
and CI did not watch the path at all. Both known live production defects
(#144, #145) were in that gap, and neither was subtle - each would have been
caught by a test that so much as executed the module.

Nothing here needs Spark, Java or a workspace. Notebooks are plain Python
with `# COMMAND ----------` separators; the only thing between them and pytest
is the handful of names the Databricks kernel injects, which `run_notebook`
supplies. The whole file runs in well under a second.
"""

import ast
import inspect
import os

import pytest
import yaml

import bronze_ingest
from bronze_ingest import directory_ingestion
from bronze_ingest.config import IngestionConfig
from tests.conftest import NOTEBOOK_DIR, FakeSpark

RESOURCES = os.path.join(os.path.dirname(NOTEBOOK_DIR), "resources")

# The widget values a deployed run supplies. Kept minimal - only what makes
# the notebook reach its summary logic.
BASE_WIDGETS = {
    "source_dir": "/Volumes/cat/sch/vol/Raw",
    "catalog": "cat",
    "schema_name": "sch",
}


def _fake_ingest(results):
    """A stand-in for ingest_directory_to_bronze that returns fixed results
    and records how it was called."""
    calls = []

    def _call(spark, **kwargs):
        calls.append(kwargs)
        return results

    _call.calls = calls
    return _call


# ---------------------------------------------------------------------------
# #144 - the empty-results path
# ---------------------------------------------------------------------------


def test_empty_results_exits_success_without_raising(run_notebook):
    """
    The #144 regression. An empty source directory is "no work to do", not a
    failure, and must not page anyone.

    This is a regression test rather than a one-off fix: FakeSpark raises
    CANNOT_INFER_EMPTY_SCHEMA on an empty inferred createDataFrame exactly as
    the real one does, so re-introducing the bug fails here rather than on a
    cluster at 3am.
    """
    run = run_notebook(
        "run_directory_ingestion",
        widgets=BASE_WIDGETS,
        patches=[(bronze_ingest, "ingest_directory_to_bronze", _fake_ingest([]))],
    )

    assert run.exited
    assert run.exit_value.startswith("SUCCESS")
    assert "nothing to ingest" in run.exit_value
    # Nothing was displayed, because there was nothing to display.
    assert run.displayed == []
    assert run.spark.created == []


# ---------------------------------------------------------------------------
# #127 - exit-status classification
# ---------------------------------------------------------------------------


def test_failed_unit_fails_the_task(run_notebook):
    results = [
        {"file": "a.json", "table": "a_bronze", "status": "success", "rows": 5},
        {"file": "b.json", "table": "b_bronze", "status": "failed", "error": "boom"},
    ]
    run = run_notebook(
        "run_directory_ingestion",
        widgets=BASE_WIDGETS,
        patches=[(bronze_ingest, "ingest_directory_to_bronze", _fake_ingest(results))],
    )

    assert run.exit_value.startswith("FAILED")
    assert "b.json" in run.exit_value


def test_skipped_unit_does_not_fail_the_task(run_notebook):
    """
    The #127 regression, previously untested.

    A folder with no JSON in it is not a failure: there is no bad data and
    nothing for a human to fix, so failing the task would fire an alert for a
    non-event and bury real failures in the same run.
    """
    results = [
        {"file": "a.json", "table": "a_bronze", "status": "success", "rows": 5},
        {"file": "empty/", "table": "empty_bronze", "status": "skipped", "reason": "no JSON files"},
    ]
    run = run_notebook(
        "run_directory_ingestion",
        widgets=BASE_WIDGETS,
        patches=[(bronze_ingest, "ingest_directory_to_bronze", _fake_ingest(results))],
    )

    assert run.exit_value.startswith("SUCCESS")
    assert "1 skipped" in run.exit_value


# ---------------------------------------------------------------------------
# The summary display - no pandas, explicit schema (#157)
# ---------------------------------------------------------------------------


def test_summary_uses_an_explicit_schema_and_no_pandas(run_notebook):
    """
    pandas was imported here but declared in no extra of setup.py - it worked
    only because the Databricks runtime ships it. An explicit schema removes
    the dependency and makes the summary's shape independent of what happened
    to be in the directory.
    """
    results = [
        {"file": "a.json", "table": "a_bronze", "status": "success", "rows": 5},
        {"file": "b/", "table": "b_bronze", "status": "skipped", "reason": "no JSON files"},
    ]
    run = run_notebook(
        "run_directory_ingestion",
        widgets=BASE_WIDGETS,
        patches=[(bronze_ingest, "ingest_directory_to_bronze", _fake_ingest(results))],
    )

    assert len(run.spark.created) == 1
    rows, schema = run.spark.created[0]
    assert schema is not None, "summary must pass an explicit schema, never infer one"
    assert len(rows) == 2
    # Heterogeneous keys collapse into one stable set of columns.
    assert rows[0] == ("a.json", "a_bronze", "success", 5, 0, "")
    assert rows[1][2] == "skipped" and rows[1][5] == "no JSON files"
    assert len(run.displayed) == 1


def test_no_notebook_imports_pandas():
    """setup.py declares no pandas in any extra. Nothing may rely on the
    runtime happening to provide it."""
    offenders = []
    for name in sorted(os.listdir(NOTEBOOK_DIR)):
        if not name.endswith(".py"):
            continue
        source = open(os.path.join(NOTEBOOK_DIR, name), encoding="utf-8").read()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import) and any(a.name == "pandas" for a in node.names):
                offenders.append(name)
            elif isinstance(node, ast.ImportFrom) and node.module == "pandas":
                offenders.append(name)
    assert offenders == [], (
        f"{offenders} import pandas, which is not declared in setup.py. Either add it "
        "to an extra with a comment saying the runtime provides it, or build the "
        "DataFrame with an explicit schema instead."
    )


# ---------------------------------------------------------------------------
# Parameter coercion
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "widget_values,expected",
    [
        ({}, {"multiline": True, "stop_on_error": False, "fail_on_quality_error": True}),
        (
            {"multiline": "false", "stop_on_error": "true", "fail_on_quality_error": "false"},
            {"multiline": False, "stop_on_error": True, "fail_on_quality_error": False},
        ),
    ],
)
def test_boolean_widgets_coerce_from_strings(run_notebook, widget_values, expected):
    """Widgets are always strings. `"false"` is truthy in Python, so a missing
    `== "true"` silently inverts a flag."""
    fake = _fake_ingest([])
    run_notebook(
        "run_directory_ingestion",
        widgets={**BASE_WIDGETS, **widget_values},
        patches=[(bronze_ingest, "ingest_directory_to_bronze", fake)],
    )
    kwargs = fake.calls[0]
    for key, value in expected.items():
        assert kwargs[key] is value, key


def test_blank_optional_widgets_become_none_or_are_omitted(run_notebook):
    fake = _fake_ingest([])
    run_notebook(
        "run_directory_ingestion",
        widgets={**BASE_WIDGETS, "catalog": "", "max_files": "", "required_columns": ""},
        patches=[(bronze_ingest, "ingest_directory_to_bronze", fake)],
    )
    kwargs = fake.calls[0]
    assert kwargs["catalog"] is None
    assert kwargs["max_files"] is None
    assert kwargs["required_columns"] == []
    # Blank id overrides are omitted entirely rather than passed as "",
    # so the package's own defaults apply.
    for key in ("batch_id", "run_id", "audit_schema_name", "registry_schema_name"):
        assert key not in kwargs


def test_comma_separated_widgets_split_and_strip(run_notebook):
    fake = _fake_ingest([])
    run_notebook(
        "run_directory_ingestion",
        widgets={**BASE_WIDGETS, "required_columns": " order_id , customer_id ,, "},
        patches=[(bronze_ingest, "ingest_directory_to_bronze", fake)],
    )
    assert fake.calls[0]["required_columns"] == ["order_id", "customer_id"]


def test_missing_source_dir_raises_before_any_ingestion(run_notebook):
    fake = _fake_ingest([])
    with pytest.raises(ValueError, match="source_dir"):
        run_notebook(
            "run_directory_ingestion",
            widgets={"source_dir": "   "},
            patches=[(bronze_ingest, "ingest_directory_to_bronze", fake)],
        )
    assert fake.calls == [], "ingestion must not start without a source_dir"


# ---------------------------------------------------------------------------
# The widget -> function contract
# ---------------------------------------------------------------------------


def test_every_kwarg_the_notebook_passes_is_accepted(run_notebook):
    """
    Every keyword the notebook sends must be a real parameter of the target,
    or a real IngestionConfig field that `**config_overrides` forwards.

    **This test cannot catch #145's class on its own, and that is worth
    stating rather than discovering later.** #157 proposed an
    `inspect.signature` check as "the direct fix for the #145 class". It is
    not, because `ingest_directory_to_bronze` ends in `**config_overrides` -
    its signature accepts any keyword by design, so neither this test nor a
    type checker can reject one. That was confirmed against mypy in #158.

    What actually guards that call is the runtime unknown-key rejection, and
    the test below covers it. This one still earns its place: it catches a
    keyword that is neither a named parameter nor a config field, which is
    the typo case.
    """
    fake = _fake_ingest([])
    run_notebook(
        "run_directory_ingestion",
        widgets=BASE_WIDGETS,
        patches=[(bronze_ingest, "ingest_directory_to_bronze", fake)],
    )

    signature = inspect.signature(directory_ingestion.ingest_directory_to_bronze)
    named = set(signature.parameters)
    config_fields = set(IngestionConfig.__dataclass_fields__)

    unknown = [k for k in fake.calls[0] if k not in named and k not in config_fields]
    assert unknown == [], (
        f"{unknown} is neither a parameter of ingest_directory_to_bronze nor an "
        "IngestionConfig field, so it would be swallowed by **config_overrides "
        "and silently ignored - the #145 failure shape."
    )


def test_directory_entrypoint_rejects_unknown_config_keys():
    """
    The real guard for #145's class on the directory path, since no static
    check can see past `**config_overrides`.

    #145 shipped because `per_file_config=` was accepted, swallowed and
    dropped, so a configured quality rule was inert in production with no
    error anywhere. What makes that impossible now is the entry point
    refusing a key it does not recognise - and refusing it before any
    discovery or Spark work, which is why `spark=None` gets that far.
    """
    with pytest.raises(ValueError) as excinfo:
        directory_ingestion.ingest_directory_to_bronze(
            None, source_dir="/x", definitely_not_a_field=True
        )
    assert "definitely_not_a_field" in str(excinfo.value)


def test_convenience_entrypoint_rejects_unknown_overrides():
    """Same guard on the single-table path, via IngestionConfig.resolve."""
    with pytest.raises(ValueError) as excinfo:
        IngestionConfig.resolve(source_path="/x", table="t", definitely_not_a_field=True)
    assert "definitely_not_a_field" in str(excinfo.value)


def test_from_dict_stays_lenient_and_that_is_deliberate():
    """
    Pins the asymmetry so it is a decision rather than an inconsistency
    someone "fixes" later.

    `from_dict` deliberately DROPS unknown keys, because config files are
    versioned artifacts that may legitimately carry keys a given package
    version does not know yet. The strictness lives at the entry points,
    where a key came from a human writing a call or a bundle parameter.

    The cost of that choice, worth stating plainly: `run_ingestion.py` builds
    its config with `from_dict`, so a typo in ITS hardcoded widget list would
    be dropped silently. The widget<->bundle contract tests below are what
    cover that gap.
    """
    config = IngestionConfig.from_dict(
        {"source_path": "/x", "table": "t", "definitely_not_a_field": True}
    )
    assert not hasattr(config, "definitely_not_a_field")


# ---------------------------------------------------------------------------
# The widget <-> bundle contract
# ---------------------------------------------------------------------------


def _bundle_notebook_tasks():
    """(notebook filename, base_parameters) for every notebook task in the
    bundle's job definitions."""
    tasks = []
    for name in sorted(os.listdir(RESOURCES)):
        if not name.endswith((".yml", ".yaml")):
            continue
        doc = yaml.safe_load(open(os.path.join(RESOURCES, name), encoding="utf-8")) or {}
        for job in (doc.get("resources", {}).get("jobs", {}) or {}).values():
            for task in job.get("tasks", []) or []:
                nt = task.get("notebook_task") or {}
                if nt.get("notebook_path"):
                    tasks.append(
                        (os.path.basename(nt["notebook_path"]), nt.get("base_parameters") or {})
                    )
    return tasks


def _declared_widgets(notebook_filename):
    """Widget names the notebook declares, read statically so no execution or
    fake kernel is needed."""
    source = open(os.path.join(NOTEBOOK_DIR, notebook_filename), encoding="utf-8").read()
    names = set()
    for node in ast.walk(ast.parse(source)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("text", "dropdown", "combobox", "multiselect")
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            names.add(node.args[0].value)
    return names


def test_bundle_declares_at_least_one_notebook_task():
    """If this fails the two contract tests below are vacuously passing."""
    assert _bundle_notebook_tasks(), "no notebook_task found in bronze_layer/resources/"


def test_every_bundle_parameter_has_a_matching_widget():
    """
    A `base_parameters` key with no matching widget is silently ignored: the
    notebook never reads it, so the configured value simply does not apply.
    That is #145's failure shape one layer up, and nothing checked it.
    """
    problems = []
    for notebook, params in _bundle_notebook_tasks():
        declared = _declared_widgets(notebook)
        for key in params:
            if key not in declared:
                problems.append(
                    f"{notebook}: bundle sets {key!r}, notebook declares no such widget"
                )
    assert problems == [], "\n".join(problems)


def test_every_required_widget_is_supplied_by_the_bundle():
    """
    The other direction. A widget with a blank default that the bundle does
    not set falls back to that blank, and the notebook either raises or - worse
    - proceeds with a default nobody chose.

    Only widgets with a blank default are required: a non-blank default is a
    deliberate choice to work without the bundle saying anything.
    """
    problems = []
    for notebook, params in _bundle_notebook_tasks():
        source = open(os.path.join(NOTEBOOK_DIR, notebook), encoding="utf-8").read()
        for node in ast.walk(ast.parse(source)):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "text"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[1], ast.Constant)
            ):
                continue
            name, default = node.args[0].value, node.args[1].value
            if default == "" and name not in params:
                problems.append(
                    f"{notebook}: widget {name!r} has a blank default and the bundle "
                    "does not set it"
                )
    assert problems == [], "\n".join(problems)


# ---------------------------------------------------------------------------
# Import surface
# ---------------------------------------------------------------------------


def test_notebooks_only_import_public_names_from_bronze_ingest():
    """
    Every name a notebook imports from `bronze_ingest` must be in `__all__`.

    The notebooks are installed against a wheel, so an import of something
    that is not public breaks at job start - after compute has spun up, which
    is the most expensive place to find out.
    """
    public = set(bronze_ingest.__all__)
    problems = []
    for name in sorted(os.listdir(NOTEBOOK_DIR)):
        if not name.endswith(".py"):
            continue
        source = open(os.path.join(NOTEBOOK_DIR, name), encoding="utf-8").read()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.ImportFrom) and node.module == "bronze_ingest":
                for alias in node.names:
                    if alias.name not in public:
                        problems.append(f"{name}: imports {alias.name!r}, not in __all__")
    assert problems == [], "\n".join(problems)


def test_all_names_in_dunder_all_actually_resolve():
    missing = [n for n in bronze_ingest.__all__ if not hasattr(bronze_ingest, n)]
    assert missing == [], f"__all__ names that do not resolve: {missing}"


# ---------------------------------------------------------------------------
# Every notebook is at least executable under the fake kernel
# ---------------------------------------------------------------------------


def test_all_notebooks_parse():
    """Cheap, and it covers the two notebooks with no behavioural test below -
    a syntax error in any of them is a job that fails at start."""
    for name in sorted(os.listdir(NOTEBOOK_DIR)):
        if name.endswith(".py"):
            source = open(os.path.join(NOTEBOOK_DIR, name), encoding="utf-8").read()
            ast.parse(source, filename=name)


def test_quarantine_replay_requires_config_path_for_row_mode(run_notebook):
    with pytest.raises(ValueError, match="config_path"):
        run_notebook("run_quarantine_replay", widgets={"replay_mode": "rows"})


def test_quarantine_replay_requires_source_dir_for_file_mode(run_notebook):
    with pytest.raises(ValueError, match="source_dir"):
        run_notebook("run_quarantine_replay", widgets={"replay_mode": "files"})


def test_run_ingestion_builds_config_from_widgets(run_notebook):
    """run_ingestion has no config_path here, so overrides alone must produce
    a valid config - and reach the writer."""
    seen = {}

    class _FakeJob:
        def __init__(self, spark, config):
            seen["config"] = config

        def run(self):
            return {"table": "sch.t", "row_count": 3}

    run = run_notebook(
        "run_ingestion",
        widgets={"source_path": "/Volumes/c/s/v/in", "schema_name": "sch", "table": "t"},
        patches=[(bronze_ingest, "BronzeIngestion", _FakeJob)],
        spark=FakeSpark(),
    )

    assert seen["config"].table == "t"
    assert seen["config"].schema_name == "sch"
    assert "row_count" in run.exit_value


# ---------------------------------------------------------------------------
# run_ai_metadata.py - the advisory lane's entrypoint (#208)
#
# The drafter is never constructed against a real endpoint here: the notebook
# builds AIFunctionsMetadataDrafter, which only stores the session and the
# endpoint name, and run_ai_metadata_job itself is faked. So these exercise
# the notebook's own logic - name qualification, schema pinning, and the
# failure semantics - with no network and no ai_query.
# ---------------------------------------------------------------------------

AI_WIDGETS = {
    "catalog": "cat",
    "schema_name": "sch",
    "audit_schema_name": "sch",
    "registry_schema_name": "sch",
    "ai_metadata_table_name": "_ai_metadata",
    "lookback_hours": "24",
    "model_id": "databricks-claude-opus-4-8",
    "run_id": "123-456",
}


def _fake_ai_job(summary):
    """Stand-in for run_ai_metadata_job that returns a fixed summary and
    records the config it was handed."""
    calls = []

    def _call(spark, job_config, drafter):
        calls.append({"job_config": job_config, "drafter": drafter})
        return summary

    _call.calls = calls
    return _call


def _run_ai(run_notebook, summary, widgets=None):
    from bronze_ingest import ai_metadata

    fake = _fake_ai_job(summary)
    w = dict(AI_WIDGETS)
    w.update(widgets or {})
    run = run_notebook(
        "run_ai_metadata",
        widgets=w,
        patches=[(ai_metadata, "run_ai_metadata_job", fake)],
    )
    return run, fake


def test_ai_notebook_qualifies_every_table_against_the_configured_catalog(run_notebook):
    """The job must read the tables the pipeline actually wrote - a bare table
    name would resolve against whatever the session default happens to be."""
    run, fake = _run_ai(
        run_notebook,
        {"processed": 1, "skipped_unchanged": 0, "skipped_failed": 0, "skipped_malformed": 0},
    )
    cfg = fake.calls[0]["job_config"]
    assert cfg.audit_table == "cat.sch._ingestion_audit"
    assert cfg.registry_table == "cat.sch._schema_registry"
    assert cfg.ai_metadata_table == "cat.sch._ai_metadata"


def test_ai_notebook_pins_audit_and_registry_to_their_own_schemas(run_notebook):
    """All three environments share one catalog. Reading another environment's
    audit trail would draft metadata about tables this one does not own."""
    run, fake = _run_ai(
        run_notebook,
        {"processed": 1, "skipped_unchanged": 0, "skipped_failed": 0, "skipped_malformed": 0},
        widgets={"audit_schema_name": "audit_sch", "registry_schema_name": "reg_sch"},
    )
    cfg = fake.calls[0]["job_config"]
    assert cfg.audit_table == "cat.audit_sch._ingestion_audit"
    assert cfg.registry_table == "cat.reg_sch._schema_registry"
    # Output still lands beside the data it describes.
    assert cfg.ai_metadata_table == "cat.sch._ai_metadata"


def test_ai_notebook_blank_audit_schema_falls_back_to_schema_name(run_notebook):
    run, fake = _run_ai(
        run_notebook,
        {"processed": 1, "skipped_unchanged": 0, "skipped_failed": 0, "skipped_malformed": 0},
        widgets={"audit_schema_name": "", "registry_schema_name": ""},
    )
    cfg = fake.calls[0]["job_config"]
    assert cfg.audit_table == "cat.sch._ingestion_audit"
    assert cfg.registry_table == "cat.sch._schema_registry"


def test_ai_notebook_blank_model_id_falls_back_to_the_pinned_endpoint(run_notebook):
    """Interactive runs supply no model_id; it must not become empty string."""
    from bronze_ingest.ai_metadata import AIFunctionsMetadataDrafter

    run, fake = _run_ai(
        run_notebook,
        {"processed": 1, "skipped_unchanged": 0, "skipped_failed": 0, "skipped_malformed": 0},
        widgets={"model_id": ""},
    )
    assert fake.calls[0]["job_config"].model_id == AIFunctionsMetadataDrafter.DEFAULT_ENDPOINT


def test_ai_notebook_succeeds_when_some_tables_were_drafted(run_notebook):
    run, _ = _run_ai(
        run_notebook,
        {"processed": 3, "skipped_unchanged": 5, "skipped_failed": 0, "skipped_malformed": 0},
    )
    assert run.exit_value.startswith("SUCCESS")
    assert "3 table(s) drafted" in run.exit_value


def test_ai_notebook_does_not_fail_the_task_for_a_few_model_failures(run_notebook):
    """A per-table model failure self-heals: the table still shows as changed
    and the next run picks it up. Failing here would page someone for a
    transient hiccup."""
    run, _ = _run_ai(
        run_notebook,
        {"processed": 2, "skipped_unchanged": 1, "skipped_failed": 1, "skipped_malformed": 1},
    )
    assert run.exit_value.startswith("SUCCESS")
    assert "1 model failure(s)" in run.exit_value
    assert "1 malformed" in run.exit_value


def test_ai_notebook_fails_when_every_candidate_failed(run_notebook):
    """Nothing drafted and everything failed is structural - a wrong endpoint,
    a missing grant, ai_query unavailable. Those do not self-heal, and a job
    that 'succeeds' nightly while writing nothing is the silent no-op this
    repo keeps finding."""
    run, _ = _run_ai(
        run_notebook,
        {"processed": 0, "skipped_unchanged": 0, "skipped_failed": 4, "skipped_malformed": 0},
    )
    assert run.exit_value.startswith("FAILED")
    assert "every candidate failed" in run.exit_value
    # The message must name the endpoint - that is the first thing to check.
    assert "databricks-claude-opus-4-8" in run.exit_value


def test_ai_notebook_treats_no_candidates_as_success(run_notebook):
    """Nothing ingested in the lookback window is a non-event, not a failure."""
    run, _ = _run_ai(
        run_notebook,
        {"processed": 0, "skipped_unchanged": 0, "skipped_failed": 0, "skipped_malformed": 0},
    )
    assert run.exit_value.startswith("SUCCESS")
    assert "no candidate tables" in run.exit_value


def test_ai_notebook_summary_uses_an_explicit_schema(run_notebook):
    """Same #144 lesson as the ingestion notebook: an inferred schema over a
    dict changes shape with its contents, and an empty one cannot infer at
    all."""
    run, _ = _run_ai(
        run_notebook,
        {"processed": 0, "skipped_unchanged": 0, "skipped_failed": 0, "skipped_malformed": 0},
    )
    assert run.spark.created, "notebook built no summary DataFrame"
    _rows, schema = run.spark.created[0]
    assert schema == "outcome STRING, tables INT"


def test_ai_notebook_rejects_a_non_numeric_lookback(run_notebook):
    import pytest as _pytest

    with _pytest.raises(ValueError, match="lookback_hours"):
        _run_ai(
            run_notebook,
            {"processed": 0, "skipped_unchanged": 0, "skipped_failed": 0, "skipped_malformed": 0},
            widgets={"lookback_hours": "soon"},
        )
