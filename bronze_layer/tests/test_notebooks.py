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
    """
    #247. This test previously asserted only that the exit STRING started with
    "FAILED" - which it did, while the task itself reported Succeeded, because
    `dbutils.notebook.exit()` exits 0 whatever string it is handed. The test
    name claimed something the assertion never checked and the code never did.

    So the assertion is now the one that matters: the notebook must RAISE,
    because an uncaught exception is the only thing that marks a notebook task
    Failed on Databricks. `run_notebook` only swallows NotebookExit, so
    anything else propagating here is a real task failure.
    """
    results = [
        {"file": "a.json", "table": "a_bronze", "status": "success", "rows": 5},
        {"file": "b.json", "table": "b_bronze", "status": "failed", "error": "boom"},
    ]
    with pytest.raises(RuntimeError) as excinfo:
        run_notebook(
            "run_directory_ingestion",
            widgets=BASE_WIDGETS,
            patches=[(bronze_ingest, "ingest_directory_to_bronze", _fake_ingest(results))],
        )

    assert str(excinfo.value).startswith("FAILED")
    assert "b.json" in str(excinfo.value)


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


def _exit_call_first_arg(node):
    """
    The first argument of a `dbutils.notebook.exit(...)` call, or None if this
    node is not such a call. Matches on the attribute chain rather than on the
    receiver's name, so a rebound alias is still caught.
    """
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if not (isinstance(func, ast.Attribute) and func.attr == "exit"):
        return None
    if not (isinstance(func.value, ast.Attribute) and func.value.attr == "notebook"):
        return None
    return node.args[0] if node.args else None


def _static_prefix(node):
    """
    The leading literal text of a str or f-string node, or "" if it does not
    begin with literal text. `f"FAILED: {n} units"` yields "FAILED: " - enough
    to classify the message without evaluating it.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr) and node.values:
        first = node.values[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            return first.value
    return ""


def test_no_notebook_reports_failure_through_notebook_exit():
    """
    #247, and the reason this is a static scan rather than another per-notebook
    behaviour test.

    `dbutils.notebook.exit(value)` stops the notebook and hands `value` back to
    the caller as a STRING. It always exits 0. A notebook that "reported"
    failure that way was marked **Succeeded** in the Jobs UI, so every alert,
    retry and downstream gate keyed on task failure stayed inert - the exact
    silent no-op this repo keeps finding, sitting in the mechanism meant to
    prevent it. Only an uncaught exception marks a notebook task Failed.

    Three notebooks did this and were fixed. The failure is invisible in
    review - the string says FAILED and the surrounding comment says it fails
    the task - and it was equally invisible in tests, because the old
    assertions checked the string rather than the outcome. So the class is
    closed here instead: any new notebook reaching for the same pattern fails
    this test with the reason attached, whether or not anyone writes it a
    behaviour test.

    Deliberately keyed on the message text, not on some registry of approved
    exit calls: "the string says FAILED" is precisely the signal that the
    author intended a failure, and intent plus `exit()` is always the bug.
    """
    offenders = []
    for name in sorted(os.listdir(NOTEBOOK_DIR)):
        if not name.endswith(".py"):
            continue
        source = open(os.path.join(NOTEBOOK_DIR, name), encoding="utf-8").read()
        for node in ast.walk(ast.parse(source)):
            arg = _exit_call_first_arg(node)
            if arg is None:
                continue
            if _static_prefix(arg).lstrip().upper().startswith("FAIL"):
                offenders.append(f"{name}:{node.lineno}")

    assert offenders == [], (
        f"{offenders} report failure via dbutils.notebook.exit(), which exits 0 and "
        "marks the job run Succeeded no matter what the string says (#247). Raise an "
        "exception instead - that is the only thing Databricks treats as a failed "
        "notebook task. Keep dbutils.notebook.exit() for success paths."
    )


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
    repo keeps finding.

    #247: this asserted the exit string only, so it passed while the job
    reported Succeeded - the silent no-op it exists to prevent, inside the
    test meant to prevent it. It now asserts the notebook RAISES, which is the
    only thing that marks the task Failed."""
    with pytest.raises(RuntimeError) as excinfo:
        _run_ai(
            run_notebook,
            {"processed": 0, "skipped_unchanged": 0, "skipped_failed": 4, "skipped_malformed": 0},
        )

    message = str(excinfo.value)
    assert message.startswith("FAILED")
    assert "every candidate failed" in message
    # The message must name the endpoint - that is the first thing to check.
    assert "databricks-claude-opus-4-8" in message


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


# ---------------------------------------------------------------------------
# Maintenance notebook (#159 item 3)
# ---------------------------------------------------------------------------

MAINTENANCE_WIDGETS = {
    "catalog": "cat",
    "schema_name": "sch",
    "tables": "_ingestion_audit,_schema_registry",
    "optimize": "true",
    "vacuum": "true",
    "vacuum_retention_hours": "",
}


def _fake_maintenance(results):
    def _run(spark, tables, **kwargs):
        _run.calls.append({"tables": tables, **kwargs})
        return results

    _run.calls = []
    return _run


def test_maintenance_notebook_qualifies_tables_and_passes_them_explicitly(run_notebook):
    """The table list is built from the widget, qualified with catalog and
    schema, and handed over as an explicit list - the notebook never asks the
    engine what tables exist."""
    import bronze_ingest.maintenance as maint

    fake = _fake_maintenance([{"table": "cat.sch._ingestion_audit", "optimize": "ok"}])
    run = run_notebook(
        "run_maintenance",
        widgets=MAINTENANCE_WIDGETS,
        patches=[(maint, "run_maintenance", fake)],
    )

    assert fake.calls[0]["tables"] == [
        "cat.sch._ingestion_audit",
        "cat.sch._schema_registry",
    ]
    assert run.exit_value.startswith("SUCCESS")


def test_maintenance_notebook_requires_an_explicit_table_list(run_notebook):
    """Blank `tables` is refused rather than defaulting to 'everything in the
    schema' - discovering tables would make this job's blast radius depend on
    whatever else lives there."""
    with pytest.raises(ValueError, match="tables job parameter is required"):
        run_notebook(
            "run_maintenance",
            widgets={**MAINTENANCE_WIDGETS, "tables": ""},
        )


def test_maintenance_notebook_blank_retention_means_use_the_table_floor(run_notebook):
    """Blank must become None, not 0.0 - a retention of zero would expire
    every file a CDF consumer still needs, which is the failure #58's floor
    exists to prevent."""
    import bronze_ingest.maintenance as maint

    fake = _fake_maintenance([])
    run_notebook(
        "run_maintenance",
        widgets=MAINTENANCE_WIDGETS,
        patches=[(maint, "run_maintenance", fake)],
    )

    assert fake.calls[0]["vacuum_retention_hours"] is None


def test_maintenance_notebook_fails_the_task_when_a_table_fails(run_notebook):
    """#247: reporting failure must RAISE. dbutils.notebook.exit() exits 0, so
    a run where every table failed to compact would be marked Succeeded."""
    import bronze_ingest.maintenance as maint

    fake = _fake_maintenance(
        [
            {"table": "cat.sch.a", "optimize": "ok", "vacuum": "ok"},
            {"table": "cat.sch.b", "optimize": "failed", "optimize_error": "boom"},
        ]
    )

    with pytest.raises(RuntimeError) as excinfo:
        run_notebook(
            "run_maintenance",
            widgets=MAINTENANCE_WIDGETS,
            patches=[(maint, "run_maintenance", fake)],
        )

    assert str(excinfo.value).startswith("FAILED")
    assert "cat.sch.b" in str(excinfo.value)


def test_maintenance_notebook_warns_but_succeeds_when_retention_was_clamped(run_notebook, caplog):
    """A clamped retention is the floor working, not a failure - but it means
    someone configured a value this job declined to honour, and that must be
    visible rather than buried in the summary table."""
    import bronze_ingest.maintenance as maint

    fake = _fake_maintenance(
        [{"table": "cat.sch.a", "optimize": "ok", "vacuum": "ok", "clamped": True}]
    )
    run = run_notebook(
        "run_maintenance",
        widgets={**MAINTENANCE_WIDGETS, "vacuum_retention_hours": "24"},
        patches=[(maint, "run_maintenance", fake)],
    )

    assert run.exit_value.startswith("SUCCESS")
    assert "raised to the table floor" in caplog.text
