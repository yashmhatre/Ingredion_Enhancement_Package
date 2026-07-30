import os
import pathlib
import sys

# Make the package importable regardless of entry point (notebook cell,
# "Run tests" button, or terminal). Computed relative to this file's own
# location instead of a hardcoded workspace path - avoids the recurring
# stale-path/wrong-identity issue entirely.
#
# This file lives at: .../Ingredion_Enhancement_Package/bronze_layer/tests/conftest.py
# The importable package lives at: .../bronze_layer/bronze_ingest/
# So we need the OUTER bronze_layer folder (parent of "tests") on sys.path.
_package_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _package_parent not in sys.path:
    sys.path.insert(0, _package_parent)

import uuid

import pytest


def file_uri(*parts) -> str:
    r"""
    A valid `file://` URI for a local path, on any platform (#74).

    Tests built these by hand as `f"file://{tmp_path}"`, which works on
    POSIX only by accident: `/tmp/x` already starts with a slash, so the
    result happens to be the well-formed `file:///tmp/x`. On Windows
    `tmp_path` is `C:\Users\...`, and the same f-string produces
    `file://C:\Users\...` - two slashes, a drive letter where the host
    should be, and backslash separators. Spark rejects it outright:
    `IllegalArgumentException: Wrong FS: file://C:\..., expected: file:///`.

    Built explicitly rather than with `pathlib.Path.as_uri()`, which the
    issue suggested. `as_uri()` percent-encodes, so it would change the URI
    this produces on Linux - where CI runs and everything currently passes -
    for no benefit on the platform that was broken. This construction is
    byte-identical to the old behaviour on POSIX and merely correct on
    Windows, which makes it the lower-risk fix.

    The trade-off that buys: a path containing a space or a `#` still
    produces an unencoded URI. pytest sanitises tmp_path names, so this does
    not arise here - but if a test ever needs such a path, use `as_uri()`
    for that case deliberately.
    """
    text = str(pathlib.PurePath(*parts)).replace("\\", "/")
    if not text.startswith("/"):
        text = "/" + text  # Windows: C:/Users/... -> /C:/Users/...
    return "file://" + text


def _get_dbutils():
    """Returns the injected dbutils object if running inside a Databricks
    notebook/workspace context, else None (e.g. local pytest, plain script)."""
    try:
        import IPython

        return IPython.get_ipython().user_ns["dbutils"]
    except Exception:  # noqa: BLE001 - no IPython/dbutils is the normal local-pytest case
        return None


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    if "SPARK_REMOTE" in os.environ:
        # Running inside Databricks - a Spark Connect session is already
        # configured via SPARK_REMOTE. Attach to it directly; calling
        # .master() here would conflict with the active spark.remote config.
        spark = SparkSession.builder.getOrCreate()
        yield spark
        return

    builder = (
        SparkSession.builder.appName("bronze_layer-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass  # delta-spark not installed - Delta-writing tests will fail with a clear error instead

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def json_test_dir(tmp_path):
    """
    Provides a real, writable directory for file-discovery tests, along
    with the source_dir string to pass into list_json_files.

    - Locally: pytest's own tmp_path, exposed as a file:// URI. Cleaned up
      automatically by pytest.
    - On Databricks: dbutils.fs can't touch arbitrary local /tmp paths
      (LocalFilesystemAccessDeniedException - a real security boundary,
      not a bug), so a scratch folder under a Unity Catalog Volume is
      used instead. Cleaned up explicitly after the test.

    Yields (write_dir, source_dir):
      write_dir  - a real filesystem path for creating test files with
                   plain Python (works identically in both environments,
                   since UC Volumes are FUSE-mounted).
      source_dir - the string to pass to list_json_files / read_json.
    """
    dbutils = _get_dbutils()

    if dbutils is not None:
        base = os.environ.get(
            "PYTEST_VOLUME_SCRATCH",
            "/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/pytest_scratch",
        )
        scratch = f"{base}/{uuid.uuid4().hex}"
        dbutils.fs.mkdirs(scratch)
        yield scratch, scratch
        dbutils.fs.rm(scratch, recurse=True)
    else:
        yield str(tmp_path), file_uri(tmp_path)


# ---------------------------------------------------------------------------
# Notebook harness (#157)
# ---------------------------------------------------------------------------
#
# bronze_layer/notebooks/ holds the deployed job entrypoints - the code that
# actually runs in production - and until now nothing tested them. Both known
# live defects (#144, #145) were there, and neither could have been caught by
# any number of library tests, because CI did not even watch the path.
#
# They are plain Python files with `# COMMAND ----------` separators, so the
# only thing standing between them and pytest is the handful of names the
# Databricks kernel injects: `dbutils`, `spark`, `display`. Supplying those
# makes the whole layer testable with no Spark, no Java and no workspace.


class NotebookExit(Exception):
    """
    Raised by the fake `dbutils.notebook.exit()`.

    On Databricks, `exit()` stops the notebook immediately and returns a value
    to the caller. Modelling it as an exception reproduces the control flow
    that matters: statements after an `exit()` do not run. A stub that merely
    recorded the value would let execution continue and quietly test a path
    production never takes.
    """

    def __init__(self, value):
        super().__init__(value)
        self.value = value


class FakeWidgets:
    """
    `dbutils.widgets`, recording declarations and serving values.

    `get()` on an undeclared widget raises, as it does on Databricks. That is
    not pedantry - it is the failure shape of #145's whole class, where the
    bundle and the notebook disagree about a parameter name and the mismatch
    surfaces as a default silently taking effect.
    """

    def __init__(self, values=None):
        self.declared = {}
        self.values = dict(values or {})

    def text(self, name, defaultValue="", label=None):  # noqa: N803 - Databricks' own casing
        self.declared[name] = defaultValue

    def dropdown(self, name, defaultValue, choices, label=None):  # noqa: N803
        self.declared[name] = defaultValue
        self.choices = getattr(self, "choices", {})
        self.choices[name] = choices

    def get(self, name):
        if name not in self.declared:
            raise ValueError(f"No widget named {name} is defined")
        return self.values.get(name, self.declared[name])

    def remove(self, name):
        self.declared.pop(name, None)

    def removeAll(self):  # noqa: N802 - Databricks' own casing
        self.declared.clear()


class FakeNotebook:
    def exit(self, value):
        raise NotebookExit(value)


class FakeFs:
    """Enough of `dbutils.fs` that a notebook touching it does not explode."""

    def __init__(self):
        self.calls = []

    def _record(self, name, *args):
        self.calls.append((name, args))
        return []

    def ls(self, *a):
        return self._record("ls", *a)

    def mkdirs(self, *a):
        return self._record("mkdirs", *a)

    def mv(self, *a):
        return self._record("mv", *a)

    def rm(self, *a):
        return self._record("rm", *a)

    def head(self, *a):
        return self._record("head", *a)


class FakeDbutils:
    def __init__(self, widget_values=None):
        self.widgets = FakeWidgets(widget_values)
        self.notebook = FakeNotebook()
        self.fs = FakeFs()


class FakeDataFrame:
    def __init__(self, rows, schema=None):
        self.rows = list(rows)
        self.schema = schema

    def count(self):
        return len(self.rows)


class FakeSpark:
    """
    Records `createDataFrame` calls so a test can assert what the summary was
    built from, without needing a session.
    """

    def __init__(self):
        self.created = []

    def createDataFrame(self, data, schema=None):  # noqa: N802 - Spark's own casing
        rows = list(data)
        if not rows and schema is None:
            # Mirrors the real failure #144 hit, so a regression cannot pass
            # here and fail on a cluster.
            raise ValueError(
                "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema from an empty dataset."
            )
        self.created.append((rows, schema))
        return FakeDataFrame(rows, schema)


class NotebookRun:
    def __init__(self, exit_value, displayed, dbutils, spark, namespace):
        self.exit_value = exit_value
        self.displayed = displayed
        self.dbutils = dbutils
        self.spark = spark
        self.namespace = namespace

    @property
    def exited(self):
        return self.exit_value is not None


NOTEBOOK_DIR = os.path.join(_package_parent, "notebooks")


@pytest.fixture
def run_notebook(monkeypatch):
    """
    Executes a notebook with a faked Databricks kernel and returns its outcome.

    Notebooks are executed rather than imported: they are scripts, not modules,
    and `exec` keeps each run's namespace isolated so one test cannot leak
    top-level state into the next.
    """
    import builtins

    def _run(name, widgets=None, patches=(), spark=None):
        path = os.path.join(NOTEBOOK_DIR, name if name.endswith(".py") else f"{name}.py")
        fake_dbutils = FakeDbutils(widgets)
        fake_spark = spark if spark is not None else FakeSpark()
        displayed = []

        monkeypatch.setattr(builtins, "dbutils", fake_dbutils, raising=False)
        monkeypatch.setattr(builtins, "spark", fake_spark, raising=False)
        monkeypatch.setattr(builtins, "display", displayed.append, raising=False)
        for target, attr, value in patches:
            monkeypatch.setattr(target, attr, value)

        with open(path, encoding="utf-8") as fh:
            source = fh.read()

        namespace = {"__name__": "__databricks_notebook__", "__file__": path}
        exit_value = None
        try:
            exec(compile(source, path, "exec"), namespace)  # noqa: S102 - executing a notebook is the point
        except NotebookExit as stop:
            exit_value = stop.value

        return NotebookRun(exit_value, displayed, fake_dbutils, fake_spark, namespace)

    return _run
