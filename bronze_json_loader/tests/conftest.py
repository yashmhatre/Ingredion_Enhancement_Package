import os
import sys

# Make the package importable regardless of entry point (notebook cell,
# "Run tests" button, or terminal). Computed relative to this file's own
# location instead of a hardcoded workspace path - avoids the recurring
# stale-path/wrong-identity issue entirely.
#
# This file lives at: .../Ingredion_Enhancement_Package/bronze_json_loader/tests/conftest.py
# The importable package lives at: .../bronze_json_loader/bronze_json_loader/
# So we need the OUTER bronze_json_loader folder (parent of "tests") on sys.path.
_package_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _package_parent not in sys.path:
    sys.path.insert(0, _package_parent)
    
import uuid
import pytest


def _get_dbutils():
    """Returns the injected dbutils object if running inside a Databricks
    notebook/workspace context, else None (e.g. local pytest, plain script)."""
    try:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]
    except Exception:
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

    spark = (
        SparkSession.builder
        .appName("bronze_json_loader-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
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
            "/Volumes/ingredion_en_dev/ingredion_dev/ext-ingredion-dev/pytest_scratch",
        )
        scratch = f"{base}/{uuid.uuid4().hex}"
        dbutils.fs.mkdirs(scratch)
        yield scratch, scratch
        dbutils.fs.rm(scratch, recurse=True)
    else:
        yield str(tmp_path), f"file://{tmp_path}"