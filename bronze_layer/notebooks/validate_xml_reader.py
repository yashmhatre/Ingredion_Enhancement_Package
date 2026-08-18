# Databricks notebook source
# MAGIC %md
# MAGIC # XML reader validation
# MAGIC Manual DBR/serverless validation for #318, #336 and #337. Set
# MAGIC `scratch_path` to a writable `/Volumes/...` directory. The notebook
# MAGIC creates replaceable fixtures there and performs no table writes.

# COMMAND ----------

# This manual notebook has no bundle job environment of its own. Install the
# wheel built by the bundle into the attached serverless session first:
#
#   %pip install /Volumes/<catalog>/<schema>/<volume>/bronze_ingest-<version>-py3-none-any.whl
#   dbutils.library.restartPython()
#
# Keeping the path operator-supplied avoids binding validation to a user's
# workspace home or to one deployment target.

# COMMAND ----------

from bronze_ingest.config import IngestionConfig
from bronze_ingest.readers import read_source

dbutils.widgets.text("scratch_path", "", "Writable /Volumes scratch directory")
SCRATCH = dbutils.widgets.get("scratch_path").strip().rstrip("/")
if not SCRATCH.startswith("/Volumes/"):
    raise ValueError("scratch_path must name a writable /Volumes/... directory")

dbutils.fs.mkdirs(SCRATCH)
dbutils.fs.put(
    f"{SCRATCH}/valid.xml",
    "<orders><order id='1'><customer><name>Ana</name></customer>"
    "<items><item>A</item></items></order></orders>",
    True,
)
dbutils.fs.put(
    f"{SCRATCH}/namespaced.xml",
    "<orders xmlns:a='urn:a' xmlns:b='urn:b'><order><a:id>1</a:id><b:id>2</b:id></order></orders>",
    True,
)
dbutils.fs.put(
    f"{SCRATCH}/collision.xml",
    "<orders xmlns:a='urn:a'><order><a:id>1</a:id><a__id>2</a__id></order></orders>",
    True,
)
dbutils.fs.put(f"{SCRATCH}/truncated.xml", "<orders><order><id>1</id></order>", True)

results: list[tuple[str, str, str]] = []


def check(name, fn):
    try:
        fn()
        results.append((name, "PASS", ""))
    except Exception as exc:  # noqa: BLE001 - validation must report every case
        results.append((name, "FAIL", f"{type(exc).__name__}: {exc}"))


def config(filename):
    return IngestionConfig(
        source_path=f"{SCRATCH}/{filename}",
        table="_xml_validation",
        source_format="xml",
        xml_row_tag="order",
    )


def nested_and_lineage():
    df = read_source(spark, config("valid.xml"))
    assert df.count() == 1
    assert {"_id", "customer", "items", "_input_file_name"}.issubset(df.columns)
    assert df.select("_input_file_name").first()[0].endswith("valid.xml")


def namespaces_preserve_prefixes():
    df = read_source(spark, config("namespaced.xml"))
    assert df.count() == 1
    assert {"a__id", "b__id"}.issubset(df.columns)


def must_raise(filename, expected):
    raised = False
    try:
        df = read_source(spark, config(filename))
        df.count()
    except Exception as exc:  # noqa: BLE001 - the failure is the assertion under test
        raised = expected.lower() in str(exc).lower()
    assert raised, f"expected {filename} to fail with {expected!r}"


check("nested_attributes_and_metadata", nested_and_lineage)
check("namespace_prefixes_preserved", namespaces_preserve_prefixes)
check("canonical_name_collision_fails", lambda: must_raise("collision.xml", "collision"))
check("truncated_document_fails_closed", lambda: must_raise("truncated.xml", "well-formed"))

display(spark.createDataFrame(results, "case STRING, status STRING, detail STRING"))
failed = [row for row in results if row[1] != "PASS"]
if failed:
    raise RuntimeError(f"FAILED: {len(failed)}/{len(results)} XML cases failed")
dbutils.notebook.exit(f"SUCCESS: all {len(results)} XML cases passed")
