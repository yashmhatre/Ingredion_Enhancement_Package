import pathlib
import re

from setuptools import setup, find_packages


def _version() -> str:
    """
    Reads __version__ from bronze_ingest/__init__.py rather than declaring a
    second copy here. The two previously drifted apart - the wheel was built
    as 0.4.0 while the installed package reported 0.3.0 - which makes the
    deployed artifact's version unknowable from inside a running job.

    Parsed rather than imported: importing the package at build time would
    pull in pyspark, which isn't a build dependency.
    """
    init = pathlib.Path(__file__).parent / "bronze_ingest" / "__init__.py"
    match = re.search(r'^__version__ = ["\']([^"\']+)["\']', init.read_text(), re.M)
    if not match:
        raise RuntimeError("Could not find __version__ in bronze_ingest/__init__.py")
    return match.group(1)


setup(
    name="bronze-ingest",
    version=_version(),
    description="Plug-and-play multi-format data ingestion into Databricks Delta bronze tables",
    # Explicit include so the built wheel contains only the package itself -
    # find_packages() with no filter would also sweep in `tests` (and anything
    # else with an __init__.py) and ship them to production compute.
    packages=find_packages(include=["bronze_ingest", "bronze_ingest.*"]),
    python_requires=">=3.8",
    install_requires=[
        "pyyaml>=5.1",
    ],
    extras_require={
        # pyspark/delta-spark are provided by the Databricks runtime already;
        # only needed if you want to run/test this package outside Databricks.
        "local": ["pyspark>=3.3.0", "delta-spark>=2.3.0"],
        # databricks-sdk is deliberately NOT in install_requires: the
        # Databricks runtime ships its own copy, and pinning a second one
        # risks a version conflict on job compute. databricks_fs.py imports
        # it defensively and falls back to the notebook-injected dbutils, so
        # the package works with or without it.
        "sdk": ["databricks-sdk>=0.30.0"],
        # `build` backs the wheel that Asset Bundles uploads and installs onto
        # job compute (see the `artifacts:` block in databricks.yml).
        "dev": [
            "pyspark>=3.3.0", "delta-spark>=2.3.0", "pytest>=7.0.0",
            "build>=1.0.0", "databricks-sdk>=0.30.0",
        ],
    },
)
