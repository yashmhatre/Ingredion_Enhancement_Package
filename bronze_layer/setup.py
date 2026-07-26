from setuptools import setup, find_packages

setup(
    name="bronze-ingest",
    version="0.4.0",
    description="Plug-and-play multi-format data ingestion into Databricks Delta bronze tables",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pyyaml>=5.1",
    ],
    extras_require={
        # pyspark/delta-spark are provided by the Databricks runtime already;
        # only needed if you want to run/test this package outside Databricks.
        "local": ["pyspark>=3.3.0", "delta-spark>=2.3.0"],
        "dev": ["pyspark>=3.3.0", "delta-spark>=2.3.0", "pytest>=7.0.0"],
    },
)
