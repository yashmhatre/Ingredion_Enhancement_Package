from setuptools import setup, find_packages

setup(
    name="bronze-json-loader",
    version="0.1.0",
    description="Plug-and-play nested JSON -> Databricks Delta bronze table ingestion",
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
