"""Public reader contract for Parquet batch ingestion."""

from bronze_ingest.config import IngestionConfig
from bronze_ingest.parquet_reader import read_parquet


class _FakeCol:
    def __init__(self, name):
        self.name = name

    def alias(self, name):
        return _FakeCol(f"{self.name} AS {name}")

    def __str__(self):
        return self.name


class _RecordingDataFrame:
    def __init__(self, sink):
        self.sink = sink

    def select(self, *columns):
        self.sink["selected"] = [str(column) for column in columns]
        return self


class _RecordingReader:
    def __init__(self, sink):
        self.sink = sink

    def format(self, value):
        self.sink["format"] = value
        return self

    def option(self, key, value):
        self.sink["options"].append((key, value))
        return self

    def schema(self, value):
        self.sink["schema"] = value
        return self

    def load(self, path):
        self.sink["loaded"] = path
        return _RecordingDataFrame(self.sink)


class _FakeSpark:
    def __init__(self, sink):
        self.read = _RecordingReader(sink)


def test_parquet_reader_preserves_payload_and_adds_lineage(monkeypatch):
    import pyspark.sql.functions as functions

    monkeypatch.setattr(functions, "col", lambda name: _FakeCol(name))
    sink = {"options": [], "selected": []}
    config = IngestionConfig(
        source_path="/Volumes/raw/orders.parquet",
        source_format="parquet",
        table="orders",
        reader_options={"mergeSchema": "true"},
    )

    result = read_parquet(_FakeSpark(sink), config)

    assert result.sink is sink
    assert sink["format"] == "parquet"
    assert sink["loaded"] == config.source_path
    assert sink["options"] == [("mergeSchema", "true")]
    assert sink["selected"][0] == "*"
    assert "_metadata.file_path AS _input_file_name" in sink["selected"]


def test_parquet_reader_applies_explicit_schema(monkeypatch):
    import pyspark.sql.functions as functions

    monkeypatch.setattr(functions, "col", lambda name: _FakeCol(name))
    sink = {"options": [], "selected": []}
    config = IngestionConfig(
        source_path="/Volumes/raw/orders.parquet",
        source_format="parquet",
        table="orders",
        schema_hint_ddl="id BIGINT, amount DECIMAL(18,2)",
    )

    read_parquet(_FakeSpark(sink), config)

    assert sink["schema"] == config.schema_hint_ddl
