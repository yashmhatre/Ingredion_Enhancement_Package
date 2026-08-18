"""Behavioral contract for fail-closed, namespace-safe XML ingestion."""

import pytest

from bronze_ingest.config import IngestionConfig
from bronze_ingest.xml_reader import (
    XMLIdentifierCollisionError,
    XMLMalformedDocumentError,
    canonicalize_xml_identifiers,
    read_xml,
)
from tests.conftest import file_uri


def _config(path):
    return IngestionConfig(
        source_path=file_uri(path),
        source_format="xml",
        xml_row_tag="record",
        table="records",
        retry_delay_seconds=0,
    )


def test_read_xml_rejects_a_truncated_document_before_returning_rows(spark, tmp_path):
    path = tmp_path / "truncated.xml"
    path.write_text("<records><record><id>1</id></record>", encoding="utf-8")

    with pytest.raises(XMLMalformedDocumentError, match="truncated.xml"):
        read_xml(spark, _config(path))


def test_failed_preflight_never_starts_the_spark_xml_reader(monkeypatch):
    import bronze_ingest.xml_reader as xml_reader

    requested_formats = []

    class _Read:
        def format(self, value):
            requested_formats.append(value)
            return self

    class _Spark:
        read = _Read()

    def reject(*_args):
        raise XMLMalformedDocumentError("bad.xml: truncated")

    monkeypatch.setattr(xml_reader, "_assert_xml_documents_well_formed", reject)

    with pytest.raises(XMLMalformedDocumentError):
        read_xml(_Spark(), _config("bad.xml"))

    assert requested_formats == []


def test_xml_identifier_canonicalization_preserves_nested_values(spark):
    source = spark.createDataFrame([("A-1", "west")], ["a:id", "region"])
    nested = source.selectExpr("struct(`a:id`, region) AS payload")

    result = canonicalize_xml_identifiers(nested)

    assert result.schema["payload"].dataType.fieldNames() == ["a__id", "region"]
    assert result.collect()[0]["payload"]["a__id"] == "A-1"


def test_xml_identifier_canonicalization_rejects_sibling_collisions(spark):
    source = spark.createDataFrame([("namespaced", "literal")], ["a:id", "a__id"])

    with pytest.raises(XMLIdentifierCollisionError, match="a:id.*a__id"):
        canonicalize_xml_identifiers(source)


def test_xml_identifier_canonicalization_reaches_arrays_and_map_values(spark):
    from pyspark.sql import Row
    from pyspark.sql.types import (
        ArrayType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    item_type = StructType([StructField("a:id", StringType(), True)])
    schema = StructType(
        [
            StructField("items", ArrayType(item_type), True),
            StructField("by_key", MapType(StringType(), item_type), True),
        ]
    )
    source = spark.createDataFrame(
        [([Row(**{"a:id": "A-1"})], {"first": Row(**{"a:id": "A-2"})})], schema
    )

    row = canonicalize_xml_identifiers(source).collect()[0]

    assert row["items"][0]["a__id"] == "A-1"
    assert row["by_key"]["first"]["a__id"] == "A-2"


def test_xml_identifier_collision_is_detected_inside_an_array_of_structs(spark):
    from pyspark.sql import Row
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    item_type = StructType(
        [
            StructField("a:id", StringType(), True),
            StructField("a__id", StringType(), True),
        ]
    )
    source = spark.createDataFrame(
        [([Row(**{"a:id": "namespaced", "a__id": "literal"})],)],
        StructType([StructField("items", ArrayType(item_type), True)]),
    )

    with pytest.raises(XMLIdentifierCollisionError, match=r"items\.\[\].*a:id.*a__id"):
        canonicalize_xml_identifiers(source)


def test_read_xml_preserves_namespace_prefixes_as_safe_identifiers(spark, tmp_path):
    path = tmp_path / "records.xml"
    path.write_text(
        '<records xmlns:a="urn:orders"><record><a:id>A-1</a:id>'
        "<details><region>west</region></details></record></records>",
        encoding="utf-8",
    )

    result = read_xml(spark, _config(path))

    assert "a__id" in result.columns
    assert result.collect()[0]["a__id"] == "A-1"
    assert result.collect()[0]["details"]["region"] == "west"
    assert "_input_file_name" in result.columns
