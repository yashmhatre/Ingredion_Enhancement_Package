"""Fail-closed XML batch reading with namespace-safe Delta identifiers."""

from typing import Any, Dict, Iterable, Optional, Tuple

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException

from .config import IngestionConfig
from .logging_utils import logger
from .retry import with_retry


class XMLMalformedDocumentError(ValueError):
    """One or more physical XML documents are not well formed."""


class XMLIdentifierCollisionError(ValueError):
    """Two sibling XML names canonicalize to the same Delta identifier."""


def _well_formedness_error(content: bytes) -> Optional[str]:
    try:
        ElementTree.fromstring(content)
    except (ElementTree.ParseError, DefusedXmlException, ValueError) as exc:
        return str(exc)
    return None


def _assert_xml_documents_well_formed(spark: Any, source_path: str) -> None:
    """Validate documents on executors and collect only bounded error details."""
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType

    validate = udf(_well_formedness_error, StringType())
    invalid = (
        spark.read.format("binaryFile")
        .load(source_path)
        .select("path", validate(col("content")).alias("_xml_error"))
        .where(col("_xml_error").isNotNull())
        .limit(20)
        .collect()
    )
    if invalid:
        details = "; ".join(f"{row['path']}: {row['_xml_error']}" for row in invalid)
        raise XMLMalformedDocumentError(f"Malformed XML document(s): {details}")


def _canonical_name(name: str) -> str:
    return name.replace(":", "__")


def _validate_data_type(data_type: Any, path: Tuple[str, ...]) -> None:
    from pyspark.sql.types import ArrayType, MapType, StructType

    if isinstance(data_type, StructType):
        _validate_no_collisions(data_type.fields, path)
    elif isinstance(data_type, ArrayType):
        _validate_data_type(data_type.elementType, path + ("[]",))
    elif isinstance(data_type, MapType):
        _validate_data_type(data_type.valueType, path + ("{}",))


def _validate_no_collisions(fields: Iterable[Any], path: Tuple[str, ...] = ()) -> None:

    by_canonical: Dict[str, str] = {}
    for field in fields:
        canonical = _canonical_name(field.name)
        previous = by_canonical.get(canonical)
        if previous is not None and previous != field.name:
            location = ".".join(path) or "<root>"
            raise XMLIdentifierCollisionError(
                f"XML identifier collision at {location}: {previous!r} and "
                f"{field.name!r} both canonicalize to {canonical!r}."
            )
        by_canonical[canonical] = field.name
        _validate_data_type(field.dataType, path + (canonical,))


def _canonicalize_column(column: Any, data_type: Any) -> Any:
    from pyspark.sql.functions import lit, struct, transform, transform_values, when
    from pyspark.sql.types import ArrayType, MapType, StructType

    if isinstance(data_type, StructType):
        canonical_struct = struct(
            *(
                _canonicalize_column(column.getField(field.name), field.dataType).alias(
                    _canonical_name(field.name)
                )
                for field in data_type.fields
            )
        )
        return when(column.isNull(), lit(None)).otherwise(canonical_struct)
    if isinstance(data_type, ArrayType):
        return transform(column, lambda item: _canonicalize_column(item, data_type.elementType))
    if isinstance(data_type, MapType):
        return transform_values(
            column, lambda _key, value: _canonicalize_column(value, data_type.valueType)
        )
    return column


def canonicalize_xml_identifiers(dataframe: Any):
    """Replace namespace separators recursively, failing on sibling collisions."""
    from pyspark.sql.functions import col

    _validate_no_collisions(dataframe.schema.fields)
    projections = []
    for field in dataframe.schema.fields:
        escaped = field.name.replace("`", "``")
        projections.append(
            _canonicalize_column(col(f"`{escaped}`"), field.dataType).alias(
                _canonical_name(field.name)
            )
        )
    return dataframe.select(*projections)


def read_xml(spark: Any, config: IngestionConfig):
    """Validate, read, canonicalize, and attach lineage to XML documents."""

    @with_retry(
        attempts=config.retry_attempts,
        delay_seconds=config.retry_delay_seconds,
        max_total_seconds=config.retry_max_total_seconds,
    )
    def _do_read():
        _assert_xml_documents_well_formed(spark, config.source_path)

        reader = spark.read.format("xml")
        if config.reader_options:
            logger.info("Applying reader_options: %s", sorted(config.reader_options))
            for key, value in config.reader_options.items():
                reader = reader.option(key, value)
        # These safety-critical options are deliberately applied last and
        # cannot be overridden through reader_options.
        reader = reader.option("rowTag", config.xml_row_tag).option("mode", "FAILFAST")

        from pyspark.sql.functions import col

        frame = reader.load(config.source_path).select(
            "*", col("_metadata.file_path").alias("_input_file_name")
        )
        return canonicalize_xml_identifiers(frame)

    return _do_read()
