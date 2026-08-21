"""Fail-closed XML batch reading with namespace-safe Delta identifiers."""

from typing import Any, Optional

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException

from .config import IngestionConfig
from .identifiers import ON_COLLISION_ERROR, IdentifierCollisionError, canonicalize_identifiers
from .logging_utils import logger
from .retry import with_retry


class XMLMalformedDocumentError(ValueError):
    """One or more physical XML documents are not well formed."""


class XMLIdentifierCollisionError(IdentifierCollisionError):
    """
    Two sibling XML names canonicalize to the same Delta identifier.

    Still its own class, and still raised from here, because XML is the one
    format that fails closed on a collision instead of disambiguating - see
    `identifiers`'s module docstring for why the policy is per-format. It
    subclasses the shared `IdentifierCollisionError` so a caller that
    catches either one keeps working.
    """


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


def canonicalize_xml_identifiers(dataframe: Any):
    """
    Rewrite namespace separators - and every other Delta-unsafe character -
    recursively, failing on sibling collisions.

    The implementation moved to `identifiers` in #374 so JSON, CSV and
    Parquet get the same identifiers from the same code; this is now a thin
    wrapper that pins the XML-specific choice, `on_collision="error"`.
    Nothing XML callers relied on changed: `:` is still rewritten to `__`
    before anything else, so `a:id` and a sibling literally named `a__id`
    still collide, and a collision is still a hard failure rather than a
    rename. What is new is that an XML name containing a space or a
    parenthesis is now canonicalized too, instead of reaching Delta intact.
    """
    try:
        return canonicalize_identifiers(dataframe, on_collision=ON_COLLISION_ERROR)
    except XMLIdentifierCollisionError:
        raise
    except IdentifierCollisionError as exc:
        raise XMLIdentifierCollisionError(f"XML identifier collision: {exc}") from None


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
