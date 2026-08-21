"""Static package-metadata contracts that do not require building a wheel."""

from pathlib import Path


def test_local_and_dev_extras_require_spark_four_for_builtin_xml():
    setup_source = (Path(__file__).parents[1] / "setup.py").read_text(encoding="utf-8")
    assert setup_source.count('"pyspark>=4.0.0"') == 2
    assert '"pyspark>=3.3.0"' not in setup_source


def test_secure_xml_parser_is_a_runtime_dependency():
    setup_source = (Path(__file__).parents[1] / "setup.py").read_text(encoding="utf-8")
    assert '"defusedxml>=0.7.1"' in setup_source
