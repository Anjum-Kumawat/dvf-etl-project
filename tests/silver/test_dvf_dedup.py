import pytest
from pyspark.sql import Row, SparkSession

from src.silver.dvf_dedup import deduplicate_dvf


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-dvf-dedup")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_exact_duplicate_rows_are_removed(spark):
    row = Row(id_mutation="2024-1", id_parcelle="750100001", valeur_fonciere=100000.0)
    df = spark.createDataFrame([row, row, row])
    result = deduplicate_dvf(df)
    assert result.count() == 1


def test_same_mutation_different_parcelle_is_preserved(spark):
    rows = [
        Row(id_mutation="2024-1", id_parcelle="750100001", valeur_fonciere=100000.0),
        Row(id_mutation="2024-1", id_parcelle="750100002", valeur_fonciere=100000.0),
    ]
    df = spark.createDataFrame(rows)
    result = deduplicate_dvf(df)
    assert result.count() == 2


def test_same_mutation_different_lot_is_preserved(spark):
    rows = [
        Row(id_mutation="2024-1", id_parcelle="750100001", lot1_numero="147"),
        Row(id_mutation="2024-1", id_parcelle="750100001", lot1_numero="99"),
    ]
    df = spark.createDataFrame(rows)
    result = deduplicate_dvf(df)
    assert result.count() == 2


def test_no_duplicates_is_a_no_op(spark):
    rows = [
        Row(id_mutation="2024-1", id_parcelle="750100001"),
        Row(id_mutation="2024-2", id_parcelle="750100002"),
    ]
    df = spark.createDataFrame(rows)
    result = deduplicate_dvf(df)
    assert result.count() == 2