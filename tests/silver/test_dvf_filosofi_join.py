import pytest
from pyspark.sql import Row, SparkSession

from src.silver.dvf_filosofi_join import join_filosofi


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-dvf-filosofi-join")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_join_matches_commune_income_data(spark):
    dvf = spark.createDataFrame([
        Row(id_mutation="2024-1", code_commune="75101"),
        Row(id_mutation="2024-2", code_commune="75101"),
    ])
    filosofi = spark.createDataFrame([
        Row(code_commune="75101", filosofi_revenu_median=35030.0, filosofi_gini_index=0.534),
    ])
    result = join_filosofi(dvf, filosofi).collect()
    assert len(result) == 2
    assert result[0]["filosofi_revenu_median"] == 35030.0
    assert result[1]["filosofi_revenu_median"] == 35030.0


def test_join_preserves_rows_with_no_commune_match(spark):
    dvf = spark.createDataFrame([Row(id_mutation="2024-1", code_commune="99999")])
    filosofi = spark.createDataFrame([
        Row(code_commune="75101", filosofi_revenu_median=35030.0, filosofi_gini_index=0.534),
    ])
    result = join_filosofi(dvf, filosofi).collect()
    assert len(result) == 1
    assert result[0]["filosofi_revenu_median"] is None


def test_join_no_fanout_multiple_communes(spark):
    dvf = spark.createDataFrame([
        Row(id_mutation="2024-1", code_commune="75101"),
        Row(id_mutation="2024-2", code_commune="75117"),
    ])
    filosofi = spark.createDataFrame([
        Row(code_commune="75101", filosofi_revenu_median=35030.0, filosofi_gini_index=0.534),
        Row(code_commune="75117", filosofi_revenu_median=40000.0, filosofi_gini_index=0.5),
    ])
    result = join_filosofi(dvf, filosofi).collect()
    assert len(result) == 2