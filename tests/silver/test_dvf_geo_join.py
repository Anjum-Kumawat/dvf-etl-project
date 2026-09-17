import pytest
from pyspark.sql import Row, SparkSession

from src.silver.dvf_geo_join import join_geo, parent_insee_commune


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-dvf-geo-join")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_paris_arrondissement_maps_to_parent_commune(spark):
    df = spark.createDataFrame([Row(code_commune="75117")])
    result = df.withColumn("parent", parent_insee_commune(df["code_commune"])).collect()
    assert result[0]["parent"] == "75056"


def test_lyon_arrondissement_maps_to_parent_commune(spark):
    df = spark.createDataFrame([Row(code_commune="69203")])
    result = df.withColumn("parent", parent_insee_commune(df["code_commune"])).collect()
    assert result[0]["parent"] == "69123"


def test_marseille_arrondissement_maps_to_parent_commune(spark):
    df = spark.createDataFrame([Row(code_commune="13208")])
    result = df.withColumn("parent", parent_insee_commune(df["code_commune"])).collect()
    assert result[0]["parent"] == "13055"


def test_ordinary_commune_maps_to_itself(spark):
    df = spark.createDataFrame([Row(code_commune="01001")])
    result = df.withColumn("parent", parent_insee_commune(df["code_commune"])).collect()
    assert result[0]["parent"] == "01001"


def test_join_matches_all_paris_arrondissements_to_one_geo_row(spark):
    dvf = spark.createDataFrame([
        Row(id_mutation="2024-1", code_commune="75101"),
        Row(id_mutation="2024-2", code_commune="75117"),
        Row(id_mutation="2024-3", code_commune="75120"),
    ])
    geo = spark.createDataFrame([
        Row(code_commune_insee="75056", geo_commune_nom="Paris", geo_commune_population=2103778),
    ])
    result = join_geo(dvf, geo).collect()
    assert len(result) == 3
    for row in result:
        assert row["geo_commune_nom"] == "Paris"
        assert row["geo_commune_population"] == 2103778


def test_join_preserves_rows_with_no_geo_match(spark):
    dvf = spark.createDataFrame([Row(id_mutation="2024-1", code_commune="99999")])
    geo = spark.createDataFrame([
        Row(code_commune_insee="75056", geo_commune_nom="Paris", geo_commune_population=2103778),
    ])
    result = join_geo(dvf, geo).collect()
    assert len(result) == 1
    assert result[0]["geo_commune_nom"] is None