import pytest
from pyspark.sql import Row, SparkSession

from src.silver.dvf_ban_join import join_ban


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-dvf-ban-join")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def _ban_row():
    return Row(
        adresse_numero=4, adresse_nom_voie="VLA PERREUR", code_postal="75020",
        code_commune="75120", ban_longitude=2.404984, ban_latitude=48.868239,
        ban_result_score=0.7961, ban_result_label="4 Villa Perreur 75020 Paris",
        ban_result_id="75120_7288_00004", ban_result_housenumber="4",
        ban_result_street="Villa Perreur", ban_result_postcode="75020",
        ban_result_city="Paris", ban_result_citycode="75120", ban_result_status="ok",
    )


def test_matching_address_brings_in_ban_columns(spark):
    dvf = spark.createDataFrame([
        Row(id_mutation="2024-1", adresse_numero=4, adresse_nom_voie="VLA PERREUR",
            code_postal="75020", code_commune="75120"),
    ])
    ban = spark.createDataFrame([_ban_row()])
    result = join_ban(dvf, ban).collect()
    assert len(result) == 1
    assert result[0]["ban_result_score"] == 0.7961
    assert result[0]["ban_result_id"] == "75120_7288_00004"


def test_dvf_row_with_no_ban_match_keeps_null_ban_columns(spark):
    dvf = spark.createDataFrame([
        Row(id_mutation="2024-2", adresse_numero=99, adresse_nom_voie="RUE INCONNUE",
            code_postal="75015", code_commune="75115"),
    ])
    ban = spark.createDataFrame([_ban_row()])
    result = join_ban(dvf, ban).collect()
    assert len(result) == 1
    assert result[0]["ban_result_score"] is None


def test_multiple_dvf_rows_same_address_both_preserved_no_duplication(spark):
    dvf = spark.createDataFrame([
        Row(id_mutation="2024-1", adresse_numero=4, adresse_nom_voie="VLA PERREUR",
            code_postal="75020", code_commune="75120"),
        Row(id_mutation="2024-1", adresse_numero=4, adresse_nom_voie="VLA PERREUR",
            code_postal="75020", code_commune="75120"),
    ])
    ban = spark.createDataFrame([_ban_row()])
    result = join_ban(dvf, ban).collect()
    assert len(result) == 2