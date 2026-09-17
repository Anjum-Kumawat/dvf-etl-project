import datetime

import pytest
from pyspark.sql import Row, SparkSession

from src.silver.dvf_dpe_join import join_dpe, most_recent_per_address


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-dvf-dpe-join")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_most_recent_per_address_keeps_only_latest(spark):
    df = spark.createDataFrame([
        Row(identifiant_ban="75113_2756_00062", dpe_numero_dpe="OLD1",
            dpe_etiquette_dpe="E", dpe_date_etablissement_dpe=datetime.date(2020, 1, 1)),
        Row(identifiant_ban="75113_2756_00062", dpe_numero_dpe="NEW1",
            dpe_etiquette_dpe="C", dpe_date_etablissement_dpe=datetime.date(2026, 1, 2)),
        Row(identifiant_ban="75113_2756_00062", dpe_numero_dpe="MID1",
            dpe_etiquette_dpe="D", dpe_date_etablissement_dpe=datetime.date(2023, 6, 1)),
    ])
    result = most_recent_per_address(df).collect()
    assert len(result) == 1
    assert result[0]["dpe_numero_dpe"] == "NEW1"
    assert result[0]["dpe_etiquette_dpe"] == "C"


def test_most_recent_per_address_preserves_distinct_addresses(spark):
    df = spark.createDataFrame([
        Row(identifiant_ban="75113_2756_00062", dpe_numero_dpe="A1",
            dpe_etiquette_dpe="E", dpe_date_etablissement_dpe=datetime.date(2024, 1, 1)),
        Row(identifiant_ban="75120_7288_00004", dpe_numero_dpe="B1",
            dpe_etiquette_dpe="B", dpe_date_etablissement_dpe=datetime.date(2024, 1, 1)),
    ])
    result = most_recent_per_address(df).collect()
    assert len(result) == 2


def test_tie_broken_deterministically_by_numero_dpe(spark):
    df = spark.createDataFrame([
        Row(identifiant_ban="75113_2756_00062", dpe_numero_dpe="AAA1",
            dpe_etiquette_dpe="E", dpe_date_etablissement_dpe=datetime.date(2024, 1, 1)),
        Row(identifiant_ban="75113_2756_00062", dpe_numero_dpe="ZZZ1",
            dpe_etiquette_dpe="C", dpe_date_etablissement_dpe=datetime.date(2024, 1, 1)),
    ])
    result = most_recent_per_address(df).collect()
    assert len(result) == 1
    assert result[0]["dpe_numero_dpe"] == "ZZZ1"


def test_join_matches_on_ban_result_id_to_identifiant_ban(spark):
    dvf = spark.createDataFrame([Row(id_mutation="2024-1", ban_result_id="75113_2756_00062")])
    dpe = spark.createDataFrame([Row(identifiant_ban="75113_2756_00062", dpe_etiquette_dpe="C")])
    result = join_dpe(dvf, dpe).collect()
    assert len(result) == 1
    assert result[0]["dpe_etiquette_dpe"] == "C"


def test_join_preserves_dvf_rows_with_no_dpe_match(spark):
    dvf = spark.createDataFrame([Row(id_mutation="2024-1", ban_result_id="75999_0000_00001")])
    dpe = spark.createDataFrame([Row(identifiant_ban="75113_2756_00062", dpe_etiquette_dpe="C")])
    result = join_dpe(dvf, dpe).collect()
    assert len(result) == 1
    assert result[0]["dpe_etiquette_dpe"] is None


def test_join_no_fanout_when_multiple_dvf_rows_share_address(spark):
    dvf = spark.createDataFrame([
        Row(id_mutation="2024-1", ban_result_id="75113_2756_00062"),
        Row(id_mutation="2024-2", ban_result_id="75113_2756_00062"),
    ])
    dpe = spark.createDataFrame([Row(identifiant_ban="75113_2756_00062", dpe_etiquette_dpe="C")])
    result = join_dpe(dvf, dpe).collect()
    assert len(result) == 2