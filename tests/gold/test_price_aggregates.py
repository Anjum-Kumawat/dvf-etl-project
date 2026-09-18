import datetime

import pytest
from pyspark.sql import Row, SparkSession

from src.gold.price_aggregates import build_price_aggregates


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-price-aggregates")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(**overrides):
    base = dict(
        id_mutation="2024-1",
        code_commune="75101",
        code_departement="75",
        nature_mutation="Vente",
        date_mutation=datetime.date(2024, 3, 15),
        valeur_fonciere=500000.0,
        surface_reelle_bati=50.0,
    )
    base.update(overrides)
    return Row(**base)


def test_excludes_non_vente_mutations(spark):
    df = spark.createDataFrame([_row(nature_mutation="Echange")])
    result = build_price_aggregates(df)
    assert result.count() == 0


def test_excludes_zero_surface(spark):
    df = spark.createDataFrame([
        _row(id_mutation="2024-1", surface_reelle_bati=0.0),
        _row(id_mutation="2024-2", surface_reelle_bati=50.0),
    ])
    result = build_price_aggregates(df)
    assert result.collect()[0]["nb_transactions"] == 1


def test_excludes_null_surface(spark):
    df = spark.createDataFrame([
        _row(id_mutation="2024-1", surface_reelle_bati=None),
        _row(id_mutation="2024-2", surface_reelle_bati=50.0),
    ])
    result = build_price_aggregates(df)
    assert result.collect()[0]["nb_transactions"] == 1


def test_excludes_null_or_zero_price(spark):
    df = spark.createDataFrame([
        _row(id_mutation="2024-1", valeur_fonciere=None),
        _row(id_mutation="2024-2", valeur_fonciere=0.0),
        _row(id_mutation="2024-3", valeur_fonciere=500000.0),
    ])
    result = build_price_aggregates(df)
    assert result.collect()[0]["nb_transactions"] == 1


def test_multi_lot_mutation_surface_summed_not_divided_per_row(spark):
    # One real mutation (94M), split across two disposition rows as DVF
    # does for multi-lot sales -- the value is REPEATED, not divided.
    # Mirrors real data: id_mutation 2024-1202784 in dept 75.
    df = spark.createDataFrame([
        _row(id_mutation="2024-9", valeur_fonciere=94000000.0, surface_reelle_bati=6.0),
        _row(id_mutation="2024-9", valeur_fonciere=94000000.0, surface_reelle_bati=269.0),
    ])
    result = build_price_aggregates(df)
    row = result.collect()[0]
    assert row["nb_transactions"] == 1  # one mutation, not two disposition rows
    expected_price_per_sqm = 94000000.0 / (6.0 + 269.0)
    assert abs(row["avg_price_per_sqm"] - expected_price_per_sqm) < 0.01


def test_groups_by_commune_and_quarter(spark):
    df = spark.createDataFrame([
        _row(id_mutation="2024-1", code_commune="75101", date_mutation=datetime.date(2024, 1, 5)),
        _row(id_mutation="2024-2", code_commune="75101", date_mutation=datetime.date(2024, 2, 20)),  # same quarter
        _row(id_mutation="2024-3", code_commune="75101", date_mutation=datetime.date(2024, 4, 5)),   # different quarter
        _row(id_mutation="2024-4", code_commune="75117", date_mutation=datetime.date(2024, 1, 5)),   # different commune
    ])
    result = build_price_aggregates(df)
    assert result.count() == 3  # (75101,Q1), (75101,Q2), (75117,Q1)
    q1_75101 = result.filter(
        (result["code_commune"] == "75101") & (result["quarter"] == "2024-Q1")
    ).collect()[0]
    assert q1_75101["nb_transactions"] == 2


def test_quarter_formatting(spark):
    df = spark.createDataFrame([_row(date_mutation=datetime.date(2024, 11, 3))])
    result = build_price_aggregates(df)
    assert result.collect()[0]["quarter"] == "2024-Q4"


def test_price_per_sqm_computed_correctly_single_row_mutation(spark):
    df = spark.createDataFrame([_row(valeur_fonciere=400000.0, surface_reelle_bati=40.0)])
    result = build_price_aggregates(df)
    row = result.collect()[0]
    assert row["avg_price_per_sqm"] == 10000.0
    assert row["avg_valeur_fonciere"] == 400000.0