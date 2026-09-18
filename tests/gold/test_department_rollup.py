import datetime

import pytest
from pyspark.sql import Row, SparkSession

from src.gold.department_rollup import build_department_rollup


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-department-rollup")
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


def test_rolls_up_multiple_communes_into_one_department_row(spark):
    df = spark.createDataFrame([
        _row(id_mutation="2024-1", code_commune="75101", code_departement="75"),
        _row(id_mutation="2024-2", code_commune="75117", code_departement="75"),
        _row(id_mutation="2024-3", code_commune="75120", code_departement="75"),
    ])
    result = build_department_rollup(df)
    assert result.count() == 1  # all three communes, same department + quarter
    row = result.collect()[0]
    assert row["code_departement"] == "75"
    assert row["nb_transactions"] == 3


def test_groups_by_department_and_quarter(spark):
    df = spark.createDataFrame([
        _row(id_mutation="2024-1", date_mutation=datetime.date(2024, 1, 5)),
        _row(id_mutation="2024-2", date_mutation=datetime.date(2024, 4, 5)),  # different quarter
    ])
    result = build_department_rollup(df)
    assert result.count() == 2


def test_excludes_non_vente_mutations(spark):
    df = spark.createDataFrame([_row(nature_mutation="Echange")])
    result = build_department_rollup(df)
    assert result.count() == 0


def test_multi_lot_mutation_fix_applies_at_department_grain_too(spark):
    df = spark.createDataFrame([
        _row(id_mutation="2024-9", valeur_fonciere=94000000.0, surface_reelle_bati=6.0),
        _row(id_mutation="2024-9", valeur_fonciere=94000000.0, surface_reelle_bati=269.0),
    ])
    result = build_department_rollup(df)
    row = result.collect()[0]
    assert row["nb_transactions"] == 1
    expected_price_per_sqm = 94000000.0 / (6.0 + 269.0)
    assert abs(row["avg_price_per_sqm"] - expected_price_per_sqm) < 0.01