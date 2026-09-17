import datetime

import pytest
from pyspark.sql import Row, SparkSession

from src.silver.dvf_quality_checks import (
    QualityCheckFailure,
    check_ban_low_confidence,
    check_built_property_missing_surface,
    check_code_postal_format,
    check_date_mutation_range,
    check_high_value_outliers,
    check_join_coverage,
    check_low_value_outliers,
    check_nature_mutation_known,
    check_no_exact_duplicates,
    check_type_local_known,
    check_valeur_fonciere_positive,
    run_quality_checks,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-dvf-quality-checks")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def _good_row(**overrides):
    base = dict(
        id_mutation="2024-1",
        nature_mutation="Vente",
        type_local="Appartement",
        code_postal="75020",
        date_mutation=datetime.date(2024, 6, 1),
        valeur_fonciere=500000.0,
        surface_reelle_bati=50.0,
        ban_result_score=0.9,
        ban_result_status="ok",
        dpe_etiquette_dpe="C",
        filosofi_revenu_median=35000.0,
        geo_commune_nom="Paris",
    )
    base.update(overrides)
    return Row(**base)


def test_nature_mutation_known_passes_on_valid_data(spark):
    df = spark.createDataFrame([_good_row()])
    result = check_nature_mutation_known(df)
    assert result.passed
    assert result.violation_count == 0


def test_nature_mutation_known_fails_on_unknown_category(spark):
    df = spark.createDataFrame([_good_row(nature_mutation="Something Unexpected")])
    result = check_nature_mutation_known(df)
    assert not result.passed
    assert result.violation_count == 1


def test_type_local_known_allows_null(spark):
    # Two rows so Spark can infer type_local's type from the non-null row --
    # a single row with only None has nothing to infer a type from.
    df = spark.createDataFrame([_good_row(type_local=None), _good_row(type_local="Maison")])
    result = check_type_local_known(df)
    assert result.passed


def test_type_local_known_fails_on_unknown_category(spark):
    df = spark.createDataFrame([_good_row(type_local="Bungalow")])
    result = check_type_local_known(df)
    assert not result.passed


def test_code_postal_format_passes_on_valid_code(spark):
    df = spark.createDataFrame([_good_row(code_postal="75015")])
    result = check_code_postal_format(df, "75")
    assert result.passed


def test_code_postal_format_fails_on_wrong_department(spark):
    df = spark.createDataFrame([_good_row(code_postal="69001")])
    result = check_code_postal_format(df, "75")
    assert not result.passed


def test_date_mutation_range_fails_outside_year(spark):
    df = spark.createDataFrame([_good_row(date_mutation=datetime.date(2023, 12, 31))])
    result = check_date_mutation_range(df, 2024)
    assert not result.passed


def test_valeur_fonciere_positive_fails_on_zero(spark):
    df = spark.createDataFrame([_good_row(valeur_fonciere=0.0)])
    result = check_valeur_fonciere_positive(df)
    assert not result.passed


def test_valeur_fonciere_positive_allows_null(spark):
    df = spark.createDataFrame([_good_row(valeur_fonciere=None), _good_row(valeur_fonciere=500000.0)])
    result = check_valeur_fonciere_positive(df)
    assert result.passed


def test_no_exact_duplicates_fails_when_present(spark):
    row = _good_row()
    df = spark.createDataFrame([row, row])
    result = check_no_exact_duplicates(df)
    assert not result.passed
    assert result.violation_count == 1


def test_built_property_missing_surface_flags_apartment_with_no_surface(spark):
    df = spark.createDataFrame([
        _good_row(type_local="Appartement", surface_reelle_bati=None),
        _good_row(type_local="Appartement", surface_reelle_bati=50.0),
    ])
    result = check_built_property_missing_surface(df)
    assert result.violation_count == 1
    assert result.passed  # WARNING severity never fails the run


def test_low_value_outliers_counts_correctly(spark):
    df = spark.createDataFrame([_good_row(valeur_fonciere=1.0), _good_row(valeur_fonciere=500000.0)])
    result = check_low_value_outliers(df)
    assert result.violation_count == 1


def test_high_value_outliers_counts_correctly(spark):
    df = spark.createDataFrame([_good_row(valeur_fonciere=20000000.0), _good_row(valeur_fonciere=500000.0)])
    result = check_high_value_outliers(df)
    assert result.violation_count == 1


def test_ban_low_confidence_counts_correctly(spark):
    df = spark.createDataFrame([_good_row(ban_result_score=0.3), _good_row(ban_result_score=0.9)])
    result = check_ban_low_confidence(df)
    assert result.violation_count == 1


def test_join_coverage_passes_above_floor(spark):
    df = spark.createDataFrame([_good_row(), _good_row()])
    result = check_join_coverage(df, "ban_result_status", 0.99, "BAN")
    assert result.passed


def test_join_coverage_fails_below_floor(spark):
    df = spark.createDataFrame([
        _good_row(ban_result_status="ok"),
        _good_row(ban_result_status=None),
    ])
    result = check_join_coverage(df, "ban_result_status", 0.99, "BAN")
    assert not result.passed


def test_run_quality_checks_passes_on_clean_data(spark):
    df = spark.createDataFrame([_good_row(), _good_row(id_mutation="2024-2")])
    results = run_quality_checks(df, 2024, "75")
    assert all(r.passed or r.severity == "WARNING" for r in results)


def test_run_quality_checks_raises_on_hard_failure(spark):
    df = spark.createDataFrame([_good_row(nature_mutation="Bogus Category")])
    with pytest.raises(QualityCheckFailure):
        run_quality_checks(df, 2024, "75")