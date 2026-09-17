import pytest
from pyspark.sql import Row, SparkSession

from src.silver.dvf_transform import cast_dvf_core_fields


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-dvf-transform")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def _raw_row(**overrides):
    base = {
        "id_mutation": "2024-1193218",
        "date_mutation": "2024-01-04",
        "numero_disposition": "000001",
        "nature_mutation": "Vente",
        "valeur_fonciere": "1042000,50",
        "adresse_numero": "4",
        "adresse_suffixe": "",
        "adresse_nom_voie": "VLA PERREUR",
        "adresse_code_voie": "7288",
        "code_postal": "07100",
        "code_commune": "75120",
        "nom_commune": "Paris 20e Arrondissement",
        "code_departement": "75",
        "ancien_code_commune": "",
        "ancien_nom_commune": "",
        "id_parcelle": "75120000BM0133",
        "ancien_id_parcelle": "",
        "numero_volume": "",
        "lot1_numero": "147",
        "lot1_surface_carrez": "",
        "lot2_numero": "99",
        "lot2_surface_carrez": "",
        "lot3_numero": "",
        "lot3_surface_carrez": "",
        "lot4_numero": "",
        "lot4_surface_carrez": "",
        "lot5_numero": "",
        "lot5_surface_carrez": "",
        "nombre_lots": "2",
        "code_type_local": "2",
        "type_local": "Appartement",
        "surface_reelle_bati": "86",
        "nombre_pieces_principales": "4",
        "code_nature_culture": "",
        "nature_culture": "",
        "code_nature_culture_speciale": "",
        "nature_culture_speciale": "",
        "surface_terrain": "",
        "longitude": "2.405228",
        "latitude": "48.868216",
    }
    base.update(overrides)
    return Row(**base)


def test_cast_produces_correct_types(spark):
    df = spark.createDataFrame([_raw_row()])
    typed = cast_dvf_core_fields(df)
    dtypes = dict(typed.dtypes)

    assert dtypes["date_mutation"] == "date"
    assert dtypes["valeur_fonciere"] == "double"
    assert dtypes["nombre_lots"] == "int"
    assert dtypes["code_postal"] == "string"


def test_comma_decimal_separator_normalized(spark):
    df = spark.createDataFrame([_raw_row(valeur_fonciere="1042000,50")])
    typed = cast_dvf_core_fields(df)
    result = typed.select("valeur_fonciere").first()[0]
    assert result == 1042000.50


def test_leading_zero_postal_code_preserved(spark):
    df = spark.createDataFrame([_raw_row(code_postal="07100")])
    typed = cast_dvf_core_fields(df)
    result = typed.select("code_postal").first()[0]
    assert result == "07100"


def test_blank_string_becomes_null(spark):
    df = spark.createDataFrame([_raw_row(surface_terrain="")])
    typed = cast_dvf_core_fields(df)
    result = typed.select("surface_terrain").first()[0]
    assert result is None


def test_blank_int_field_becomes_null(spark):
    df = spark.createDataFrame([_raw_row(adresse_numero="")])
    typed = cast_dvf_core_fields(df)
    result = typed.select("adresse_numero").first()[0]
    assert result is None