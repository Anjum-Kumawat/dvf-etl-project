"""RETL0-40: schema and typing for DVF core fields (Bronze -> Silver).

Reads raw Bronze DVF rows (all-string columns, see dvf_schema.py) and casts
each field to its correct Silver type. Deduplication (RETL0-41) and
enrichment joins (RETL0-42..45) happen in later tickets, not here.

Typing decisions (mirrored in the Silver data dictionary, RETL0-94):
  - code_postal, code_commune, code_departement, id_parcelle,
    adresse_code_voie, code_type_local, code_nature_culture,
    code_nature_culture_speciale, lotN_numero: kept as STRING — these are
    identifiers with meaningful leading zeros, not numeric quantities.
  - valeur_fonciere, surface_reelle_bati, surface_terrain,
    lotN_surface_carrez, longitude, latitude: DOUBLE. DVF exports use a
    comma decimal separator ("1042000,50"), normalized to a dot first.
  - numero_disposition, adresse_numero, nombre_lots,
    nombre_pieces_principales: INT.
  - date_mutation: DATE (source is already YYYY-MM-DD).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType

STRING_FIELDS = [
    "id_mutation", "nature_mutation", "adresse_suffixe", "adresse_nom_voie",
    "adresse_code_voie", "code_postal", "code_commune", "nom_commune",
    "code_departement", "ancien_code_commune", "ancien_nom_commune",
    "id_parcelle", "ancien_id_parcelle", "numero_volume",
    "lot1_numero", "lot2_numero", "lot3_numero", "lot4_numero", "lot5_numero",
    "code_type_local", "type_local", "code_nature_culture", "nature_culture",
    "code_nature_culture_speciale", "nature_culture_speciale",
]

INT_FIELDS = [
    "numero_disposition", "adresse_numero", "nombre_lots",
    "nombre_pieces_principales",
]

DOUBLE_FIELDS = [
    "valeur_fonciere",
    "lot1_surface_carrez", "lot2_surface_carrez", "lot3_surface_carrez",
    "lot4_surface_carrez", "lot5_surface_carrez",
    "surface_reelle_bati", "surface_terrain", "longitude", "latitude",
]


def _normalize_decimal(col_name: str):
    """Replace a comma decimal separator with a dot before casting to double."""
    return F.regexp_replace(F.col(col_name), ",", ".")


def cast_dvf_core_fields(df: DataFrame) -> DataFrame:
    """Cast raw all-string Bronze DVF columns to typed Silver columns.

    Blank strings ("") are normalized to NULL for every known column first,
    so downstream numeric/date casts don't silently misbehave on "".
    """
    out = df
    all_fields = STRING_FIELDS + INT_FIELDS + DOUBLE_FIELDS + ["date_mutation"]

    for name in all_fields:
        out = out.withColumn(
            name, F.when(F.trim(F.col(name)) == "", None).otherwise(F.col(name))
        )

    out = out.withColumn("date_mutation", F.col("date_mutation").cast(DateType()))

    for name in INT_FIELDS:
        out = out.withColumn(name, F.col(name).cast(IntegerType()))

    for name in DOUBLE_FIELDS:
        out = out.withColumn(name, _normalize_decimal(name).cast(DoubleType()))

    for name in STRING_FIELDS:
        out = out.withColumn(name, F.col(name).cast(StringType()))

    return out
