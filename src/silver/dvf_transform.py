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

RETL0-49 addendum: numeric/date fields are cast via try_cast (raised as an
F.expr, since pyspark.sql.functions has no try_cast wrapper in this Spark
version) rather than plain .cast(...). Blank strings are already normalized
to NULL below before any cast runs, and a NULL always casts to NULL safely
regardless of Spark's ANSI mode -- that part of this module was already
correct. The residual gap is a genuinely malformed NON-blank value: under
PySpark 4.1.1's ANSI SQL mode default, a plain .cast() on a malformed
non-null string raises CAST_INVALID_INPUT instead of returning NULL, which
is exactly the failure mode found and fixed in src/silver/dvf_filosofi_join.py
for a different source. DVF is a large, real government export that has
already shown at least one undocumented quirk (the exact full-row
duplicates found in RETL0-41), so hardened here for consistency with the
rest of the Silver layer -- not because this specific crash has been
observed in DVF's own core fields.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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


def _try_cast_col(col_name: str, to_type: str):
    return F.expr(f"try_cast(`{col_name}` as {to_type})")


def _normalize_and_try_cast_double(col_name: str):
    """Replace a comma decimal separator with a dot, then try_cast to
    double -- see module docstring's RETL0-49 addendum."""
    return F.expr(f"try_cast(regexp_replace(`{col_name}`, ',', '.') as double)")


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

    out = out.withColumn("date_mutation", _try_cast_col("date_mutation", "date"))

    for name in INT_FIELDS:
        out = out.withColumn(name, _try_cast_col(name, "int"))

    for name in DOUBLE_FIELDS:
        out = out.withColumn(name, _normalize_and_try_cast_double(name))

    for name in STRING_FIELDS:
        out = out.withColumn(name, F.col(name).cast(StringType()))

    return out