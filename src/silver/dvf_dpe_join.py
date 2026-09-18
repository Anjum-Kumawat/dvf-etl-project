"""RETL0-43: Silver join between DVF (already BAN-enriched) and DPE energy
diagnostics.

Join key: DVF's ban_result_id (added by the RETL0-42 BAN join) matches DPE's
identifiant_ban -- both are IGN BAN address identifiers, confirmed against
real data (e.g. "75113_2756_00062" appears in both sources in the same
format). This join must run AFTER the BAN join, since ban_result_id does not
exist on DVF rows before that point.

IMPORTANT LIMITATION, found by checking real cardinality before writing this
join (scripts/dpe_duplicate_check.py): DPE records are NOT one-per-address.
Department 75 has 845,261 DPE records across only 70,777 unique
identifiant_ban values -- a BAN address ID identifies a building entrance,
and DPE diagnostics exist per DWELLING UNIT within that building (the
most-repeated address in dept 75 has 3,621 separate DPE records). DVF/BAN
data does not carry a per-unit key (floor, unit label) that would let us
pick the exact diagnostic for the specific unit sold in a mutation.

Given that, this join is a documented MVP-scope approximation: for each
identifiant_ban we keep only the MOST RECENT DPE record (by
date_etablissement_dpe, tie-broken by numero_dpe for determinism), and join
that single "building-representative" record onto every DVF row at that
address. Read it as "the latest known energy profile for this building,"
not as certified for the specific unit sold. Recorded in the Silver data
dictionary (RETL0-94) and called out in the final report.

RETL0-49 addendum: date_etablissement_dpe is cast via try_cast (raised as
an F.expr, since pyspark.sql.functions has no try_cast wrapper in this
Spark version) rather than plain .cast("date"). Under PySpark 4.1.1's ANSI
SQL mode default, a plain .cast() raises CAST_INVALID_INPUT on any blank or
malformed value instead of returning NULL -- the same failure mode found
and fixed in src/silver/dvf_filosofi_join.py for a different source.
Applied here preventively, by the same causal reasoning -- not because this
specific crash has been observed yet. surface_habitable_logement is not
changed: it is declared DoubleType directly in DPE_BRONZE_SCHEMA and parsed
by Spark's JSON reader (PERMISSIVE mode nulls unparseable values rather
than raising), a different, already-safe code path from the explicit
StringType-then-.cast() pattern this addendum addresses.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

DPE_BRONZE_SCHEMA = StructType([
    StructField("numero_dpe", StringType(), True),
    StructField("identifiant_ban", StringType(), True),
    StructField("adresse_ban", StringType(), True),
    StructField("code_insee_ban", StringType(), True),
    StructField("code_postal_ban", StringType(), True),
    StructField("nom_commune_ban", StringType(), True),
    StructField("numero_voie_ban", StringType(), True),
    StructField("nom_rue_ban", StringType(), True),
    StructField("etiquette_dpe", StringType(), True),
    StructField("etiquette_ges", StringType(), True),
    StructField("surface_habitable_logement", DoubleType(), True),
    StructField("type_batiment", StringType(), True),
    StructField("periode_construction", StringType(), True),
    StructField("date_etablissement_dpe", StringType(), True),
    StructField("date_reception_dpe", StringType(), True),
    StructField("coordonnee_cartographique_x_ban", DoubleType(), True),
    StructField("coordonnee_cartographique_y_ban", DoubleType(), True),
])


def dpe_bronze_path(extraction_date: str, dept: str) -> str:
    return f"s3a://bronze/dpe/extraction_date={extraction_date}/department={dept}/{dept}.jsonl"


def most_recent_per_address(df: DataFrame) -> DataFrame:
    """Reduce raw DPE rows (already selected/renamed with a dpe_ prefix) to
    one row per identifiant_ban: the most recent diagnostic by
    dpe_date_etablissement_dpe, ties broken by dpe_numero_dpe descending for
    determinism."""
    window = Window.partitionBy("identifiant_ban").orderBy(
        F.col("dpe_date_etablissement_dpe").desc_nulls_last(),
        F.col("dpe_numero_dpe").desc_nulls_last(),
    )
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def read_dpe_silver(spark: SparkSession, extraction_date: str, dept: str) -> DataFrame:
    """Read raw DPE Bronze JSONL, select/rename Silver columns, and reduce to
    one representative record per identifiant_ban."""
    raw = spark.read.schema(DPE_BRONZE_SCHEMA).json(dpe_bronze_path(extraction_date, dept))

    typed = raw.select(
        raw["identifiant_ban"],
        raw["numero_dpe"].alias("dpe_numero_dpe"),
        raw["etiquette_dpe"].alias("dpe_etiquette_dpe"),
        raw["etiquette_ges"].alias("dpe_etiquette_ges"),
        raw["surface_habitable_logement"].alias("dpe_surface_habitable_logement"),
        raw["type_batiment"].alias("dpe_type_batiment"),
        raw["periode_construction"].alias("dpe_periode_construction"),
        F.expr("try_cast(`date_etablissement_dpe` as date)").alias("dpe_date_etablissement_dpe"),
    )

    return most_recent_per_address(typed)


def join_dpe(dvf_df: DataFrame, dpe_df: DataFrame) -> DataFrame:
    """Left join DVF (already BAN-enriched) to the one-per-address DPE
    summary on ban_result_id == identifiant_ban."""
    dpe_renamed = dpe_df.withColumnRenamed("identifiant_ban", "_dpe_identifiant_ban")
    joined = dvf_df.join(
        dpe_renamed,
        dvf_df["ban_result_id"] == dpe_renamed["_dpe_identifiant_ban"],
        how="left",
    )
    return joined.drop("_dpe_identifiant_ban")