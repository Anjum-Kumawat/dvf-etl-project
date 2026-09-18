"""RETL0-42: Silver join between typed/deduplicated DVF rows and BAN
(Base Adresse Nationale) geocoding results.

Join key: (adresse_numero, adresse_nom_voie, code_postal, code_commune).
This mirrors exactly the dedup key used when addresses were extracted for
geocoding (src/ingestion/enrichment/ban_ingest.py:extract_unique_addresses),
so every DVF row's address tuple maps to at most one BAN result row.

This is a LEFT join, not an inner join: extract_unique_addresses() skips any
DVF row whose adresse_nom_voie is blank (e.g. land/parcel-only rows have no
street address), so those rows have no BAN counterpart at all and must keep
flowing through Silver with NULL ban_* columns rather than being dropped.

Both DVF and BAN carry an independent longitude/latitude (DVF's own export
vs BAN's own geocoding of the same address) -- both are kept, prefixed, as
a deliberate data-quality cross-check rather than picking one arbitrarily.

RETL0-49 addendum: numeric fields below are cast via try_cast (raised as an
F.expr, since pyspark.sql.functions has no try_cast wrapper in this Spark
version) rather than plain .cast(...). Under PySpark 4.1.1's ANSI SQL mode
default, a plain .cast() raises CAST_INVALID_INPUT on any blank or
malformed value instead of returning NULL -- exactly the failure mode found
and fixed in src/silver/dvf_filosofi_join.py for a different source. BAN's
CSV columns are all read as StringType (see BAN_BRONZE_SCHEMA), so a row
with a missing house number, an unmatched geocode, or a blank coordinate
would hit the same crash. Applied here preventively, by the same causal
reasoning -- not because this specific crash has been observed yet.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

BAN_BRONZE_COLUMNS = [
    "id", "adresse_numero", "adresse_nom_voie", "code_postal", "code_commune",
    "longitude", "latitude", "result_score", "result_score_next", "result_label",
    "result_type", "result_id", "result_banId", "result_housenumber",
    "result_name", "result_street", "result_postcode", "result_city",
    "result_context", "result_citycode", "result_oldcitycode", "result_oldcity",
    "result_district", "result_status",
]

BAN_BRONZE_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in BAN_BRONZE_COLUMNS]
)

JOIN_KEY = ["adresse_numero", "adresse_nom_voie", "code_postal", "code_commune"]


def ban_bronze_path(publication: str, year: int, dept: str) -> str:
    return f"s3a://bronze/ban/publication={publication}/year={year}/department={dept}/{dept}.csv"


def _try_cast(col_name: str, to_type: str):
    return F.expr(f"try_cast(`{col_name}` as {to_type})")


def read_ban_silver(spark: SparkSession, publication: str, year: int, dept: str) -> DataFrame:
    """Read raw BAN Bronze CSV and cast/rename to the columns Silver needs."""
    raw = (
        spark.read.schema(BAN_BRONZE_SCHEMA)
        .option("header", True)
        .csv(ban_bronze_path(publication, year, dept))
    )

    return raw.select(
        _try_cast("adresse_numero", "int").alias("adresse_numero"),
        raw["adresse_nom_voie"].alias("adresse_nom_voie"),
        raw["code_postal"].alias("code_postal"),
        raw["code_commune"].alias("code_commune"),
        _try_cast("longitude", "double").alias("ban_longitude"),
        _try_cast("latitude", "double").alias("ban_latitude"),
        _try_cast("result_score", "double").alias("ban_result_score"),
        raw["result_label"].alias("ban_result_label"),
        raw["result_id"].alias("ban_result_id"),
        raw["result_housenumber"].alias("ban_result_housenumber"),
        raw["result_street"].alias("ban_result_street"),
        raw["result_postcode"].alias("ban_result_postcode"),
        raw["result_city"].alias("ban_result_city"),
        raw["result_citycode"].alias("ban_result_citycode"),
        raw["result_status"].alias("ban_result_status"),
    )


def join_ban(dvf_df: DataFrame, ban_df: DataFrame) -> DataFrame:
    """Left join typed/deduplicated DVF rows to their BAN geocoding result.

    BAN addresses are already unique per extract_unique_addresses(), so this
    left join cannot multiply DVF rows -- each DVF row matches at most one
    BAN row.
    """
    return dvf_df.join(ban_df, on=JOIN_KEY, how="left")