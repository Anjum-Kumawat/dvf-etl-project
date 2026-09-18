"""RETL0-45: Silver join between DVF and geo.api.gouv.fr administrative
reference data (commune name, population, EPCI, region, centroid).

The join-key mismatch flagged in geo_ingest.py's module docstring (RETL0-39)
is real and applies here -- unlike Filosofi (RETL0-44), where the same
concern turned out NOT to apply once checked against real data. Confirmed
against the real Bronze snapshot: querying codeDepartement=75 returns
exactly ONE commune, code "75056" ("Paris", population 2,103,778) --
geo.api.gouv.fr does not know about the fiscal arrondissement codes
(75101-75120) that DVF/BAN/DPE/Filosofi all use. Paris, Lyon, and Marseille
are each a single INSEE commune split into fiscal arrondissements only for
tax/electoral purposes; geo.api.gouv.fr, a canonical administrative-
boundaries source, only knows the parent commune.

This join therefore needs a derived key, not a direct code match: map DVF's
code_commune to its PARENT INSEE commune code before joining:
  751xx -> 75056 (Paris)
  692xx -> 69123 (Lyon)      [not present in this project's dept-75 pilot]
  132xx -> 13055 (Marseille) [not present in this project's dept-75 pilot]
Any other code_commune (an ordinary, non-PLM commune) maps to itself.

RETL0-49 addendum: department 93's first real run surfaced a second,
different-shaped case of the same underlying problem -- DVF using a
commune code that geo.api.gouv.fr's CURRENT administrative snapshot no
longer recognizes as independent. 345 rows (all of commune 93059,
Pierrefitte-sur-Seine) failed to match. Investigated directly: querying
geo.api.gouv.fr live for codeDepartement=93 returns 39 communes, and code
93059 is absent from both that live response AND our already-stored
Bronze snapshot (confirmed identical, 39/39, no ingestion discrepancy) --
so this is not a Bronze/ingestion bug. Root cause (confirmed via INSEE's
own metadata pages): Pierrefitte-sur-Seine (93059) merged into Saint-Denis
(93066) as a commune déléguée effective 2025-01-01. DVF's 2024 transaction
data predates the merger and still uses 93059; geo.api.gouv.fr reflects
current boundaries and only knows 93066. Same fix shape as the PLM case
above -- map the pre-merger code to its current parent -- so it's folded
into the same lookup rather than special-cased separately. This is a
known limitation, not an exhaustive audit: any other 2025 (or later)
commune merger affecting a department this project processes would need
the same real-data investigation before adding its mapping here.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Pre-merger/pre-arrondissement-split code -> current parent INSEE commune
# code. Each entry here was confirmed against real data (a live Bronze
# snapshot or ingestion showing the old code unmatched) before being added
# -- see the module docstring for how each one was found.
MERGED_OR_SPLIT_COMMUNE_PARENT = {
    # Pierrefitte-sur-Seine -> Saint-Denis, merger effective 2025-01-01
    # (RETL0-49; confirmed via INSEE commune-déléguée metadata).
    "93059": "93066",
}


def geo_bronze_path(extraction_date: str, dept: str) -> str:
    return f"s3a://bronze/geo/extraction_date={extraction_date}/department={dept}/{dept}.json"


def parent_insee_commune(code_commune_col):
    """Map a DVF-style code_commune to its parent INSEE commune code. Paris/
    Lyon/Marseille fiscal arrondissement codes collapse to their parent
    commune; any code in MERGED_OR_SPLIT_COMMUNE_PARENT collapses to its
    post-merger parent; every other code is already a real, current
    commune and maps to itself."""
    result = (
        F.when(code_commune_col.startswith("751"), F.lit("75056"))
        .when(code_commune_col.startswith("692"), F.lit("69123"))
        .when(code_commune_col.startswith("132"), F.lit("13055"))
    )
    for old_code, new_code in MERGED_OR_SPLIT_COMMUNE_PARENT.items():
        result = result.when(code_commune_col == old_code, F.lit(new_code))
    return result.otherwise(code_commune_col)


def read_geo_silver(spark: SparkSession, extraction_date: str, dept: str) -> DataFrame:
    """Read the Bronze geo.api.gouv.fr snapshot (a single pretty-printed
    JSON document nesting department/region/communes) and flatten the
    commune array into Silver columns."""
    raw = spark.read.option("multiLine", True).json(geo_bronze_path(extraction_date, dept))

    exploded = raw.select(
        F.explode("communes").alias("c"),
        F.col("region.nom").alias("geo_region_nom"),
    )

    return exploded.select(
        exploded["c.code"].alias("code_commune_insee"),
        exploded["c.nom"].alias("geo_commune_nom"),
        exploded["c.population"].alias("geo_commune_population"),
        exploded["c.epci.nom"].alias("geo_epci_nom"),
        exploded["c.centre.coordinates"].getItem(0).alias("geo_centre_longitude"),
        exploded["c.centre.coordinates"].getItem(1).alias("geo_centre_latitude"),
        exploded["geo_region_nom"],
    )


def join_geo(dvf_df: DataFrame, geo_df: DataFrame) -> DataFrame:
    """Left join DVF to geo.api.gouv.fr reference data via the derived
    parent INSEE commune code (see parent_insee_commune). Every Paris
    arrondissement (and every mapped merged/split commune) resolves to the
    same single geo row -- a many-to-one join, no fan-out risk."""
    dvf_with_key = dvf_df.withColumn(
        "_parent_insee_commune", parent_insee_commune(dvf_df["code_commune"])
    )
    geo_renamed = geo_df.withColumnRenamed("code_commune_insee", "_geo_code_commune_insee")
    joined = dvf_with_key.join(
        geo_renamed,
        dvf_with_key["_parent_insee_commune"] == geo_renamed["_geo_code_commune_insee"],
        how="left",
    )
    return joined.drop("_parent_insee_commune", "_geo_code_commune_insee")