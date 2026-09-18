"""Diagnostic (not part of the pipeline): investigate department 93's real
quality-check failures from its first Silver run --
  - 2.1: 156 rows with an unknown nature_mutation (never seen in dept 75 or 92)
  - 5-geo: 345 rows with no geo match (both dept 75 and dept 92 were 100%)
  - 5-BAN / 5-DPE: lower coverage than either prior department

Run with: python -m scripts.dept93_quality_diagnostic
"""
from src.common.spark_session import get_spark_session
from src.silver.dvf_ban_join import join_ban, read_ban_silver
from src.silver.dvf_dedup import deduplicate_dvf
from src.silver.dvf_dpe_join import join_dpe, read_dpe_silver
from src.silver.dvf_geo_join import join_geo, read_geo_silver
from src.silver.dvf_quality_checks import KNOWN_NATURE_MUTATION
from src.silver.dvf_schema import DVF_BRONZE_SCHEMA
from src.silver.dvf_transform import cast_dvf_core_fields

YEAR = 2024
DEPT = "93"
PUBLICATION = "2026-04"
DPE_EXTRACTION_DATE = "2026-09-18"
GEO_EXTRACTION_DATE = "2026-09-18"

spark = get_spark_session("dept93-quality-diagnostic")

raw = (
    spark.read.schema(DVF_BRONZE_SCHEMA)
    .option("header", True)
    .csv(f"s3a://bronze/dvf/publication={PUBLICATION}/year={YEAR}/department={DEPT}/{DEPT}.csv.gz")
)
typed = cast_dvf_core_fields(raw)
deduped = deduplicate_dvf(typed)

print("=" * 70)
print("2.1 -- unknown nature_mutation values")
print("=" * 70)
bad_nature = deduped.filter(~deduped["nature_mutation"].isin(list(KNOWN_NATURE_MUTATION)))
print(f"Total violating rows: {bad_nature.count()}")
print("\nDistinct unknown nature_mutation values (with counts):")
bad_nature.groupBy("nature_mutation").count().orderBy("count", ascending=False).show(20, truncate=False)
print("\nSample violating rows:")
bad_nature.select(
    "id_mutation", "nature_mutation", "code_commune", "nom_commune", "valeur_fonciere"
).show(10, truncate=False)

ban = read_ban_silver(spark, PUBLICATION, YEAR, DEPT)
ban_enriched = join_ban(deduped, ban)
dpe = read_dpe_silver(spark, DPE_EXTRACTION_DATE, DEPT)
dpe_enriched = join_dpe(ban_enriched, dpe)
geo = read_geo_silver(spark, GEO_EXTRACTION_DATE, DEPT)
enriched = join_geo(dpe_enriched, geo)

print("\n" + "=" * 70)
print("5-geo -- rows with no geo match")
print("=" * 70)
bad_geo = enriched.filter(enriched["geo_commune_nom"].isNull())
print(f"Total violating rows: {bad_geo.count()}")
print("\nDistinct (code_commune, nom_commune) among violators:")
bad_geo.select("code_commune", "nom_commune").distinct().orderBy("code_commune").show(50, truncate=False)

print("\n" + "=" * 70)
print("5-BAN / 5-DPE -- coverage by commune (lowest first)")
print("=" * 70)
print("\nBAN match rate by commune:")
enriched.groupBy("code_commune", "nom_commune").agg(
    {"ban_result_status": "count"}
).withColumnRenamed("count(ban_result_status)", "ban_matched").createOrReplaceTempView("ban_by_commune")
enriched.groupBy("code_commune").count().withColumnRenamed("count", "total").createOrReplaceTempView("total_by_commune")
spark.sql("""
    SELECT b.code_commune, t.total, b.ban_matched,
           ROUND(b.ban_matched / t.total * 100, 1) AS ban_pct
    FROM ban_by_commune b JOIN total_by_commune t ON b.code_commune = t.code_commune
    ORDER BY ban_pct ASC
""").show(40, truncate=False)

spark.stop()