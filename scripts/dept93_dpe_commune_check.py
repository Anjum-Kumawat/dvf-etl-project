"""Diagnostic (not part of the pipeline): check whether department 93's
DPE join shortfall (62.6% vs the 70% floor) is spread across communes
(genuine variance) or concentrated in a few (a possible bug), the same way
the geo-join gap turned out to be 100% concentrated in one commune.

Run with: python -m scripts.dept93_dpe_commune_check
"""
from src.common.spark_session import get_spark_session
from src.silver.dvf_ban_join import join_ban, read_ban_silver
from src.silver.dvf_dedup import deduplicate_dvf
from src.silver.dvf_dpe_join import join_dpe, read_dpe_silver
from src.silver.dvf_schema import DVF_BRONZE_SCHEMA
from src.silver.dvf_transform import cast_dvf_core_fields

YEAR = 2024
DEPT = "93"
PUBLICATION = "2026-04"
DPE_EXTRACTION_DATE = "2026-09-18"

spark = get_spark_session("dept93-dpe-commune-check")

raw = (
    spark.read.schema(DVF_BRONZE_SCHEMA)
    .option("header", True)
    .csv(f"s3a://bronze/dvf/publication={PUBLICATION}/year={YEAR}/department={DEPT}/{DEPT}.csv.gz")
)
typed = cast_dvf_core_fields(raw)
deduped = deduplicate_dvf(typed)

ban = read_ban_silver(spark, PUBLICATION, YEAR, DEPT)
ban_enriched = join_ban(deduped, ban)
dpe = read_dpe_silver(spark, DPE_EXTRACTION_DATE, DEPT)
dpe_enriched = join_dpe(ban_enriched, dpe)

print("DPE match rate by commune:")
dpe_enriched.groupBy("code_commune", "nom_commune").agg(
    {"dpe_etiquette_dpe": "count"}
).withColumnRenamed("count(dpe_etiquette_dpe)", "dpe_matched").createOrReplaceTempView("dpe_by_commune")
dpe_enriched.groupBy("code_commune").count().withColumnRenamed("count", "total").createOrReplaceTempView("total_by_commune")
spark.sql("""
    SELECT d.code_commune, t.total, d.dpe_matched,
           ROUND(d.dpe_matched / t.total * 100, 1) AS dpe_pct
    FROM dpe_by_commune d JOIN total_by_commune t ON d.code_commune = t.code_commune
    ORDER BY dpe_pct ASC
""").show(40, truncate=False)

spark.stop()