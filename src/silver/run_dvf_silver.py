"""RETL0-40 through RETL0-45, RETL0-11 entrypoint: Bronze (MinIO/S3A) ->
typed, deduplicated, BAN/DPE/Filosofi/geo-enriched, quality-checked Silver
DVF -> PostgreSQL.

Usage:
    python -m src.silver.run_dvf_silver --year 2024 --dept 75 --publication 2026-04 --dpe-extraction-date 2026-09-17 --geo-extraction-date 2026-09-17
"""
import argparse
import os

import psycopg2

from src.common.spark_session import get_spark_session
from src.silver.dvf_ban_join import join_ban, read_ban_silver
from src.silver.dvf_dedup import deduplicate_dvf
from src.silver.dvf_dpe_join import join_dpe, read_dpe_silver
from src.silver.dvf_filosofi_join import join_filosofi, read_filosofi_silver
from src.silver.dvf_geo_join import join_geo, read_geo_silver
from src.silver.dvf_quality_checks import QualityCheckFailure, run_quality_checks
from src.silver.dvf_schema import DVF_BRONZE_SCHEMA
from src.silver.dvf_transform import cast_dvf_core_fields

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "dvf")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "dvf")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "dvf")


def bronze_path(publication: str, year: int, dept: str) -> str:
    return f"s3a://bronze/dvf/publication={publication}/year={year}/department={dept}/{dept}.csv.gz"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--dept", type=str, required=True)
    parser.add_argument("--publication", type=str, required=True)
    parser.add_argument("--dpe-extraction-date", type=str, required=True)
    parser.add_argument("--geo-extraction-date", type=str, required=True)
    args = parser.parse_args()

    spark = get_spark_session("silver-dvf-schema-typing")

    raw = (
        spark.read.schema(DVF_BRONZE_SCHEMA)
        .option("header", True)
        .csv(bronze_path(args.publication, args.year, args.dept))
    )

    typed = cast_dvf_core_fields(raw)
    typed_count = typed.count()

    deduped = deduplicate_dvf(typed)
    deduped_count = deduped.count()
    print(
        f"Typed rows: {typed_count}, after dedup: {deduped_count} "
        f"({typed_count - deduped_count} exact duplicate rows removed)"
    )

    ban = read_ban_silver(spark, args.publication, args.year, args.dept)
    ban_enriched = join_ban(deduped, ban)
    ban_enriched_count = ban_enriched.count()
    ban_matched_count = ban_enriched.filter(ban_enriched["ban_result_status"].isNotNull()).count()
    print(
        f"BAN join: {ban_enriched_count} rows after join "
        f"({ban_matched_count} matched a BAN geocoding result, "
        f"{ban_enriched_count - ban_matched_count} had no BAN match)"
    )

    dpe = read_dpe_silver(spark, args.dpe_extraction_date, args.dept)
    dpe_enriched = join_dpe(ban_enriched, dpe)
    dpe_enriched_count = dpe_enriched.count()
    dpe_matched_count = dpe_enriched.filter(dpe_enriched["dpe_etiquette_dpe"].isNotNull()).count()
    print(
        f"DPE join: {dpe_enriched_count} rows after join "
        f"({dpe_matched_count} matched a DPE energy diagnostic, "
        f"{dpe_enriched_count - dpe_matched_count} had none)"
    )

    filosofi = read_filosofi_silver(spark, args.dept)
    filosofi_enriched = join_filosofi(dpe_enriched, filosofi)
    filosofi_enriched_count = filosofi_enriched.count()
    filosofi_matched_count = filosofi_enriched.filter(
        filosofi_enriched["filosofi_revenu_median"].isNotNull()
    ).count()
    print(
        f"Filosofi join: {filosofi_enriched_count} rows after join "
        f"({filosofi_matched_count} matched commune income data, "
        f"{filosofi_enriched_count - filosofi_matched_count} had none)"
    )

    geo = read_geo_silver(spark, args.geo_extraction_date, args.dept)
    enriched = join_geo(filosofi_enriched, geo)
    enriched_count = enriched.count()
    geo_matched_count = enriched.filter(enriched["geo_commune_nom"].isNotNull()).count()
    print(
        f"Geo join: {enriched_count} rows after join "
        f"({geo_matched_count} matched administrative reference data, "
        f"{enriched_count - geo_matched_count} had none)"
    )

    try:
        run_quality_checks(enriched, args.year, args.dept)
    except QualityCheckFailure as exc:
        print(f"ERROR: quality checks failed, aborting before write: {exc}")
        raise SystemExit(1)

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    # Partition-safe write: delete only this department's existing rows,
    # then append this run's rows. A plain overwrite would wipe every other
    # department's data each time Dagster materializes one department
    # partition at a time -- see
    # docs/ingestion/incremental-ingestion-strategy.md. Uses psycopg2
    # directly (already a dependency inside the Dagster container, and
    # added to requirements.txt for local runs) rather than trying to reuse
    # Spark's own JVM/JDBC classloader, which does not expose the
    # postgresql driver to plain java.sql.DriverManager calls made through
    # py4j even though Spark's own JDBC writer can see it fine internally.
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("DELETE FROM silver_dvf WHERE code_departement = %s", (args.dept,))
                conn.commit()
            except psycopg2.errors.UndefinedTable:
                # First-ever run: table doesn't exist yet. mode("append")
                # below creates it, so there is nothing to delete.
                conn.rollback()
    finally:
        conn.close()

    (
        enriched.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "silver_dvf")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"Wrote {enriched_count} rows to PostgreSQL table silver_dvf")
    spark.stop()


if __name__ == "__main__":
    main()