"""RETL0-40 / RETL0-41 / RETL0-42 entrypoint:
Bronze (MinIO/S3A) -> typed, deduplicated, BAN-enriched Silver DVF -> PostgreSQL.

Usage:
    python -m src.silver.run_dvf_silver --year 2024 --dept 75 --publication 2026-04
"""
import argparse
import os

from src.common.spark_session import get_spark_session
from src.silver.dvf_ban_join import join_ban, read_ban_silver
from src.silver.dvf_dedup import deduplicate_dvf
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
    enriched = join_ban(deduped, ban)
    enriched_count = enriched.count()
    matched_count = enriched.filter(enriched["ban_result_status"].isNotNull()).count()
    print(
        f"BAN join: {enriched_count} rows after join "
        f"({matched_count} matched a BAN geocoding result, "
        f"{enriched_count - matched_count} had no BAN match)"
    )

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    (
        enriched.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "silver_dvf")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print(f"Wrote {enriched_count} rows to PostgreSQL table silver_dvf")
    spark.stop()


if __name__ == "__main__":
    main()