"""RETL0-46 entrypoint: Silver DVF (PostgreSQL) -> Gold price aggregates ->
PostgreSQL.

Usage:
    python -m src.gold.run_gold
"""
import os

from src.common.spark_session import get_spark_session
from src.gold.price_aggregates import build_price_aggregates, read_silver_dvf

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5434")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "dvf")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "dvf")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "dvf")


def main():
    spark = get_spark_session("gold-price-aggregates")

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    silver = read_silver_dvf(spark, jdbc_url, POSTGRES_USER, POSTGRES_PASSWORD)
    silver_count = silver.count()
    print(f"Read {silver_count} rows from silver_dvf")

    price_agg = build_price_aggregates(silver)
    price_agg_count = price_agg.count()
    print(f"Built {price_agg_count} rows for gold_price_by_municipality_quarter")

    (
        price_agg.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "gold_price_by_municipality_quarter")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print(f"Wrote {price_agg_count} rows to PostgreSQL table gold_price_by_municipality_quarter")
    spark.stop()


if __name__ == "__main__":
    main()