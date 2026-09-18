"""RETL0-46 / RETL0-47 entrypoint: Silver DVF (PostgreSQL) -> Gold price
aggregates (municipality x quarter, department x quarter) -> PostgreSQL.

Usage:
    python -m src.gold.run_gold
"""
import os

from src.common.spark_session import get_spark_session
from src.gold.department_rollup import build_department_rollup
from src.gold.price_aggregates import build_price_aggregates, read_silver_dvf

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5434")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "dvf")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "dvf")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "dvf")


def _write(df, jdbc_url, table_name):
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def main():
    spark = get_spark_session("gold-price-aggregates")

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    silver = read_silver_dvf(spark, jdbc_url, POSTGRES_USER, POSTGRES_PASSWORD)
    silver_count = silver.count()
    print(f"Read {silver_count} rows from silver_dvf")

    price_agg = build_price_aggregates(silver)
    price_agg_count = price_agg.count()
    print(f"Built {price_agg_count} rows for gold_price_by_municipality_quarter")
    _write(price_agg, jdbc_url, "gold_price_by_municipality_quarter")
    print(f"Wrote {price_agg_count} rows to PostgreSQL table gold_price_by_municipality_quarter")

    dept_rollup = build_department_rollup(silver)
    dept_rollup_count = dept_rollup.count()
    print(f"Built {dept_rollup_count} rows for gold_price_by_department_quarter")
    _write(dept_rollup, jdbc_url, "gold_price_by_department_quarter")
    print(f"Wrote {dept_rollup_count} rows to PostgreSQL table gold_price_by_department_quarter")

    spark.stop()


if __name__ == "__main__":
    main()