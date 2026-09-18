"""RETL0-46: Gold municipality x quarter aggregate fact table.

Grain: one row per (code_commune, quarter) -- per the ticket's acceptance
criteria. Filters:
  - nature_mutation = 'Vente' (excludes Echange, Adjudication, Vente terrain
    à bâtir -- atypical price formation, not ordinary market sales)
  - surface_reelle_bati IS NOT NULL AND > 0 (required to compute
    price_per_sqm at all)
  - valeur_fonciere IS NOT NULL AND > 0 (implicit requirement for
    price_per_sqm to be computable)

CRITICAL finding, confirmed against real data before finalizing this logic:
DVF repeats the FULL transaction value (valeur_fonciere) on EVERY
disposition row of a multi-lot mutation (a single sale spanning several
apartments/commercial units/dependencies), rather than allocating it across
rows. Real example: mutation 2024-1202784 in dept 75 has 40 disposition
rows, each carrying the identical valeur_fonciere=94000000, with individual
surface_reelle_bati ranging 6-950 sqm. Computing price_per_sqm per
DISPOSITION ROW (that row's own small surface) produced nonsensical results
up to ~15,700,000 EUR/sqm and inflated nb_transactions by counting one real
sale as 40. The correct grain for price_per_sqm is the MUTATION (the actual
transaction), not the disposition row: _mutation_level() sums surface
across all qualifying rows of a mutation first, keeping the value once,
before price_per_sqm or nb_transactions is computed.

Median is computed via percentile_approx (Spark has no exact median
aggregate); acceptable for aggregate reporting at this row-count scale.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_silver_dvf(spark: SparkSession, jdbc_url: str, user: str, password: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "silver_dvf")
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def _mutation_level(df: DataFrame) -> DataFrame:
    """Reduce qualifying Silver DVF disposition rows to one row per
    id_mutation: sum surface_reelle_bati across all rows of a mutation,
    keep valeur_fonciere once (it's identical across every row of a
    mutation by construction -- see module docstring)."""
    row_filtered = df.filter(
        (F.col("nature_mutation") == "Vente")
        & F.col("surface_reelle_bati").isNotNull()
        & (F.col("surface_reelle_bati") > 0)
        & F.col("valeur_fonciere").isNotNull()
        & (F.col("valeur_fonciere") > 0)
    )

    return row_filtered.groupBy(
        "id_mutation", "code_commune", "code_departement", "date_mutation"
    ).agg(
        F.first("valeur_fonciere").alias("valeur_fonciere"),
        F.sum("surface_reelle_bati").alias("surface_reelle_bati"),
    )


def _filtered_priced(df: DataFrame) -> DataFrame:
    """Shared by RETL0-46 (commune grain) and RETL0-47 (department grain,
    src/gold/department_rollup.py): mutation-level rows with price_per_sqm
    and quarter computed."""
    mutation_level = _mutation_level(df)

    priced = mutation_level.withColumn(
        "price_per_sqm", F.col("valeur_fonciere") / F.col("surface_reelle_bati")
    )

    return priced.withColumn(
        "quarter",
        F.concat(
            F.year("date_mutation").cast("string"),
            F.lit("-Q"),
            F.quarter("date_mutation").cast("string"),
        ),
    )


def _aggregate(df: DataFrame, group_cols: list) -> DataFrame:
    return df.groupBy(*group_cols).agg(
        F.count("*").alias("nb_transactions"),
        F.expr("percentile_approx(price_per_sqm, 0.5)").alias("median_price_per_sqm"),
        F.avg("price_per_sqm").alias("avg_price_per_sqm"),
        F.expr("percentile_approx(valeur_fonciere, 0.5)").alias("median_valeur_fonciere"),
        F.avg("valeur_fonciere").alias("avg_valeur_fonciere"),
        F.avg("surface_reelle_bati").alias("avg_surface_reelle_bati"),
    )


def build_price_aggregates(df: DataFrame) -> DataFrame:
    """Aggregate Silver DVF to one row per (code_commune, quarter)."""
    priced = _filtered_priced(df)
    return _aggregate(priced, ["code_commune", "quarter"])