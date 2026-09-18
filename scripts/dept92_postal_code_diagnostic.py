"""Diagnostic (not part of the pipeline): show department 92's DVF rows
whose code_postal doesn't match the ^92[0-9]{3}$ pattern, to understand
why quality rule 2.3 fails for a department other than 75.
"""
from src.common.spark_session import get_spark_session
from src.silver.dvf_schema import DVF_BRONZE_SCHEMA
from src.silver.dvf_transform import cast_dvf_core_fields

spark = get_spark_session("dept92-postal-code-diagnostic")

raw = (
    spark.read.schema(DVF_BRONZE_SCHEMA)
    .option("header", True)
    .csv("s3a://bronze/dvf/publication=2026-04/year=2024/department=92/92.csv.gz")
)
typed = cast_dvf_core_fields(raw)

bad = typed.filter(~typed["code_postal"].rlike("^92[0-9]{3}$"))

print(f"Total rows with code_postal not matching ^92[0-9]{{3}}$: {bad.count()}")

print("\nDistinct violating code_postal values:")
bad.select("code_postal").distinct().orderBy("code_postal").show(50, truncate=False)

print("\nDistinct (code_commune, nom_commune) among violators:")
bad.select("code_commune", "nom_commune").distinct().orderBy("code_commune").show(50, truncate=False)

print("\nSample violating rows:")
bad.select(
    "id_mutation", "code_postal", "code_commune", "nom_commune", "adresse_nom_voie"
).show(20, truncate=False)

spark.stop()