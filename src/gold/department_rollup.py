"""RETL0-47: Gold department/city rollup -- same pattern as RETL0-46
(price_per_sqm, transaction volumes), at a coarser grain: (code_departement,
quarter) instead of (code_commune, quarter). Since this project's pilot
scope is department 75 (Paris, a single department that is also a single
city), this rollup produces one row per quarter for all of Paris combined.

Reuses the exact same filters and mutation-level fix as RETL0-46 (see
src/gold/price_aggregates.py's module docstring for the multi-lot mutation
finding) via _filtered_priced() -- not re-implemented here, so both Gold
tables stay consistent by construction and the fix can't drift between them.
"""
from pyspark.sql import DataFrame

from src.gold.price_aggregates import _aggregate, _filtered_priced


def build_department_rollup(df: DataFrame) -> DataFrame:
    """Aggregate Silver DVF to one row per (code_departement, quarter)."""
    priced = _filtered_priced(df)
    return _aggregate(priced, ["code_departement", "quarter"])