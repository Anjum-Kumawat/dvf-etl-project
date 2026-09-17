"""RETL0-41: deduplication for the Silver DVF table.

DVF's raw export legitimately repeats id_mutation across multiple rows for
a single real-estate transaction (one sale can span several cadastral
parcels, several copropriete lots, or several local types in the same
disposition) -- those are NOT duplicates and must be preserved.

Separately, DVF's export has a known data-quality quirk: some rows are
repeated as EXACT full-row duplicates (every column identical, including
id_parcelle and lot numbers). On this project's dept-75/2024 data this
affected 7,830 of 74,800 Silver rows (10.5%) -- confirmed via direct query
against silver_dvf before writing this code (not assumed).

This module only removes exact full-row duplicates. It does not deduplicate
on any subset of columns (e.g. id_mutation alone), because that would
incorrectly collapse legitimate multi-parcel/multi-lot transactions.
"""
from pyspark.sql import DataFrame


def deduplicate_dvf(df: DataFrame) -> DataFrame:
    """Drop exact full-row duplicates. Rows differing in any column
    (e.g. a different id_parcelle or lot number for the same id_mutation)
    are preserved -- they represent real, distinct disposition lines."""
    return df.dropDuplicates()
