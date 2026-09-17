"""RETL0-44: Silver join between DVF and INSEE Filosofi commune-level income
data (2021 vintage, FILO2021_DISP_COM.csv -- disposable income indicators;
the headline commune file INSEE distributes, one of six variants in the same
archive; the other five (DEC, *_PAUVRES, TRDECILES) are out of MVP scope).

Join key: code_commune (both sides).

CORRECTION to an earlier assumption: prior project documentation
(geo_ingest.py's module docstring, RETL0-39) flagged that Paris fiscal
arrondissement codes (75101-75120) don't match the canonical INSEE commune
code (75056) used by some national reference sources. That concern was
verified specifically for geo.api.gouv.fr (RETL0-45), not assumed to apply
everywhere. Checked directly against the real Filosofi file before writing
this join: Filosofi's CODGEO lists Paris BY ARRONDISSEMENT (75101, 75102,
..., 75120, each its own row), not as a single aggregated 75056 row -- i.e.
Filosofi already uses the same commune-code convention as DVF/BAN. No
special-casing is needed here; it's a plain equi-join on code_commune.

Bronze holds Filosofi's national file unfiltered and still zipped (INSEE
publishes one file for all of France, and filosofi_ingest.py deliberately
stores it exactly as published, not repackaged). This module downloads the
zip from MinIO and extracts the CSV member AT RUN TIME rather than reading
a manually-unzipped local file, so the Silver pipeline stays reproducible
on a machine that has never run the ingestion script's local unzip step.
Filtering to the pilot department happens here, in Silver.

INSEE suppresses (with the literal string "s") any indicator computed from
too small a population to publish reliably (statistical secrecy). Casting
"s" to a numeric type yields NULL, which is correct -- a suppressed cell is
genuinely unknown, not zero. This doesn't affect the (large-population)
Paris arrondissements for our selected columns, but is a real characteristic
of the source worth documenting, not a bug.
"""
import tempfile
import zipfile
from pathlib import Path

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.spark_session import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY

FILOSOFI_BUCKET = "bronze"
FILOSOFI_KEY = "filosofi/vintage=2021/communes.zip"
FILOSOFI_MEMBER = "FILO2021_DISP_COM.csv"


def _download_and_extract_filosofi_csv(dest_dir: Path) -> Path:
    """Download the Filosofi Bronze zip from MinIO and extract just the
    disposable-income CSV member."""
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    zip_path = dest_dir / "communes.zip"
    client.download_file(FILOSOFI_BUCKET, FILOSOFI_KEY, str(zip_path))

    with zipfile.ZipFile(zip_path) as zf:
        zf.extract(FILOSOFI_MEMBER, path=dest_dir)

    return dest_dir / FILOSOFI_MEMBER


def join_filosofi(dvf_df: DataFrame, filosofi_df: DataFrame) -> DataFrame:
    """Left join DVF to Filosofi commune-level income data on code_commune.
    Filosofi has exactly one row per commune, so this is a many-to-one join
    (many DVF mutations per commune) -- no fan-out risk."""
    filosofi_renamed = filosofi_df.withColumnRenamed("code_commune", "_filosofi_code_commune")
    joined = dvf_df.join(
        filosofi_renamed,
        dvf_df["code_commune"] == filosofi_renamed["_filosofi_code_commune"],
        how="left",
    )
    return joined.drop("_filosofi_code_commune")


def read_filosofi_silver(spark: SparkSession, dept: str) -> DataFrame:
    """Download Filosofi Bronze from MinIO, extract the disposable-income
    CSV, and select/filter the Silver columns for the given department."""
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = _download_and_extract_filosofi_csv(Path(tmp))

        raw = spark.read.option("header", True).option("sep", ";").csv(str(csv_path))

        def _num(col_name):
            return F.regexp_replace(F.col(col_name), ",", ".").cast("double")

        selected = raw.select(
            raw["CODGEO"].alias("code_commune"),
            raw["NBMEN21"].cast("int").alias("filosofi_nb_menages"),
            raw["NBPERS21"].cast("int").alias("filosofi_nb_personnes"),
            _num("Q221").alias("filosofi_revenu_median"),
            _num("GI21").alias("filosofi_gini_index"),
            _num("S80S2021").alias("filosofi_s80_s20_ratio"),
            _num("RD").alias("filosofi_interdecile_ratio"),
            _num("PPSOC21").alias("filosofi_pct_revenu_social"),
        )

        filtered = selected.filter(selected["code_commune"].startswith(dept))
        # Force materialization now, while the temp dir (and extracted CSV)
        # still exist -- Spark's CSV reader is lazy, and the temp file would
        # otherwise be deleted before any downstream action triggers the read.
        filtered = filtered.cache()
        filtered.count()

    return filtered