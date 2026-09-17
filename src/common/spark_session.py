"""Shared SparkSession factory for reading Bronze from MinIO (S3A) and
writing Silver to PostgreSQL (JDBC)."""
import os

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"
POSTGRESQL_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


def get_spark_session(app_name: str) -> SparkSession:
    """Build a SparkSession configured for MinIO (S3A) and PostgreSQL JDBC."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            f"{HADOOP_AWS_PACKAGE},{POSTGRESQL_JDBC_PACKAGE}",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    