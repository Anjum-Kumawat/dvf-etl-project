"""One-off connectivity check for Spark reading Bronze via S3A from MinIO."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("s3a-smoke-test")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

df = spark.read.csv(
    "s3a://bronze/dvf/publication=2026-04/year=2024/department=75/75.csv.gz",
    header=True,
)
print(f"Row count: {df.count()}")
df.show(5)
spark.stop()