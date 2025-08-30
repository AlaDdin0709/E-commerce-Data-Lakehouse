from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Créer la session Spark avec support Delta Lake
builder = SparkSession.builder \
    .appName("ReadDeltaSelectedColumns") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.minio.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Lire la table Delta
delta_path = "s3a://transactions/bronze"
df = spark.read.format("delta").load(delta_path)

# Sélectionner les colonnes et afficher 10 lignes
df.select("shipping_address", "timestamp_raw") \
  .limit(10) \
  .show(truncate=False)

spark.stop()
