# silver_to_gold_social_media.py (Enhanced with Year/Month/Day partitioning)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, max as spark_max, year, month, dayofmonth
from delta import DeltaTable

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires."""
    return SparkSession.builder \
        .appName("SilverToGold_SocialMedia_V3") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-api.minio.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def check_dim_date_exists(spark, path):
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"❌ ERREUR: La table 'dim_date' est introuvable à {path}.")
        return False
    print("✅ Table dim_date trouvée.")
    return True

def get_max_silver_timestamp(spark, fact_table_path):
    if not DeltaTable.isDeltaTable(spark, fact_table_path):
        return None
    try:
        return spark.read.format("delta").load(fact_table_path) \
            .agg(spark_max("silver_load_timestamp")).collect()[0][0]
    except:
        return None

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("=== DÉMARRAGE: TRANSFORMATION SILVER -> GOLD POUR SOCIAL MEDIA (AVEC PARTITIONING YEAR/MONTH/DAY) ===")

    silver_path = "s3a://social-media/silver"
    gold_base_path = "s3a://social-media/gold"
    dim_date_path = "s3a://common/gold/dim_date"
    fact_posts_path = f"{gold_base_path}/fact_posts"
    nlp_results_path = f"{gold_base_path}/nlp_sentiment_analysis"

    try:
        if not check_dim_date_exists(spark, dim_date_path):
            spark.stop()
            return

        silver_df = spark.read.format("delta").load(silver_path)
        dim_date_df = spark.read.format("delta").load(dim_date_path)

        max_silver_ts = get_max_silver_timestamp(spark, fact_posts_path)
        new_silver_df = (
            silver_df.filter(col("silver_load_timestamp") > max_silver_ts)
            if max_silver_ts else silver_df
        )

        if new_silver_df.count() == 0:
            print("✅ Aucune nouvelle donnée à traiter.")
            spark.stop()
            return

        print(f"📊 {new_silver_df.count()} nouveaux posts à traiter.")

        # Enrichissement avec dimensions de date et partitioning columns
        enriched_df = new_silver_df.join(
            dim_date_df,
            to_date(new_silver_df.processing_timestamp) == dim_date_df.full_date,
            "left"
        ).withColumnRenamed("date_key", "post_date_key") \
         .withColumn("post_date", to_date(col("processing_timestamp"))) \
         .withColumn("year", year("post_date")) \
         .withColumn("month", month("post_date")) \
         .withColumn("day", dayofmonth("post_date")) \
         .fillna({"post_date_key": -1})

        enriched_df.cache()

        print("\n--- Insertion dans fact_posts avec partitioning year/month/day ---")
        fact_posts_df = enriched_df.select(
            "post_id",
            "user_id",
            "platform",
            "likes",
            col("processing_timestamp").alias("post_timestamp"),
            "post_date_key",
            "silver_load_timestamp",
            "year", "month", "day"
        )
        fact_posts_df.write.format("delta").mode("append") \
            .partitionBy("year", "month", "day").save(fact_posts_path)
        print(f"✅ {fact_posts_df.count()} enregistrements ajoutés à fact_posts (partitionnés par year/month/day).")

        print("\n--- Création de la table nlp_sentiment_analysis avec partitioning year/month/day ---")
        nlp_results_df = enriched_df.select(
            "content",
            lit(None).cast("string").alias("sentiment"),
            "post_date_key",
            "year", "month", "day"
        )
        nlp_results_df.write.format("delta").mode("append") \
            .partitionBy("year", "month", "day").save(nlp_results_path)
        print(f"✅ {nlp_results_df.count()} lignes ajoutées à nlp_sentiment_analysis (partitionnées par year/month/day).")

        enriched_df.unpersist()

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🎉 TRANSFORMATION TERMINÉE.")
        spark.stop()

if __name__ == "__main__":
    main()
