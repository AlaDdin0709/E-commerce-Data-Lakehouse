# silver_to_gold_customer_images.py (Fixed Ambiguous Column References)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date, max as spark_max, year, month, dayofmonth
from delta import DeltaTable

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires."""
    return SparkSession.builder \
        .appName("SilverToGold_CustomerImages_V3_Fixed") \
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
        print(f"❌ ERREUR: La table 'dim_date' est introuvable à {path}."); 
        return False
    print("✅ Table dim_date trouvée."); 
    return True

def get_max_silver_timestamp(spark, dim_table_path):
    if not DeltaTable.isDeltaTable(spark, dim_table_path): 
        return None
    try: 
        return spark.read.format("delta").load(dim_table_path).agg(spark_max("silver_load_timestamp")).collect()[0][0]
    except: 
        return None

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("=== DÉMARRAGE: TRANSFORMATION SILVER -> GOLD POUR CUSTOMER IMAGES (AVEC PARTITIONING YEAR/MONTH/DAY) ===")

    silver_path = "s3a://customer-images/silver"
    gold_base_path = "s3a://customer-images/gold"
    dim_date_path = "s3a://common/gold/dim_date"
    dim_image_path = f"{gold_base_path}/dim_image"
    agg_uploads_path = f"{gold_base_path}/agg_image_uploads_by_day"

    try:
        if not check_dim_date_exists(spark, dim_date_path): 
            spark.stop(); 
            return
            
        silver_df = spark.read.format("delta").load(silver_path)
        dim_date_df = spark.read.format("delta").load(dim_date_path)
        max_silver_ts = get_max_silver_timestamp(spark, dim_image_path)
        new_silver_df = silver_df.filter(col("silver_load_timestamp") > max_silver_ts) if max_silver_ts else silver_df

        if new_silver_df.count() == 0: 
            print("✅ Aucune nouvelle donnée à traiter."); 
            spark.stop(); 
            return

        print(f"📊 {new_silver_df.count()} nouvelles métadonnées d'image à traiter.")

        print("\n--- Création/Mise à jour de dim_image avec partitioning year/month/day ---")
        dim_updates_df = new_silver_df \
            .join(dim_date_df, to_date(new_silver_df.upload_timestamp) == dim_date_df.full_date, "left") \
            .withColumn("upload_date", to_date(col("upload_timestamp"))) \
            .withColumn("partition_year", year(col("upload_date"))) \
            .withColumn("partition_month", month(col("upload_date"))) \
            .withColumn("partition_day", dayofmonth(col("upload_date"))) \
            .select(
                "image_id", "s3_path", "customer_id", "order_id", "upload_timestamp", 
                col("date_key").alias("upload_date_key"), "silver_load_timestamp",
                "partition_year", "partition_month", "partition_day"
            ) \
            .fillna({"upload_date_key": -1})
        
        if DeltaTable.isDeltaTable(spark, dim_image_path):
            DeltaTable.forPath(spark, dim_image_path).alias("t").merge(
                dim_updates_df.alias("s"), "t.image_id = s.image_id"
            ).whenNotMatchedInsertAll().execute()
        else:
            dim_updates_df.write.format("delta").mode("overwrite") \
                .partitionBy("partition_year", "partition_month", "partition_day").save(dim_image_path)
        print(f"✅ Table dim_image synchronisée (partitionnée par partition_year/partition_month/partition_day).")

        print("\n--- Recalcul de agg_image_uploads_by_day avec partitioning year/month/day ---")
        full_dim_df = spark.read.format("delta").load(dim_image_path)
        
        # Re-read dim_date to avoid column conflicts
        dim_date_fresh_df = spark.read.format("delta").load(dim_date_path)
        
        agg_df = full_dim_df.filter("upload_date_key != -1") \
            .join(dim_date_fresh_df, full_dim_df.upload_date_key == dim_date_fresh_df.date_key) \
            .withColumn("agg_partition_year", year(col("full_date"))) \
            .withColumn("agg_partition_month", month(col("full_date"))) \
            .withColumn("agg_partition_day", dayofmonth(col("full_date"))) \
            .groupBy("upload_date_key", "full_date", "day_name", "is_weekend", 
                    "agg_partition_year", "agg_partition_month", "agg_partition_day") \
            .agg(count("*").alias("image_count"))
        
        agg_df.write.format("delta").mode("overwrite") \
            .partitionBy("agg_partition_year", "agg_partition_month", "agg_partition_day").save(agg_uploads_path)
        print(f"✅ Table agg_image_uploads_by_day recalculée (partitionnée par agg_partition_year/agg_partition_month/agg_partition_day).")
        
    except Exception as e:
        print(f"❌ Erreur: {e}"); 
        import traceback; traceback.print_exc()
    finally:
        print("\n🎉 TRANSFORMATION TERMINÉE."); 
        spark.stop()

if __name__ == "__main__":
    main()
