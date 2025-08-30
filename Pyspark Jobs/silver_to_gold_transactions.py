# silver_to_gold_transactions.py (Fixed Ambiguous Column References)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat_ws, to_date, lit, max as spark_max, year, month, dayofmonth
from delta import DeltaTable

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires."""
    return SparkSession.builder \
        .appName("SilverToGold_Transactions_V3_Fixed") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-api.minio.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def check_dim_date_exists(spark, path):
    """Vérifie si la table dim_date existe."""
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"❌ ERREUR: La table 'dim_date' est introuvable à {path}.")
        print("Veuillez d'abord exécuter le script 'generate_dim_date.py'.")
        return False
    print("✅ Table dim_date trouvée.")
    return True

def get_max_silver_timestamp(spark, fact_table_path):
    """Obtenir le dernier timestamp traité en se basant sur la table de faits Gold."""
    if not DeltaTable.isDeltaTable(spark, fact_table_path): 
        return None
    try: 
        return spark.read.format("delta").load(fact_table_path).agg(spark_max("silver_load_timestamp")).collect()[0][0]
    except: 
        return None

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("=== DÉMARRAGE: TRANSFORMATION SILVER -> GOLD POUR TRANSACTIONS (AVEC PARTITIONING YEAR/MONTH/DAY) ===")

    silver_path = "s3a://transactions/silver"
    gold_base_path = "s3a://transactions/gold"
    dim_date_path = "s3a://common/gold/dim_date"
    dim_customer_path = f"{gold_base_path}/dim_customer"
    dim_product_path = f"{gold_base_path}/dim_product"
    dim_location_path = f"{gold_base_path}/dim_location"
    fact_orders_path = f"{gold_base_path}/fact_orders"

    try:
        if not check_dim_date_exists(spark, dim_date_path):
            spark.stop()
            return

        silver_df = spark.read.format("delta").load(silver_path)
        dim_date_df = spark.read.format("delta").load(dim_date_path)
        max_silver_ts = get_max_silver_timestamp(spark, fact_orders_path)
        new_silver_df = silver_df.filter(col("silver_load_timestamp") > max_silver_ts) if max_silver_ts else silver_df
        
        if new_silver_df.count() == 0: 
            print("✅ Aucune nouvelle donnée à traiter."); 
            spark.stop(); 
            return
        
        print(f"📊 {new_silver_df.count()} nouveaux enregistrements à traiter.")
        new_silver_df.cache()

        print("\n--- Création/Mise à jour de dim_customer ---")
        customer_updates_df = new_silver_df.select("customer_id", "customer_first_name", "customer_last_name").distinct()
        if DeltaTable.isDeltaTable(spark, dim_customer_path):
            DeltaTable.forPath(spark, dim_customer_path).alias("t").merge(customer_updates_df.alias("s"), "t.customer_id = s.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            customer_updates_df.write.format("delta").mode("overwrite").save(dim_customer_path)
        print(f"✅ Table dim_customer synchronisée.")
        
        print("\n--- Création/Mise à jour de dim_product ---")
        product_updates_df = new_silver_df.select("product_id", "product_name", "category").distinct()
        if DeltaTable.isDeltaTable(spark, dim_product_path):
            DeltaTable.forPath(spark, dim_product_path).alias("t").merge(product_updates_df.alias("s"), "t.product_id = s.product_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            product_updates_df.write.format("delta").mode("overwrite").save(dim_product_path)
        print(f"✅ Table dim_product synchronisée.")
        
        print("\n--- Création/Mise à jour de dim_location ---")
        location_updates_df = new_silver_df.filter("city is not null and region is not null").withColumn("location_id", md5(concat_ws("||", "city", "region"))).select("location_id", "city", "region").distinct()
        if DeltaTable.isDeltaTable(spark, dim_location_path):
            DeltaTable.forPath(spark, dim_location_path).alias("t").merge(location_updates_df.alias("s"), "t.location_id = s.location_id").whenNotMatchedInsertAll().execute()
        else:
            location_updates_df.write.format("delta").mode("overwrite").save(dim_location_path)
        print(f"✅ Table dim_location synchronisée.")

        print("\n--- Insertion dans fact_orders avec partitioning year/month/day ---")
        dim_location_df = spark.read.format("delta").load(dim_location_path)
        
        # First, create the join and add partition columns with unique names
        fact_df = new_silver_df \
            .join(dim_location_df, ["city", "region"], "left") \
            .join(dim_date_df, to_date(new_silver_df.processing_timestamp) == dim_date_df.full_date, "left") \
            .withColumn("order_date", to_date(col("processing_timestamp"))) \
            .withColumn("partition_year", year(col("order_date"))) \
            .withColumn("partition_month", month(col("order_date"))) \
            .withColumn("partition_day", dayofmonth(col("order_date"))) \
            .select(
                "order_id", "customer_id", "product_id", "location_id", 
                col("date_key").alias("order_date_key"), 
                "amount", "payment_method", "payment_status", 
                col("is_returned").alias("is_returned_flag"), 
                (col("discount_code").isNotNull()).alias("has_discount_flag"), 
                col("processing_timestamp").alias("order_timestamp"), 
                "silver_load_timestamp",
                "partition_year", "partition_month", "partition_day"
            ) \
            .fillna({"order_date_key": -1})
        
        # Écriture avec partitioning hiérarchique year/month/day
        fact_df.write.format("delta").mode("append") \
            .partitionBy("partition_year", "partition_month", "partition_day") \
            .save(fact_orders_path)
        print(f"✅ {fact_df.count()} nouveaux enregistrements ajoutés à fact_orders (partitionnés par partition_year/partition_month/partition_day).")
        
        new_silver_df.unpersist()

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback; traceback.print_exc()
    finally:
        print("\n🎉 TRANSFORMATION TERMINÉE.")
        spark.stop()

if __name__ == "__main__":
    main()
