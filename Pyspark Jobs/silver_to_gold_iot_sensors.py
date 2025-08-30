# silver_to_gold_iot_sensors.py (Fixed Ambiguous Column References)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, window, to_date, when, max as spark_max, year, month, dayofmonth
from delta import DeltaTable

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires."""
    return SparkSession.builder \
        .appName("SilverToGold_IoTSensors_V3_Fixed") \
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
    
def get_max_silver_timestamp(spark, fact_table_path):
    if not DeltaTable.isDeltaTable(spark, fact_table_path): 
        return None
    try: 
        return spark.read.format("delta").load(fact_table_path).agg(spark_max("silver_load_timestamp")).collect()[0][0]
    except: 
        return None

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("=== DÉMARRAGE: TRANSFORMATION SILVER -> GOLD POUR IOT SENSORS (AVEC PARTITIONING YEAR/MONTH/DAY) ===")

    silver_path = "s3a://iot-sensors/silver"
    gold_base_path = "s3a://iot-sensors/gold"
    dim_date_path = "s3a://common/gold/dim_date"
    dim_sensor_path = f"{gold_base_path}/dim_sensor"
    fact_readings_path = f"{gold_base_path}/fact_sensor_readings"
    agg_health_path = f"{gold_base_path}/agg_device_health_hourly"

    try:
        if not check_dim_date_exists(spark, dim_date_path): 
            spark.stop(); 
            return
        
        silver_df = spark.read.format("delta").load(silver_path)
        dim_date_df = spark.read.format("delta").load(dim_date_path)
        max_silver_ts = get_max_silver_timestamp(spark, fact_readings_path)
        new_silver_df = silver_df.filter(col("silver_load_timestamp") > max_silver_ts) if max_silver_ts else silver_df

        if new_silver_df.count() == 0: 
            print("✅ Aucune nouvelle donnée à traiter."); 
            spark.stop(); 
            return

        print(f"📊 {new_silver_df.count()} nouvelles lectures à traiter.")
        
        # Enrichissement avec dimensions de date et partitioning columns
        enriched_df = new_silver_df.join(dim_date_df, to_date(new_silver_df.processing_timestamp) == dim_date_df.full_date, "left") \
            .withColumnRenamed("date_key", "reading_date_key") \
            .withColumn("reading_date", to_date(col("processing_timestamp"))) \
            .withColumn("partition_year", year(col("reading_date"))) \
            .withColumn("partition_month", month(col("reading_date"))) \
            .withColumn("partition_day", dayofmonth(col("reading_date"))) \
            .fillna({"reading_date_key": -1})
        enriched_df.cache()

        print("\n--- Création/Mise à jour de dim_sensor ---")
        sensor_updates_df = enriched_df.select("sensor_id", "device_type", "firmware_version").distinct()
        if DeltaTable.isDeltaTable(spark, dim_sensor_path):
            DeltaTable.forPath(spark, dim_sensor_path).alias("t").merge(
                sensor_updates_df.alias("s"), "t.sensor_id = s.sensor_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            sensor_updates_df.write.format("delta").mode("overwrite").save(dim_sensor_path)
        print(f"✅ Table dim_sensor synchronisée.")
        
        print("\n--- Insertion dans fact_sensor_readings avec partitioning year/month/day ---")
        fact_df = enriched_df.select(
            "sensor_id", 
            col("processing_timestamp").alias("reading_timestamp"), 
            "reading_date_key", 
            "temperature", 
            "humidity", 
            "battery_level", 
            "silver_load_timestamp",
            "partition_year", "partition_month", "partition_day"
        )
        fact_df.write.format("delta").mode("append") \
            .partitionBy("partition_year", "partition_month", "partition_day").save(fact_readings_path)
        print(f"✅ {fact_df.count()} enregistrements ajoutés à fact_sensor_readings (partitionnés par partition_year/partition_month/partition_day).")

        print("\n--- Recalcul de agg_device_health_hourly avec partitioning year/month/day ---")
        full_facts_df = spark.read.format("delta").load(fact_readings_path)
        
        agg_df = full_facts_df.groupBy(
            window(col("reading_timestamp"), "1 hour").alias("hour_window"), col("sensor_id")
        ).agg(
            avg("temperature").alias("avg_temperature"), 
            avg("humidity").alias("avg_humidity"), 
            min("battery_level").alias("min_battery_level"), 
            max("battery_level").alias("max_battery_level"), 
            max("reading_timestamp").alias("last_reading_in_window")
        ).withColumn("agg_date", to_date(col("hour_window.start"))) \
         .withColumn("agg_partition_year", year(col("agg_date"))) \
         .withColumn("agg_partition_month", month(col("agg_date"))) \
         .withColumn("agg_partition_day", dayofmonth(col("agg_date")))
        
        agg_df.write.format("delta").mode("overwrite") \
            .partitionBy("agg_partition_year", "agg_partition_month", "agg_partition_day").save(agg_health_path)
        print(f"✅ Table agg_device_health_hourly recalculée (partitionnée par agg_partition_year/agg_partition_month/agg_partition_day).")
        
        enriched_df.unpersist()

    except Exception as e:
        print(f"❌ Erreur: {e}"); 
        import traceback; traceback.print_exc()
    finally:
        print("\n🎉 TRANSFORMATION TERMINÉE."); 
        spark.stop()

if __name__ == "__main__":
    main()
