from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires"""
    spark = SparkSession.builder \
        .appName("IoTSensorsKafkaProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-api.minio.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.kubernetes.file.upload.path", "/tmp") \
        .config("spark.kubernetes.executor.deleteOnTermination", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_offset_file_path(topic_name):
    """Obtenir le chemin du fichier d'offset pour un topic"""
    return f"s3a://kafka-offsets/{topic_name}_last_offset.json"

def ensure_offset_bucket_exists(spark):
    """Créer le bucket kafka-offsets s'il n'existe pas"""
    try:
        test_df = spark.createDataFrame([("test",)], ["value"])
        test_path = "s3a://kafka-offsets/_test_bucket_exists"
        test_df.write.mode("overwrite").json(test_path)
        test_df.limit(0).write.mode("overwrite").json(test_path)
        print("✅ Bucket kafka-offsets existe et est accessible")
    except Exception as e:
        if "NoSuchBucket" in str(e) or "does not exist" in str(e):
            print("📁 Création du bucket kafka-offsets...")
            try:
                temp_df = spark.createDataFrame([("init",)], ["temp"])
                temp_df.write.mode("overwrite").json("s3a://kafka-offsets/temp_init")
                temp_df.limit(0).write.mode("overwrite").json("s3a://kafka-offsets/temp_init")
                print("✅ Bucket kafka-offsets créé avec succès")
            except Exception as create_error:
                print(f"❌ Impossible de créer le bucket kafka-offsets: {str(create_error)}")
                print("🔧 Veuillez créer manuellement le bucket 'kafka-offsets' dans MinIO")
        else:
            print(f"⚠️ Erreur lors de la vérification du bucket: {str(e)}")

def get_all_topic_partitions(spark, topic_name, bootstrap_servers):
    """Obtenir toutes les partitions d'un topic Kafka"""
    try:
        # Essayer de lire quelques messages pour découvrir les partitions
        sample_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        if sample_df.count() > 0:
            partitions = sample_df.select("partition").distinct().collect()
            discovered_partitions = sorted([row.partition for row in partitions])
            print(f"Partitions détectées pour {topic_name}: {discovered_partitions}")
            return discovered_partitions
        
        # Si aucun message, essayer avec latest pour voir les partitions vides
        latest_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # Méthode de fallback : assumer des partitions standard (0, 1, 2)
        print(f"⚠️ Impossible de détecter automatiquement les partitions pour {topic_name}, utilisation des partitions par défaut (0, 1, 2)")
        return [0, 1, 2]
        
    except Exception as e:
        print(f"Erreur lors de la détection des partitions pour {topic_name}: {str(e)}")
        print("Utilisation des partitions par défaut (0, 1, 2)")
        return [0, 1, 2]

def read_last_offsets(spark, topic_name, bootstrap_servers="172.17.34.127:9092"):
    """Lire les derniers offsets sauvegardés depuis MinIO"""
    offset_path = get_offset_file_path(topic_name)
    
    try:
        offset_schema = StructType([
            StructField("partition", LongType(), True),
            StructField("offset", LongType(), True),
            StructField("topic", StringType(), True),
            StructField("saved_at", TimestampType(), True)
        ])
        
        offset_df = spark.read.schema(offset_schema).json(offset_path)
        
        if offset_df.count() > 0:
            # Obtenir toutes les partitions du topic
            all_partitions = get_all_topic_partitions(spark, topic_name, bootstrap_servers)
            
            # Créer un dictionnaire avec tous les offsets
            offsets_by_partition = {}
            
            # D'abord, initialiser toutes les partitions avec "earliest" (offset -2)
            for partition in all_partitions:
                offsets_by_partition[str(partition)] = -2
            
            # Ensuite, mettre à jour avec les offsets sauvegardés
            rows = offset_df.collect()
            for row in rows:
                partition = str(row.partition)
                offset = row.offset + 1  # Commencer au message suivant
                offsets_by_partition[partition] = offset
            
            kafka_offsets = json.dumps({topic_name: offsets_by_partition})
            print(f"Offsets configurés pour {topic_name}: {kafka_offsets}")
            return kafka_offsets
        else:
            print(f"Aucun offset trouvé pour {topic_name}, démarrage depuis le début")
            return "earliest"
            
    except Exception as e:
        if "NoSuchBucket" in str(e):
            print(f"⚠️ Bucket kafka-offsets n'existe pas. Démarrage depuis le début pour {topic_name}")
        elif "Path does not exist" in str(e) or "NoSuchKey" in str(e):
            print(f"📄 Pas de fichier d'offset pour {topic_name} (première exécution). Démarrage depuis le début")
        else:
            print(f"Impossible de lire les offsets pour {topic_name}: {str(e)}")
        return "earliest"

def save_last_offsets(spark, df, topic_name):
    """Sauvegarder les derniers offsets traités vers MinIO"""
    if df is None or df.count() == 0:
        print(f"Aucun offset à sauvegarder pour {topic_name}")
        return
    
    try:
        max_offsets = df.groupBy("kafka_partition") \
                       .agg(max("kafka_offset").alias("max_offset")) \
                       .select(
                           col("kafka_partition").alias("partition"),
                           col("max_offset").alias("offset"),
                           lit(topic_name).alias("topic"),
                           current_timestamp().alias("saved_at")
                       )
        
        offset_path = get_offset_file_path(topic_name)
        
        max_offsets.coalesce(1).write \
                   .mode("overwrite") \
                   .json(offset_path)
        
        print(f"Offsets sauvegardés pour {topic_name}:")
        max_offsets.show()
        
    except Exception as e:
        print(f"Erreur lors de la sauvegarde des offsets pour {topic_name}: {str(e)}")

def process_iot_sensors_data(spark):
    """Traiter les données du topic iot_sensors (JSON)"""
    print("=== TRAITEMENT DU TOPIC IOT SENSORS ===")
    
    bootstrap_servers = "172.17.34.127:9092"
    starting_offsets = read_last_offsets(spark, "iot_sensors", bootstrap_servers)
    
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "iot_sensors") \
        .option("startingOffsets", starting_offsets) \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print(f"Nombre total de messages lus depuis Kafka: {df.count()}")
    
    if df.count() == 0:
        print("Aucun nouveau message trouvé dans Kafka pour iot_sensors.")
        return None
    
    json_df = df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        current_timestamp().alias("processing_timestamp")
    )
    
    json_df = json_df.filter(col("json_data").isNotNull() & (col("json_data") != ""))
    
    print(f"Nombre de messages valides après filtrage: {json_df.count()}")
    
    if json_df.count() == 0:
        return None
    
    print("=== EXEMPLES DE DONNÉES JSON IOT SENSORS ===")
    json_df.select("json_data").show(3, truncate=False)
    
    return json_df

def write_iot_sensors_to_json(df, output_path):
    """Écrire les données IoT sensors vers MinIO en format JSON"""
    
    if df is None or df.count() == 0:
        print("Aucune donnée IoT sensors à écrire dans MinIO")
        return
    
    print(f"Écriture de {df.count()} enregistrements IoT sensors vers MinIO...")
    
    try:
        df_with_partitions = df.withColumn("year", year(col("processing_timestamp"))) \
                               .withColumn("month", month(col("processing_timestamp"))) \
                               .withColumn("day", dayofmonth(col("processing_timestamp"))) \
                               .withColumn("hour", hour(col("processing_timestamp")))
        
        df_coalesced = df_with_partitions.coalesce(1)
        
        df_coalesced.write \
            .mode("append") \
            .partitionBy("year", "month", "day", "hour") \
            .option("compression", "gzip") \
            .option("maxRecordsPerFile", 10000) \
            .json(output_path)
        
        print(f"Données IoT sensors écrites avec succès dans: {output_path}")
        
        print("=== STATISTIQUES DES DONNÉES IOT SENSORS ÉCRITES ===")
        print(f"Nombre total d'enregistrements: {df.count()}")
        df.select("kafka_partition").distinct().show()
        
    except Exception as e:
        if "FileAlreadyExistsException" in str(e) or "destination file exists" in str(e):
            print("⚠️ Conflit de fichiers, utilisation d'un timestamp unique...")
            try:
                import time
                timestamp_suffix = int(time.time() * 1000)
                
                df_with_unique_id = df.withColumn("batch_timestamp", lit(timestamp_suffix))
                unique_path = f"{output_path}/batch_{timestamp_suffix}"
                
                df_with_unique_id.coalesce(1).write \
                    .mode("overwrite") \
                    .option("compression", "gzip") \
                    .json(unique_path)
                
                print(f"✅ Données IoT sensors écrites avec timestamp unique: {unique_path}")
                
            except Exception as e3:
                print(f"❌ Erreur même avec timestamp unique: {str(e3)}")
                raise e3
        else:
            print(f"Erreur lors de l'écriture JSON vers MinIO: {str(e)}")
            try:
                print("Tentative d'écriture avec timestamp unique...")
                import time
                timestamp_suffix = int(time.time() * 1000)
                unique_path = f"{output_path}/batch_{timestamp_suffix}"
                
                df.coalesce(1).write \
                    .mode("overwrite") \
                    .option("compression", "gzip") \
                    .json(unique_path)
                    
                print("✅ Données IoT sensors écrites avec timestamp unique!")
            except Exception as e2:
                print(f"❌ Erreur lors de l'écriture avec timestamp: {str(e2)}")
                raise e2

def main():
    """Fonction principale pour traiter le topic iot_sensors"""
    print("=== DÉMARRAGE DU TRAITEMENT IOT SENSORS ===")
    
    spark = create_spark_session()
    
    try:
        ensure_offset_bucket_exists(spark)
        
        processed_df = process_iot_sensors_data(spark)
        
        if processed_df is not None and processed_df.count() > 0:
            output_path = "s3a://iot-sensors/raw"
            write_iot_sensors_to_json(processed_df, output_path)
            
            save_last_offsets(spark, processed_df, "iot_sensors")
            
            print(f"✅ Topic iot_sensors traité avec succès! {processed_df.count()} enregistrements traités.")
        else:
            print("⚠️ Aucune donnée valide à traiter pour iot_sensors")
            
    except Exception as e:
        print(f"❌ Erreur lors du traitement de iot_sensors: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        print("Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
