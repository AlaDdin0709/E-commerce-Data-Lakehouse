from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires"""
    spark = SparkSession.builder \
        .appName("SocialMediaKafkaProcessor") \
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

def read_last_offsets(spark, topic_name):
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
            offsets_by_partition = {}
            rows = offset_df.collect()
            for row in rows:
                partition = str(row.partition)
                offset = row.offset + 1
                offsets_by_partition[partition] = offset
            
            kafka_offsets = json.dumps({topic_name: offsets_by_partition})
            print(f"Offsets trouvés pour {topic_name}: {kafka_offsets}")
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

def process_social_media_data(spark):
    """Traiter les données du topic social_media (JSON)"""
    print("=== TRAITEMENT DU TOPIC SOCIAL MEDIA ===")
    
    starting_offsets = read_last_offsets(spark, "social_media")
    
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.17.34.127:9092") \
        .option("subscribe", "social_media") \
        .option("startingOffsets", starting_offsets) \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print(f"Nombre total de messages lus depuis Kafka: {df.count()}")
    
    if df.count() == 0:
        print("Aucun nouveau message trouvé dans Kafka pour social_media.")
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
    
    print("=== EXEMPLES DE DONNÉES JSON SOCIAL MEDIA ===")
    json_df.select("json_data").show(3, truncate=False)
    
    return json_df

def write_social_media_to_json(df, output_path):
    """Écrire les données social media vers MinIO en format JSON"""
    
    if df is None or df.count() == 0:
        print("Aucune donnée social media à écrire dans MinIO")
        return
    
    print(f"Écriture de {df.count()} enregistrements social media vers MinIO...")
    
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
        
        print(f"Données social media écrites avec succès dans: {output_path}")
        
        print("=== STATISTIQUES DES DONNÉES SOCIAL MEDIA ÉCRITES ===")
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
                
                print(f"✅ Données social media écrites avec timestamp unique: {unique_path}")
                
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
                    
                print("✅ Données social media écrites avec timestamp unique!")
            except Exception as e2:
                print(f"❌ Erreur lors de l'écriture avec timestamp: {str(e2)}")
                raise e2

def main():
    """Fonction principale pour traiter le topic social_media"""
    print("=== DÉMARRAGE DU TRAITEMENT SOCIAL MEDIA ===")
    
    spark = create_spark_session()
    
    try:
        ensure_offset_bucket_exists(spark)
        
        processed_df = process_social_media_data(spark)
        
        if processed_df is not None and processed_df.count() > 0:
            output_path = "s3a://social-media/raw"
            write_social_media_to_json(processed_df, output_path)
            
            save_last_offsets(spark, processed_df, "social_media")
            
            print(f"✅ Topic social_media traité avec succès! {processed_df.count()} enregistrements traités.")
        else:
            print("⚠️ Aucune donnée valide à traiter pour social_media")
            
    except Exception as e:
        print(f"❌ Erreur lors du traitement de social_media: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        print("Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
