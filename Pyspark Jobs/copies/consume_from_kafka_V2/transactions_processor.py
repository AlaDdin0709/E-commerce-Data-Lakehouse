from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import csv
import io
import json

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires"""
    spark = SparkSession.builder \
        .appName("TransactionsKafkaProcessor") \
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

def parse_kafka_csv_message(csv_message):
    """
    Parser spécialisé pour les messages Kafka qui contiennent :
    - L'en-tête CSV sur la première ligne
    - Les données réelles sur les lignes suivantes
    - Séparés par \n dans le même message
    """
    if csv_message is None or csv_message.strip() == "":
        return None
    
    try:
        lines = csv_message.strip().split('\n')
        
        if len(lines) < 2:
            return None
        
        header_line = lines[0]
        data_lines = lines[1:]
        
        if not header_line.startswith('order_id,customer_id'):
            reader = csv.reader(io.StringIO(csv_message))
            return next(reader)
        
        parsed_rows = []
        for data_line in data_lines:
            if data_line.strip():
                try:
                    reader = csv.reader(io.StringIO(data_line))
                    row = next(reader)
                    parsed_rows.append(row)
                except Exception as e:
                    print(f"Erreur parsing ligne: {data_line}, erreur: {str(e)}")
                    continue
        
        return parsed_rows[0] if parsed_rows else None
        
    except Exception as e:
        print(f"Erreur parsing message Kafka: {csv_message[:100]}..., erreur: {str(e)}")
        return None

def validate_transaction_row(parsed_row):
    """Valider qu'une ligne de transaction contient des données valides"""
    if parsed_row is None or len(parsed_row) < 14:
        return False
    
    order_id = parsed_row[0] if len(parsed_row) > 0 else ""
    customer_id = parsed_row[1] if len(parsed_row) > 1 else ""
    amount_str = parsed_row[7] if len(parsed_row) > 7 else ""
    
    if not order_id or order_id == "order_id" or order_id.strip() == "":
        return False
    
    if not customer_id or customer_id == "customer_id" or customer_id.strip() == "":
        return False
    
    if amount_str == "amount" or amount_str == "NULL" or not amount_str or amount_str.strip() == "":
        return False
    
    try:
        amount_value = float(amount_str)
        if amount_value < 0 or amount_value > 100000:
            return False
    except (ValueError, TypeError):
        return False
    
    if len(order_id) < 10:
        return False
    
    return True

def process_transactions_data(spark):
    """Traiter les données du topic transactions"""
    print("=== TRAITEMENT DU TOPIC TRANSACTIONS ===")
    
    starting_offsets = read_last_offsets(spark, "transactions")
    
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.17.34.127:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", starting_offsets) \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print(f"Nombre total de messages lus depuis Kafka: {df.count()}")
    
    if df.count() == 0:
        print("Aucun nouveau message trouvé dans Kafka pour transactions.")
        return None
    
    kafka_df = df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("csv_data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset")
    )
    
    kafka_df = kafka_df.filter(col("csv_data").isNotNull() & (col("csv_data") != ""))
    
    print(f"Nombre de messages valides après filtrage: {kafka_df.count()}")
    
    if kafka_df.count() == 0:
        return None
    
    print("=== EXEMPLES DE DONNÉES BRUTES KAFKA ===")
    kafka_df.select("csv_data").show(3, truncate=False)
    
    parse_kafka_csv_udf = udf(parse_kafka_csv_message, ArrayType(StringType()))
    
    parsed_df = kafka_df.withColumn("parsed_data", parse_kafka_csv_udf(col("csv_data")))
    parsed_df = parsed_df.filter(col("parsed_data").isNotNull())
    parsed_df = parsed_df.filter(size(col("parsed_data")) >= 14)
    
    print(f"Nombre de messages après parsing CSV: {parsed_df.count()}")
    
    if parsed_df.count() == 0:
        print("⚠️ AUCUNE DONNÉE VALIDE APRÈS PARSING!")
        return None
    
    validate_row_udf = udf(validate_transaction_row, BooleanType())
    with_validation = parsed_df.withColumn("is_valid_transaction", validate_row_udf(col("parsed_data")))
    
    valid_count = with_validation.filter(col("is_valid_transaction") == True).count()
    invalid_count = with_validation.filter(col("is_valid_transaction") == False).count()
    
    print(f"Lignes valides: {valid_count}")
    print(f"Lignes invalides: {invalid_count}")
    
    if valid_count == 0:
        print("⚠️ AUCUNE TRANSACTION VALIDE TROUVÉE!")
        return None
    
    valid_transactions = with_validation.filter(col("is_valid_transaction") == True)
    
    transactions_df = valid_transactions.select(
        col("parsed_data")[0].alias("order_id"),
        col("parsed_data")[1].alias("customer_id"),
        col("parsed_data")[2].alias("customer_first_name"),
        col("parsed_data")[3].alias("customer_last_name"),
        col("parsed_data")[4].alias("product_id"),
        col("parsed_data")[5].alias("product_name"),
        col("parsed_data")[6].alias("category"),
        col("parsed_data")[7].alias("amount"),
        col("parsed_data")[8].alias("payment_method"),
        col("parsed_data")[9].alias("payment_status"),
        col("parsed_data")[10].alias("discount_code"),
        col("parsed_data")[11].alias("shipping_address"),
        col("parsed_data")[12].alias("timestamp_raw"),
        col("parsed_data")[14].alias("is_returned"),
        col("kafka_timestamp"),
        col("kafka_partition"),
        col("kafka_offset")
    )
    
    final_df = transactions_df.withColumn("processing_date", current_date()) \
                             .withColumn("processing_timestamp", current_timestamp())
    
    print(f"✅ Nombre final d'enregistrements à traiter: {final_df.count()}")
    
    if final_df.count() > 0:
        print("=== EXEMPLES D'ENREGISTREMENTS FINAUX ===")
        final_df.select("order_id", "customer_id", "amount", "payment_status", "category").show(5)
    
    return final_df

def write_transactions_to_csv(df, output_path):
    """Écrire les données transactions vers MinIO en format CSV"""
    
    if df is None or df.count() == 0:
        print("Aucune donnée transactions à écrire dans MinIO")
        return
    
    print(f"Écriture de {df.count()} enregistrements transactions vers MinIO en CSV...")
    
    try:
        df_with_partitions = df.withColumn("year", year(col("processing_date"))) \
                               .withColumn("month", month(col("processing_date"))) \
                               .withColumn("day", dayofmonth(col("processing_date")))
        
        df_with_partitions.coalesce(1).write \
            .mode("append") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .partitionBy("year", "month", "day") \
            .csv(output_path)
        
        print(f"Données transactions écrites avec succès en CSV dans: {output_path}")
        
        print("=== STATISTIQUES DES DONNÉES TRANSACTIONS ÉCRITES ===")
        df.groupBy("payment_status").count().show()
        df.groupBy("category").count().show()
        
    except Exception as e:
        print(f"Erreur lors de l'écriture CSV vers MinIO: {str(e)}")
        try:
            print("Tentative d'écriture simple sans partitioning...")
            df.coalesce(1).write \
                .mode("append") \
                .option("header", "true") \
                .csv(output_path.replace("/partitioned", "/simple"))
            print("Données écrites en CSV simple!")
        except Exception as e2:
            print(f"Erreur même avec écriture simple: {str(e2)}")
            raise e2

def main():
    """Fonction principale pour traiter le topic transactions"""
    print("=== DÉMARRAGE DU TRAITEMENT TRANSACTIONS ===")
    
    spark = create_spark_session()
    
    try:
        ensure_offset_bucket_exists(spark)
        
        processed_df = process_transactions_data(spark)
        
        if processed_df is not None and processed_df.count() > 0:
            output_path = "s3a://transactions/raw/csv-data"
            write_transactions_to_csv(processed_df, output_path)
            
            save_last_offsets(spark, processed_df, "transactions")
            
            print(f"✅ Topic transactions traité avec succès! {processed_df.count()} enregistrements traités.")
        else:
            print("⚠️ Aucune donnée valide à traiter pour transactions")
            
    except Exception as e:
        print(f"❌ Erreur lors du traitement de transactions: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        print("Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
