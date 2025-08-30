from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import csv
import io
import json
import os

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires"""
    spark = SparkSession.builder \
        .appName("KafkaToMinIOMultiTopicConsumer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-api.minio.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
        # Tester si le bucket existe en essayant d'écrire un fichier test
        test_df = spark.createDataFrame([("test",)], ["value"])
        test_path = "s3a://kafka-offsets/_test_bucket_exists"
        test_df.write.mode("overwrite").json(test_path)
        
        # Supprimer le fichier test
        test_df.limit(0).write.mode("overwrite").json(test_path)
        print("✅ Bucket kafka-offsets existe et est accessible")
    except Exception as e:
        if "NoSuchBucket" in str(e) or "does not exist" in str(e):
            print("📁 Création du bucket kafka-offsets...")
            try:
                # Créer le bucket en écrivant un fichier temporaire puis le supprimer
                temp_df = spark.createDataFrame([("init",)], ["temp"])
                temp_df.write.mode("overwrite").json("s3a://kafka-offsets/temp_init")
                
                # Supprimer le fichier temporaire
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
        # Essayer de lire le fichier d'offset avec un schéma défini
        from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
        
        offset_schema = StructType([
            StructField("partition", LongType(), True),
            StructField("offset", LongType(), True),
            StructField("topic", StringType(), True),
            StructField("saved_at", TimestampType(), True)
        ])
        
        offset_df = spark.read.schema(offset_schema).json(offset_path)
        
        if offset_df.count() > 0:
            # Convertir en format JSON pour Kafka - AVEC le nom du topic
            offsets_by_partition = {}
            rows = offset_df.collect()
            for row in rows:
                partition = str(row.partition)
                offset = row.offset + 1  # +1 pour commencer après le dernier message traité
                offsets_by_partition[partition] = offset
            
            # Format JSON pour Kafka - CORRECT avec nom du topic
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
        # Obtenir les offsets maximum par partition
        max_offsets = df.groupBy("kafka_partition") \
                       .agg(max("kafka_offset").alias("max_offset")) \
                       .select(
                           col("kafka_partition").alias("partition"),
                           col("max_offset").alias("offset"),
                           lit(topic_name).alias("topic"),
                           current_timestamp().alias("saved_at")
                       )
        
        offset_path = get_offset_file_path(topic_name)
        
        # Sauvegarder les offsets (remplacer le fichier existant)
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
        # Diviser le message par les retours à la ligne
        lines = csv_message.strip().split('\n')
        
        if len(lines) < 2:
            # Si on a qu'une seule ligne, c'est probablement juste un en-tête
            return None
        
        # La première ligne est l'en-tête, les suivantes sont les données
        header_line = lines[0]
        data_lines = lines[1:]
        
        # Vérifier que la première ligne est bien un en-tête
        if not header_line.startswith('order_id,customer_id'):
            # Si ce n'est pas un en-tête reconnu, traiter comme une ligne de données normale
            reader = csv.reader(io.StringIO(csv_message))
            return next(reader)
        
        # Parser chaque ligne de données (ignorer l'en-tête)
        parsed_rows = []
        for data_line in data_lines:
            if data_line.strip():  # Ignorer les lignes vides
                try:
                    reader = csv.reader(io.StringIO(data_line))
                    row = next(reader)
                    parsed_rows.append(row)
                except Exception as e:
                    print(f"Erreur parsing ligne: {data_line}, erreur: {str(e)}")
                    continue
        
        # Retourner la première ligne de données valide
        return parsed_rows[0] if parsed_rows else None
        
    except Exception as e:
        print(f"Erreur parsing message Kafka: {csv_message[:100]}..., erreur: {str(e)}")
        return None

def validate_transaction_row(parsed_row):
    """Valider qu'une ligne de transaction contient des données valides"""
    if parsed_row is None or len(parsed_row) < 14:
        return False
    
    # Vérifications de base
    order_id = parsed_row[0] if len(parsed_row) > 0 else ""
    customer_id = parsed_row[1] if len(parsed_row) > 1 else ""
    amount_str = parsed_row[7] if len(parsed_row) > 7 else ""
    
    # L'order_id ne doit pas être vide ou être le nom de la colonne
    if not order_id or order_id == "order_id" or order_id.strip() == "":
        return False
    
    # Le customer_id ne doit pas être vide ou être le nom de la colonne
    if not customer_id or customer_id == "customer_id" or customer_id.strip() == "":
        return False
    
    # Le montant doit être convertible en nombre
    if amount_str == "amount" or amount_str == "NULL" or not amount_str or amount_str.strip() == "":
        return False
    
    try:
        amount_value = float(amount_str)
        # Vérifier que le montant est raisonnable (pas négatif, pas trop grand)
        if amount_value < 0 or amount_value > 100000:
            return False
    except (ValueError, TypeError):
        return False
    
    # Vérifier que l'order_id ressemble à un UUID ou ID valide
    if len(order_id) < 10:  # IDs trop courts sont suspects
        return False
    
    return True

def process_transactions_topic_fixed(spark):
    """Traitement du topic transactions avec parser spécialisé pour le format Kafka"""
    print("=== TRAITEMENT DU TOPIC TRANSACTIONS (FORMAT KAFKA SPÉCIALISÉ) ===")
    
    # Lire les derniers offsets
    starting_offsets = read_last_offsets(spark, "transactions")
    
    # Lire depuis Kafka avec les offsets appropriés
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.17.34.127:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", starting_offsets) \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print(f"Nombre total de messages lus depuis Kafka (transactions): {df.count()}")
    
    if df.count() == 0:
        print("Aucun nouveau message trouvé dans Kafka pour transactions.")
        return None
    
    # Convertir la valeur Kafka en string
    kafka_df = df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("csv_data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset")
    )
    
    # Filtrer les valeurs nulles ou vides
    kafka_df = kafka_df.filter(col("csv_data").isNotNull() & (col("csv_data") != ""))
    
    print(f"Nombre de messages valides après filtrage: {kafka_df.count()}")
    
    if kafka_df.count() == 0:
        return None
    
    # DÉBOGAGE: Afficher quelques exemples de données brutes
    print("=== EXEMPLES DE DONNÉES BRUTES KAFKA ===")
    kafka_df.select("csv_data").show(3, truncate=False)
    
    # UDF pour parser les messages Kafka spécialisés
    parse_kafka_csv_udf = udf(parse_kafka_csv_message, ArrayType(StringType()))
    
    # Parser les données avec le nouveau parser
    parsed_df = kafka_df.withColumn("parsed_data", parse_kafka_csv_udf(col("csv_data")))
    
    # Filtrer les lignes où le parsing a échoué ou retourné None
    parsed_df = parsed_df.filter(col("parsed_data").isNotNull())
    
    # Vérifier que nous avons assez de colonnes (14 colonnes attendues)
    parsed_df = parsed_df.filter(size(col("parsed_data")) >= 14)
    
    print(f"Nombre de messages après parsing CSV spécialisé: {parsed_df.count()}")
    
    if parsed_df.count() == 0:
        print("⚠️ AUCUNE DONNÉE VALIDE APRÈS PARSING SPÉCIALISÉ!")
        return None
    
    # DÉBOGAGE: Afficher la structure des données parsées
    print("=== EXEMPLES DE DONNÉES PARSÉES (NOUVELLES) ===")
    parsed_df.select("parsed_data").show(5, truncate=False)
    
    # Validation supplémentaire des données
    validate_row_udf = udf(validate_transaction_row, BooleanType())
    with_validation = parsed_df.withColumn("is_valid_transaction", validate_row_udf(col("parsed_data")))
    
    invalid_count = with_validation.filter(col("is_valid_transaction") == False).count()
    valid_count = with_validation.filter(col("is_valid_transaction") == True).count()
    
    print(f"=== VALIDATION DES DONNÉES ===")
    print(f"Lignes valides: {valid_count}")
    print(f"Lignes invalides: {invalid_count}")
    
    if valid_count == 0:
        print("⚠️ AUCUNE TRANSACTION VALIDE TROUVÉE APRÈS VALIDATION!")
        return None
    
    # Garder seulement les transactions valides
    valid_transactions = with_validation.filter(col("is_valid_transaction") == True)
    
    # Extraire les colonnes individuelles
    transactions_df = valid_transactions.select(
        col("parsed_data")[0].alias("order_id"),
        col("parsed_data")[1].alias("customer_id"),
        col("parsed_data")[2].alias("customer_first_name"),
        col("parsed_data")[3].alias("customer_last_name"),
        col("parsed_data")[4].alias("product_id"),
        col("parsed_data")[5].alias("product_name"),
        col("parsed_data")[6].alias("category"),
        col("parsed_data")[7].cast("double").alias("amount"),
        col("parsed_data")[8].alias("payment_method"),
        col("parsed_data")[9].alias("payment_status"),
        col("parsed_data")[10].alias("discount_code"),
        col("parsed_data")[11].alias("shipping_address"),
        col("parsed_data")[12].alias("timestamp_raw"),  # Garder en string d'abord
        when(col("parsed_data")[13].isin("True", "true", "TRUE", "1"), True)
        .when(col("parsed_data")[13].isin("False", "false", "FALSE", "0"), False)
        .otherwise(None).alias("is_returned"),
        col("kafka_timestamp"),
        col("kafka_partition"),
        col("kafka_offset")
    )
    
    # DÉBOGAGE: Vérifier les données extraites
    print("=== DONNÉES EXTRAITES AVANT CONVERSION TIMESTAMP ===")
    print(f"Nombre d'enregistrements: {transactions_df.count()}")
    transactions_df.select("order_id", "customer_id", "amount", "timestamp_raw", "payment_status").show(5, truncate=False)
    
    # Conversion des timestamps
    print("=== CONVERSION DES TIMESTAMPS ===")
    
    timestamp_df = transactions_df.withColumn(
        "timestamp",
        coalesce(
            to_timestamp(col("timestamp_raw"), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(col("timestamp_raw"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("timestamp_raw"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            to_timestamp(col("timestamp_raw"), "dd/MM/yyyy HH:mm:ss"),
            to_timestamp(col("timestamp_raw"), "MM/dd/yyyy HH:mm:ss"),
            to_timestamp(col("timestamp_raw")),  # Format par défaut
        )
    )
    
    # Vérifier la conversion des timestamps
    valid_timestamps = timestamp_df.filter(col("timestamp").isNotNull()).count()
    total_records = timestamp_df.count()
    
    print(f"Timestamps convertis avec succès: {valid_timestamps}/{total_records}")
    
    if valid_timestamps == 0:
        print("⚠️ AUCUN TIMESTAMP CONVERTI - UTILISATION DE LA DATE ACTUELLE")
        final_df = timestamp_df.withColumn("timestamp", current_timestamp())
    else:
        final_df = timestamp_df.filter(col("timestamp").isNotNull())
        print(f"✅ {valid_timestamps} timestamps convertis avec succès!")
    
    # Ajouter des colonnes de partitioning
    final_df = final_df.withColumn("year", year(col("timestamp"))) \
                       .withColumn("month", month(col("timestamp"))) \
                       .withColumn("day", dayofmonth(col("timestamp"))) \
                       .withColumn("processing_date", current_date())
    
    # Supprimer la colonne timestamp_raw temporaire
    final_df = final_df.drop("timestamp_raw")
    
    print(f"✅ Nombre final d'enregistrements à traiter: {final_df.count()}")
    
    # DÉBOGAGE: Afficher quelques enregistrements finaux
    if final_df.count() > 0:
        print("=== EXEMPLES D'ENREGISTREMENTS FINAUX ===")
        final_df.select("order_id", "customer_id", "amount", "timestamp", "payment_status", "category").show(10)
        
        # Statistiques rapides
        print("=== STATISTIQUES RAPIDES ===")
        print("Montants (min/max/avg):")
        final_df.select(
            min("amount").alias("min_amount"), 
            max("amount").alias("max_amount"),
            avg("amount").alias("avg_amount")
        ).show()
        
        print("Top 5 catégories:")
        final_df.groupBy("category").count().orderBy(desc("count")).show(5)
        
        print("Statuts de paiement:")
        final_df.groupBy("payment_status").count().show()
    
    return final_df

def process_json_topic(spark, topic_name, destination_bucket):
    """Traitement générique pour les topics JSON avec gestion des offsets"""
    print(f"=== TRAITEMENT DU TOPIC {topic_name.upper()} ===")
    
    # Lire les derniers offsets
    starting_offsets = read_last_offsets(spark, topic_name)
    
    # Lire depuis Kafka avec les offsets appropriés
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.17.34.127:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", starting_offsets) \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print(f"Nombre total de messages lus depuis Kafka ({topic_name}): {df.count()}")
    
    if df.count() == 0:
        print(f"Aucun nouveau message trouvé dans Kafka pour {topic_name}.")
        return None
    
    # Convertir la valeur en string et ajouter métadonnées
    json_df = df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        current_timestamp().alias("processing_timestamp")
    )
    
    # Filtrer les valeurs nulles ou vides
    json_df = json_df.filter(col("json_data").isNotNull() & (col("json_data") != ""))
    
    print(f"Nombre de messages valides après filtrage ({topic_name}): {json_df.count()}")
    
    if json_df.count() == 0:
        return None
    
    # Afficher quelques exemples
    print(f"=== EXEMPLES DE DONNÉES JSON ({topic_name}) ===")
    json_df.select("json_data").show(3, truncate=False)
    
    return json_df

def write_transactions_to_minio(df, output_path):
    """Écrire les données transactions vers MinIO en format Delta"""
    
    if df is None or df.count() == 0:
        print("Aucune donnée transactions à écrire dans MinIO")
        return
    
    print(f"Écriture de {df.count()} enregistrements transactions vers MinIO...")
    
    try:
        # Écriture en format Delta avec partitioning
        df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("mergeSchema", "true") \
            .save(output_path)
        
        print(f"Données transactions écrites avec succès dans: {output_path}")
        
        # Afficher quelques statistiques
        print("=== STATISTIQUES DES DONNÉES TRANSACTIONS ÉCRITES ===")
        df.groupBy("payment_status").count().show()
        df.groupBy("category").count().show()
        
    except Exception as e:
        print(f"Erreur lors de l'écriture Delta vers MinIO: {str(e)}")
        # Fallback vers Parquet si Delta échoue
        print("Tentative d'écriture en format Parquet...")
        try:
            df.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .parquet(output_path.replace("delta-data", "parquet-data"))
            print("Données écrites en format Parquet avec succès!")
        except Exception as e2:
            print(f"Erreur Parquet aussi: {str(e2)}")
            # Dernier recours: écriture simple sans partitioning
            print("Écriture simple sans partitioning...")
            df.write \
                .mode("append") \
                .parquet(output_path.replace("delta-data", "simple-parquet"))
            print("Données écrites en format Parquet simple!")

def write_json_to_minio(df, topic_name, bucket_path):
    """Écrire les données JSON vers MinIO avec gestion des conflits de fichiers"""
    
    if df is None or df.count() == 0:
        print(f"Aucune donnée {topic_name} à écrire dans MinIO")
        return
    
    print(f"Écriture de {df.count()} enregistrements {topic_name} vers MinIO...")
    
    try:
        # Ajouter colonnes de partitioning basées sur la date de traitement
        df_with_partitions = df.withColumn("year", year(col("processing_timestamp"))) \
                               .withColumn("month", month(col("processing_timestamp"))) \
                               .withColumn("day", dayofmonth(col("processing_timestamp"))) \
                               .withColumn("hour", hour(col("processing_timestamp"))) \
                               .withColumn("batch_id", lit(int(current_timestamp().cast("long").collect()[0][0] / 1000)))
        
        # Écriture en format JSON avec partitioning par date et heure pour éviter les conflits
        output_path = f"s3a://{bucket_path}"
        
        # Utiliser coalesce pour réduire le nombre de fichiers et éviter les conflits
        df_coalesced = df_with_partitions.coalesce(1)
        
        df_coalesced.write \
            .mode("append") \
            .partitionBy("year", "month", "day", "hour") \
            .option("compression", "gzip") \
            .option("maxRecordsPerFile", 10000) \
            .json(output_path)
        
        print(f"Données {topic_name} écrites avec succès dans: {output_path}")
        
        # Afficher quelques statistiques
        print(f"=== STATISTIQUES DES DONNÉES {topic_name.upper()} ÉCRITES ===")
        print(f"Nombre total d'enregistrements: {df.count()}")
        df.select("kafka_partition").distinct().show()
        
    except Exception as e:
        if "FileAlreadyExistsException" in str(e) or "destination file exists" in str(e):
            print(f"⚠️ Conflit de fichiers pour {topic_name}, utilisation d'un timestamp unique...")
            try:
                # Ajouter un timestamp unique pour éviter les conflits
                import time
                timestamp_suffix = int(time.time() * 1000)  # timestamp en millisecondes
                
                df_with_unique_id = df.withColumn("batch_timestamp", lit(timestamp_suffix))
                
                # Chemin avec timestamp unique
                unique_path = f"s3a://{bucket_path}/batch_{timestamp_suffix}"
                
                df_with_unique_id.coalesce(1).write \
                    .mode("overwrite") \
                    .option("compression", "gzip") \
                    .json(unique_path)
                
                print(f"✅ Données {topic_name} écrites avec timestamp unique: {unique_path}")
                
            except Exception as e3:
                print(f"❌ Erreur même avec timestamp unique: {str(e3)}")
                raise e3
        else:
            print(f"Erreur lors de l'écriture JSON {topic_name} vers MinIO: {str(e)}")
            # Fallback: écriture avec timestamp unique
            try:
                print(f"Tentative d'écriture avec timestamp unique pour {topic_name}...")
                import time
                timestamp_suffix = int(time.time() * 1000)
                unique_path = f"s3a://{bucket_path}/batch_{timestamp_suffix}"
                
                df.coalesce(1).write \
                    .mode("overwrite") \
                    .option("compression", "gzip") \
                    .json(unique_path)
                    
                print(f"✅ Données {topic_name} écrites avec timestamp unique!")
            except Exception as e2:
                print(f"❌ Erreur lors de l'écriture avec timestamp: {str(e2)}")
                raise e2

def validate_data(df, data_type="transactions"):
    """Valider les données avant écriture"""
    print(f"\n=== VALIDATION DES DONNÉES {data_type.upper()} ===")
    
    if df is None or df.count() == 0:
        print("AUCUNE DONNÉE À VALIDER!")
        return
    
    # Statistiques générales
    total_count = df.count()
    print(f"Nombre total d'enregistrements: {total_count}")
    
    if data_type == "transactions":
        # Vérifier les valeurs nulles dans les colonnes importantes
        important_cols = ["order_id", "customer_id", "amount", "timestamp", "payment_status"]
        for col_name in important_cols:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                print(f"Valeurs nulles dans {col_name}: {null_count}")
        
        # Vérifier la plage de dates
        if "timestamp" in df.columns:
            date_range = df.select(min("timestamp").alias("date_min"), max("timestamp").alias("date_max"))
            print("Plage de dates:")
            date_range.show()
        
        # Vérifier les montants
        if "amount" in df.columns:
            amount_stats = df.select(
                min("amount").alias("amount_min"),
                max("amount").alias("amount_max"),
                avg("amount").alias("amount_avg")
            )
            print("Statistiques des montants:")
            amount_stats.show()
            
        # Vérifier les doublons d'order_id
        if "order_id" in df.columns:
            unique_orders = df.select("order_id").distinct().count()
            print(f"Ordres uniques: {unique_orders}/{total_count}")
            if unique_orders < total_count:
                print(f"⚠️ {total_count - unique_orders} doublons d'order_id détectés!")
    else:
        # Validation pour les données JSON
        print(f"Validation des données JSON pour {data_type}")
        if "json_data" in df.columns:
            null_json_count = df.filter(col("json_data").isNull()).count()
            print(f"Messages JSON nulls: {null_json_count}")
        
        if "kafka_partition" in df.columns:
            print("Distribution par partition Kafka:")
            df.groupBy("kafka_partition").count().show()
    
    print(f"=== FIN VALIDATION {data_type.upper()} ===\n")

def main():
    """Fonction principale avec parser CSV corrigé"""
    print("=== DÉMARRAGE DU JOB KAFKA VERS MINIO (PARSER CSV CORRIGÉ) ===")
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # S'assurer que le bucket kafka-offsets existe
        ensure_offset_bucket_exists(spark)
        
        # Configuration des topics avec le nouveau processor
        topics_config = {
            "transactions": {
                "processor": process_transactions_topic_fixed,  # NOUVEAU PROCESSOR
                "writer": write_transactions_to_minio,
                "output_path": "s3a://transactions/raw/delta-data"
            },
            "social_media": {
                "processor": lambda spark: process_json_topic(spark, "social_media", "social-media/raw"),
                "writer": lambda df, path: write_json_to_minio(df, "social_media", "social-media/raw"),
                "output_path": "social-media/raw"
            },
            "iot_sensors": {
                "processor": lambda spark: process_json_topic(spark, "iot_sensors", "iot-sensors/raw"),
                "writer": lambda df, path: write_json_to_minio(df, "iot_sensors", "iot-sensors/raw"),
                "output_path": "iot-sensors/raw"
            },
            "customer_images": {
                "processor": lambda spark: process_json_topic(spark, "customer_images", "customer-images/raw"),
                "writer": lambda df, path: write_json_to_minio(df, "customer_images", "customer-images/raw"),
                "output_path": "customer-images/raw"
            }
        }
        
        # Traiter chaque topic
        for topic_name, config in topics_config.items():
            print(f"\n{'='*70}")
            print(f"TRAITEMENT DU TOPIC: {topic_name.upper()}")
            print(f"{'='*70}")
            
            try:
                # Traiter les données du topic
                df = config["processor"](spark)
                
                if df is not None and df.count() > 0:
                    # Valider les données
                    validate_data(df, topic_name)
                    
                    # Écrire vers MinIO
                    config["writer"](df, config["output_path"])
                    
                    # CRUCIAL: Sauvegarder les offsets APRÈS l'écriture réussie
                    save_last_offsets(spark, df, topic_name)
                    
                    print(f"✅ Topic {topic_name} traité avec succès! {df.count()} enregistrements traités.")
                else:
                    print(f"⚠️ Aucune nouvelle donnée à traiter pour {topic_name}")
                    
            except Exception as e:
                print(f"❌ Erreur lors du traitement de {topic_name}: {str(e)}")
                print(f"⚠️ Les offsets pour {topic_name} ne seront pas mis à jour")
                import traceback
                traceback.print_exc()
                # Continuer avec les autres topics
                continue
        
        print(f"\n{'='*70}")
        print("=== TRAITEMENT TERMINÉ ===")
        print(f"{'='*70}")
        
    except Exception as e:
        print(f"ERREUR GLOBALE lors du traitement: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        print("Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
