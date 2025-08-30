from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import csv
import io

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires"""
    spark = SparkSession.builder \
        .appName("KafkaToMinIOBatchConsumer") \
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
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def parse_csv_line(csv_line):
    """Parser personnalisé pour gérer les JSON imbriqués dans le CSV"""
    if csv_line is None:
        return None
    
    try:
        reader = csv.reader(io.StringIO(csv_line))
        row = next(reader)
        return row
    except Exception as e:
        print(f"Erreur lors du parsing de la ligne: {csv_line}, erreur: {str(e)}")
        return None

def process_kafka_batch(spark):
    """Lire et traiter tous les messages disponibles dans Kafka"""
    
    print("Lecture de tous les messages disponibles depuis Kafka...")
    
    # Lire depuis Kafka en mode batch - tous les messages disponibles
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.17.34.127:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print(f"Nombre total de messages lus depuis Kafka: {df.count()}")
    
    if df.count() == 0:
        print("Aucun message trouvé dans Kafka. Arrêt du traitement.")
        return None
    
    # Convertir la valeur Kafka en string
    kafka_df = df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("csv_data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    )
    
    # Filtrer les valeurs nulles ou vides
    kafka_df = kafka_df.filter(col("csv_data").isNotNull() & (col("csv_data") != ""))
    
    print(f"Nombre de messages valides après filtrage: {kafka_df.count()}")
    
    # DÉBOGAGE: Afficher quelques exemples de données brutes
    print("=== EXEMPLES DE DONNÉES BRUTES ===")
    kafka_df.select("csv_data").show(5, truncate=False)
    
    # UDF pour parser les lignes CSV
    parse_csv_udf = udf(parse_csv_line, ArrayType(StringType()))
    
    # Parser les données CSV
    parsed_df = kafka_df.withColumn("parsed_data", parse_csv_udf(col("csv_data")))
    
    # Filtrer les lignes où le parsing a échoué
    parsed_df = parsed_df.filter(col("parsed_data").isNotNull())
    
    # Vérifier que nous avons assez de colonnes (14 colonnes attendues)
    parsed_df = parsed_df.filter(size(col("parsed_data")) >= 14)
    
    print(f"Nombre de messages après parsing CSV: {parsed_df.count()}")
    
    # DÉBOGAGE: Afficher la structure des données parsées
    print("=== EXEMPLES DE DONNÉES PARSÉES ===")
    parsed_df.select("parsed_data").show(5, truncate=False)
    
    # DÉBOGAGE: Afficher les timestamps bruts (colonne 12)
    print("=== TIMESTAMPS BRUTS (Colonne 12) ===")
    parsed_df.select(col("parsed_data")[12].alias("raw_timestamp")).show(10, truncate=False)
    
    # Extraire les colonnes individuelles SANS conversion de timestamp d'abord
    transactions_df = parsed_df.select(
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
        col("partition"),
        col("offset")
    )
    
    # DÉBOGAGE: Vérifier les données avant conversion timestamp
    print("=== DONNÉES AVANT CONVERSION TIMESTAMP ===")
    print(f"Nombre d'enregistrements avant conversion: {transactions_df.count()}")
    transactions_df.select("timestamp_raw", "amount", "payment_status").show(10, truncate=False)
    
    # Essayer différents formats de timestamp
    print("=== TENTATIVE DE CONVERSION TIMESTAMP ===")
    
    # Essayer plusieurs formats courants
    timestamp_df = transactions_df.withColumn(
        "timestamp",
        coalesce(
            to_timestamp(col("timestamp_raw"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("timestamp_raw"), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(col("timestamp_raw"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            to_timestamp(col("timestamp_raw"), "dd/MM/yyyy HH:mm:ss"),
            to_timestamp(col("timestamp_raw"), "MM/dd/yyyy HH:mm:ss"),
            to_timestamp(col("timestamp_raw")),  # Format par défaut
        )
    )
    
    # Vérifier combien de timestamps ont été convertis avec succès
    valid_timestamps = timestamp_df.filter(col("timestamp").isNotNull()).count()
    total_records = timestamp_df.count()
    
    print(f"Timestamps convertis avec succès: {valid_timestamps}/{total_records}")
    
    if valid_timestamps == 0:
        print("⚠️ AUCUN TIMESTAMP CONVERTI - GARDER LES DONNÉES SANS FILTRAGE")
        # Utiliser une date par défaut ou la date actuelle
        final_df = timestamp_df.withColumn("timestamp", current_timestamp())
    else:
        final_df = timestamp_df.filter(col("timestamp").isNotNull())
    
    # Ajouter des colonnes de partitioning
    final_df = final_df.withColumn("year", year(col("timestamp"))) \
                       .withColumn("month", month(col("timestamp"))) \
                       .withColumn("day", dayofmonth(col("timestamp"))) \
                       .withColumn("processing_date", current_date())
    
    # Supprimer la colonne timestamp_raw temporaire
    final_df = final_df.drop("timestamp_raw")
    
    print(f"Nombre final d'enregistrements à traiter: {final_df.count()}")
    
    # DÉBOGAGE: Afficher quelques enregistrements finaux
    if final_df.count() > 0:
        print("=== EXEMPLES D'ENREGISTREMENTS FINAUX ===")
        final_df.select("order_id", "amount", "timestamp", "payment_status").show(5)
    
    return final_df

def write_to_minio(df, output_path):
    """Écrire les données vers MinIO en format Delta"""
    
    if df is None or df.count() == 0:
        print("Aucune donnée à écrire dans MinIO")
        return
    
    print(f"Écriture de {df.count()} enregistrements vers MinIO...")
    
    try:
        # Écriture en format Delta avec partitioning
        df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("mergeSchema", "true") \
            .save(output_path)
        
        print(f"Données écrites avec succès dans: {output_path}")
        
        # Afficher quelques statistiques
        print("=== STATISTIQUES DES DONNÉES ÉCRITES ===")
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

def validate_data(df):
    """Valider les données avant écriture"""
    print("\n=== VALIDATION DES DONNÉES ===")
    
    if df is None or df.count() == 0:
        print("AUCUNE DONNÉE À VALIDER!")
        return
    
    # Statistiques générales
    total_count = df.count()
    print(f"Nombre total d'enregistrements: {total_count}")
    
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
    
    print("=== FIN VALIDATION ===\n")

def main():
    """Fonction principale"""
    print("=== DÉMARRAGE DU JOB KAFKA VERS MINIO (VERSION DEBUG) ===")
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # Traiter tous les messages Kafka disponibles
        transactions_df = process_kafka_batch(spark)
        
        if transactions_df is not None and transactions_df.count() > 0:
            # Valider les données
            validate_data(transactions_df)
            
            # Chemin de sortie dans MinIO
            output_path = "s3a://transactions/raw/delta-data"
            
            # Écrire vers MinIO
            write_to_minio(transactions_df, output_path)
            
            print("=== JOB TERMINÉ AVEC SUCCÈS ===")
        else:
            print("=== AUCUNE DONNÉE À TRAITER ===")
        
    except Exception as e:
        print(f"ERREUR lors du traitement: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        print("Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
