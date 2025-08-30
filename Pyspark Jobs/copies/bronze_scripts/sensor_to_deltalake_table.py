from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import json
from datetime import datetime

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("IoTSensorsJSONToDeltaLake") \
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
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_processed_files_path():
    """Obtenir le chemin du fichier qui stocke la liste des fichiers JSON IoT déjà traités"""
    return "s3a://iot-sensors/metadata/processed_json_files.json"

def ensure_metadata_bucket_exists(spark):
    """Créer le dossier metadata s'il n'existe pas"""
    try:
        test_df = spark.createDataFrame([("test",)], ["value"])
        test_path = "s3a://iot-sensors/metadata/_test_metadata_exists"
        test_df.write.mode("overwrite").json(test_path)
        test_df.limit(0).write.mode("overwrite").json(test_path)
        print("✅ Dossier metadata iot-sensors existe et est accessible")
    except Exception as e:
        print(f"📁 Création du dossier metadata iot-sensors: {str(e)}")

def read_processed_files_list(spark):
    """Lire la liste des fichiers JSON IoT déjà traités"""
    processed_files_path = get_processed_files_path()
    
    try:
        schema = StructType([
            StructField("file_path", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("record_count", LongType(), True)
        ])
        
        processed_files_df = spark.read.schema(schema).json(processed_files_path)
        
        if processed_files_df.count() > 0:
            processed_files = [row.file_path for row in processed_files_df.collect()]
            print(f"📄 {len(processed_files)} fichiers JSON IoT déjà traités trouvés")
            return set(processed_files)
        else:
            print("📄 Aucun fichier JSON IoT traité précédemment (première exécution)")
            return set()
            
    except Exception as e:
        if "Path does not exist" in str(e) or "NoSuchKey" in str(e):
            print("📄 Pas de fichier de métadonnées JSON IoT (première exécution)")
        else:
            print(f"⚠️ Erreur lors de la lecture des métadonnées JSON IoT: {str(e)}")
        return set()

def save_processed_files_list(spark, new_files, record_counts):
    """Sauvegarder la liste mise à jour des fichiers JSON IoT traités"""
    if not new_files:
        print("ℹ️ Aucun nouveau fichier JSON IoT à ajouter aux métadonnées")
        return
    
    try:
        print(f"💾 Sauvegarde de {len(new_files)} nouveaux fichiers JSON IoT dans les métadonnées...")
        
        # Lire les fichiers déjà traités
        existing_files = read_processed_files_list(spark)
        
        # Combiner avec les nouveaux fichiers
        all_files = existing_files.union(new_files)
        
        # Créer le DataFrame avec les métadonnées
        files_data = []
        for file_path in all_files:
            record_count = record_counts.get(file_path, 0)
            files_data.append({
                "file_path": file_path,
                "processed_at": datetime.now(),
                "record_count": record_count
            })
        
        files_df = spark.createDataFrame(files_data)
        
        processed_files_path = get_processed_files_path()
        
        files_df.coalesce(1).write \
               .mode("overwrite") \
               .json(processed_files_path)
        
        print(f"✅ Liste des fichiers JSON IoT traités mise à jour: {len(all_files)} fichiers au total")
        
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des métadonnées JSON IoT: {str(e)}")
        import traceback
        traceback.print_exc()

def discover_json_files(spark):
    """Découvrir tous les fichiers JSON dans le dossier source IoT sensors"""
    json_source_path = "s3a://iot-sensors/raw"
    
    try:
        print(f"🔍 Recherche des fichiers JSON IoT dans {json_source_path}...")
        
        json_files = []
        
        try:
            # Lecture récursive pour découvrir tous les fichiers JSON/JSON.GZ
            temp_df = spark.read.option("recursiveFileLookup", "true").text(json_source_path)
            input_files = temp_df.inputFiles()
            json_files = [f for f in input_files if (f.endswith('.json') or f.endswith('.json.gz')) and not f.endswith('_SUCCESS')]
            
            print(f"✅ Découverte réussie: {len(json_files)} fichiers JSON IoT trouvés")
            
        except Exception as recursive_error:
            print(f"⚠️ Erreur avec méthode récursive: {str(recursive_error)}")
            print("🔄 Tentative avec lecture directe...")
            
            try:
                # Fallback: lecture directe
                temp_df = spark.read.text(json_source_path)
                input_files = temp_df.inputFiles()
                json_files = [f for f in input_files if (f.endswith('.json') or f.endswith('.json.gz')) and not f.endswith('_SUCCESS')]
                
                print(f"✅ Méthode directe réussie: {len(json_files)} fichiers JSON IoT trouvés")
                
            except Exception as direct_error:
                print(f"❌ Toutes les méthodes ont échoué: {str(direct_error)}")
                return []
        
        # Afficher les fichiers trouvés
        if json_files:
            print(f"📂 {len(json_files)} fichiers JSON IoT découverts:")
            for i, file in enumerate(json_files):
                file_type = "JSON compressé" if file.endswith('.json.gz') else "JSON"
                print(f"  {i+1}. {file.split('/')[-1]} ({file_type})")
        else:
            print("⚠️ Aucun fichier JSON IoT trouvé")
        
        return json_files
        
    except Exception as e:
        print(f"❌ Erreur lors de la découverte des fichiers JSON IoT: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def parse_iot_sensors_json(spark, json_files, processed_files):
    """Parser les fichiers JSON des capteurs IoT non traités"""
    # Filtrer les fichiers déjà traités
    new_files = [f for f in json_files if f not in processed_files]
    
    if not new_files:
        print("✅ Tous les fichiers JSON IoT ont déjà été traités")
        return None, set(), {}
    
    print(f"📊 {len(new_files)} nouveaux fichiers JSON IoT à traiter (ignorant {len(processed_files)} fichiers déjà traités):")
    for file in new_files:
        file_type = "JSON compressé" if file.endswith('.json.gz') else "JSON"
        print(f"  - {file.split('/')[-1]} ({file_type})")
    
    try:
        combined_df = None
        record_counts = {}
        total_lines = 0
        
        # Lire fichier par fichier LIGNE PAR LIGNE
        for file_path in new_files:
            try:
                file_type = "JSON compressé" if file_path.endswith('.json.gz') else "JSON"
                print(f"  📄 Lecture ligne par ligne: {file_path.split('/')[-1]} ({file_type})")
                
                # Lire comme texte ligne par ligne
                if file_path.endswith('.json.gz'):
                    lines_df = spark.read \
                        .option("compression", "gzip") \
                        .text(file_path)
                else:
                    lines_df = spark.read.text(file_path)
                
                file_lines = lines_df.count()
                print(f"    📊 {file_lines} lignes trouvées")
                
                if file_lines > 0:
                    # Ajouter le nom du fichier source
                    lines_df = lines_df.withColumn("source_file", lit(file_path))
                    
                    # Parser chaque ligne comme JSON Kafka
                    kafka_schema = StructType([
                        StructField("json_data", StringType(), True),
                        StructField("kafka_offset", LongType(), True),
                        StructField("kafka_partition", LongType(), True),
                        StructField("kafka_timestamp", StringType(), True),
                        StructField("processing_timestamp", StringType(), True)
                    ])
                    
                    # Parser les métadonnées Kafka
                    kafka_df = lines_df.withColumn(
                        "kafka_parsed",
                        from_json(col("value"), kafka_schema)
                    ).select(
                        col("source_file"),
                        col("kafka_parsed.*")
                    ).filter(col("json_data").isNotNull())
                    
                    kafka_count = kafka_df.count()
                    print(f"    ✅ {kafka_count} enregistrements Kafka IoT parsés")
                    
                    if kafka_count > 0:
                        if combined_df is None:
                            combined_df = kafka_df
                        else:
                            combined_df = combined_df.union(kafka_df)
                        
                        total_lines += kafka_count
                        record_counts[file_path] = kafka_count
                    else:
                        record_counts[file_path] = 0
                else:
                    print(f"    ⚠️ Fichier vide ignoré: {file_path}")
                    record_counts[file_path] = 0
                    
            except Exception as file_error:
                print(f"    ❌ Erreur lors de la lecture de {file_path}: {str(file_error)}")
                record_counts[file_path] = 0
                continue
        
        if combined_df is not None and total_lines > 0:
            print(f"✅ Total des enregistrements Kafka IoT collectés: {total_lines}")
            
            # Parser le JSON imbriqué des données IoT sensors
            print("🔍 Parsing du JSON IoT sensors imbriqué...")
            
            # Schéma pour les données IoT sensors imbriquées (basé sur le diagnostic)
            iot_sensors_schema = StructType([
                StructField("sensor_id", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("temperature", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
                StructField("pressure", DoubleType(), True),
                StructField("battery_level", DoubleType(), True),
                StructField("signal_strength", DoubleType(), True),
                StructField("location", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("status", StringType(), True),
                StructField("firmware_version", StringType(), True)
            ])
            
            # Parser le JSON imbriqué des capteurs IoT
            final_df = combined_df.withColumn(
                "iot_sensors_parsed",
                from_json(col("json_data"), iot_sensors_schema)
            ).select(
                col("source_file"),
                col("kafka_offset"),
                col("kafka_partition"),
                col("kafka_timestamp"),
                col("processing_timestamp"),
                col("json_data").alias("raw_json_data"),
                # Extraire les champs IoT sensors
                col("iot_sensors_parsed.sensor_id").alias("sensor_id"),
                col("iot_sensors_parsed.device_type").alias("device_type"),
                col("iot_sensors_parsed.temperature").alias("temperature"),
                col("iot_sensors_parsed.humidity").alias("humidity"),
                col("iot_sensors_parsed.pressure").alias("pressure"),
                col("iot_sensors_parsed.battery_level").alias("battery_level"),
                col("iot_sensors_parsed.signal_strength").alias("signal_strength"),
                col("iot_sensors_parsed.location").alias("location"),
                col("iot_sensors_parsed.timestamp").alias("sensor_timestamp"),
                col("iot_sensors_parsed.status").alias("status"),
                col("iot_sensors_parsed.firmware_version").alias("firmware_version")
            )
            
            # Convertir les timestamps
            final_df = final_df \
                .withColumn("kafka_timestamp", to_timestamp(col("kafka_timestamp"))) \
                .withColumn("processing_timestamp", to_timestamp(col("processing_timestamp"))) \
                .withColumn("sensor_timestamp", to_timestamp(col("sensor_timestamp")))
            
            # Ajouter les métadonnées de partitionnement pour Delta Lake
            final_df = final_df.withColumn("delta_load_timestamp", current_timestamp())
            
            # Gérer le partitionnement basé sur processing_timestamp ou kafka_timestamp
            if "processing_timestamp" in final_df.columns:
                final_df = final_df.withColumn("partition_year", year(col("processing_timestamp"))) \
                                 .withColumn("partition_month", month(col("processing_timestamp"))) \
                                 .withColumn("partition_day", dayofmonth(col("processing_timestamp")))
            elif "kafka_timestamp" in final_df.columns:
                final_df = final_df.withColumn("partition_year", year(col("kafka_timestamp"))) \
                                 .withColumn("partition_month", month(col("kafka_timestamp"))) \
                                 .withColumn("partition_day", dayofmonth(col("kafka_timestamp")))
            else:
                # Fallback à la date actuelle
                final_df = final_df.withColumn("partition_year", year(current_timestamp())) \
                                 .withColumn("partition_month", month(current_timestamp())) \
                                 .withColumn("partition_day", dayofmonth(current_timestamp()))
            
            # Filtre pour conserver les données IoT valides
            final_df = final_df.filter(
                (col("sensor_id").isNotNull()) | 
                (col("device_type").isNotNull()) |
                (col("temperature").isNotNull()) |
                (col("humidity").isNotNull())
            )
            
            # S'assurer que les partitions ne sont pas NULL
            final_df = final_df.filter(
                col("partition_year").isNotNull() & 
                col("partition_month").isNotNull() & 
                col("partition_day").isNotNull()
            )
            
            final_count = final_df.count()
            print(f"✅ Enregistrements IoT finaux après traitement: {final_count}")
            
            # Afficher un échantillon des données IoT
            print(f"\n📊 Échantillon des données IoT sensors:")
            final_df.select("sensor_id", "device_type", "temperature", "humidity", "battery_level", "location") \
                   .filter(col("sensor_id").isNotNull()) \
                   .show(5, truncate=False)
            
            return final_df, set(new_files), record_counts
        else:
            print("⚠️ Aucun enregistrement IoT valide trouvé dans les nouveaux fichiers JSON")
            return None, set(new_files), record_counts
            
    except Exception as e:
        print(f"❌ Erreur lors du parsing des fichiers JSON IoT: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, set(), {}

def create_or_update_iot_sensors_delta_table(spark, df, delta_path):
    """Créer ou mettre à jour la table Delta Lake pour IoT sensors"""
    if df is None:
        print("⚠️ Aucune donnée IoT sensors à écrire dans Delta Lake")
        return
    
    print(f"🔄 Traitement de {df.count()} nouveaux enregistrements IoT sensors pour Delta Lake...")
    
    try:
        # Validation des colonnes de partitionnement
        print("🔍 Validation des colonnes de partitionnement...")
        
        partition_columns = ["partition_year", "partition_month", "partition_day"]
        for col_name in partition_columns:
            if col_name not in df.columns:
                print(f"❌ Colonne de partitionnement manquante: {col_name}")
                return
            
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"❌ {null_count} valeurs NULL trouvées dans {col_name}")
                return
        
        # Vérifier si la table Delta existe et utiliser APPEND si elle existe
        if DeltaTable.isDeltaTable(spark, delta_path):
            print("📊 Table Delta IoT sensors existante trouvée - Mode APPEND")
            
            # Utiliser APPEND pour ajouter de nouvelles données
            df.write \
              .format("delta") \
              .mode("append") \
              .partitionBy("partition_year", "partition_month", "partition_day") \
              .option("mergeSchema", "true") \
              .option("delta.autoOptimize.optimizeWrite", "true") \
              .option("delta.autoOptimize.autoCompact", "true") \
              .save(delta_path)
            
            print(f"✅ Nouveaux enregistrements IoT ajoutés à la table existante")
            
        else:
            print("🆕 Création d'une nouvelle table Delta Lake IoT sensors")
            
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .partitionBy("partition_year", "partition_month", "partition_day") \
              .option("delta.autoOptimize.optimizeWrite", "true") \
              .option("delta.autoOptimize.autoCompact", "true") \
              .save(delta_path)
            
            print(f"✅ Table Delta IoT sensors créée avec {df.count()} enregistrements")
        
        # Afficher des statistiques IoT
        print("\n=== STATISTIQUES DE LA TABLE DELTA IOT SENSORS ===")
        try:
            delta_table = DeltaTable.forPath(spark, delta_path)
            final_df = delta_table.toDF()
            
            total_count = final_df.count()
            print(f"📊 Total des enregistrements IoT dans la table: {total_count}")
            
            # Structure des partitions
            print("\n📁 Structure des partitions:")
            partition_info = final_df.groupBy("partition_year", "partition_month", "partition_day").count() \
                                    .orderBy("partition_year", "partition_month", "partition_day")
            partition_info.show()
            
            # Statistiques par type de device IoT
            if "device_type" in final_df.columns:
                print("\n🔧 Répartition par type de capteur IoT:")
                final_df.groupBy("device_type").count().orderBy(desc("count")).show()
            
            # Statistiques des capteurs
            if "sensor_id" in final_df.columns:
                print("\n📡 Nombre de capteurs uniques:")
                sensor_count = final_df.select("sensor_id").distinct().count()
                print(f"    Capteurs uniques: {sensor_count}")
            
            # Analyse des métriques IoT (température, humidité, etc.)
            print("\n🌡️ Statistiques des métriques IoT:")
            numeric_columns = ["temperature", "humidity", "pressure", "battery_level", "signal_strength"]
            available_metrics = [col_name for col_name in numeric_columns if col_name in final_df.columns]
            
            if available_metrics:
                stats_df = final_df.select(*available_metrics).describe()
                stats_df.show()
            
            # Vérifier les données récentes
            print("\n📊 Échantillon des données IoT récentes:")
            recent_data = final_df.select("sensor_id", "device_type", "temperature", "humidity", "battery_level", "kafka_timestamp") \
                                 .filter(col("sensor_id").isNotNull()) \
                                 .orderBy(desc("kafka_timestamp")) \
                                 .limit(5)
            recent_data.show(truncate=False)
            
        except Exception as stats_error:
            print(f"⚠️ Impossible d'afficher les statistiques IoT: {str(stats_error)}")
        
        # Optimiser la table
        print("🔧 Optimisation de la table Delta IoT sensors...")
        try:
            spark.sql(f"OPTIMIZE delta.`{delta_path}`")
            print("✅ Table Delta IoT sensors optimisée")
        except Exception as opt_error:
            print(f"⚠️ Optimisation échouée: {str(opt_error)}")
        
        print(f"\n📋 Table Delta IoT sensors disponible à: {delta_path}")
        
    except Exception as e:
        print(f"❌ Erreur lors de la création/mise à jour de la table Delta IoT sensors: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e

def main():
    """Fonction principale pour convertir les JSON IoT sensors en Delta Lake"""
    print("=== CONVERSION JSON IOT SENSORS VERS DELTA LAKE ===")
    
    spark = create_spark_session()
    
    try:
        # S'assurer que le dossier metadata existe
        ensure_metadata_bucket_exists(spark)
        
        # Découvrir tous les fichiers JSON IoT
        json_files = discover_json_files(spark)
        
        if not json_files:
            print("⚠️ Aucun fichier JSON IoT sensors trouvé à traiter")
            return
        
        # Lire les fichiers déjà traités
        processed_files = read_processed_files_list(spark)
        print(f"📋 Fichiers IoT déjà traités: {len(processed_files)}")
        
        # Parser SEULEMENT les nouveaux fichiers JSON IoT
        new_data_df, new_files_processed, record_counts = parse_iot_sensors_json(spark, json_files, processed_files)
        
        if new_data_df is not None and new_data_df.count() > 0:
            # Chemin de la table Delta Lake IoT sensors
            delta_table_path = "s3a://iot-sensors/bronze"
            
            # Créer ou mettre à jour la table Delta avec APPEND
            create_or_update_iot_sensors_delta_table(spark, new_data_df, delta_table_path)
            
            # Sauvegarder les métadonnées SEULEMENT après succès
            if new_files_processed:
                print("💾 Sauvegarde des nouveaux fichiers IoT traités dans les métadonnées...")
                save_processed_files_list(spark, new_files_processed, record_counts)
            
            print(f"✅ Conversion IoT sensors terminée avec succès! {new_data_df.count()} nouveaux enregistrements ajoutés à Delta Lake.")
            print(f"📍 Table Delta Lake IoT sensors disponible à: {delta_table_path}")
            
        else:
            print("ℹ️ Aucune nouvelle donnée IoT sensors à traiter - tous les fichiers sont déjà traités")
            
    except Exception as e:
        print(f"❌ Erreur lors de la conversion IoT sensors: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
