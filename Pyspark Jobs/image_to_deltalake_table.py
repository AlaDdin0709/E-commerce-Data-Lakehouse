from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import json
from datetime import datetime

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("CustomerImagesJSONToDeltaLake") \
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
    """Obtenir le chemin du fichier qui stocke la liste des fichiers JSON images déjà traités"""
    return "s3a://customer-images/metadata/processed_json_files.json"

def ensure_metadata_bucket_exists(spark):
    """Créer le dossier metadata s'il n'existe pas"""
    try:
        test_df = spark.createDataFrame([("test",)], ["value"])
        test_path = "s3a://customer-images/metadata/_test_metadata_exists"
        test_df.write.mode("overwrite").json(test_path)
        test_df.limit(0).write.mode("overwrite").json(test_path)
        print("✅ Dossier metadata customer-images existe et est accessible")
    except Exception as e:
        print(f"📁 Création du dossier metadata customer-images: {str(e)}")

def read_processed_files_list(spark):
    """Lire la liste des fichiers JSON images déjà traités"""
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
            print(f"📄 {len(processed_files)} fichiers JSON images déjà traités trouvés")
            return set(processed_files)
        else:
            print("📄 Aucun fichier JSON images traité précédemment (première exécution)")
            return set()
            
    except Exception as e:
        if "Path does not exist" in str(e) or "NoSuchKey" in str(e):
            print("📄 Pas de fichier de métadonnées JSON images (première exécution)")
        else:
            print(f"⚠️ Erreur lors de la lecture des métadonnées JSON images: {str(e)}")
        return set()

def save_processed_files_list(spark, new_files, record_counts):
    """Sauvegarder la liste mise à jour des fichiers JSON images traités"""
    if not new_files:
        print("ℹ️ Aucun nouveau fichier JSON images à ajouter aux métadonnées")
        return
    
    try:
        print(f"💾 Sauvegarde de {len(new_files)} nouveaux fichiers JSON images dans les métadonnées...")
        
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
        
        print(f"✅ Liste des fichiers JSON images traités mise à jour: {len(all_files)} fichiers au total")
        
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des métadonnées JSON images: {str(e)}")
        import traceback
        traceback.print_exc()

def discover_json_files(spark):
    """Découvrir tous les fichiers JSON dans le dossier source customer images"""
    json_source_path = "s3a://customer-images/raw"
    
    try:
        print(f"🔍 Recherche des fichiers JSON images dans {json_source_path}...")
        
        json_files = []
        
        try:
            # Lecture récursive pour découvrir tous les fichiers JSON/JSON.GZ
            temp_df = spark.read.option("recursiveFileLookup", "true").text(json_source_path)
            input_files = temp_df.inputFiles()
            json_files = [f for f in input_files if (f.endswith('.json') or f.endswith('.json.gz')) and not f.endswith('_SUCCESS')]
            
            print(f"✅ Découverte réussie: {len(json_files)} fichiers JSON images trouvés")
            
        except Exception as recursive_error:
            print(f"⚠️ Erreur avec méthode récursive: {str(recursive_error)}")
            print("🔄 Tentative avec lecture directe...")
            
            try:
                # Fallback: lecture directe
                temp_df = spark.read.text(json_source_path)
                input_files = temp_df.inputFiles()
                json_files = [f for f in input_files if (f.endswith('.json') or f.endswith('.json.gz')) and not f.endswith('_SUCCESS')]
                
                print(f"✅ Méthode directe réussie: {len(json_files)} fichiers JSON images trouvés")
                
            except Exception as direct_error:
                print(f"❌ Toutes les méthodes ont échoué: {str(direct_error)}")
                return []
        
        # Afficher les fichiers trouvés
        if json_files:
            print(f"📂 {len(json_files)} fichiers JSON images découverts:")
            for i, file in enumerate(json_files):
                file_type = "JSON compressé" if file.endswith('.json.gz') else "JSON"
                print(f"  {i+1}. {file.split('/')[-1]} ({file_type})")
        else:
            print("⚠️ Aucun fichier JSON images trouvé")
        
        return json_files
        
    except Exception as e:
        print(f"❌ Erreur lors de la découverte des fichiers JSON images: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def parse_customer_images_json(spark, json_files, processed_files):
    """Parser les fichiers JSON des images clients non traités"""
    # Filtrer les fichiers déjà traités
    new_files = [f for f in json_files if f not in processed_files]
    
    if not new_files:
        print("✅ Tous les fichiers JSON images ont déjà été traités")
        return None, set(), {}
    
    print(f"📊 {len(new_files)} nouveaux fichiers JSON images à traiter (ignorant {len(processed_files)} fichiers déjà traités):")
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
                    print(f"    ✅ {kafka_count} enregistrements Kafka images parsés")
                    
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
            print(f"✅ Total des enregistrements Kafka images collectés: {total_lines}")
            
            # Parser le JSON imbriqué des données customer images
            print("🔍 Parsing du JSON customer images imbriqué...")
            
            # Schéma pour les données customer images imbriquées (basé sur le diagnostic)
            customer_images_schema = StructType([
                StructField("image_id", StringType(), True),
                StructField("s3_path", StringType(), True),
                StructField("upload_timestamp", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("order_id", StringType(), True),
                StructField("image_type", StringType(), True),
                StructField("file_size", LongType(), True),
                StructField("image_format", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True),
                StructField("quality_score", DoubleType(), True),
                StructField("processing_status", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True)
            ])
            
            # Parser le JSON imbriqué des images clients
            final_df = combined_df.withColumn(
                "customer_images_parsed",
                from_json(col("json_data"), customer_images_schema)
            ).select(
                col("source_file"),
                col("kafka_offset"),
                col("kafka_partition"),
                col("kafka_timestamp"),
                col("processing_timestamp"),
                col("json_data").alias("raw_json_data"),
                # Extraire les champs customer images
                col("customer_images_parsed.image_id").alias("image_id"),
                col("customer_images_parsed.s3_path").alias("s3_path"),
                col("customer_images_parsed.upload_timestamp").alias("upload_timestamp"),
                col("customer_images_parsed.customer_id").alias("customer_id"),
                col("customer_images_parsed.order_id").alias("order_id"),
                col("customer_images_parsed.image_type").alias("image_type"),
                col("customer_images_parsed.file_size").alias("file_size"),
                col("customer_images_parsed.image_format").alias("image_format"),
                col("customer_images_parsed.width").alias("width"),
                col("customer_images_parsed.height").alias("height"),
                col("customer_images_parsed.quality_score").alias("quality_score"),
                col("customer_images_parsed.processing_status").alias("processing_status"),
                col("customer_images_parsed.metadata").alias("image_metadata")
            )
            
            # Convertir les timestamps
            final_df = final_df \
                .withColumn("kafka_timestamp", to_timestamp(col("kafka_timestamp"))) \
                .withColumn("processing_timestamp", to_timestamp(col("processing_timestamp"))) \
                .withColumn("upload_timestamp", to_timestamp(col("upload_timestamp")))
            
            # Extraire l'ordre de la colonne order_id ou du s3_path
            final_df = final_df.withColumn(
                "extracted_order_id",
                when(col("order_id").isNotNull(), col("order_id"))
                .otherwise(
                    regexp_extract(col("s3_path"), r"order_(\d+)_", 1)
                )
            )
            
            # Extraire les dimensions de l'image si disponibles
            final_df = final_df.withColumn(
                "image_size_mb",
                when(col("file_size").isNotNull(), round(col("file_size") / 1024 / 1024, 2))
                .otherwise(lit(None))
            )
            
            # Ajouter les métadonnées de partitionnement pour Delta Lake
            final_df = final_df.withColumn("delta_load_timestamp", current_timestamp())
            
            # Gérer le partitionnement basé sur upload_timestamp ou processing_timestamp
            if "upload_timestamp" in final_df.columns:
                final_df = final_df.withColumn("partition_year", year(col("upload_timestamp"))) \
                                 .withColumn("partition_month", month(col("upload_timestamp"))) \
                                 .withColumn("partition_day", dayofmonth(col("upload_timestamp")))
            elif "processing_timestamp" in final_df.columns:
                final_df = final_df.withColumn("partition_year", year(col("processing_timestamp"))) \
                                 .withColumn("partition_month", month(col("processing_timestamp"))) \
                                 .withColumn("partition_day", dayofmonth(col("processing_timestamp")))
            else:
                # Fallback à la date actuelle
                final_df = final_df.withColumn("partition_year", year(current_timestamp())) \
                                 .withColumn("partition_month", month(current_timestamp())) \
                                 .withColumn("partition_day", dayofmonth(current_timestamp()))
            
            # Filtre pour conserver les données d'images valides
            final_df = final_df.filter(
                (col("image_id").isNotNull()) | 
                (col("s3_path").isNotNull()) |
                (col("customer_id").isNotNull()) |
                (col("extracted_order_id").isNotNull())
            )
            
            # S'assurer que les partitions ne sont pas NULL
            final_df = final_df.filter(
                col("partition_year").isNotNull() & 
                col("partition_month").isNotNull() & 
                col("partition_day").isNotNull()
            )
            
            final_count = final_df.count()
            print(f"✅ Enregistrements images finaux après traitement: {final_count}")
            
            # Afficher un échantillon des données d'images
            print(f"\n🖼️ Échantillon des données customer images:")
            final_df.select("image_id", "s3_path", "customer_id", "extracted_order_id", "image_type", "file_size") \
                   .filter(col("image_id").isNotNull()) \
                   .show(5, truncate=False)
            
            return final_df, set(new_files), record_counts
        else:
            print("⚠️ Aucun enregistrement d'images valide trouvé dans les nouveaux fichiers JSON")
            return None, set(new_files), record_counts
            
    except Exception as e:
        print(f"❌ Erreur lors du parsing des fichiers JSON images: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, set(), {}

def create_or_update_customer_images_delta_table(spark, df, delta_path):
    """Créer ou mettre à jour la table Delta Lake pour customer images"""
    if df is None:
        print("⚠️ Aucune donnée customer images à écrire dans Delta Lake")
        return
    
    print(f"🔄 Traitement de {df.count()} nouveaux enregistrements customer images pour Delta Lake...")
    
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
            print("📊 Table Delta customer images existante trouvée - Mode APPEND")
            
            # Utiliser APPEND pour ajouter de nouvelles données
            df.write \
              .format("delta") \
              .mode("append") \
              .partitionBy("partition_year", "partition_month", "partition_day") \
              .option("mergeSchema", "true") \
              .option("delta.autoOptimize.optimizeWrite", "true") \
              .option("delta.autoOptimize.autoCompact", "true") \
              .save(delta_path)
            
            print(f"✅ Nouveaux enregistrements images ajoutés à la table existante")
            
        else:
            print("🆕 Création d'une nouvelle table Delta Lake customer images")
            
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .partitionBy("partition_year", "partition_month", "partition_day") \
              .option("delta.autoOptimize.optimizeWrite", "true") \
              .option("delta.autoOptimize.autoCompact", "true") \
              .save(delta_path)
            
            print(f"✅ Table Delta customer images créée avec {df.count()} enregistrements")
        
        # Afficher des statistiques des images
        print("\n=== STATISTIQUES DE LA TABLE DELTA CUSTOMER IMAGES ===")
        try:
            delta_table = DeltaTable.forPath(spark, delta_path)
            final_df = delta_table.toDF()
            
            total_count = final_df.count()
            print(f"📊 Total des enregistrements d'images dans la table: {total_count}")
            
            # Structure des partitions
            print("\n📁 Structure des partitions:")
            partition_info = final_df.groupBy("partition_year", "partition_month", "partition_day").count() \
                                    .orderBy("partition_year", "partition_month", "partition_day")
            partition_info.show()
            
            # Statistiques par type d'image
            if "image_type" in final_df.columns:
                print("\n🖼️ Répartition par type d'image:")
                final_df.groupBy("image_type").count().orderBy(desc("count")).show()
            
            # Statistiques par format d'image
            if "image_format" in final_df.columns:
                print("\n📸 Répartition par format d'image:")
                final_df.groupBy("image_format").count().orderBy(desc("count")).show()
            
            # Statistiques des clients et commandes
            if "customer_id" in final_df.columns:
                print("\n👥 Nombre de clients uniques:")
                customer_count = final_df.select("customer_id").filter(col("customer_id").isNotNull()).distinct().count()
                print(f"    Clients uniques: {customer_count}")
            
            if "extracted_order_id" in final_df.columns:
                print("\n📦 Nombre de commandes uniques:")
                order_count = final_df.select("extracted_order_id").filter(col("extracted_order_id").isNotNull()).distinct().count()
                print(f"    Commandes uniques: {order_count}")
            
            # Analyse des tailles de fichiers
            if "file_size" in final_df.columns:
                print("\n📏 Statistiques des tailles de fichiers:")
                size_stats = final_df.select("file_size", "image_size_mb").filter(col("file_size").isNotNull())
                if size_stats.count() > 0:
                    size_stats.describe().show()
            
            # Analyse des dimensions d'images
            if "width" in final_df.columns and "height" in final_df.columns:
                print("\n📐 Statistiques des dimensions d'images:")
                dimension_stats = final_df.select("width", "height").filter(col("width").isNotNull() & col("height").isNotNull())
                if dimension_stats.count() > 0:
                    dimension_stats.describe().show()
            
            # Vérifier les données récentes
            print("\n📊 Échantillon des images récentes:")
            recent_data = final_df.select("image_id", "s3_path", "customer_id", "extracted_order_id", "image_format", "upload_timestamp") \
                                 .filter(col("image_id").isNotNull()) \
                                 .orderBy(desc("upload_timestamp")) \
                                 .limit(5)
            recent_data.show(truncate=False)
            
        except Exception as stats_error:
            print(f"⚠️ Impossible d'afficher les statistiques images: {str(stats_error)}")
        
        # Optimiser la table
        print("🔧 Optimisation de la table Delta customer images...")
        try:
            spark.sql(f"OPTIMIZE delta.`{delta_path}`")
            print("✅ Table Delta customer images optimisée")
        except Exception as opt_error:
            print(f"⚠️ Optimisation échouée: {str(opt_error)}")
        
        print(f"\n📋 Table Delta customer images disponible à: {delta_path}")
        
    except Exception as e:
        print(f"❌ Erreur lors de la création/mise à jour de la table Delta customer images: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e

def main():
    """Fonction principale pour convertir les JSON customer images en Delta Lake"""
    print("=== CONVERSION JSON CUSTOMER IMAGES VERS DELTA LAKE ===")
    
    spark = create_spark_session()
    
    try:
        # S'assurer que le dossier metadata existe
        ensure_metadata_bucket_exists(spark)
        
        # Découvrir tous les fichiers JSON images
        json_files = discover_json_files(spark)
        
        if not json_files:
            print("⚠️ Aucun fichier JSON customer images trouvé à traiter")
            return
        
        # Lire les fichiers déjà traités
        processed_files = read_processed_files_list(spark)
        print(f"📋 Fichiers images déjà traités: {len(processed_files)}")
        
        # Parser SEULEMENT les nouveaux fichiers JSON images
        new_data_df, new_files_processed, record_counts = parse_customer_images_json(spark, json_files, processed_files)
        
        if new_data_df is not None and new_data_df.count() > 0:
            # Chemin de la table Delta Lake customer images
            delta_table_path = "s3a://customer-images/bronze"
            
            # Créer ou mettre à jour la table Delta avec APPEND
            create_or_update_customer_images_delta_table(spark, new_data_df, delta_table_path)
            
            # Sauvegarder les métadonnées SEULEMENT après succès
            if new_files_processed:
                print("💾 Sauvegarde des nouveaux fichiers images traités dans les métadonnées...")
                save_processed_files_list(spark, new_files_processed, record_counts)
            
            print(f"✅ Conversion customer images terminée avec succès! {new_data_df.count()} nouveaux enregistrements ajoutés à Delta Lake.")
            print(f"📍 Table Delta Lake customer images disponible à: {delta_table_path}")
            
        else:
            print("ℹ️ Aucune nouvelle donnée customer images à traiter - tous les fichiers sont déjà traités")
            
    except Exception as e:
        print(f"❌ Erreur lors de la conversion customer images: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
