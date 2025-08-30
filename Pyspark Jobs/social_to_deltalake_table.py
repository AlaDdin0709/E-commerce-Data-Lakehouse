from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import json
from datetime import datetime

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("SocialMediaJSONToDeltaLake") \
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
    """Obtenir le chemin du fichier qui stocke la liste des fichiers JSON déjà traités"""
    return "s3a://social-media/metadata/processed_json_files.json"

def ensure_metadata_bucket_exists(spark):
    """Créer le dossier metadata s'il n'existe pas"""
    try:
        test_df = spark.createDataFrame([("test",)], ["value"])
        test_path = "s3a://social-media/metadata/_test_metadata_exists"
        test_df.write.mode("overwrite").json(test_path)
        test_df.limit(0).write.mode("overwrite").json(test_path)
        print("✅ Dossier metadata social-media existe et est accessible")
    except Exception as e:
        print(f"📁 Création du dossier metadata social-media: {str(e)}")

def read_processed_files_list(spark):
    """Lire la liste des fichiers JSON déjà traités"""
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
            print(f"📄 {len(processed_files)} fichiers JSON déjà traités trouvés")
            return set(processed_files)
        else:
            print("📄 Aucun fichier JSON traité précédemment (première exécution)")
            return set()
            
    except Exception as e:
        if "Path does not exist" in str(e) or "NoSuchKey" in str(e):
            print("📄 Pas de fichier de métadonnées JSON (première exécution)")
        else:
            print(f"⚠️ Erreur lors de la lecture des métadonnées JSON: {str(e)}")
        return set()

def save_processed_files_list(spark, new_files, record_counts):
    """Sauvegarder la liste mise à jour des fichiers JSON traités"""
    if not new_files:
        print("ℹ️ Aucun nouveau fichier JSON à ajouter aux métadonnées")
        return
    
    try:
        print(f"💾 Sauvegarde de {len(new_files)} nouveaux fichiers JSON dans les métadonnées...")
        
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
        
        print(f"✅ Liste des fichiers JSON traités mise à jour: {len(all_files)} fichiers au total")
        
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des métadonnées JSON: {str(e)}")
        import traceback
        traceback.print_exc()

def discover_json_files(spark):
    """Découvrir tous les fichiers JSON dans le dossier source social media"""
    json_source_path = "s3a://social-media/raw"
    
    try:
        print(f"🔍 Recherche des fichiers JSON dans {json_source_path}...")
        
        json_files = []
        
        try:
            # Essayer de lire avec la structure partitionnée year=*/month=*/day=*/hour=*
            partition_pattern = f"{json_source_path}/year=*/month=*/day=*/hour=*"
            print(f"📁 Pattern de recherche partitionné: {partition_pattern}")
            
            temp_df = spark.read.text(partition_pattern)
            input_files = temp_df.inputFiles()
            json_files = [f for f in input_files if (f.endswith('.json') or f.endswith('.json.gz')) and not f.endswith('_SUCCESS')]
            
            print(f"✅ Méthode partitionnée réussie: {len(json_files)} fichiers JSON trouvés")
            
        except Exception as partition_error:
            print(f"⚠️ Erreur avec pattern partitionné: {str(partition_error)}")
            print("🔄 Tentative avec méthode récursive...")
            
            try:
                # Fallback: lecture récursive
                temp_df = spark.read.option("recursiveFileLookup", "true").text(json_source_path)
                input_files = temp_df.inputFiles()
                json_files = [f for f in input_files if (f.endswith('.json') or f.endswith('.json.gz')) and not f.endswith('_SUCCESS')]
                
                print(f"✅ Méthode récursive réussie: {len(json_files)} fichiers JSON trouvés")
                
            except Exception as recursive_error:
                print(f"⚠️ Erreur avec méthode récursive: {str(recursive_error)}")
                print("🔄 Tentative avec lecture directe...")
                
                try:
                    # Dernière tentative: lecture directe
                    temp_df = spark.read.text(json_source_path)
                    input_files = temp_df.inputFiles()
                    json_files = [f for f in input_files if (f.endswith('.json') or f.endswith('.json.gz')) and not f.endswith('_SUCCESS')]
                    
                    print(f"✅ Méthode directe réussie: {len(json_files)} fichiers JSON trouvés")
                    
                except Exception as direct_error:
                    print(f"❌ Toutes les méthodes ont échoué: {str(direct_error)}")
                    return []
        
        # Afficher les fichiers trouvés par partition
        if json_files:
            print(f"📂 {len(json_files)} fichiers JSON découverts:")
            
            # Grouper par partition
            partition_groups = {}
            for file in json_files:
                if "/year=" in file and "/month=" in file and "/day=" in file:
                    parts = file.split("/")
                    year_part = [p for p in parts if p.startswith("year=")]
                    month_part = [p for p in parts if p.startswith("month=")]
                    day_part = [p for p in parts if p.startswith("day=")]
                    hour_part = [p for p in parts if p.startswith("hour=")]
                    
                    if year_part and month_part and day_part:
                        if hour_part:
                            partition_key = f"{year_part[0]}/{month_part[0]}/{day_part[0]}/{hour_part[0]}"
                        else:
                            partition_key = f"{year_part[0]}/{month_part[0]}/{day_part[0]}"
                        
                        if partition_key not in partition_groups:
                            partition_groups[partition_key] = []
                        partition_groups[partition_key].append(file)
                else:
                    if "other" not in partition_groups:
                        partition_groups["other"] = []
                    partition_groups["other"].append(file)
            
            # Afficher par partition avec indication de compression
            for partition, files in sorted(partition_groups.items()):
                print(f"  📁 {partition}: {len(files)} fichier(s)")
                for file in files[:2]:
                    file_type = "JSON compressé" if file.endswith('.json.gz') else "JSON"
                    print(f"    - {file.split('/')[-1]} ({file_type})")
                if len(files) > 2:
                    print(f"    ... et {len(files) - 2} autres fichiers")
        else:
            print("⚠️ Aucun fichier JSON trouvé")
        
        return json_files
        
    except Exception as e:
        print(f"❌ Erreur lors de la découverte des fichiers JSON: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def parse_social_media_json(spark, json_files, processed_files):
    """Parser les fichiers JSON des réseaux sociaux non traités"""
    # CORRECTION 1: Filtrer correctement les fichiers déjà traités
    new_files = [f for f in json_files if f not in processed_files]
    
    if not new_files:
        print("✅ Tous les fichiers JSON ont déjà été traités")
        return None, set(), {}
    
    print(f"📊 {len(new_files)} nouveaux fichiers JSON à traiter (ignorant {len(processed_files)} fichiers déjà traités):")
    for file in new_files[:3]:
        file_type = "JSON compressé" if file.endswith('.json.gz') else "JSON"
        print(f"  - {file.split('/')[-1]} ({file_type})")
    if len(new_files) > 3:
        print(f"  ... et {len(new_files) - 3} autres fichiers")
    
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
                    print(f"    ✅ {kafka_count} enregistrements Kafka parsés")
                    
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
            print(f"✅ Total des enregistrements collectés: {total_lines}")
            
            # Parser le JSON imbriqué des données social media (AVEC TEXTE ARABE)
            print("🔍 Parsing du JSON social media imbriqué...")
            
            # Schéma pour les données social media imbriquées
            social_media_schema = StructType([
                StructField("post_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("text", StringType(), True),  # IMPORTANT: texte arabe
                StructField("platform", StringType(), True),
                StructField("likes", LongType(), True),
                StructField("shares", LongType(), True),
                StructField("timestamp", StringType(), True),
                StructField("sentiment", StringType(), True)
            ])
            
            # Parser le JSON imbriqué
            final_df = combined_df.withColumn(
                "social_media_parsed",
                from_json(col("json_data"), social_media_schema)
            ).select(
                col("source_file"),
                col("kafka_offset"),
                col("kafka_partition"),
                col("kafka_timestamp"),
                col("processing_timestamp"),
                col("json_data").alias("raw_json_data"),
                # Extraire les champs social media
                col("social_media_parsed.post_id").alias("post_id"),
                col("social_media_parsed.user_id").alias("user_id"),
                col("social_media_parsed.text").alias("content"),  # TEXTE ARABE
                col("social_media_parsed.platform").alias("platform"),
                col("social_media_parsed.likes").alias("likes"),
                col("social_media_parsed.shares").alias("shares"),
                col("social_media_parsed.timestamp").alias("post_timestamp"),
                col("social_media_parsed.sentiment").alias("sentiment")
            )
            
            # Convertir les timestamps
            final_df = final_df \
                .withColumn("kafka_timestamp", to_timestamp(col("kafka_timestamp"))) \
                .withColumn("processing_timestamp", to_timestamp(col("processing_timestamp"))) \
                .withColumn("post_timestamp", to_timestamp(col("post_timestamp")))
            
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
            
            # Filtre pour conserver plus de données
            final_df = final_df.filter(
                (col("post_id").isNotNull()) | 
                (col("user_id").isNotNull()) |
                (col("content").isNotNull())  # Garder les enregistrements avec du contenu
            )
            
            # S'assurer que les partitions ne sont pas NULL
            final_df = final_df.filter(
                col("partition_year").isNotNull() & 
                col("partition_month").isNotNull() & 
                col("partition_day").isNotNull()
            )
            
            final_count = final_df.count()
            print(f"✅ Enregistrements finaux après traitement: {final_count}")
            
            # Afficher un échantillon avec le texte arabe
            print(f"\n📝 Échantillon avec texte arabe conservé:")
            final_df.select("post_id", "user_id", "platform", "content", "likes") \
                   .filter(col("content").isNotNull()) \
                   .show(3, truncate=False)
            
            return final_df, set(new_files), record_counts
        else:
            print("⚠️ Aucun enregistrement valide trouvé dans les nouveaux fichiers JSON")
            return None, set(new_files), record_counts
            
    except Exception as e:
        print(f"❌ Erreur lors du parsing des fichiers JSON: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, set(), {}

def create_or_update_social_media_delta_table(spark, df, delta_path):
    """Créer ou mettre à jour la table Delta Lake pour social media - CORRECTION APPEND"""
    if df is None:
        print("⚠️ Aucune donnée social media à écrire dans Delta Lake")
        return
    
    print(f"🔄 Traitement de {df.count()} nouveaux enregistrements social media pour Delta Lake...")
    
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
        
        # CORRECTION 2: Vérifier si la table Delta existe et utiliser APPEND si elle existe
        if DeltaTable.isDeltaTable(spark, delta_path):
            print("📊 Table Delta social media existante trouvée - Mode APPEND")
            
            # CORRECTION: Utiliser APPEND au lieu d'OVERWRITE
            df.write \
              .format("delta") \
              .mode("append") \
              .partitionBy("partition_year", "partition_month", "partition_day") \
              .option("mergeSchema", "true") \
              .option("delta.autoOptimize.optimizeWrite", "true") \
              .option("delta.autoOptimize.autoCompact", "true") \
              .save(delta_path)
            
            print(f"✅ Nouveaux enregistrements ajoutés à la table social media existante")
            
        else:
            print("🆕 Création d'une nouvelle table Delta Lake social media")
            
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .partitionBy("partition_year", "partition_month", "partition_day") \
              .option("delta.autoOptimize.optimizeWrite", "true") \
              .option("delta.autoOptimize.autoCompact", "true") \
              .save(delta_path)
            
            print(f"✅ Table Delta social media créée avec {df.count()} enregistrements")
        
        # Afficher des statistiques
        print("\n=== STATISTIQUES DE LA TABLE DELTA SOCIAL MEDIA ===")
        try:
            delta_table = DeltaTable.forPath(spark, delta_path)
            final_df = delta_table.toDF()
            
            total_count = final_df.count()
            print(f"📊 Total des enregistrements dans la table: {total_count}")
            
            # Structure des partitions
            print("\n📁 Structure des partitions:")
            partition_info = final_df.groupBy("partition_year", "partition_month", "partition_day").count() \
                                    .orderBy("partition_year", "partition_month", "partition_day")
            partition_info.show()
            
            # Statistiques par plateforme si disponible
            if "platform" in final_df.columns:
                print("\n📊 Répartition par plateforme:")
                final_df.groupBy("platform").count().orderBy(desc("count")).show()
            
            # Vérifier que le texte arabe est bien conservé
            print("\n📱 Vérification du texte arabe:")
            arabic_content = final_df.select("post_id", "user_id", "platform", "content") \
                                   .filter(col("content").isNotNull()) \
                                   .limit(3)
            arabic_content.show(truncate=False)
            
            # Statistiques temporelles
            if "kafka_timestamp" in final_df.columns:
                print("\n📊 Répartition temporelle (derniers messages):")
                final_df.select("kafka_timestamp", "platform", "user_id") \
                        .orderBy(desc("kafka_timestamp")) \
                        .show(10)
            
        except Exception as stats_error:
            print(f"⚠️ Impossible d'afficher les statistiques: {str(stats_error)}")
        
        # Optimiser la table seulement si nécessaire
        print("🔧 Optimisation de la table Delta social media...")
        try:
            spark.sql(f"OPTIMIZE delta.`{delta_path}`")
            print("✅ Table Delta social media optimisée")
        except Exception as opt_error:
            print(f"⚠️ Optimisation échouée: {str(opt_error)}")
        
        print(f"\n📋 Table Delta social media disponible à: {delta_path}")
        
    except Exception as e:
        print(f"❌ Erreur lors de la création/mise à jour de la table Delta social media: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e

def main():
    """Fonction principale pour convertir les JSON social media en Delta Lake"""
    print("=== CONVERSION JSON SOCIAL MEDIA VERS DELTA LAKE - VERSION CORRIGÉE APPEND ===")
    
    spark = create_spark_session()
    
    try:
        # S'assurer que le dossier metadata existe
        ensure_metadata_bucket_exists(spark)
        
        # Découvrir tous les fichiers JSON
        json_files = discover_json_files(spark)
        
        if not json_files:
            print("⚠️ Aucun fichier JSON social media trouvé à traiter")
            return
        
        # CORRECTION 3: Lire correctement les fichiers déjà traités
        processed_files = read_processed_files_list(spark)
        print(f"📋 Fichiers déjà traités: {len(processed_files)}")
        
        # Parser SEULEMENT les nouveaux fichiers JSON
        new_data_df, new_files_processed, record_counts = parse_social_media_json(spark, json_files, processed_files)
        
        if new_data_df is not None and new_data_df.count() > 0:
            # Chemin de la table Delta Lake social media
            delta_table_path = "s3a://social-media/bronze"
            
            # Créer ou mettre à jour la table Delta avec APPEND
            create_or_update_social_media_delta_table(spark, new_data_df, delta_table_path)
            
            # CORRECTION 4: Sauvegarder les métadonnées SEULEMENT après succès
            if new_files_processed:
                print("💾 Sauvegarde des nouveaux fichiers traités dans les métadonnées...")
                save_processed_files_list(spark, new_files_processed, record_counts)
            
            print(f"✅ Conversion social media terminée avec succès! {new_data_df.count()} nouveaux enregistrements ajoutés à Delta Lake.")
            print(f"📍 Table Delta Lake social media disponible à: {delta_table_path}")
            
        else:
            print("ℹ️ Aucune nouvelle donnée social media à traiter - tous les fichiers sont déjà traités")
            
    except Exception as e:
        print(f"❌ Erreur lors de la conversion social media: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
