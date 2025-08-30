from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import os
from datetime import datetime

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("CSVToDeltaLakeProcessor") \
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
    """Obtenir le chemin du fichier qui stocke la liste des fichiers déjà traités"""
    return "s3a://transactions/metadata/processed_files.json"

def ensure_metadata_bucket_exists(spark):
    """Créer le dossier metadata s'il n'existe pas"""
    try:
        test_df = spark.createDataFrame([("test",)], ["value"])
        test_path = "s3a://transactions/metadata/_test_metadata_exists"
        test_df.write.mode("overwrite").json(test_path)
        test_df.limit(0).write.mode("overwrite").json(test_path)
        print("✅ Dossier metadata existe et est accessible")
    except Exception as e:
        print(f"📁 Création du dossier metadata: {str(e)}")

def read_processed_files_list(spark):
    """Lire la liste des fichiers déjà traités"""
    processed_files_path = get_processed_files_path()
    
    try:
        schema = StructType([
            StructField("file_path", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("file_size", LongType(), True)
        ])
        
        processed_files_df = spark.read.schema(schema).json(processed_files_path)
        
        if processed_files_df.count() > 0:
            processed_files = [row.file_path for row in processed_files_df.collect()]
            print(f"📄 {len(processed_files)} fichiers déjà traités trouvés")
            print("Fichiers traités:")
            for f in processed_files:
                print(f"  - {f}")
            return set(processed_files)
        else:
            print("📄 Aucun fichier traité précédemment (première exécution)")
            return set()
            
    except Exception as e:
        if "Path does not exist" in str(e) or "NoSuchKey" in str(e):
            print("📄 Pas de fichier de métadonnées (première exécution)")
        else:
            print(f"⚠️ Erreur lors de la lecture des métadonnées: {str(e)}")
        return set()

def save_processed_files_list(spark, new_files):
    """Sauvegarder la liste mise à jour des fichiers traités"""
    if not new_files:
        print("ℹ️ Aucun nouveau fichier à ajouter aux métadonnées")
        return
    
    try:
        print(f"💾 Sauvegarde de {len(new_files)} nouveaux fichiers dans les métadonnées...")
        
        # Lire les fichiers déjà traités
        existing_files = read_processed_files_list(spark)
        print(f"📋 Fichiers existants: {len(existing_files)}")
        
        # Combiner avec les nouveaux fichiers
        all_files = existing_files.union(new_files)
        print(f"📋 Total des fichiers après ajout: {len(all_files)}")
        
        # Créer le DataFrame avec les métadonnées
        files_data = []
        for file_path in all_files:
            files_data.append({
                "file_path": file_path,
                "processed_at": datetime.now(),
                "file_size": 0  # On pourrait calculer la taille réelle si nécessaire
            })
        
        files_df = spark.createDataFrame(files_data)
        
        processed_files_path = get_processed_files_path()
        
        # CORRECTION: Utiliser un seul fichier de sortie et écraser complètement
        files_df.coalesce(1).write \
               .mode("overwrite") \
               .json(processed_files_path)
        
        print(f"✅ Liste des fichiers traités mise à jour: {len(all_files)} fichiers au total")
        
        # Vérification immédiate pour confirmer la sauvegarde
        verification_df = spark.read.json(processed_files_path)
        verification_count = verification_df.count()
        print(f"🔍 Vérification: {verification_count} fichiers sauvegardés dans les métadonnées")
        
        if verification_count != len(all_files):
            print(f"⚠️ ATTENTION: Discordance entre attendu ({len(all_files)}) et sauvegardé ({verification_count})")
        else:
            print("✅ Sauvegarde des métadonnées confirmée")
        
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des métadonnées: {str(e)}")
        import traceback
        traceback.print_exc()

def discover_csv_files(spark):
    """Découvrir tous les fichiers CSV dans le dossier source partitionné"""
    csv_source_path = "s3a://transactions/raw/csv-data"
    
    try:
        # Lister tous les fichiers CSV récursivement avec la structure partitionnée
        csv_files = []
        
        print(f"🔍 Recherche des fichiers CSV dans {csv_source_path} (structure partitionnée)...")
        
        try:
            # Lire avec la structure de partitionnement year=*/month=*/day=*
            # Utilise le pattern pour les partitions Hive
            partition_pattern = f"{csv_source_path}/year=*/month=*/day=*"
            print(f"📁 Pattern de recherche: {partition_pattern}")
            
            temp_df = spark.read.option("header", "true").csv(partition_pattern)
            input_files = temp_df.inputFiles()
            csv_files = [f for f in input_files if f.endswith('.csv') and not f.endswith('_SUCCESS')]
            
            print(f"✅ Méthode partitionnée réussie: {len(csv_files)} fichiers trouvés")
            
        except Exception as partition_error:
            print(f"⚠️ Erreur avec pattern partitionné: {str(partition_error)}")
            print("🔄 Tentative avec méthode récursive...")
            
            try:
                # Fallback: essayer de lire récursivement tous les sous-dossiers
                temp_df = spark.read.option("header", "true").option("recursiveFileLookup", "true").csv(csv_source_path)
                input_files = temp_df.inputFiles()
                csv_files = [f for f in input_files if f.endswith('.csv') and not f.endswith('_SUCCESS')]
                
                print(f"✅ Méthode récursive réussie: {len(csv_files)} fichiers trouvés")
                
            except Exception as recursive_error:
                print(f"⚠️ Erreur avec méthode récursive: {str(recursive_error)}")
                print("🔄 Tentative avec lecture directe...")
                
                try:
                    # Dernière tentative: lecture directe
                    temp_df = spark.read.option("header", "true").csv(csv_source_path)
                    input_files = temp_df.inputFiles()
                    csv_files = [f for f in input_files if f.endswith('.csv') and not f.endswith('_SUCCESS')]
                    
                    print(f"✅ Méthode directe réussie: {len(csv_files)} fichiers trouvés")
                    
                except Exception as direct_error:
                    print(f"❌ Toutes les méthodes ont échoué: {str(direct_error)}")
                    return []
        
        # Filtrer et afficher les fichiers trouvés
        if csv_files:
            print(f"📂 {len(csv_files)} fichiers CSV découverts:")
            
            # Grouper par partition pour un affichage plus clair
            partition_groups = {}
            for file in csv_files:
                # Extraire la partition du chemin
                if "/year=" in file and "/month=" in file and "/day=" in file:
                    parts = file.split("/")
                    year_part = [p for p in parts if p.startswith("year=")]
                    month_part = [p for p in parts if p.startswith("month=")]
                    day_part = [p for p in parts if p.startswith("day=")]
                    
                    if year_part and month_part and day_part:
                        partition_key = f"{year_part[0]}/{month_part[0]}/{day_part[0]}"
                        if partition_key not in partition_groups:
                            partition_groups[partition_key] = []
                        partition_groups[partition_key].append(file)
                else:
                    # Fichier sans partition claire
                    if "other" not in partition_groups:
                        partition_groups["other"] = []
                    partition_groups["other"].append(file)
            
            # Afficher par partition
            for partition, files in sorted(partition_groups.items()):
                print(f"  📁 {partition}: {len(files)} fichier(s)")
                for file in files[:2]:  # Afficher les 2 premiers de chaque partition
                    print(f"    - {file}")
                if len(files) > 2:
                    print(f"    ... et {len(files) - 2} autres fichiers")
        else:
            print("⚠️ Aucun fichier CSV trouvé")
        
        return csv_files
        
    except Exception as e:
        print(f"❌ Erreur lors de la découverte des fichiers CSV: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def read_new_csv_files(spark, csv_files, processed_files):
    """Lire uniquement les nouveaux fichiers CSV non traités"""
    new_files = [f for f in csv_files if f not in processed_files]
    
    if not new_files:
        print("✅ Tous les fichiers CSV ont déjà été traités")
        return None, set()
    
    print(f"📊 {len(new_files)} nouveaux fichiers CSV à traiter:")
    for file in new_files[:3]:
        print(f"  - {file}")
    if len(new_files) > 3:
        print(f"  ... et {len(new_files) - 3} autres fichiers")
    
    try:
        # Définir le schéma explicite pour les transactions
        transaction_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("customer_first_name", StringType(), True),
            StructField("customer_last_name", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("discount_code", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("timestamp_raw", StringType(), True),
            StructField("is_returned", StringType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("kafka_partition", LongType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("processing_date", DateType(), True),
            StructField("processing_timestamp", TimestampType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True)
        ])
        
        # Option 1: Essayer de lire tous les nouveaux fichiers en une seule fois
        try:
            print("🔄 Tentative de lecture groupée des nouveaux fichiers...")
            combined_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(transaction_schema) \
                .csv(new_files)
            
            # Ajouter une colonne pour identifier les fichiers sources
            combined_df = combined_df.withColumn("source_file", input_file_name())
            
            total_records = combined_df.count()
            print(f"✅ Lecture groupée réussie: {total_records} enregistrements")
            
        except Exception as batch_error:
            print(f"⚠️ Lecture groupée échouée: {str(batch_error)}")
            print("🔄 Passage à la lecture fichier par fichier...")
            
            # Option 2: Lire fichier par fichier (fallback)
            combined_df = None
            
            for file_path in new_files:
                try:
                    print(f"  📄 Lecture de: {file_path}")
                    
                    file_df = spark.read \
                        .option("header", "true") \
                        .option("inferSchema", "false") \
                        .schema(transaction_schema) \
                        .csv(file_path)
                    
                    file_count = file_df.count()
                    print(f"    ✅ {file_count} enregistrements")
                    
                    if file_count > 0:
                        # Ajouter le nom du fichier source
                        file_df = file_df.withColumn("source_file", lit(file_path))
                        
                        if combined_df is None:
                            combined_df = file_df
                        else:
                            combined_df = combined_df.union(file_df)
                    else:
                        print(f"    ⚠️ Fichier vide ignoré: {file_path}")
                        
                except Exception as file_error:
                    print(f"    ❌ Erreur lors de la lecture de {file_path}: {str(file_error)}")
                    continue
        
        if combined_df is not None:
            total_records = combined_df.count()
            print(f"✅ Total des nouveaux enregistrements: {total_records}")
            
            # Nettoyer et valider les données
            cleaned_df = combined_df.filter(
                col("order_id").isNotNull() & 
                (col("order_id") != "order_id") &
                col("customer_id").isNotNull() & 
                (col("customer_id") != "customer_id") &
                col("amount").isNotNull() &
                (col("amount") != "amount")
            )
            
            # CORRECTION CRITIQUE: Gérer les colonnes de partitionnement NULL
            print("🔧 Correction des colonnes de partitionnement...")
            
            # Vérifier l'état actuel des colonnes de partitionnement
            print("État des colonnes de partitionnement avant correction:")
            cleaned_df.select("processing_date", "year", "month", "day").show(5)
            
            # Option 1: Si processing_date existe et est valide
            processing_date_not_null = cleaned_df.filter(col("processing_date").isNotNull()).count()
            total_cleaned = cleaned_df.count()
            
            print(f"processing_date non-null: {processing_date_not_null}/{total_cleaned}")
            
            if processing_date_not_null > 0:
                # Utiliser processing_date pour générer les partitions
                final_df = cleaned_df.withColumn("year", year(col("processing_date"))) \
                                   .withColumn("month", month(col("processing_date"))) \
                                   .withColumn("day", dayofmonth(col("processing_date")))
                print("✅ Utilisation de processing_date pour les partitions")
            else:
                # Option 2: Si processing_timestamp existe
                processing_ts_not_null = cleaned_df.filter(col("processing_timestamp").isNotNull()).count()
                print(f"processing_timestamp non-null: {processing_ts_not_null}/{total_cleaned}")
                
                if processing_ts_not_null > 0:
                    final_df = cleaned_df.withColumn("processing_date", to_date(col("processing_timestamp"))) \
                                       .withColumn("year", year(col("processing_timestamp"))) \
                                       .withColumn("month", month(col("processing_timestamp"))) \
                                       .withColumn("day", dayofmonth(col("processing_timestamp")))
                    print("✅ Utilisation de processing_timestamp pour les partitions")
                else:
                    # Option 3: Utiliser la date actuelle comme fallback
                    print("⚠️ Aucune date valide trouvée, utilisation de la date actuelle")
                    current_date_val = current_date()
                    final_df = cleaned_df.withColumn("processing_date", current_date_val) \
                                       .withColumn("year", year(current_date_val)) \
                                       .withColumn("month", month(current_date_val)) \
                                       .withColumn("day", dayofmonth(current_date_val))
            
            # S'assurer que les colonnes de partitionnement ne sont pas NULL
            final_df = final_df.filter(
                col("year").isNotNull() & 
                col("month").isNotNull() & 
                col("day").isNotNull()
            )
            
            # Convertir les types de données et ajouter les métadonnées
            final_df = final_df.withColumn("amount_numeric", col("amount").cast(DoubleType())) \
                             .withColumn("is_returned_bool", col("is_returned").cast(BooleanType())) \
                             .withColumn("delta_load_timestamp", current_timestamp())
            
            # Vérifier le résultat final
            final_count = final_df.count()
            print(f"✅ Enregistrements valides après nettoyage et correction: {final_count}")
            
            if final_count > 0:
                print("État final des colonnes de partitionnement:")
                final_df.select("processing_date", "year", "month", "day").show(5)
                
                # Vérifier qu'il n'y a plus de NULL dans les colonnes de partitionnement
                null_partitions = final_df.filter(
                    col("year").isNull() | col("month").isNull() | col("day").isNull()
                ).count()
                
                if null_partitions > 0:
                    print(f"⚠️ ATTENTION: {null_partitions} enregistrements ont encore des partitions NULL!")
                    final_df = final_df.filter(
                        col("year").isNotNull() & 
                        col("month").isNotNull() & 
                        col("day").isNotNull()
                    )
                    print(f"Enregistrements après filtrage des NULL: {final_df.count()}")
                else:
                    print("✅ Toutes les colonnes de partitionnement sont valides")
            
            return final_df, set(new_files)
        else:
            print("⚠️ Aucun enregistrement valide trouvé dans les nouveaux fichiers")
            return None, set(new_files)
            
    except Exception as e:
        print(f"❌ Erreur lors de la lecture des fichiers CSV: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, set()

def create_or_update_delta_table(spark, df, delta_path):
    """Créer ou mettre à jour la table Delta Lake avec partitionnement"""
    if df is None:
        print("⚠️ Aucune donnée à écrire dans Delta Lake")
        return
    
    print(f"🔄 Traitement de {df.count()} enregistrements pour Delta Lake...")
    
    try:
        # VALIDATION CRITIQUE: S'assurer que les colonnes de partitionnement sont valides
        print("🔍 Validation des colonnes de partitionnement...")
        
        # Vérifier que les colonnes de partitionnement existent et ne sont pas NULL
        partition_columns = ["year", "month", "day"]
        for col_name in partition_columns:
            if col_name not in df.columns:
                print(f"❌ Colonne de partitionnement manquante: {col_name}")
                return
            
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"❌ {null_count} valeurs NULL trouvées dans la colonne de partitionnement {col_name}")
                print("🛠️ Tentative de correction...")
                
                # Essayer de corriger en utilisant la date actuelle
                if col_name == "year":
                    df = df.withColumn(col_name, when(col(col_name).isNull(), year(current_date())).otherwise(col(col_name)))
                elif col_name == "month":
                    df = df.withColumn(col_name, when(col(col_name).isNull(), month(current_date())).otherwise(col(col_name)))
                elif col_name == "day":
                    df = df.withColumn(col_name, when(col(col_name).isNull(), dayofmonth(current_date())).otherwise(col(col_name)))
                
                # Re-vérifier
                remaining_null = df.filter(col(col_name).isNull()).count()
                if remaining_null > 0:
                    print(f"❌ Impossible de corriger les valeurs NULL dans {col_name}")
                    return
                else:
                    print(f"✅ Valeurs NULL corrigées dans {col_name}")
        
        # Afficher un échantillon des valeurs de partitionnement
        print("📋 Échantillon des colonnes de partitionnement:")
        df.select("year", "month", "day").distinct().orderBy("year", "month", "day").show()
        
        # S'assurer que les valeurs de partitionnement sont dans des plages valides
        year_stats = df.select(min("year").alias("min_year"), max("year").alias("max_year")).collect()[0]
        month_stats = df.select(min("month").alias("min_month"), max("month").alias("max_month")).collect()[0]
        day_stats = df.select(min("day").alias("min_day"), max("day").alias("max_day")).collect()[0]
        
        print(f"📊 Plages de partitionnement:")
        print(f"  Années: {year_stats.min_year} - {year_stats.max_year}")
        print(f"  Mois: {month_stats.min_month} - {month_stats.max_month}")
        print(f"  Jours: {day_stats.min_day} - {day_stats.max_day}")
        
        # Validation des plages
        if year_stats.min_year < 2020 or year_stats.max_year > 2030:
            print("⚠️ Années suspectes détectées")
        if month_stats.min_month < 1 or month_stats.max_month > 12:
            print("❌ Mois invalides détectés")
            df = df.filter((col("month") >= 1) & (col("month") <= 12))
        if day_stats.min_day < 1 or day_stats.max_day > 31:
            print("❌ Jours invalides détectés")
            df = df.filter((col("day") >= 1) & (col("day") <= 31))
        
        final_count_after_validation = df.count()
        print(f"📊 Enregistrements après validation: {final_count_after_validation}")
        
        if final_count_after_validation == 0:
            print("❌ Aucun enregistrement valide après validation des partitions")
            return
        
        # CORRECTION: Invalider le cache Spark avant de compter
        print("🔄 Invalidation du cache Spark...")
        spark.sql("CLEAR CACHE")
        
        # Vérifier si la table Delta existe déjà
        if DeltaTable.isDeltaTable(spark, delta_path):
            print("📊 Table Delta existante trouvée - Mode APPEND avec partitionnement")
            
            # CORRECTION: Recréer la référence à la table Delta pour éviter les erreurs de cache
            try:
                # Rafraîchir la table Delta
                spark.sql(f"REFRESH TABLE delta.`{delta_path}`")
            except Exception as refresh_error:
                print(f"⚠️ Impossible de rafraîchir la table: {str(refresh_error)}")
            
            # Charger la table Delta existante avec une nouvelle référence
            delta_table = DeltaTable.forPath(spark, delta_path)
            
            # Afficher les statistiques actuelles avec gestion d'erreur
            try:
                current_count = delta_table.toDF().count()
                print(f"📈 Enregistrements actuels dans Delta: {current_count}")
            except Exception as count_error:
                print(f"⚠️ Impossible de compter les enregistrements existants: {str(count_error)}")
                print("🔄 Tentative de lecture directe...")
                try:
                    current_df = spark.read.format("delta").load(delta_path)
                    current_count = current_df.count()
                    print(f"📈 Enregistrements actuels dans Delta (lecture directe): {current_count}")
                except Exception as direct_count_error:
                    print(f"❌ Impossible de lire la table existante: {str(direct_count_error)}")
                    current_count = 0
            
            # Ajouter les nouvelles données avec partitionnement (APPEND)
            print("💾 Écriture des nouvelles données en mode APPEND...")
            df.write \
              .format("delta") \
              .mode("append") \
              .partitionBy("year", "month", "day") \
              .option("mergeSchema", "true") \
              .save(delta_path)
            
            # Afficher les nouvelles statistiques
            try:
                updated_delta_table = DeltaTable.forPath(spark, delta_path)
                new_count = updated_delta_table.toDF().count()
                print(f"📈 Enregistrements après ajout: {new_count} (+{new_count - current_count})")
            except Exception as new_count_error:
                print(f"⚠️ Impossible de compter après ajout: {str(new_count_error)}")
            
        else:
            print("🆕 Création d'une nouvelle table Delta Lake avec partitionnement")
            
            # Créer la nouvelle table Delta avec partitionnement
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .partitionBy("year", "month", "day") \
              .option("delta.autoOptimize.optimizeWrite", "true") \
              .option("delta.autoOptimize.autoCompact", "true") \
              .save(delta_path)
            
            print(f"✅ Table Delta créée avec {df.count()} enregistrements")
            print("📁 Partitionnement: year/month/day")
        
        # Afficher des statistiques de la table mise à jour
        print("\n=== STATISTIQUES DE LA TABLE DELTA PARTITIONNÉE ===")
        try:
            delta_table = DeltaTable.forPath(spark, delta_path)
            final_df = delta_table.toDF()
            
            print(f"📊 Total des enregistrements: {final_df.count()}")
            
            # Afficher la structure des partitions
            print("\n📁 Structure des partitions:")
            partition_info = final_df.groupBy("year", "month", "day").count() \
                                    .orderBy("year", "month", "day")
            partition_info.show()
            
            # Vérifier qu'il n'y a plus de partitions NULL
            null_partitions = final_df.filter(
                col("year").isNull() | col("month").isNull() | col("day").isNull()
            ).count()
            
            if null_partitions > 0:
                print(f"❌ PROBLÈME CRITIQUE: {null_partitions} enregistrements avec partitions NULL détectés!")
            else:
                print("✅ Toutes les partitions sont valides")
            
            print("\n📊 Répartition par statut de paiement:")
            final_df.groupBy("payment_status").count().orderBy(desc("count")).show()
            
            print("\n📊 Répartition par catégorie (top 10):")
            final_df.groupBy("category").count().orderBy(desc("count")).show(10)
            
        except Exception as stats_error:
            print(f"⚠️ Impossible d'afficher les statistiques: {str(stats_error)}")
        
        # Optimiser la table Delta
        print("🔧 Optimisation de la table Delta partitionnée...")
        try:
            spark.sql(f"OPTIMIZE delta.`{delta_path}`")
            print("✅ Table Delta optimisée")
        except Exception as opt_error:
            print(f"⚠️ Optimisation échouée: {str(opt_error)}")
        
        # Afficher des informations sur le partitionnement
        print(f"\n📋 INFORMATIONS DE PARTITIONNEMENT:")
        print(f"📍 Chemin de la table: {delta_path}")
        print("📁 Structure attendue: /year=YYYY/month=MM/day=DD/")
        
        # Test de lecture d'une partition spécifique
        try:
            sample_partition = df.select("year", "month", "day").first()
            if sample_partition:
                test_partition_df = spark.read.format("delta").load(delta_path) \
                    .filter((col("year") == sample_partition.year) & 
                           (col("month") == sample_partition.month) & 
                           (col("day") == sample_partition.day))
                partition_count = test_partition_df.count()
                print(f"🧪 Test de lecture partition year={sample_partition.year}/month={sample_partition.month}/day={sample_partition.day}: {partition_count} enregistrements")
        except Exception as test_error:
            print(f"⚠️ Test de partition échoué: {str(test_error)}")
        
    except Exception as e:
        print(f"❌ Erreur lors de la création/mise à jour de la table Delta: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e

def main():
    """Fonction principale pour convertir les CSV en Delta Lake"""
    print("=== DÉMARRAGE DE LA CONVERSION CSV VERS DELTA LAKE ===")
    
    spark = create_spark_session()
    
    try:
        # S'assurer que le dossier metadata existe
        ensure_metadata_bucket_exists(spark)
        
        # Découvrir tous les fichiers CSV
        csv_files = discover_csv_files(spark)
        
        if not csv_files:
            print("⚠️ Aucun fichier CSV trouvé à traiter")
            return
        
        # Lire la liste des fichiers déjà traités
        processed_files = read_processed_files_list(spark)
        
        # Lire uniquement les nouveaux fichiers CSV
        new_data_df, new_files_processed = read_new_csv_files(spark, csv_files, processed_files)
        
        if new_data_df is not None and new_data_df.count() > 0:
            # Chemin de la table Delta Lake
            delta_table_path = "s3a://transactions/bronze"
            
            # Créer ou mettre à jour la table Delta
            create_or_update_delta_table(spark, new_data_df, delta_table_path)
            
            # CORRECTION CRITIQUE: Sauvegarder les métadonnées SEULEMENT après succès
            print("💾 Sauvegarde des fichiers traités dans les métadonnées...")
            save_processed_files_list(spark, new_files_processed)
            
            print(f"✅ Conversion terminée avec succès! {new_data_df.count()} nouveaux enregistrements ajoutés à Delta Lake.")
            print(f"📍 Table Delta Lake disponible à: {delta_table_path}")
            
            # Vérification finale des métadonnées
            print("\n=== VÉRIFICATION FINALE DES MÉTADONNÉES ===")
            final_processed_files = read_processed_files_list(spark)
            print(f"📋 Nombre total de fichiers marqués comme traités: {len(final_processed_files)}")
            
        else:
            print("ℹ️ Aucune nouvelle donnée à traiter")
            # Même si pas de nouvelles données, marquer les fichiers comme traités si ils étaient dans la liste
            if new_files_processed:
                print("💾 Marquage des fichiers vides comme traités...")
                save_processed_files_list(spark, new_files_processed)
            
    except Exception as e:
        print(f"❌ Erreur lors de la conversion: {str(e)}")
        import traceback
        traceback.print_exc()
        # CORRECTION: Ne pas sauvegarder les métadonnées en cas d'erreur
        print("⚠️ Les fichiers ne seront pas marqués comme traités à cause de l'erreur")
        raise e
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
