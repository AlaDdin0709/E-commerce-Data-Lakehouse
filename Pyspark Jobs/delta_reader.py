from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import DeltaTable
import sys

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires"""
    spark = SparkSession.builder \
        .appName("DeltaTableRepair") \
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

def count_total_records(spark, table_path):
    """Compter le nombre total d'enregistrements dans la table Delta"""
    print("\n🔍 COMPTAGE DES ENREGISTREMENTS TOTAUX")
    
    try:
        # Méthode 1: Via SQL (la plus rapide)
        try:
            count_df = spark.sql(f"SELECT COUNT(*) as total_count FROM delta.`{table_path}`")
            total_count = count_df.collect()[0]["total_count"]
            print(f"✅ Nombre total d'enregistrements (via SQL): {total_count:,}")
            return total_count
        except Exception as sql_error:
            print(f"⚠️ Impossible de compter via SQL: {str(sql_error)}")
            
        # Méthode 2: Via DataFrame (plus lent mais plus robuste)
        try:
            total_count = spark.read.format("delta").load(table_path).count()
            print(f"✅ Nombre total d'enregistrements (via DataFrame): {total_count:,}")
            return total_count
        except Exception as df_error:
            print(f"❌ Impossible de compter via DataFrame: {str(df_error)}")
            return None
            
    except Exception as e:
        print(f"❌ Erreur générale lors du comptage: {str(e)}")
        return None

def diagnose_delta_table_issues(spark, table_path):
    """Diagnostiquer les problèmes de la table Delta"""
    print(f"=== DIAGNOSTIC DE LA TABLE DELTA: {table_path} ===")
    
    try:
        # 1. Vérifier que la table Delta existe
        if not DeltaTable.isDeltaTable(spark, table_path):
            print(f"❌ ERREUR: Pas une table Delta valide au chemin {table_path}")
            return False
        
        print("✅ Table Delta détectée")
        
        # 2. Essayer de charger la table Delta
        try:
            delta_table = DeltaTable.forPath(spark, table_path)
            print("✅ Table Delta chargée avec succès")
        except Exception as load_error:
            print(f"❌ Erreur lors du chargement de la table Delta: {str(load_error)}")
            return False
        
        # 3. Vérifier l'historique Delta
        print("\n🔍 VÉRIFICATION DE L'HISTORIQUE DELTA:")
        try:
            history_df = delta_table.history()
            history_count = history_df.count()
            print(f"📜 Nombre d'opérations dans l'historique: {history_count}")
            
            if history_count > 0:
                print("📋 Dernières opérations:")
                history_df.select("version", "timestamp", "operation", "operationParameters") \
                         .orderBy(desc("version")) \
                         .show(5, truncate=False)
                
                # Vérifier la version actuelle
                current_version = history_df.first().version
                print(f"📈 Version actuelle: {current_version}")
            
        except Exception as history_error:
            print(f"⚠️ Erreur lors de la lecture de l'historique: {str(history_error)}")
        
        # 4. Essayer de lire les métadonnées sans lire les données
        print("\n🔍 VÉRIFICATION DES MÉTADONNÉES:")
        try:
            df_schema_only = spark.read.format("delta").load(table_path).limit(0)
            print(f"✅ Schéma accessible: {len(df_schema_only.columns)} colonnes")
            print(f"📋 Colonnes: {df_schema_only.columns}")
        except Exception as schema_error:
            print(f"❌ Erreur lors de la lecture du schéma: {str(schema_error)}")
        
        # 5. Compter le nombre total d'enregistrements
        total_records = count_total_records(spark, table_path)
        if total_records is not None:
            print(f"📊 Total des enregistrements: {total_records:,}")
        
        # 6. Essayer de lire un petit échantillon
        print("\n🔍 TEST DE LECTURE D'ÉCHANTILLON:")
        try:
            sample_df = spark.read.format("delta").load(table_path).limit(1)
            sample_count = sample_df.count()
            print(f"✅ Échantillon lu avec succès: {sample_count} enregistrement")
        except Exception as sample_error:
            print(f"❌ Erreur lors de la lecture d'échantillon: {str(sample_error)}")
            print("🔍 Erreur détaillée:")
            print(str(sample_error))
            
            # Analyser le type d'erreur
            if "No such file or directory" in str(sample_error):
                print("\n🚨 DIAGNOSTIC: Fichiers Parquet manquants détectés!")
                return "missing_files"
            elif "SparkFileNotFoundException" in str(sample_error):
                print("\n🚨 DIAGNOSTIC: Fichiers référencés dans les métadonnées mais absents du stockage!")
                return "missing_files"
            else:
                print(f"\n🚨 DIAGNOSTIC: Erreur inconnue: {type(sample_error).__name__}")
                return "unknown_error"
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur générale lors du diagnostic: {str(e)}")
        return False

def repair_delta_table_missing_files(spark, table_path):
    """Réparer une table Delta avec des fichiers manquants"""
    print(f"\n=== RÉPARATION DE LA TABLE DELTA: {table_path} ===")
    
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Option 1: Utiliser une version antérieure
        print("🔧 OPTION 1: Restaurer à une version antérieure")
        try:
            history_df = delta_table.history()
            versions = history_df.select("version").orderBy(desc("version")).collect()
            
            print(f"📋 Versions disponibles: {[row.version for row in versions[:5]]}")
            
            # Essayer les versions précédentes une par une
            for version_row in versions[1:6]:  # Ignorer la version actuelle, essayer les 5 suivantes
                version = version_row.version
                print(f"🔄 Test de la version {version}...")
                
                try:
                    # Essayer de lire cette version
                    version_df = spark.read.format("delta").option("versionAsOf", version).load(table_path)
                    test_count = version_df.count()
                    print(f"✅ Version {version} accessible avec {test_count} enregistrements")
                    
                    # Demander si on veut restaurer cette version
                    print(f"🔄 Restauration à la version {version}...")
                    delta_table.restoreToVersion(version)
                    print(f"✅ Table restaurée à la version {version}")
                    return True
                    
                except Exception as version_error:
                    print(f"❌ Version {version} également corrompue: {str(version_error)}")
                    continue
            
            print("❌ Aucune version antérieure utilisable trouvée")
            
        except Exception as restore_error:
            print(f"❌ Erreur lors de la restauration: {str(restore_error)}")
        
        # Option 2: Reconstruire la table depuis les fichiers existants
        print("\n🔧 OPTION 2: Reconstruire depuis les fichiers Parquet existants")
        try:
            # Lister tous les fichiers Parquet encore existants
            existing_files_df = spark.sql(f"""
                SELECT input_file_name() as file_path, *
                FROM delta.`{table_path}`
                WHERE input_file_name() IS NOT NULL
            """)
            
            print("Cette option nécessite une intervention manuelle plus complexe.")
            print("Recommandation: Utiliser l'Option 3 (reconstruction complète)")
            
        except Exception as rebuild_error:
            print(f"❌ Erreur lors de la reconstruction: {str(rebuild_error)}")
        
        return False
        
    except Exception as e:
        print(f"❌ Erreur lors de la réparation: {str(e)}")
        return False

def clean_and_rebuild_delta_table(spark, table_path, backup_path=None):
    """Nettoyer complètement et reconstruire la table Delta"""
    print(f"\n=== RECONSTRUCTION COMPLÈTE DE LA TABLE DELTA ===")
    
    try:
        # 1. Créer une sauvegarde si possible
        if backup_path:
            print(f"💾 Tentative de sauvegarde vers {backup_path}...")
            try:
                delta_table = DeltaTable.forPath(spark, table_path)
                backup_df = delta_table.toDF()
                backup_count = backup_df.count()
                
                backup_df.write.format("delta").mode("overwrite").save(backup_path)
                print(f"✅ Sauvegarde créée: {backup_count} enregistrements dans {backup_path}")
                
            except Exception as backup_error:
                print(f"⚠️ Impossible de créer une sauvegarde: {str(backup_error)}")
                print("🔄 Continuation sans sauvegarde...")
        
        # 2. Supprimer complètement la table Delta actuelle
        print(f"🗑️ Suppression de la table Delta corrompue...")
        try:
            # Note: En production, vous devriez utiliser des commandes système pour supprimer
            # Pour MinIO/S3, cela nécessiterait des appels API spécifiques
            print("⚠️ ATTENTION: Suppression manuelle requise!")
            print(f"Exécutez manuellement: rm -rf {table_path}")
            print("Ou utilisez l'interface MinIO pour supprimer le dossier")
            
            # Alternative: Essayer de recréer par-dessus
            print("🔄 Tentative de recréation par écrasement...")
            
        except Exception as delete_error:
            print(f"⚠️ Erreur lors de la suppression: {str(delete_error)}")
        
        # 3. Reconstruire depuis les fichiers CSV originaux
        print("🔄 Pour reconstruire complètement:")
        print("1. Supprimez manuellement le dossier de la table Delta")
        print("2. Relancez votre script de conversion CSV vers Delta")
        print("3. Assurez-vous que les fichiers CSV sources sont toujours disponibles")
        
        return False
        
    except Exception as e:
        print(f"❌ Erreur lors de la reconstruction: {str(e)}")
        return False

def clear_spark_cache_and_refresh(spark, table_path):
    """Nettoyer le cache Spark et rafraîchir les métadonnées"""
    print(f"\n=== NETTOYAGE DU CACHE SPARK ===")
    
    try:
        # 1. Vider tout le cache Spark
        print("🧹 Vidage du cache Spark...")
        spark.sql("CLEAR CACHE")
        spark.catalog.clearCache()
        print("✅ Cache Spark vidé")
        
        # 2. Rafraîchir la table spécifiquement
        print(f"🔄 Rafraîchissement de la table {table_path}...")
        try:
            # Essayer différentes méthodes de rafraîchissement
            spark.sql(f"REFRESH TABLE delta.`{table_path}`")
            print("✅ Table rafraîchie via SQL")
        except Exception as refresh_error:
            print(f"⚠️ Rafraîchissement SQL échoué: {str(refresh_error)}")
            
            try:
                # Alternative: recréer le DataFrame
                df = spark.read.format("delta").load(table_path)
                df.cache()
                df.count()  # Force l'évaluation
                print("✅ DataFrame recréé et mis en cache")
            except Exception as df_error:
                print(f"❌ Impossible de recréer le DataFrame: {str(df_error)}")
        
        # 3. Invalider les métadonnées du catalog
        print("🔄 Invalidation des métadonnées du catalog...")
        try:
            spark.sql("REFRESH")
            print("✅ Métadonnées du catalog invalidées")
        except Exception as catalog_error:
            print(f"⚠️ Invalidation du catalog échouée: {str(catalog_error)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors du nettoyage du cache: {str(e)}")
        return False

def safe_read_delta_table(spark, table_path):
    """Lecture sécurisée de la table Delta avec gestion d'erreurs"""
    print(f"=== LECTURE SÉCURISÉE DE LA TABLE DELTA ===")
    
    try:
        # 1. Nettoyer d'abord le cache
        clear_spark_cache_and_refresh(spark, table_path)
        
        # 2. Essayer une lecture progressive
        print("🔍 Tentative de lecture progressive...")
        
        # Étape 1: Lire seulement le schéma
        try:
            schema_df = spark.read.format("delta").load(table_path).limit(0)
            print(f"✅ Schéma lu: {len(schema_df.columns)} colonnes")
        except Exception as schema_error:
            print(f"❌ Impossible de lire le schéma: {str(schema_error)}")
            return None
        
        # Étape 2: Essayer de lire par petits lots
        try:
            print("🔍 Lecture par petits échantillons...")
            
            for limit in [1, 10, 100, 1000]:
                try:
                    sample_df = spark.read.format("delta").load(table_path).limit(limit)
                    count = sample_df.count()
                    print(f"✅ Lecture réussie pour {limit} enregistrements (résultat: {count})")
                    
                    if count > 0:
                        # Montrer un échantillon
                        print("📋 Échantillon des données:")
                        sample_df.show(5, truncate=True)
                        return sample_df
                    
                except Exception as sample_error:
                    print(f"❌ Échec pour {limit} enregistrements: {str(sample_error)}")
                    continue
            
            print("❌ Toutes les tentatives de lecture par échantillon ont échoué")
            
        except Exception as progressive_error:
            print(f"❌ Erreur lors de la lecture progressive: {str(progressive_error)}")
        
        # Étape 3: Essayer de lire une partition spécifique
        try:
            print("🔍 Tentative de lecture par partition...")
            
            # Essayer de lire les métadonnées de partitionnement
            delta_table = DeltaTable.forPath(spark, table_path)
            
            # Lire une partition spécifique qui pourrait être valide
            partition_df = spark.read.format("delta").load(table_path) \
                              .filter((col("year") == 2025) & (col("month") == 7) & (col("day") == 29))
            
            partition_count = partition_df.count()
            print(f"✅ Partition 2025/7/29 contient {partition_count} enregistrements")
            
            if partition_count > 0:
                print("📋 Données de la partition:")
                partition_df.show(5, truncate=True)
                return partition_df
            
        except Exception as partition_error:
            print(f"❌ Lecture par partition échouée: {str(partition_error)}")
        
        return None
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture sécurisée: {str(e)}")
        return None

def main():
    """Fonction principale de diagnostic et réparation"""
    print("=== OUTIL DE DIAGNOSTIC ET RÉPARATION DELTA LAKE ===")
    
    table_path = "s3a://transactions/bronze"
    backup_path = "s3a://transactions/bronze_backup"
    
    spark = create_spark_session()
    
    try:
        # 1. Diagnostic complet
        diagnosis_result = diagnose_delta_table_issues(spark, table_path)
        
        if diagnosis_result == "missing_files":
            print("\n🔧 PROBLÈME DÉTECTÉ: Fichiers Parquet manquants")
            print("💡 Solutions possibles:")
            print("1. Restaurer à une version antérieure")
            print("2. Reconstruire depuis les CSV originaux")
            print("3. Nettoyer le cache et réessayer")
            
            # Essayer le nettoyage du cache d'abord
            if clear_spark_cache_and_refresh(spark, table_path):
                print("🔄 Nouvelle tentative après nettoyage du cache...")
                result_df = safe_read_delta_table(spark, table_path)
                
                if result_df is not None:
                    print("✅ Problème résolu par le nettoyage du cache!")
                    return
            
            # Si le cache ne résout pas le problème, essayer la réparation
            print("🔧 Tentative de réparation...")
            if not repair_delta_table_missing_files(spark, table_path):
                print("❌ Réparation automatique impossible")
                print("💡 SOLUTION RECOMMANDÉE:")
                print("1. Sauvegardez les fichiers CSV originaux")
                print("2. Supprimez complètement le dossier s3a://transactions/bronze")
                print("3. Relancez votre script de conversion CSV vers Delta")
                print("4. Vérifiez que les colonnes de partitionnement ne contiennent pas de valeurs NULL")
                
        elif diagnosis_result == True:
            print("✅ Table Delta semble fonctionnelle")
            result_df = safe_read_delta_table(spark, table_path)
            
            if result_df is not None:
                print("✅ Lecture réussie!")
            else:
                print("⚠️ Problème lors de la lecture malgré le diagnostic positif")
        
        else:
            print("❌ Diagnostic a révélé des problèmes majeurs")
            print("💡 Reconstruction complète recommandée")
        
        # Conseils de prévention
        print(f"\n📋 CONSEILS DE PRÉVENTION:")
        print("1. Toujours valider que les colonnes de partitionnement ne sont pas NULL avant d'écrire")
        print("2. Utiliser des transactions Delta pour les écritures")
        print("3. Faire des sauvegardes régulières")
        print("4. Utiliser VACUUM avec précaution (garde l'historique)")
        print("5. Monitorer l'espace disque pour éviter les écritures incomplètes")
        
    except Exception as e:
        print(f"❌ Erreur lors du diagnostic/réparation: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
