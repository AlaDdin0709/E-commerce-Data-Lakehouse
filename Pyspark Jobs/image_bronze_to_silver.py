from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import re
from datetime import datetime

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("CustomerImagesBronzeToSilverCleaner") \
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

def register_delta_tables(spark, bronze_path, silver_path):
    """Enregistrer les tables Delta pour utilisation avec Spark SQL"""
    try:
        # Lire la table Bronze et créer une vue temporaire
        bronze_df = spark.read.format("delta").load(bronze_path)
        bronze_df.createOrReplaceTempView("bronze_customer_images")
        print("✅ Table Bronze Customer Images enregistrée comme vue temporaire")
        
        # Vérifier si la table Silver existe
        if DeltaTable.isDeltaTable(spark, silver_path):
            silver_df = spark.read.format("delta").load(silver_path)
            silver_df.createOrReplaceTempView("silver_customer_images")
            print("✅ Table Silver Customer Images existante enregistrée comme vue temporaire")
            return True
        else:
            print("ℹ️ Table Silver Customer Images n'existe pas encore - première exécution")
            return False
            
    except Exception as e:
        print(f"❌ Erreur lors de l'enregistrement des tables: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def get_max_processing_timestamp_silver(spark, silver_exists):
    """Obtenir le timestamp de traitement maximum de la table Silver"""
    if not silver_exists:
        print("🆕 Première exécution - traitement de toutes les données Bronze")
        return None
    
    try:
        max_timestamp_sql = """
            SELECT MAX(processing_timestamp) as max_timestamp
            FROM silver_customer_images
        """
        
        result = spark.sql(max_timestamp_sql).collect()
        
        if result and result[0].max_timestamp:
            max_timestamp = result[0].max_timestamp
            print(f"📅 Dernier timestamp traité en Silver: {max_timestamp}")
            return max_timestamp
        else:
            print("ℹ️ Aucun timestamp trouvé en Silver - traitement complet")
            return None
            
    except Exception as e:
        print(f"⚠️ Erreur lors de la récupération du max timestamp: {str(e)}")
        return None

def check_image_id_uniqueness(spark):
    """Vérifier l'unicité des image_id dans les nouvelles données et compter les doublons à éliminer"""
    try:
        duplicates_sql = """
            SELECT image_id, COUNT(*) as count
            FROM bronze_customer_images
            WHERE image_id IS NOT NULL AND TRIM(image_id) != ''
            GROUP BY image_id
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 10
        """
        
        duplicates_df = spark.sql(duplicates_sql)
        duplicate_count = duplicates_df.count()
        
        if duplicate_count > 0:
            print(f"⚠️ ATTENTION: {duplicate_count} image_id avec doublons trouvés (seul le plus récent sera conservé):")
            duplicates_df.show(10, truncate=False)
            
            # Calculer le nombre total de doublons à éliminer
            total_duplicates_sql = """
                SELECT SUM(count - 1) as total_to_remove
                FROM (
                    SELECT image_id, COUNT(*) as count
                    FROM bronze_customer_images
                    WHERE image_id IS NOT NULL AND TRIM(image_id) != ''
                    GROUP BY image_id
                    HAVING COUNT(*) > 1
                ) duplicates
            """
            
            result = spark.sql(total_duplicates_sql).collect()
            if result and result[0].total_to_remove:
                total_to_remove = result[0].total_to_remove
                print(f"🗑️ {total_to_remove} enregistrements dupliqués seront éliminés")
            
        else:
            print("✅ Aucun image_id dupliqué trouvé")
            
        return duplicate_count
        
    except Exception as e:
        print(f"⚠️ Erreur lors de la vérification des doublons: {str(e)}")
        return 0

def create_silver_table_sql(spark, silver_path, max_timestamp_silver):
    """Créer la requête SQL pour nettoyer et transformer les données customer images avec déduplication"""
    
    # Condition pour le traitement incrémental
    incremental_condition = ""
    if max_timestamp_silver:
        incremental_condition = f"AND processing_timestamp > '{max_timestamp_silver}'"
    
    # Requête SQL avec déduplication par image_id (garde le plus récent)
    cleaning_sql = f"""
        WITH ranked_data AS (
            SELECT 
                -- Nettoyage et validation du image_id (clé unique)
                CASE 
                    WHEN image_id IS NOT NULL AND TRIM(image_id) != '' 
                    THEN TRIM(image_id)
                    ELSE NULL 
                END as image_id,
                
                -- Nettoyage du s3_path - accepter les chemins relatifs aussi
                CASE 
                    WHEN s3_path IS NOT NULL AND TRIM(s3_path) != '' 
                    THEN TRIM(s3_path)
                    ELSE NULL 
                END as s3_path,
                
                -- Validation du customer_id
                CASE 
                    WHEN customer_id IS NOT NULL AND TRIM(customer_id) != '' 
                    THEN TRIM(customer_id)
                    ELSE NULL 
                END as customer_id,
                
                -- Utiliser order_id s'il existe, sinon extracted_order_id
                CASE 
                    WHEN order_id IS NOT NULL AND TRIM(order_id) != '' 
                    THEN TRIM(order_id)
                    WHEN extracted_order_id IS NOT NULL AND TRIM(extracted_order_id) != '' 
                    THEN TRIM(extracted_order_id)
                    ELSE NULL 
                END as order_id,
                
                processing_timestamp,
                upload_timestamp,
                partition_year,
                partition_month,
                partition_day,
                current_timestamp() as silver_load_timestamp,
                
                -- Ranger par processing_timestamp DESC pour garder le plus récent
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(image_id) 
                    ORDER BY processing_timestamp DESC
                ) as rn
                
            FROM bronze_customer_images
            WHERE image_id IS NOT NULL
                AND TRIM(image_id) != ''
                AND processing_timestamp IS NOT NULL
                AND partition_year IS NOT NULL
                AND partition_month IS NOT NULL
                AND partition_day IS NOT NULL
                {incremental_condition}
        )
        SELECT 
            image_id,
            s3_path,
            customer_id,
            order_id,
            processing_timestamp,
            upload_timestamp,
            partition_year,
            partition_month,
            partition_day,
            silver_load_timestamp
        FROM ranked_data
        WHERE rn = 1
    """
    
    return cleaning_sql

def execute_bronze_to_silver_transformation(spark, bronze_path, silver_path):
    """Exécuter la transformation complète Bronze vers Silver pour Customer Images"""
    
    print("=== DÉMARRAGE DE LA TRANSFORMATION BRONZE VERS SILVER CUSTOMER IMAGES ===")
    
    # 1. Enregistrer les tables Delta
    silver_exists = register_delta_tables(spark, bronze_path, silver_path)
    
    # 2. Vérifier l'unicité des image_id
    duplicate_count = check_image_id_uniqueness(spark)
    
    # 3. Obtenir le timestamp maximum de Silver pour traitement incrémental
    max_timestamp_silver = get_max_processing_timestamp_silver(spark, silver_exists)
    
    # 4. Créer la requête SQL de nettoyage
    cleaning_sql = create_silver_table_sql(spark, silver_path, max_timestamp_silver)
    
    print("📋 Requête SQL de transformation créée")
    print("🔍 Vérification des nouvelles données à traiter...")
    
    # 5. Exécuter la requête et compter les résultats
    try:
        cleaned_df = spark.sql(cleaning_sql)
        new_records_count = cleaned_df.count()
        
        if new_records_count == 0:
            print("✅ Aucune nouvelle donnée à traiter - Silver est à jour")
            return
        
        print(f"📊 {new_records_count} nouveaux enregistrements à traiter")
        
        # 6. Vérifier l'unicité des image_id dans les données nettoyées
        print("\n🔍 VÉRIFICATION DE L'UNICITÉ DES IMAGE_ID APRÈS NETTOYAGE:")
        unique_images_sql = """
            SELECT COUNT(DISTINCT image_id) as unique_images,
                   COUNT(*) as total_records
            FROM (
                SELECT * FROM (""" + cleaning_sql + """) as cleaned_data
            ) t
        """
        
        unique_result = spark.sql(unique_images_sql).collect()[0]
        unique_images = unique_result.unique_images
        total_records = unique_result.total_records
        
        print(f"📊 Images uniques: {unique_images}")
        print(f"📊 Total enregistrements: {total_records}")
        
        if unique_images == total_records:
            print("✅ Tous les image_id sont maintenant uniques après déduplication")
        else:
            print(f"❌ ERREUR: Déduplication échouée - {total_records - unique_images} doublons restants")
        
        # 7. Afficher un échantillon des données nettoyées
        print("\n🔍 ÉCHANTILLON DES DONNÉES NETTOYÉES:")
        sample_df = cleaned_df.limit(5)
        sample_df.select("image_id", "s3_path", "customer_id", "order_id").show(truncate=False)
        
        # 8. Vérifier la qualité des données nettoyées
        print("\n📊 QUALITÉ DES DONNÉES APRÈS NETTOYAGE:")
        
        # Compter les valeurs nulles pour les champs critiques
        null_image_id = cleaned_df.filter(col("image_id").isNull()).count()
        null_s3_path = cleaned_df.filter(col("s3_path").isNull()).count()
        null_customer_id = cleaned_df.filter(col("customer_id").isNull()).count()
        null_order_id = cleaned_df.filter(col("order_id").isNull()).count()
        
        print(f"🖼️ Image ID manquants/invalides: {null_image_id}/{new_records_count}")
        print(f"📁 Chemins S3 manquants/invalides: {null_s3_path}/{new_records_count}")
        print(f"👤 Customer ID manquants/invalides: {null_customer_id}/{new_records_count}")
        print(f"📦 Order ID manquants/invalides: {null_order_id}/{new_records_count}")
        
        # 9. Écrire les données dans la table Silver
        print(f"\n💾 Écriture de {new_records_count} enregistrements vers Silver...")
        
        if silver_exists:
            print("📊 Mode APPEND - Ajout des nouvelles données")
            
            cleaned_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("partition_year", "partition_month", "partition_day") \
                .option("mergeSchema", "true") \
                .save(silver_path)
        else:
            print("🆕 Mode OVERWRITE - Création de la nouvelle table Silver")
            
            cleaned_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("partition_year", "partition_month", "partition_day") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .save(silver_path)
        
        # 10. Vérification finale
        print("\n✅ TRANSFORMATION TERMINÉE AVEC SUCCÈS")
        
        # Comptage final et vérification des image_id uniques en Silver
        silver_final_df = spark.read.format("delta").load(silver_path)
        total_silver_count = silver_final_df.count()
        
        # Vérifier l'unicité finale en Silver
        silver_final_df.createOrReplaceTempView("final_silver")
        final_unique_sql = """
            SELECT COUNT(DISTINCT image_id) as unique_images,
                   COUNT(*) as total_records
            FROM final_silver
        """
        
        final_result = spark.sql(final_unique_sql).collect()[0]
        final_unique_images = final_result.unique_images
        final_total_records = final_result.total_records
        
        print(f"📈 Total des enregistrements en Silver: {total_silver_count:,}")
        print(f"🔑 Images uniques en Silver: {final_unique_images:,}")
        
        if final_unique_images == final_total_records:
            print("✅ Tous les image_id sont uniques en Silver - Déduplication réussie!")
        else:
            print(f"❌ ERREUR: {final_total_records - final_unique_images} doublons présents en Silver")
        
        # 11. Statistiques business
        print("\n📊 STATISTIQUES BUSINESS CUSTOMER IMAGES:")
        
        # Vérification des s3_path non-null
        s3_non_null_sql = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN s3_path IS NOT NULL THEN 1 ELSE 0 END) as s3_path_present,
                SUM(CASE WHEN s3_path IS NULL THEN 1 ELSE 0 END) as s3_path_missing
            FROM final_silver
        """
        
        s3_stats = spark.sql(s3_non_null_sql).collect()[0]
        print(f"📁 S3 Paths présents: {s3_stats.s3_path_present}/{s3_stats.total}")
        print(f"📁 S3 Paths manquants: {s3_stats.s3_path_missing}/{s3_stats.total}")
        
        # Nombre de clients uniques
        customer_stats_sql = """
            SELECT COUNT(DISTINCT customer_id) as unique_customers
            FROM final_silver
            WHERE customer_id IS NOT NULL
        """
        customer_result = spark.sql(customer_stats_sql).collect()[0]
        print(f"👥 Clients uniques: {customer_result.unique_customers}")
        
        # Nombre de commandes uniques
        order_stats_sql = """
            SELECT COUNT(DISTINCT order_id) as unique_orders
            FROM final_silver
            WHERE order_id IS NOT NULL
        """
        order_result = spark.sql(order_stats_sql).collect()[0]
        print(f"📦 Commandes uniques: {order_result.unique_orders}")
        
        print(f"\n🎉 TRANSFORMATION BRONZE -> SILVER CUSTOMER IMAGES COMPLÉTÉE!")
        print(f"📍 Table Silver disponible à: {silver_path}")
        print(f"📊 {new_records_count:,} nouveaux enregistrements ajoutés")
        print(f"📈 Total des enregistrements: {total_silver_count:,}")
        print(f"🔑 Images uniques: {final_unique_images:,}")
        
        # Résumé des colonnes conservées
        print(f"\n📋 COLONNES CONSERVÉES EN SILVER:")
        print("   1. image_id (unique identifier)")
        print("   2. s3_path")
        print("   3. customer_id")
        print("   4. order_id")
        print("   5. processing_timestamp")
        print("   6. upload_timestamp")
        print("   7. partition_year")
        print("   8. partition_month")
        print("   9. partition_day")
        print("  10. silver_load_timestamp")
        
    except Exception as e:
        print(f"❌ Erreur lors de la transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e

def main():
    """Fonction principale"""
    print("=== NETTOYAGE BRONZE VERS SILVER CUSTOMER IMAGES AVEC SPARK SQL ===")
    
    spark = create_spark_session()
    
    try:
        # Chemins des tables
        bronze_path = "s3a://customer-images/bronze"
        silver_path = "s3a://customer-images/silver"
        
        # Vérifier que la table Bronze existe
        if not DeltaTable.isDeltaTable(spark, bronze_path):
            print(f"❌ Table Bronze non trouvée à: {bronze_path}")
            return
        
        print(f"✅ Table Bronze trouvée à: {bronze_path}")
        
        # Exécuter la transformation
        execute_bronze_to_silver_transformation(spark, bronze_path, silver_path)
        
    except Exception as e:
        print(f"❌ Erreur dans le processus principal: {str(e)}")
        raise e
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
