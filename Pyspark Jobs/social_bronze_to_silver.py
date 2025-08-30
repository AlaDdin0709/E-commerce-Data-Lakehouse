from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import re
from datetime import datetime

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("SocialMediaBronzeToSilverCleaner") \
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
        bronze_df.createOrReplaceTempView("bronze_social_media")
        print("✅ Table Bronze Social Media enregistrée comme vue temporaire")
        
        # Vérifier si la table Silver existe
        if DeltaTable.isDeltaTable(spark, silver_path):
            silver_df = spark.read.format("delta").load(silver_path)
            silver_df.createOrReplaceTempView("silver_social_media")
            print("✅ Table Silver Social Media existante enregistrée comme vue temporaire")
            return True
        else:
            print("ℹ️ Table Silver Social Media n'existe pas encore - première exécution")
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
            FROM silver_social_media
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

def create_cleaning_functions(spark):
    """Créer les UDFs pour le nettoyage des données social media"""
    
    def clean_content_udf(content):
        """Nettoyer le contenu du post (préserver l'arabe)"""
        if not content:
            return None
        
        try:
            # Nettoyer les caractères d'échappement et espaces excessifs
            cleaned = content.strip()
            
            # Supprimer les caractères de contrôle mais préserver l'arabe
            cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
            
            # Normaliser les espaces multiples
            cleaned = re.sub(r'\s+', ' ', cleaned)
            
            # Retourner None si le contenu devient vide après nettoyage
            if len(cleaned.strip()) == 0:
                return None
                
            return cleaned.strip()
            
        except Exception:
            return None
    
    def clean_platform_udf(platform):
        """Nettoyer et normaliser le nom de la plateforme"""
        if not platform:
            return None
            
        try:
            # Nettoyer et normaliser
            cleaned = platform.strip().lower()
            
            # Mapping des plateformes communes
            platform_mapping = {
                'twitter': 'Twitter',
                'facebook': 'Facebook',
                'instagram': 'Instagram',
                'tiktok': 'TikTok',
                'linkedin': 'LinkedIn',
                'youtube': 'YouTube',
                'snapchat': 'Snapchat'
            }
            
            return platform_mapping.get(cleaned, platform.strip().title())
            
        except Exception:
            return None
    
    # Enregistrer les UDFs
    spark.udf.register("clean_content", clean_content_udf, StringType())
    spark.udf.register("clean_platform", clean_platform_udf, StringType())
    
    print("✅ Fonctions de nettoyage UDF Social Media enregistrées")

def create_silver_table_sql(spark, silver_path, max_timestamp_silver):
    """Créer la requête SQL pour nettoyer et transformer les données social media"""
    
    # Condition pour le traitement incrémental
    incremental_condition = ""
    if max_timestamp_silver:
        incremental_condition = f"AND processing_timestamp > '{max_timestamp_silver}'"
    
    cleaning_sql = f"""
        SELECT 
            post_id,
            user_id,
            clean_content(content) as content,
            clean_platform(platform) as platform,
            CASE 
                WHEN likes IS NULL OR likes < 0 THEN 0
                ELSE likes
            END as likes,
            processing_timestamp,
            partition_year,
            partition_month,
            partition_day,
            current_timestamp() as silver_load_timestamp
        FROM bronze_social_media
        WHERE post_id IS NOT NULL 
            AND user_id IS NOT NULL
            AND content IS NOT NULL
            AND platform IS NOT NULL
            AND processing_timestamp IS NOT NULL
            AND partition_year IS NOT NULL
            AND partition_month IS NOT NULL
            AND partition_day IS NOT NULL
            {incremental_condition}
    """
    
    return cleaning_sql

def execute_bronze_to_silver_transformation(spark, bronze_path, silver_path):
    """Exécuter la transformation complète Bronze vers Silver pour Social Media"""
    
    print("=== DÉMARRAGE DE LA TRANSFORMATION BRONZE VERS SILVER SOCIAL MEDIA ===")
    
    # 1. Enregistrer les tables Delta
    silver_exists = register_delta_tables(spark, bronze_path, silver_path)
    
    # 2. Obtenir le timestamp maximum de Silver pour traitement incrémental
    max_timestamp_silver = get_max_processing_timestamp_silver(spark, silver_exists)
    
    # 3. Créer les fonctions de nettoyage
    create_cleaning_functions(spark)
    
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
        
        # 6. Afficher un échantillon des données nettoyées
        print("\n🔍 ÉCHANTILLON DES DONNÉES NETTOYÉES:")
        sample_df = cleaned_df.limit(3)
        sample_df.select("post_id", "user_id", "platform", "content", "likes").show(truncate=False)
        
        # 7. Vérifier la qualité des données nettoyées
        print("\n📊 QUALITÉ DES DONNÉES APRÈS NETTOYAGE:")
        
        # Compter les valeurs nulles pour les champs critiques
        null_content = cleaned_df.filter(col("content").isNull()).count()
        null_platform = cleaned_df.filter(col("platform").isNull()).count()
        
        print(f"📝 Contenu manquant: {null_content}/{new_records_count}")
        print(f"📱 Plateformes manquantes: {null_platform}/{new_records_count}")
        
        # Statistiques par plateforme
        print("\n📊 RÉPARTITION PAR PLATEFORME:")
        platform_stats = cleaned_df.groupBy("platform").count().orderBy(desc("count"))
        platform_stats.show()
        
        # 8. Écrire les données dans la table Silver
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
        
        # 9. Vérification finale
        print("\n✅ TRANSFORMATION TERMINÉE AVEC SUCCÈS")
        
        # Enregistrer la nouvelle table Silver pour les statistiques finales
        silver_final_df = spark.read.format("delta").load(silver_path)
        silver_final_df.createOrReplaceTempView("silver_social_media_final")
        
        # Statistiques finales
        total_silver_count = spark.sql("SELECT COUNT(*) as count FROM silver_social_media_final").collect()[0].count
        print(f"📈 Total des enregistrements en Silver: {total_silver_count}")
        
        # Répartition par partition
        print("\n📁 RÉPARTITION PAR PARTITION:")
        partition_stats = spark.sql("""
            SELECT partition_year, partition_month, partition_day, COUNT(*) as count
            FROM silver_social_media_final
            GROUP BY partition_year, partition_month, partition_day
            ORDER BY partition_year DESC, partition_month DESC, partition_day DESC
        """)
        partition_stats.show()
        
        # Quelques statistiques business social media
        print("\n📊 STATISTIQUES SOCIAL MEDIA:")
        
        # Top utilisateurs les plus actifs
        user_stats = spark.sql("""
            SELECT user_id, COUNT(*) as post_count, SUM(likes) as total_likes
            FROM silver_social_media_final
            GROUP BY user_id
            ORDER BY post_count DESC
            LIMIT 10
        """)
        print("👥 Top 10 utilisateurs les plus actifs:")
        user_stats.show()
        
        # Statistiques des likes
        likes_stats = spark.sql("""
            SELECT 
                AVG(likes) as avg_likes,
                MAX(likes) as max_likes,
                MIN(likes) as min_likes,
                SUM(likes) as total_likes
            FROM silver_social_media_final
        """)
        print("👍 Statistiques des likes:")
        likes_stats.show()
        
        # 10. Optimiser la table Silver
        print("\n🔧 Optimisation de la table Silver...")
        try:
            spark.sql(f"OPTIMIZE delta.`{silver_path}`")
            print("✅ Table Silver optimisée")
        except Exception as opt_error:
            print(f"⚠️ Optimisation échouée: {str(opt_error)}")
        
        print(f"\n🎉 TRANSFORMATION BRONZE -> SILVER SOCIAL MEDIA COMPLÉTÉE!")
        print(f"📍 Table Silver disponible à: {silver_path}")
        print(f"📊 {new_records_count} nouveaux enregistrements ajoutés")
        print(f"📈 Total des enregistrements: {total_silver_count}")
        
    except Exception as e:
        print(f"❌ Erreur lors de la transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e

def main():
    """Fonction principale"""
    print("=== NETTOYAGE BRONZE VERS SILVER SOCIAL MEDIA AVEC SPARK SQL ===")
    
    spark = create_spark_session()
    
    try:
        # Chemins des tables
        bronze_path = "s3a://social-media/bronze"
        silver_path = "s3a://social-media/silver"
        
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
