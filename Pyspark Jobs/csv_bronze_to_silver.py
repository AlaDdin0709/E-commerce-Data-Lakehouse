from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import re
from datetime import datetime

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("BronzeToSilverCleaner") \
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
        # Méthode alternative pour enregistrer la table Bronze
        # Lire la table Delta et créer une vue temporaire
        bronze_df = spark.read.format("delta").load(bronze_path)
        bronze_df.createOrReplaceTempView("bronze_transactions")
        print("✅ Table Bronze enregistrée comme vue temporaire")
        
        # Vérifier si la table Silver existe
        if DeltaTable.isDeltaTable(spark, silver_path):
            silver_df = spark.read.format("delta").load(silver_path)
            silver_df.createOrReplaceTempView("silver_transactions")
            print("✅ Table Silver existante enregistrée comme vue temporaire")
            return True
        else:
            print("ℹ️ Table Silver n'existe pas encore - première exécution")
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
            FROM silver_transactions
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
    """Créer les UDFs pour le nettoyage des données"""
    
    def clean_city_udf(shipping_address):
        """Nettoyer et extraire la ville de shipping_address"""
        if not shipping_address:
            return None
        
        try:
            # Patterns pour extraire la ville
            patterns = [
                r'"city":\s*"([^"]+)"',  # "city": "Toronto"
                r'\\city\\":\s*\\"([^\\]+)\\"',  # \"city\": \"Toronto\"
                r'city.*?:\s*["\']([^"\']+)["\']',  # Variations génériques
            ]
            
            for pattern in patterns:
                match = re.search(pattern, shipping_address, re.IGNORECASE)
                if match:
                    city = match.group(1)
                    # Nettoyer les échappements Unicode
                    city = city.encode().decode('unicode_escape') if '\\u' in city else city
                    return city.strip()
            
            # Si aucun pattern ne marche, essayer d'extraire tout ce qui ressemble à un nom de ville
            clean_text = re.sub(r'[{}"\\\']', '', shipping_address)
            clean_text = re.sub(r'city\s*:', '', clean_text, flags=re.IGNORECASE)
            clean_text = clean_text.strip()
            
            if len(clean_text) > 0 and len(clean_text) < 50:  # Limite raisonnable
                return clean_text
                
            return None
            
        except Exception:
            return None
    
    def clean_region_udf(timestamp_raw):
        """Nettoyer et extraire la région de timestamp_raw"""
        if not timestamp_raw:
            return None
            
        try:
            # Patterns pour extraire la région
            patterns = [
                r'"region":\s*"([^"]+)"',  # "region": "Ontario"
                r'\\region\\":\s*\\"([^\\]+)\\"',  # \"region\": \"Ontario\"
                r'region.*?:\s*["\']([^"\']+)["\']',  # Variations génériques
            ]
            
            for pattern in patterns:
                match = re.search(pattern, timestamp_raw, re.IGNORECASE)
                if match:
                    region = match.group(1)
                    # Nettoyer les échappements Unicode
                    region = region.encode().decode('unicode_escape') if '\\u' in region else region
                    return region.strip()
            
            # Si aucun pattern ne marche
            clean_text = re.sub(r'[{}"\\\']', '', timestamp_raw)
            clean_text = re.sub(r'region\s*:', '', clean_text, flags=re.IGNORECASE)
            clean_text = clean_text.strip()
            
            if len(clean_text) > 0 and len(clean_text) < 100:  # Limite raisonnable
                return clean_text
                
            return None
            
        except Exception:
            return None
    
    # Enregistrer les UDFs
    spark.udf.register("clean_city", clean_city_udf, StringType())
    spark.udf.register("clean_region", clean_region_udf, StringType())
    
    print("✅ Fonctions de nettoyage UDF enregistrées")

def create_silver_table_sql(spark, silver_path, max_timestamp_silver):
    """Créer la requête SQL pour nettoyer et transformer les données"""
    
    # Condition pour le traitement incrémental
    incremental_condition = ""
    if max_timestamp_silver:
        incremental_condition = f"AND processing_timestamp > '{max_timestamp_silver}'"
    
    cleaning_sql = f"""
        SELECT 
            order_id,
            customer_id,
            customer_first_name,
            customer_last_name,
            product_id,
            product_name,
            category,
            CAST(amount AS DOUBLE) as amount,
            payment_method,
            payment_status,
            discount_code,
            clean_city(shipping_address) as city,
            clean_region(timestamp_raw) as region,
            CASE 
                WHEN LOWER(TRIM(is_returned)) IN ('true', '1', 'yes', 't') THEN true
                WHEN LOWER(TRIM(is_returned)) IN ('false', '0', 'no', 'f') THEN false
                ELSE false
            END as is_returned,
            processing_timestamp,
            year,
            month,
            day,
            current_timestamp() as silver_load_timestamp
        FROM bronze_transactions
        WHERE order_id IS NOT NULL 
            AND customer_id IS NOT NULL
            AND product_id IS NOT NULL
            AND amount IS NOT NULL
            AND processing_timestamp IS NOT NULL
            {incremental_condition}
    """
    
    return cleaning_sql

def execute_bronze_to_silver_transformation(spark, bronze_path, silver_path):
    """Exécuter la transformation complète Bronze vers Silver"""
    
    print("=== DÉMARRAGE DE LA TRANSFORMATION BRONZE VERS SILVER ===")
    
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
        sample_df.select("order_id", "city", "region", "is_returned", "amount").show(truncate=False)
        
        # 7. Vérifier la qualité des données nettoyées
        print("\n📊 QUALITÉ DES DONNÉES APRÈS NETTOYAGE:")
        
        # Compter les valeurs nulles pour les champs critiques
        null_cities = cleaned_df.filter(col("city").isNull()).count()
        null_regions = cleaned_df.filter(col("region").isNull()).count()
        
        print(f"🏙️ Villes manquantes: {null_cities}/{new_records_count}")
        print(f"🌍 Régions manquantes: {null_regions}/{new_records_count}")
        
        # Afficher quelques exemples de transformation
        print("\n🧹 EXEMPLES DE TRANSFORMATION:")
        transformation_examples = spark.sql(f"""
            SELECT 
                shipping_address,
                timestamp_raw,
                clean_city(shipping_address) as city,
                clean_region(timestamp_raw) as region
            FROM bronze_transactions
            WHERE shipping_address IS NOT NULL 
                AND timestamp_raw IS NOT NULL
                {f"AND processing_timestamp > '{max_timestamp_silver}'" if max_timestamp_silver else ""}
            LIMIT 5
        """)
        transformation_examples.show(truncate=False)
        
        # 8. Écrire les données dans la table Silver
        print(f"\n💾 Écriture de {new_records_count} enregistrements vers Silver...")
        
        if silver_exists:
            print("📊 Mode APPEND - Ajout des nouvelles données")
            
            cleaned_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .option("mergeSchema", "true") \
                .save(silver_path)
        else:
            print("🆕 Mode OVERWRITE - Création de la nouvelle table Silver")
            
            cleaned_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("year", "month", "day") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .save(silver_path)
        
        # 9. Vérification finale
        print("\n✅ TRANSFORMATION TERMINÉE AVEC SUCCÈS")
        
        # Enregistrer la nouvelle table Silver pour les statistiques finales
        silver_final_df = spark.read.format("delta").load(silver_path)
        silver_final_df.createOrReplaceTempView("silver_transactions_final")
        
        # Statistiques finales
        total_silver_count = spark.sql("SELECT COUNT(*) as count FROM silver_transactions_final").collect()[0].count
        print(f"📈 Total des enregistrements en Silver: {total_silver_count}")
        
        # Répartition par partition
        print("\n📁 RÉPARTITION PAR PARTITION:")
        partition_stats = spark.sql("""
            SELECT year, month, day, COUNT(*) as count
            FROM silver_transactions_final
            GROUP BY year, month, day
            ORDER BY year DESC, month DESC, day DESC
        """)
        partition_stats.show()
        
        # Quelques statistiques business
        print("\n📊 STATISTIQUES BUSINESS:")
        
        # Répartition par statut de paiement
        payment_stats = spark.sql("""
            SELECT payment_status, COUNT(*) as count
            FROM silver_transactions_final
            GROUP BY payment_status
            ORDER BY count DESC
        """)
        print("💳 Répartition par statut de paiement:")
        payment_stats.show()
        
        # Top 10 des villes
        city_stats = spark.sql("""
            SELECT city, COUNT(*) as count
            FROM silver_transactions_final
            WHERE city IS NOT NULL
            GROUP BY city
            ORDER BY count DESC
            LIMIT 10
        """)
        print("🏙️ Top 10 des villes:")
        city_stats.show()
        
        # Taux de retour
        return_stats = spark.sql("""
            SELECT 
                is_returned,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM silver_transactions_final
            GROUP BY is_returned
        """)
        print("🔄 Taux de retour:")
        return_stats.show()
        
        # 10. Optimiser la table Silver
        print("\n🔧 Optimisation de la table Silver...")
        try:
            spark.sql(f"OPTIMIZE delta.`{silver_path}`")
            print("✅ Table Silver optimisée")
        except Exception as opt_error:
            print(f"⚠️ Optimisation échouée: {str(opt_error)}")
        
        print(f"\n🎉 TRANSFORMATION BRONZE -> SILVER COMPLÉTÉE!")
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
    print("=== NETTOYAGE BRONZE VERS SILVER AVEC SPARK SQL ===")
    
    spark = create_spark_session()
    
    try:
        # Chemins des tables
        bronze_path = "s3a://transactions/bronze"
        silver_path = "s3a://transactions/silver"
        
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
