from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import re
from datetime import datetime
import math  # Utiliser math.round au lieu de round() pour éviter le conflit

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("IoTSensorsBronzeToSilverCleaner") \
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
        bronze_df.createOrReplaceTempView("bronze_iot_sensors")
        print("✅ Table Bronze IoT Sensors enregistrée comme vue temporaire")
        
        # Vérifier si la table Silver existe
        if DeltaTable.isDeltaTable(spark, silver_path):
            silver_df = spark.read.format("delta").load(silver_path)
            silver_df.createOrReplaceTempView("silver_iot_sensors")
            print("✅ Table Silver IoT Sensors existante enregistrée comme vue temporaire")
            return True
        else:
            print("ℹ️ Table Silver IoT Sensors n'existe pas encore - première exécution")
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
            FROM silver_iot_sensors
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

def check_sensor_id_uniqueness(spark):
    """Vérifier l'unicité des sensor_id dans les nouvelles données et compter les doublons à éliminer"""
    try:
        duplicates_sql = """
            SELECT sensor_id, COUNT(*) as count
            FROM bronze_iot_sensors
            WHERE sensor_id IS NOT NULL AND TRIM(sensor_id) != ''
            GROUP BY sensor_id
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 10
        """
        
        duplicates_df = spark.sql(duplicates_sql)
        duplicate_count = duplicates_df.count()
        
        if duplicate_count > 0:
            print(f"⚠️ ATTENTION: {duplicate_count} sensor_id avec doublons trouvés (seul le plus récent sera conservé):")
            duplicates_df.show(10, truncate=False)
            
            # Calculer le nombre total de doublons à éliminer
            total_duplicates_sql = """
                SELECT SUM(count - 1) as total_to_remove
                FROM (
                    SELECT sensor_id, COUNT(*) as count
                    FROM bronze_iot_sensors
                    WHERE sensor_id IS NOT NULL AND TRIM(sensor_id) != ''
                    GROUP BY sensor_id
                    HAVING COUNT(*) > 1
                ) duplicates
            """
            
            result = spark.sql(total_duplicates_sql).collect()
            if result and result[0].total_to_remove:
                total_to_remove = result[0].total_to_remove
                print(f"🗑️ {total_to_remove} enregistrements dupliqués seront éliminés")
            
        else:
            print("✅ Aucun sensor_id dupliqué trouvé")
            
        return duplicate_count
        
    except Exception as e:
        print(f"⚠️ Erreur lors de la vérification des doublons: {str(e)}")
        return 0

def create_silver_table_sql(spark, silver_path, max_timestamp_silver):
    """Créer la requête SQL pour nettoyer et transformer les données IoT sensors"""
    
    # Condition pour le traitement incrémental
    incremental_condition = ""
    if max_timestamp_silver:
        incremental_condition = f"AND processing_timestamp > '{max_timestamp_silver}'"
    
    # Requête SQL avec déduplication par sensor_id (garde le plus récent)
    cleaning_sql = f"""
        WITH ranked_data AS (
            SELECT 
                -- Nettoyage et validation du sensor_id (doit être unique et non vide)
                CASE 
                    WHEN sensor_id IS NOT NULL AND TRIM(sensor_id) != '' 
                    THEN TRIM(sensor_id)
                    ELSE NULL 
                END as sensor_id,
                device_type,
                -- Validation directe des températures (-50°C à 150°C)
                CASE 
                    WHEN temperature IS NOT NULL 
                        AND CAST(temperature AS DOUBLE) BETWEEN -50.0 AND 150.0 
                    THEN ROUND(CAST(temperature AS DOUBLE), 2)
                    ELSE NULL 
                END as temperature,
                -- Validation directe de l'humidité (0% à 100%)
                CASE 
                    WHEN humidity IS NOT NULL 
                        AND CAST(humidity AS DOUBLE) BETWEEN 0.0 AND 100.0 
                    THEN ROUND(CAST(humidity AS DOUBLE), 2)
                    ELSE NULL 
                END as humidity,
                -- Validation directe du niveau de batterie (0% à 100%)
                CASE 
                    WHEN battery_level IS NOT NULL 
                        AND CAST(battery_level AS DOUBLE) BETWEEN 0.0 AND 100.0 
                    THEN ROUND(CAST(battery_level AS DOUBLE), 2)
                    ELSE NULL 
                END as battery_level,
                -- Nettoyage de la version firmware
                CASE 
                    WHEN firmware_version IS NOT NULL AND TRIM(firmware_version) != '' 
                    THEN TRIM(firmware_version)
                    ELSE NULL 
                END as firmware_version,
                processing_timestamp,
                partition_year,
                partition_month,
                partition_day,
                current_timestamp() as silver_load_timestamp,
                -- Ranger par processing_timestamp DESC pour garder le plus récent
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(sensor_id) 
                    ORDER BY processing_timestamp DESC
                ) as rn
            FROM bronze_iot_sensors
            WHERE device_type IS NOT NULL
                AND processing_timestamp IS NOT NULL
                AND partition_year IS NOT NULL
                AND partition_month IS NOT NULL
                AND partition_day IS NOT NULL
                AND sensor_id IS NOT NULL
                AND TRIM(sensor_id) != ''
                {incremental_condition}
        )
        SELECT 
            sensor_id,
            device_type,
            temperature,
            humidity,
            battery_level,
            firmware_version,
            processing_timestamp,
            partition_year,
            partition_month,
            partition_day,
            silver_load_timestamp
        FROM ranked_data
        WHERE rn = 1
    """
    
    return cleaning_sql

def execute_bronze_to_silver_transformation(spark, bronze_path, silver_path):
    """Exécuter la transformation complète Bronze vers Silver pour IoT Sensors"""
    
    print("=== DÉMARRAGE DE LA TRANSFORMATION BRONZE VERS SILVER IOT SENSORS ===")
    
    # 1. Enregistrer les tables Delta
    silver_exists = register_delta_tables(spark, bronze_path, silver_path)
    
    # 2. Vérifier l'unicité des sensor_id
    duplicate_count = check_sensor_id_uniqueness(spark)
    
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
        
        # 6. Vérifier l'unicité des sensor_id dans les données nettoyées
        print("\n🔍 VÉRIFICATION DE L'UNICITÉ DES SENSOR_ID APRÈS NETTOYAGE:")
        unique_sensors_sql = """
            SELECT COUNT(DISTINCT sensor_id) as unique_sensors,
                   COUNT(*) as total_records
            FROM (
                SELECT * FROM (""" + cleaning_sql + """) as cleaned_data
            ) t
        """
        
        unique_result = spark.sql(unique_sensors_sql).collect()[0]
        unique_sensors = unique_result.unique_sensors
        total_records = unique_result.total_records
        
        print(f"📊 Sensors uniques: {unique_sensors}")
        print(f"📊 Total enregistrements: {total_records}")
        
        if unique_sensors == total_records:
            print("✅ Tous les sensor_id sont maintenant uniques après déduplication")
        else:
            print(f"❌ ERREUR: Déduplication échouée - {total_records - unique_sensors} doublons restants")
        
        # 7. Afficher un échantillon des données nettoyées
        print("\n🔍 ÉCHANTILLON DES DONNÉES NETTOYÉES:")
        sample_df = cleaned_df.limit(5)
        sample_df.select("sensor_id", "device_type", "temperature", "humidity", "battery_level", "firmware_version").show(truncate=False)
        
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
        
        # Comptage final et vérification des sensor_id uniques en Silver
        silver_final_df = spark.read.format("delta").load(silver_path)
        total_silver_count = silver_final_df.count()
        
        # Vérifier l'unicité finale en Silver
        silver_final_df.createOrReplaceTempView("final_silver")
        final_unique_sql = """
            SELECT COUNT(DISTINCT sensor_id) as unique_sensors,
                   COUNT(*) as total_records
            FROM final_silver
        """
        
        final_result = spark.sql(final_unique_sql).collect()[0]
        final_unique_sensors = final_result.unique_sensors
        final_total_records = final_result.total_records
        
        print(f"📈 Total des enregistrements en Silver: {total_silver_count:,}")
        print(f"🔑 Sensors uniques en Silver: {final_unique_sensors:,}")
        
        if final_unique_sensors == final_total_records:
            print("✅ Tous les sensor_id sont uniques en Silver - Déduplication réussie!")
        else:
            print(f"❌ ERREUR: {final_total_records - final_unique_sensors} doublons présents en Silver")
        
        print(f"\n🎉 TRANSFORMATION BRONZE -> SILVER IOT SENSORS COMPLÉTÉE!")
        print(f"📍 Table Silver disponible à: {silver_path}")
        print(f"📊 {new_records_count:,} nouveaux enregistrements ajoutés")
        print(f"📈 Total des enregistrements: {total_silver_count:,}")
        print(f"🔑 Sensors uniques: {final_unique_sensors:,}")
        
        # Résumé des colonnes conservées
        print(f"\n📋 COLONNES CONSERVÉES EN SILVER:")
        print("   1. sensor_id (unique identifier)")
        print("   2. device_type")
        print("   3. temperature")
        print("   4. humidity") 
        print("   5. battery_level")
        print("   6. firmware_version")
        print("   7. processing_timestamp")
        print("   8. partition_year")
        print("   9. partition_month")
        print("  10. partition_day")
        print("  11. silver_load_timestamp")
        
    except Exception as e:
        print(f"❌ Erreur lors de la transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e

def main():
    """Fonction principale"""
    print("=== NETTOYAGE BRONZE VERS SILVER IOT SENSORS AVEC SPARK SQL ===")
    
    spark = create_spark_session()
    
    try:
        # Chemins des tables
        bronze_path = "s3a://iot-sensors/bronze"
        silver_path = "s3a://iot-sensors/silver"
        
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
