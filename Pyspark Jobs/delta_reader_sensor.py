from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import sys

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("IoTSensorsDeltaTableViewer") \
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

def display_iot_sensors_table_info(spark, delta_path):
    """Afficher les informations essentielles de la table Delta Lake IoT Sensors"""
    print("=" * 80)
    print("🤖 INFORMATIONS DE LA TABLE DELTA LAKE IOT SENSORS")
    print("=" * 80)
    
    try:
        # Vérifier que la table Delta existe
        if not DeltaTable.isDeltaTable(spark, delta_path):
            print(f"❌ ERREUR: Pas une table Delta valide au chemin {delta_path}")
            return False
        
        # Charger la table Delta
        delta_table = DeltaTable.forPath(spark, delta_path)
        df = delta_table.toDF()
        
        # 1. Nombre total de lignes
        print("📊 NOMBRE TOTAL DE LIGNES:")
        total_rows = df.count()
        print(f"   📈 Total: {total_rows:,} enregistrements de capteurs")
        
        # 2. Liste des colonnes
        table_columns = df.columns
        print(f"\n📋 COLONNES DE LA TABLE ({len(table_columns)} colonnes):")
        for i, column_name in enumerate(table_columns, 1):
            print(f"   {i:2d}. {column_name}")
        
        # 3. Statistiques spécifiques IoT
        print(f"\n🔧 STATISTIQUES SPÉCIFIQUES IOT SENSORS:")
        print("-" * 60)
        
        # Nombre de capteurs uniques
        if "sensor_id" in df.columns:
            sensor_count = df.select("sensor_id").filter(col("sensor_id").isNotNull()).distinct().count()
            print(f"   📡 Capteurs uniques: {sensor_count}")
        
        # Types de devices
        if "device_type" in df.columns:
            print(f"\n   🔧 Répartition par type de capteur:")
            device_stats = df.groupBy("device_type").count().orderBy(desc("count"))
            device_stats.show(truncate=False)
        
        # Plages de températures
        if "temperature" in df.columns:
            temp_stats = df.select("temperature").filter(col("temperature").isNotNull()).describe()
            print(f"\n   🌡️ Statistiques des températures:")
            temp_stats.show()
        
        # Niveaux de batterie
        if "battery_level" in df.columns:
            battery_stats = df.select("battery_level").filter(col("battery_level").isNotNull()).describe()
            print(f"\n   🔋 Statistiques des niveaux de batterie:")
            battery_stats.show()
        
        # 4. Partitions
        print(f"\n📁 STRUCTURE DES PARTITIONS:")
        print("-" * 60)
        partition_cols = ["partition_year", "partition_month", "partition_day"]
        if all(column_name in df.columns for column_name in partition_cols):
            partition_info = df.groupBy("partition_year", "partition_month", "partition_day").count() \
                              .orderBy("partition_year", "partition_month", "partition_day")
            partition_info.show()
        
        # 5. Données récentes
        print(f"\n📊 ÉCHANTILLON DES 10 DERNIERS ENREGISTREMENTS:")
        print("-" * 80)
        recent_columns = ["sensor_id", "device_type", "temperature", "humidity", "battery_level", "kafka_timestamp"]
        available_recent_cols = [column_name for column_name in recent_columns if column_name in df.columns]
        
        if "kafka_timestamp" in df.columns:
            df.select(*available_recent_cols) \
              .filter(col("sensor_id").isNotNull()) \
              .orderBy(desc("kafka_timestamp")) \
              .show(10, truncate=False)
        else:
            df.select(*available_recent_cols) \
              .filter(col("sensor_id").isNotNull()) \
              .show(10, truncate=False)
        
        # 6. Alertes IoT (capteurs avec batterie faible)
        if "battery_level" in df.columns:
            print(f"\n⚠️ ALERTES IOT - CAPTEURS AVEC BATTERIE FAIBLE (<20%):")
            print("-" * 60)
            low_battery = df.filter(col("battery_level") < 20) \
                           .select("sensor_id", "device_type", "battery_level", "kafka_timestamp") \
                           .orderBy("battery_level")
            
            low_battery_count = low_battery.count()
            if low_battery_count > 0:
                print(f"   🔴 {low_battery_count} capteurs avec batterie faible détectés!")
                low_battery.show(10, truncate=False)
            else:
                print("   ✅ Tous les capteurs ont un niveau de batterie acceptable")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'affichage des informations IoT: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Fonction principale pour afficher les informations essentielles de la table Delta Lake IoT Sensors"""
    print("🚀 AFFICHAGE SIMPLIFIÉ DE LA TABLE DELTA LAKE IOT SENSORS")
    
    # Chemin de la table Delta IoT Sensors
    delta_table_path = "s3a://iot-sensors/bronze"
    
    spark = create_spark_session()
    
    try:
        # Afficher les informations essentielles
        success = display_iot_sensors_table_info(spark, delta_table_path)
        
        if success:
            print("\n✅ Affichage IoT Sensors terminé avec succès!")
            print(f"📍 Table accessible via: {delta_table_path}")
        else:
            print("❌ Impossible de charger la table Delta IoT Sensors. Vérifiez qu'elle existe.")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'affichage IoT Sensors: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
