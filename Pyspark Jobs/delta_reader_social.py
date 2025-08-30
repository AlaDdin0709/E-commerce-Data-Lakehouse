from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import sys

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("SocialMediaDeltaTableViewer") \
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

def display_simple_table_info(spark, delta_path):
    """Afficher les informations essentielles de la table Delta Lake"""
    print("=" * 80)
    print("🗂️  INFORMATIONS DE LA TABLE DELTA LAKE SOCIAL MEDIA")
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
        print(f"   📈 Total: {total_rows:,} lignes")
        
        # 2. Liste des colonnes
        columns = df.columns
        print(f"\n📋 COLONNES DE LA TABLE ({len(columns)} colonnes):")
        for i, col in enumerate(columns, 1):
            print(f"   {i:2d}. {col}")
        
        # 3. Exemple des 10 premières lignes
        print(f"\n📋 EXEMPLE DES 10 PREMIÈRES LIGNES:")
        print("-" * 80)
        df.show(10, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'affichage des informations: {str(e)}")
        return False

def main():
    """Fonction principale pour afficher les informations essentielles de la table Delta Lake"""
    print("🚀 AFFICHAGE SIMPLIFIÉ DE LA TABLE DELTA LAKE SOCIAL MEDIA")
    
    # Chemin de la table Delta
    delta_table_path = "s3a://social-media/bronze"
    
    spark = create_spark_session()
    
    try:
        # Afficher les informations essentielles
        success = display_simple_table_info(spark, delta_table_path)
        
        if success:
            print("\n✅ Affichage terminé avec succès!")
        else:
            print("❌ Impossible de charger la table Delta. Vérifiez qu'elle existe.")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'affichage: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
