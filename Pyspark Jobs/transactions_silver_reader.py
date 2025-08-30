from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("TransactionsSilverReader") \
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

def analyze_transactions_silver_table_simple(spark, silver_path):
    """Analyse simplifiée de la table Silver des transactions"""
    print("=== LECTURE SIMPLE - TABLE SILVER TRANSACTIONS ===")
    
    try:
        # Vérifier si la table existe
        if not DeltaTable.isDeltaTable(spark, silver_path):
            print(f"❌ Table Silver non trouvée à: {silver_path}")
            return
        
        print(f"✅ Table Silver trouvée à: {silver_path}")
        
        # Charger la table Delta
        silver_df = spark.read.format("delta").load(silver_path)
        
        # 1. NOMBRE TOTAL DE LIGNES
        total_records = silver_df.count()
        print(f"\n📊 NOMBRE TOTAL DE LIGNES: {total_records:,}")
        
        # 2. LISTE DES COLONNES
        print(f"\n📋 COLONNES ({len(silver_df.columns)} au total):")
        for i, col_name in enumerate(silver_df.columns, 1):
            print(f"  {i:2d}. {col_name}")
        
        # 3. ÉCHANTILLON DE 10 LIGNES
        print(f"\n🔍 ÉCHANTILLON DE 10 LIGNES:")
        silver_df.show(10, truncate=False)
        
        print(f"\n✅ Lecture terminée - {total_records:,} transactions au total, {len(silver_df.columns)} colonnes")
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture de la table Silver: {str(e)}")

def main():
    """Fonction principale pour lire la table Silver des transactions"""
    print("=== LECTEUR SIMPLE - TRANSACTIONS SILVER ===")
    
    spark = create_spark_session()
    
    try:
        # Chemin de la table Silver
        silver_path = "s3a://transactions/silver"
        
        # Analyser la table Silver
        analyze_transactions_silver_table_simple(spark, silver_path)
        
    except Exception as e:
        print(f"❌ Erreur dans le processus principal: {str(e)}")
        raise e
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
