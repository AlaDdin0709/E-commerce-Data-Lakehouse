from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import sys

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("CustomerImagesDeltaTableViewer") \
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

def display_customer_images_table_info(spark, delta_path):
    """Afficher les informations essentielles de la table Delta Lake Customer Images"""
    print("=" * 80)
    print("🖼️  INFORMATIONS DE LA TABLE DELTA LAKE CUSTOMER IMAGES")
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
        print(f"   📈 Total: {total_rows:,} images")
        
        # 2. Liste des colonnes
        table_columns = df.columns
        print(f"\n📋 COLONNES DE LA TABLE ({len(table_columns)} colonnes):")
        for i, column_name in enumerate(table_columns, 1):
            print(f"   {i:2d}. {column_name}")
        
        # 3. Statistiques spécifiques aux images
        print(f"\n🖼️ STATISTIQUES SPÉCIFIQUES CUSTOMER IMAGES:")
        print("-" * 60)
        
        # Nombre de clients uniques
        if "customer_id" in df.columns:
            customer_count = df.select("customer_id").filter(col("customer_id").isNotNull()).distinct().count()
            print(f"   👥 Clients uniques: {customer_count}")
        
        # Nombre de commandes uniques
        if "extracted_order_id" in df.columns:
            order_count = df.select("extracted_order_id").filter(col("extracted_order_id").isNotNull()).distinct().count()
            print(f"   📦 Commandes uniques: {order_count}")
        elif "order_id" in df.columns:
            order_count = df.select("order_id").filter(col("order_id").isNotNull()).distinct().count()
            print(f"   📦 Commandes uniques: {order_count}")
        
        # Types d'images
        if "image_type" in df.columns:
            print(f"\n   📸 Répartition par type d'image:")
            image_type_stats = df.groupBy("image_type").count().orderBy(desc("count"))
            image_type_stats.show(truncate=False)
        
        # Formats d'images
        if "image_format" in df.columns:
            print(f"\n   🎨 Répartition par format d'image:")
            format_stats = df.groupBy("image_format").count().orderBy(desc("count"))
            format_stats.show(truncate=False)
        
        # Statistiques des tailles de fichiers
        if "file_size" in df.columns:
            size_stats = df.select("file_size", "image_size_mb").filter(col("file_size").isNotNull())
            if size_stats.count() > 0:
                print(f"\n   📏 Statistiques des tailles de fichiers:")
                size_stats.describe().show()
        
        # Statistiques des dimensions
        if "width" in df.columns and "height" in df.columns:
            dimension_stats = df.select("width", "height").filter(col("width").isNotNull() & col("height").isNotNull())
            if dimension_stats.count() > 0:
                print(f"\n   📐 Statistiques des dimensions d'images:")
                dimension_stats.describe().show()
        
        # 4. Partitions
        print(f"\n📁 STRUCTURE DES PARTITIONS:")
        print("-" * 60)
        partition_cols = ["partition_year", "partition_month", "partition_day"]
        if all(column_name in df.columns for column_name in partition_cols):
            partition_info = df.groupBy("partition_year", "partition_month", "partition_day").count() \
                              .orderBy("partition_year", "partition_month", "partition_day")
            partition_info.show()
        
        # 5. Images récentes
        print(f"\n📊 ÉCHANTILLON DES 10 DERNIÈRES IMAGES:")
        print("-" * 80)
        recent_columns = ["image_id", "s3_path", "customer_id", "extracted_order_id", "image_format", "upload_timestamp"]
        available_recent_cols = [column_name for column_name in recent_columns if column_name in df.columns]
        
        if "upload_timestamp" in df.columns:
            df.select(*available_recent_cols) \
              .filter(col("image_id").isNotNull()) \
              .orderBy(desc("upload_timestamp")) \
              .show(10, truncate=False)
        elif "kafka_timestamp" in df.columns:
            df.select(*available_recent_cols) \
              .filter(col("image_id").isNotNull()) \
              .orderBy(desc("kafka_timestamp")) \
              .show(10, truncate=False)
        else:
            df.select(*available_recent_cols) \
              .filter(col("image_id").isNotNull()) \
              .show(10, truncate=False)
        
        # 6. Analyse de la qualité des images
        if "quality_score" in df.columns:
            print(f"\n⭐ ANALYSE DE LA QUALITÉ DES IMAGES:")
            print("-" * 60)
            quality_stats = df.select("quality_score").filter(col("quality_score").isNotNull())
            if quality_stats.count() > 0:
                quality_stats.describe().show()
                
                # Images de faible qualité
                low_quality = df.filter(col("quality_score") < 0.5) \
                               .select("image_id", "customer_id", "quality_score", "s3_path") \
                               .orderBy("quality_score")
                
                low_quality_count = low_quality.count()
                if low_quality_count > 0:
                    print(f"   🔴 {low_quality_count} images de faible qualité détectées!")
                    low_quality.show(5, truncate=False)
                else:
                    print("   ✅ Toutes les images ont une qualité acceptable")
        
        # 7. Images par client (top 10)
        if "customer_id" in df.columns:
            print(f"\n👥 TOP 10 CLIENTS AVEC LE PLUS D'IMAGES:")
            print("-" * 60)
            top_customers = df.groupBy("customer_id").count() \
                             .orderBy(desc("count")) \
                             .limit(10)
            top_customers.show(truncate=False)
        
        # 8. Échantillon détaillé
        print(f"\n📋 EXEMPLE DÉTAILLÉ DES 5 PREMIÈRES IMAGES:")
        print("-" * 80)
        sample_columns = ["image_id", "s3_path", "customer_id", "image_format", "file_size", "upload_timestamp"]
        available_sample_cols = [col for col in sample_columns if col in df.columns]
        df.select(*available_sample_cols).show(5, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'affichage des informations images: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Fonction principale pour afficher les informations essentielles de la table Delta Lake Customer Images"""
    print("🚀 AFFICHAGE SIMPLIFIÉ DE LA TABLE DELTA LAKE CUSTOMER IMAGES")
    
    # Chemin de la table Delta Customer Images
    delta_table_path = "s3a://customer-images/bronze"
    
    spark = create_spark_session()
    
    try:
        # Afficher les informations essentielles
        success = display_customer_images_table_info(spark, delta_table_path)
        
        if success:
            print("\n✅ Affichage Customer Images terminé avec succès!")
            print(f"📍 Table accessible via: {delta_table_path}")
        else:
            print("❌ Impossible de charger la table Delta Customer Images. Vérifiez qu'elle existe.")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'affichage Customer Images: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
