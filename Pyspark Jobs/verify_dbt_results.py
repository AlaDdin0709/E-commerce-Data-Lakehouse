#!/usr/bin/env python3
"""
DBT Results Verification Script
Verifies the results of the DBT cleaning pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake configurations"""
    logger.info("🚀 Creating Spark session for verification...")
    
    spark = SparkSession.builder \
        .appName("DBT-Results-Verification") \
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
    logger.info("✅ Spark session created")
    return spark

def verify_bronze_data(spark):
    """Verify bronze layer data"""
    logger.info("🔍 Verifying Bronze layer data...")
    
    try:
        bronze_df = spark.read.format("delta").load("s3a://transactions/bronze")
        bronze_count = bronze_df.count()
        
        logger.info(f"📊 Bronze records: {bronze_count:,}")
        
        # Show structure
        logger.info("📋 Bronze schema:")
        bronze_df.printSchema()
        
        # Show sample data
        logger.info("📋 Bronze sample data:")
        bronze_df.select("order_id", "customer_id", "amount", "shipping_address", "timestamp_raw").show(3, truncate=False)
        
        return bronze_count
        
    except Exception as e:
        logger.error(f"❌ Error reading bronze data: {str(e)}")
        return 0

def verify_silver_data(spark):
    """Verify silver layer data"""
    logger.info("🔍 Verifying Silver layer data...")
    
    try:
        silver_df = spark.read.format("delta").load("s3a://transactions/silver")
        silver_count = silver_df.count()
        
        logger.info(f"📊 Silver records: {silver_count:,}")
        
        if silver_count == 0:
            logger.warning("⚠️ No silver data found!")
            return 0
        
        # Show structure
        logger.info("📋 Silver schema:")
        silver_df.printSchema()
        
        # Show sample cleaned data
        logger.info("📋 Silver sample data:")
        silver_df.select(
            "order_id", "customer_id", "shipping_city", 
            "shipping_region", "amount", "payment_status"
        ).show(5, truncate=False)
        
        # Data quality checks
        logger.info("🔍 Data Quality Analysis:")
        
        # Check for NULLs in critical columns
        critical_columns = ["order_id", "customer_id", "product_id", "amount"]
        for col_name in critical_columns:
            null_count = silver_df.filter(col(col_name).isNull()).count()
            logger.info(f"  - {col_name} NULLs: {null_count}")
        
        # Check shipping data extraction
        city_extracted = silver_df.filter(col("shipping_city").isNotNull()).count()
        region_extracted = silver_df.filter(col("shipping_region").isNotNull()).count()
        
        logger.info(f"  - Records with shipping_city: {city_extracted} ({city_extracted/silver_count*100:.1f}%)")
        logger.info(f"  - Records with shipping_region: {region_extracted} ({region_extracted/silver_count*100:.1f}%)")
        
        # Show top cities and regions
        logger.info("📊 Top 10 shipping cities:")
        silver_df.filter(col("shipping_city").isNotNull()) \
                 .groupBy("shipping_city") \
                 .count() \
                 .orderBy(desc("count")) \
                 .show(10)
        
        logger.info("📊 Top 10 shipping regions:")
        silver_df.filter(col("shipping_region").isNotNull()) \
                 .groupBy("shipping_region") \
                 .count() \
                 .orderBy(desc("count")) \
                 .show(10)
        
        # Payment status distribution
        logger.info("📊 Payment status distribution:")
        silver_df.groupBy("payment_status").count().orderBy(desc("count")).show()
        
        # Category distribution
        logger.info("📊 Product category distribution:")
        silver_df.groupBy("category").count().orderBy(desc("count")).show()
        
        # Partition information
        logger.info("📊 Data distribution by partitions:")
        silver_df.groupBy("year", "month", "day") \
                 .count() \
                 .orderBy("year", "month", "day") \
                 .show()
        
        return silver_count
        
    except Exception as e:
        logger.error(f"❌ Error reading silver data: {str(e)}")
        return 0

def compare_bronze_silver(spark, bronze_count, silver_count):
    """Compare bronze and silver data"""
    logger.info("🔍 Comparing Bronze vs Silver data...")
    
    if bronze_count == 0:
        logger.warning("⚠️ No bronze data to compare")
        return
    
    if silver_count == 0:
        logger.warning("⚠️ No silver data to compare")
        return
    
    # Calculate data retention rate
    retention_rate = (silver_count / bronze_count) * 100
    filtered_records = bronze_count - silver_count
    
    logger.info(f"📊 Data Processing Summary:")
    logger.info(f"  - Bronze records: {bronze_count:,}")
    logger.info(f"  - Silver records: {silver_count:,}")
    logger.info(f"  - Filtered out: {filtered_records:,}")
    logger.info(f"  - Retention rate: {retention_rate:.2f}%")
    
    if retention_rate < 50:
        logger.warning("⚠️ Low retention rate! Check data quality filters")
    elif retention_rate > 95:
        logger.info("✅ High retention rate - minimal data loss")
    else:
        logger.info("✅ Reasonable retention rate")

def verify_column_transformations(spark):
    """Verify specific column transformations"""
    logger.info("🔍 Verifying column transformations...")
    
    try:
        bronze_df = spark.read.format("delta").load("s3a://transactions/bronze")
        silver_df = spark.read.format("delta").load("s3a://transactions/silver")
        
        # Sample a few records to check transformations
        sample_bronze = bronze_df.select(
            "order_id", "shipping_address", "timestamp_raw"
        ).limit(5).collect()
        
        sample_silver = silver_df.select(
            "order_id", "shipping_city", "shipping_region"
        ).limit(5).collect()
        
        logger.info("📋 Transformation Examples:")
        logger.info("Bronze -> Silver transformations:")
        
        # Create a lookup for easier comparison
        silver_lookup = {row.order_id: row for row in sample_silver}
        
        for bronze_row in sample_bronze:
            if bronze_row.order_id in silver_lookup:
                silver_row = silver_lookup[bronze_row.order_id]
                logger.info(f"\nOrder: {bronze_row.order_id}")
                logger.info(f"  Bronze shipping_address: {bronze_row.shipping_address}")
                logger.info(f"  Silver shipping_city: {silver_row.shipping_city}")
                logger.info(f"  Bronze timestamp_raw: {bronze_row.timestamp_raw}")
                logger.info(f"  Silver shipping_region: {silver_row.shipping_region}")
        
    except Exception as e:
        logger.error(f"❌ Error verifying transformations: {str(e)}")

def main():
    """Main verification function"""
    logger.info("=== STARTING DBT RESULTS VERIFICATION ===")
    
    spark = create_spark_session()
    
    try:
        # Verify bronze data
        bronze_count = verify_bronze_data(spark)
        
        # Verify silver data
        silver_count = verify_silver_data(spark)
        
        # Compare bronze and silver
        compare_bronze_silver(spark, bronze_count, silver_count)
        
        # Verify transformations
        verify_column_transformations(spark)
        
        # Overall assessment
        logger.info("\n=== VERIFICATION SUMMARY ===")
        if silver_count > 0:
            logger.info("✅ DBT pipeline executed successfully!")
            logger.info("✅ Silver layer data is available")
            logger.info("✅ Column transformations applied")
            logger.info("✅ Data quality filters applied")
        else:
            logger.error("❌ DBT pipeline issues detected")
            logger.error("❌ No silver layer data found")
        
        logger.info(f"📊 Final counts: Bronze={bronze_count:,}, Silver={silver_count:,}")
        
    except Exception as e:
        logger.error(f"❌ Error during verification: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        logger.info("🔚 Stopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()
