# explore_global_dw_aggregates.py - Explore Global DW Aggregation Tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from delta import DeltaTable

def create_spark_session():
    """Create Spark session with necessary configurations."""
    return SparkSession.builder \
        .appName("ExploreGlobalDW_Aggregates") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-api.minio.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def explore_customer_360(spark, global_dw_path):
    """Explore the Customer 360 aggregate table."""
    print("\n" + "="*80)
    print("📊 EXPLORING GLOBAL_AGG_CUSTOMER_360")
    print("="*80)
    
    try:
        customer_360_df = spark.read.format("delta").load(f"{global_dw_path}/global_agg_customer_360")
        
        print(f"\n📋 SCHEMA:")
        customer_360_df.printSchema()
        
        print(f"\n📊 BASIC STATISTICS:")
        print(f"   • Total Customers: {customer_360_df.count():,}")
        print(f"   • Columns: {len(customer_360_df.columns)}")
        
        print(f"\n📈 TOP 10 CUSTOMERS BY CUSTOMER SCORE:")
        customer_360_df.select(
            "customer_id", 
            "total_activities", 
            "total_transactions", 
            "total_image_uploads", 
            "total_social_posts", 
            "total_spent", 
            "customer_score"
        ).orderBy(desc("customer_score")).show(10, truncate=False)
        
        print(f"\n💰 TOP 10 CUSTOMERS BY TOTAL SPENT:")
        customer_360_df.select(
            "customer_id", 
            "total_spent", 
            "total_transactions", 
            "avg_transaction_value", 
            "customer_score"
        ).orderBy(desc("total_spent")).show(10, truncate=False)
        
        print(f"\n🏆 TOP 10 MOST ACTIVE CUSTOMERS:")
        customer_360_df.select(
            "customer_id", 
            "total_activities", 
            "total_transactions", 
            "total_image_uploads", 
            "total_social_posts", 
            "first_activity_date", 
            "last_activity_date"
        ).orderBy(desc("total_activities")).show(10, truncate=False)
        
        print(f"\n📊 CUSTOMER ACTIVITY DISTRIBUTION:")
        activity_stats = customer_360_df.agg(
            spark_sum("total_transactions").alias("total_transactions_all"),
            spark_sum("total_image_uploads").alias("total_image_uploads_all"),
            spark_sum("total_social_posts").alias("total_social_posts_all"),
            spark_sum("total_spent").alias("total_revenue_all"),
            avg("customer_score").alias("avg_customer_score"),
            spark_max("customer_score").alias("max_customer_score"),
            spark_min("customer_score").alias("min_customer_score")
        ).collect()[0]
        
        print(f"   • Total Transactions Across All Customers: {activity_stats['total_transactions_all']:,}")
        print(f"   • Total Image Uploads Across All Customers: {activity_stats['total_image_uploads_all']:,}")
        print(f"   • Total Social Posts Across All Customers: {activity_stats['total_social_posts_all']:,}")
        print(f"   • Total Revenue Across All Customers: ${activity_stats['total_revenue_all']:,.2f}")
        print(f"   • Average Customer Score: {activity_stats['avg_customer_score']:.2f}")
        print(f"   • Max Customer Score: {activity_stats['max_customer_score']}")
        print(f"   • Min Customer Score: {activity_stats['min_customer_score']}")
        
        print(f"\n🎯 CUSTOMER SEGMENTS BY SCORE:")
        customer_360_df.createOrReplaceTempView("customer_360")
        segments_df = spark.sql("""
            SELECT 
                CASE 
                    WHEN customer_score >= 50 THEN 'VIP (50+)'
                    WHEN customer_score >= 20 THEN 'Active (20-49)'
                    WHEN customer_score >= 10 THEN 'Regular (10-19)'
                    WHEN customer_score >= 5 THEN 'Occasional (5-9)'
                    ELSE 'New (0-4)'
                END as segment,
                COUNT(*) as customer_count,
                AVG(total_spent) as avg_spent_per_segment,
                SUM(total_spent) as total_segment_revenue
            FROM customer_360 
            GROUP BY 1 
            ORDER BY MIN(customer_score) DESC
        """)
        segments_df.show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error exploring customer_360: {e}")

def explore_monthly_summary(spark, global_dw_path):
    """Explore the Monthly Summary aggregate table."""
    print("\n" + "="*80)
    print("📅 EXPLORING GLOBAL_AGG_MONTHLY_SUMMARY")
    print("="*80)
    
    try:
        monthly_summary_df = spark.read.format("delta").load(f"{global_dw_path}/global_agg_monthly_summary")
        
        print(f"\n📋 SCHEMA:")
        monthly_summary_df.printSchema()
        
        print(f"\n📊 BASIC STATISTICS:")
        print(f"   • Total Monthly Records: {monthly_summary_df.count():,}")
        print(f"   • Columns: {len(monthly_summary_df.columns)}")
        
        print(f"\n📈 ALL MONTHLY SUMMARY DATA:")
        monthly_summary_df.orderBy("year", "month").show(50, truncate=False)
        
        print(f"\n💰 REVENUE TRENDS BY MONTH:")
        monthly_summary_df.select(
            "year", "month", "month_name", 
            "monthly_revenue", "monthly_orders", 
            "avg_monthly_order_value"
        ).orderBy("year", "month").show(50, truncate=False)
        
        print(f"\n📱 DIGITAL ENGAGEMENT TRENDS:")
        # Check if digital columns exist
        columns = monthly_summary_df.columns
        digital_cols = ["year", "month", "month_name"]
        
        if "monthly_image_uploads" in columns:
            digital_cols.append("monthly_image_uploads")
        if "monthly_sensor_readings" in columns:
            digital_cols.append("monthly_sensor_readings")
        if "avg_monthly_temperature" in columns:
            digital_cols.extend(["avg_monthly_temperature", "avg_monthly_humidity", "avg_monthly_battery_level"])
        
        monthly_summary_df.select(*digital_cols).orderBy("year", "month").show(50, truncate=False)
        
        print(f"\n🏆 BEST PERFORMING MONTHS:")
        monthly_summary_df.select(
            "year", "month", "month_name", 
            "monthly_revenue", "monthly_orders"
        ).orderBy(desc("monthly_revenue")).show(10, truncate=False)
        
        print(f"\n📊 MONTHLY BUSINESS SUMMARY STATISTICS:")
        summary_stats = monthly_summary_df.agg(
            spark_sum("monthly_revenue").alias("total_revenue_all_months"),
            spark_sum("monthly_orders").alias("total_orders_all_months"),
            avg("monthly_revenue").alias("avg_monthly_revenue"),
            avg("monthly_orders").alias("avg_monthly_orders"),
            spark_max("monthly_revenue").alias("best_month_revenue"),
            spark_min("monthly_revenue").alias("worst_month_revenue")
        ).collect()[0]
        
        print(f"   • Total Revenue (All Months): ${summary_stats['total_revenue_all_months']:,.2f}")
        print(f"   • Total Orders (All Months): {summary_stats['total_orders_all_months']:,}")
        print(f"   • Average Monthly Revenue: ${summary_stats['avg_monthly_revenue']:,.2f}")
        print(f"   • Average Monthly Orders: {summary_stats['avg_monthly_orders']:.0f}")
        print(f"   • Best Month Revenue: ${summary_stats['best_month_revenue']:,.2f}")
        print(f"   • Worst Month Revenue: ${summary_stats['worst_month_revenue']:,.2f}")
        
        # Year over Year if multiple years
        print(f"\n📈 YEARLY COMPARISON:")
        yearly_comparison = monthly_summary_df.groupBy("year").agg(
            spark_sum("monthly_revenue").alias("yearly_revenue"),
            spark_sum("monthly_orders").alias("yearly_orders"),
            avg("avg_monthly_order_value").alias("avg_order_value_year"),
            count("*").alias("months_with_data")
        ).orderBy("year")
        yearly_comparison.show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error exploring monthly_summary: {e}")

def explore_operational_daily(spark, global_dw_path):
    """Quick peek at the operational daily fact table."""
    print("\n" + "="*80)
    print("📊 QUICK PEEK: GLOBAL_FACT_OPERATIONAL_DAILY")
    print("="*80)
    
    try:
        operational_df = spark.read.format("delta").load(f"{global_dw_path}/global_fact_operational_daily")
        
        print(f"\n📋 SCHEMA:")
        operational_df.printSchema()
        
        print(f"\n📊 SAMPLE DATA (Latest 20 Days):")
        operational_df.join(
            spark.read.format("delta").load(f"{global_dw_path}/global_dim_date").select("date_key", "full_date"),
            "date_key"
        ).select("full_date", "daily_revenue", "daily_orders", "daily_image_uploads") \
         .orderBy(desc("full_date")).show(20, truncate=False)
        
        print(f"\n📈 OPERATIONAL SUMMARY:")
        op_stats = operational_df.agg(
            spark_sum("daily_revenue").alias("total_revenue"),
            spark_sum("daily_orders").alias("total_orders"),
            spark_sum("daily_image_uploads").alias("total_images"),
            avg("daily_revenue").alias("avg_daily_revenue"),
            count("*").alias("total_days")
        ).collect()[0]
        
        print(f"   • Total Days of Data: {op_stats['total_days']:,}")
        print(f"   • Total Revenue: ${op_stats['total_revenue']:,.2f}")
        print(f"   • Total Orders: {op_stats['total_orders']:,}")
        print(f"   • Total Image Uploads: {op_stats['total_images']:,}")
        print(f"   • Average Daily Revenue: ${op_stats['avg_daily_revenue']:,.2f}")
        
    except Exception as e:
        print(f"❌ Error exploring operational_daily: {e}")

def main():
    """Main function to explore Global DW aggregation tables."""
    print("""
    🔍 GLOBAL DATA WAREHOUSE AGGREGATION EXPLORER
    =============================================
    
    Let's explore what's inside your aggregation tables!
    """)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    global_dw_path = "s3a://global-dw"
    
    try:
        # Explore Customer 360 View
        explore_customer_360(spark, global_dw_path)
        
        # Explore Monthly Summary
        explore_monthly_summary(spark, global_dw_path)
        
        # Quick peek at operational daily
        explore_operational_daily(spark, global_dw_path)
        
        print(f"\n" + "="*80)
        print("🎯 EXPLORATION COMPLETED!")
        print("="*80)
        print("✅ Customer 360: Customer profiles with activity scores")
        print("✅ Monthly Summary: Business KPIs aggregated by month")
        print("✅ Operational Daily: Daily business metrics timeline")
        
    except Exception as e:
        print(f"❌ Error during exploration: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
