# create_global_dw.py - Global Data Warehouse Consolidator
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, coalesce, when, max as spark_max, min as spark_min, 
    sum as spark_sum, count, avg, current_timestamp, to_timestamp,
    concat_ws, md5, row_number, desc, asc, concat
)
from pyspark.sql.window import Window
from delta import DeltaTable
from datetime import datetime

def create_spark_session():
    """Create Spark session with necessary configurations."""
    return SparkSession.builder \
        .appName("GlobalDW_Consolidator") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-api.minio.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def check_source_tables_availability(spark):
    """Check availability of all domain gold tables."""
    print("🔍 CHECKING SOURCE TABLE AVAILABILITY...")
    
    source_tables = {
        # Common
        "dim_date": "s3a://common/gold/dim_date",
        
        # Transactions Domain
        "transactions_dim_customer": "s3a://transactions/gold/dim_customer",
        "transactions_dim_product": "s3a://transactions/gold/dim_product", 
        "transactions_dim_location": "s3a://transactions/gold/dim_location",
        "transactions_fact_orders": "s3a://transactions/gold/fact_orders",
        
        # Customer Images Domain
        "images_dim_image": "s3a://customer-images/gold/dim_image",
        "images_agg_uploads": "s3a://customer-images/gold/agg_image_uploads_by_day",
        
        # IoT Sensors Domain
        "iot_dim_sensor": "s3a://iot-sensors/gold/dim_sensor",
        "iot_fact_readings": "s3a://iot-sensors/gold/fact_sensor_readings",
        
        # Social Media Domain
        "social_fact_posts": "s3a://social-media/gold/fact_posts",
        "social_nlp_analysis": "s3a://social-media/gold/nlp_sentiment_analysis"
    }
    
    available_tables = {}
    missing_tables = []
    
    for table_name, path in source_tables.items():
        if DeltaTable.isDeltaTable(spark, path):
            try:
                df = spark.read.format("delta").load(path)
                record_count = df.count()
                available_tables[table_name] = {
                    "path": path,
                    "count": record_count,
                    "columns": len(df.columns)
                }
                print(f"✅ {table_name}: {record_count:,} records, {len(df.columns)} columns")
            except Exception as e:
                print(f"⚠️ {table_name}: Table exists but error reading - {e}")
        else:
            missing_tables.append(table_name)
            print(f"❌ {table_name}: NOT FOUND at {path}")
    
    if missing_tables:
        print(f"\n⚠️ WARNING: {len(missing_tables)} tables are missing.")
        print("The Global DW will be created with available tables only.")
    
    return available_tables

def create_global_dimensions(spark, available_tables, global_dw_path):
    """Create consolidated global dimension tables."""
    print("\n📊 CREATING GLOBAL DIMENSIONS...")
    
    # 1. Global Date Dimension (Copy from Common)
    if "dim_date" in available_tables:
        print("\n--- Creating global_dim_date ---")
        dim_date_df = spark.read.format("delta").load(available_tables["dim_date"]["path"])
        
        # Add global-specific enhancements
        enhanced_date_df = dim_date_df.withColumn("created_in_global_dw", current_timestamp())
        
        enhanced_date_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(f"{global_dw_path}/global_dim_date")
        
        print(f"✅ global_dim_date created with {enhanced_date_df.count():,} records")
    
    # 2. Global Customer Dimension (Consolidated)
    print("\n--- Creating global_dim_customer ---")
    
    # Base customers from transactions
    customers_from_transactions = None
    if "transactions_dim_customer" in available_tables:
        customers_from_transactions = spark.read.format("delta") \
            .load(available_tables["transactions_dim_customer"]["path"]) \
            .select(
                "customer_id",
                "customer_first_name", 
                "customer_last_name",
                concat(col("customer_first_name"), lit(" "), col("customer_last_name")).alias("full_name"),
                lit("transactions").alias("source_domain"),
                lit(True).alias("has_transactions")
            )
    
    # Customers from images
    customers_from_images = None
    if "images_dim_image" in available_tables:
        customers_from_images = spark.read.format("delta") \
            .load(available_tables["images_dim_image"]["path"]) \
            .select("customer_id").distinct() \
            .withColumn("source_domain", lit("images")) \
            .withColumn("has_images", lit(True))
    
    # Consolidate customers
    if customers_from_transactions is not None:
        global_customers_df = customers_from_transactions
        
        # Add image activity if available
        if customers_from_images is not None:
            global_customers_df = global_customers_df \
                .join(customers_from_images.select("customer_id", "has_images"), 
                      "customer_id", "left") \
                .fillna({"has_images": False})
        else:
            global_customers_df = global_customers_df.withColumn("has_images", lit(False))
        
        # Add metadata
        global_customers_df = global_customers_df.withColumn("created_in_global_dw", current_timestamp())
        
        global_customers_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(f"{global_dw_path}/global_dim_customer")
        
        print(f"✅ global_dim_customer created with {global_customers_df.count():,} records")
    
    # 3. Global Product Dimension (From Transactions)
    if "transactions_dim_product" in available_tables:
        print("\n--- Creating global_dim_product ---")
        
        products_df = spark.read.format("delta") \
            .load(available_tables["transactions_dim_product"]["path"]) \
            .withColumn("created_in_global_dw", current_timestamp())
        
        products_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(f"{global_dw_path}/global_dim_product")
        
        print(f"✅ global_dim_product created with {products_df.count():,} records")
    
    # 4. Global Location Dimension (From Transactions)
    if "transactions_dim_location" in available_tables:
        print("\n--- Creating global_dim_location ---")
        
        locations_df = spark.read.format("delta") \
            .load(available_tables["transactions_dim_location"]["path"]) \
            .withColumn("created_in_global_dw", current_timestamp())
        
        locations_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(f"{global_dw_path}/global_dim_location")
        
        print(f"✅ global_dim_location created with {locations_df.count():,} records")
    
    # 5. Global Sensor Dimension (From IoT)
    if "iot_dim_sensor" in available_tables:
        print("\n--- Creating global_dim_sensor ---")
        
        sensors_df = spark.read.format("delta") \
            .load(available_tables["iot_dim_sensor"]["path"]) \
            .withColumn("created_in_global_dw", current_timestamp())
        
        sensors_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(f"{global_dw_path}/global_dim_sensor")
        
        print(f"✅ global_dim_sensor created with {sensors_df.count():,} records")

def create_global_facts(spark, available_tables, global_dw_path):
    """Create consolidated global fact tables."""
    print("\n📈 CREATING GLOBAL FACT TABLES...")
    
    # 1. Global Customer Activity Timeline
    print("\n--- Creating global_fact_customer_activity ---")
    
    activities = []
    
    # Add transactions
    if "transactions_fact_orders" in available_tables:
        transactions_df = spark.read.format("delta") \
            .load(available_tables["transactions_fact_orders"]["path"]) \
            .select(
                "customer_id",
                col("order_timestamp").alias("activity_timestamp"),
                col("order_date_key").alias("activity_date_key"),
                lit("transaction").alias("activity_type"),
                "order_id",
                "amount",
                lit(None).alias("image_id"),
                lit(None).alias("post_id"),
                lit(None).alias("sensor_id")
            )
        activities.append(transactions_df)
    
    # Add image uploads
    if "images_dim_image" in available_tables:
        images_df = spark.read.format("delta") \
            .load(available_tables["images_dim_image"]["path"]) \
            .select(
                "customer_id",
                col("upload_timestamp").alias("activity_timestamp"),
                col("upload_date_key").alias("activity_date_key"),
                lit("image_upload").alias("activity_type"),
                lit(None).alias("order_id"),
                lit(None).alias("amount"),
                "image_id",
                lit(None).alias("post_id"),
                lit(None).alias("sensor_id")
            )
        activities.append(images_df)
    
    # Add social media posts (if customer_id exists)
    if "social_fact_posts" in available_tables:
        # Note: Assuming user_id can be mapped to customer_id
        social_df = spark.read.format("delta") \
            .load(available_tables["social_fact_posts"]["path"]) \
            .select(
                col("user_id").alias("customer_id"),
                col("post_timestamp").alias("activity_timestamp"), 
                col("post_date_key").alias("activity_date_key"),
                lit("social_post").alias("activity_type"),
                lit(None).alias("order_id"),
                lit(None).alias("amount"),
                lit(None).alias("image_id"),
                "post_id",
                lit(None).alias("sensor_id")
            )
        activities.append(social_df)
    
    if activities:
        # Union all activities
        customer_activity_df = activities[0]
        for df in activities[1:]:
            customer_activity_df = customer_activity_df.union(df)
        
        # Add sequence number for ordering
        window_spec = Window.partitionBy("customer_id").orderBy("activity_timestamp")
        customer_activity_df = customer_activity_df \
            .withColumn("activity_sequence", row_number().over(window_spec)) \
            .withColumn("created_in_global_dw", current_timestamp())
        
        customer_activity_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("activity_date_key") \
            .save(f"{global_dw_path}/global_fact_customer_activity")
        
        print(f"✅ global_fact_customer_activity created with {customer_activity_df.count():,} records")
    
    # 2. Global Operational Metrics (Daily Rollup)
    print("\n--- Creating global_fact_operational_daily ---")
    
    # Get date dimension for joining
    if "dim_date" in available_tables:
        dim_date_df = spark.read.format("delta").load(available_tables["dim_date"]["path"])
        
        operational_metrics = []
        
        # Transaction metrics
        if "transactions_fact_orders" in available_tables:
            transaction_metrics = spark.read.format("delta") \
                .load(available_tables["transactions_fact_orders"]["path"]) \
                .groupBy("order_date_key") \
                .agg(
                    spark_sum("amount").alias("daily_revenue"),
                    count("order_id").alias("daily_orders"),
                    avg("amount").alias("avg_order_value")
                )
            operational_metrics.append(transaction_metrics)
        
        # Image upload metrics
        if "images_agg_uploads" in available_tables:
            image_metrics = spark.read.format("delta") \
                .load(available_tables["images_agg_uploads"]["path"]) \
                .select(
                    col("upload_date_key").alias("order_date_key"),
                    col("image_count").alias("daily_image_uploads")
                )
            operational_metrics.append(image_metrics)
        
        # IoT sensor readings metrics (if available)
        if "iot_fact_readings" in available_tables:
            iot_readings_df = spark.read.format("delta") \
                .load(available_tables["iot_fact_readings"]["path"])
            
            # Check if reading_date_key exists, if not derive from timestamp
            if "reading_date_key" in iot_readings_df.columns:
                iot_metrics = iot_readings_df \
                    .groupBy("reading_date_key") \
                    .agg(
                        count("*").alias("daily_sensor_readings"),
                        avg("temperature").alias("avg_temperature"),
                        avg("humidity").alias("avg_humidity"),
                        avg("battery_level").alias("avg_battery_level")
                    ) \
                    .withColumnRenamed("reading_date_key", "order_date_key")
            else:
                # If no date_key, skip IoT metrics for now
                print("⚠️ IoT readings table missing date_key, skipping IoT metrics")
                iot_metrics = None
            
            if iot_metrics is not None:
                operational_metrics.append(iot_metrics)
        
        if operational_metrics:
            # Start with date dimension
            operational_daily_df = dim_date_df.select("date_key", "full_date")
            
            # Join all metrics
            for metrics_df in operational_metrics:
                operational_daily_df = operational_daily_df \
                    .join(metrics_df, operational_daily_df.date_key == metrics_df.order_date_key, "left") \
                    .drop("order_date_key")
            
            # Fill nulls with zeros and add metadata
            numeric_cols = [col_name for col_name in operational_daily_df.columns 
                          if col_name not in ["date_key", "full_date"]]
            
            for col_name in numeric_cols:
                operational_daily_df = operational_daily_df.fillna({col_name: 0})
            
            operational_daily_df = operational_daily_df \
                .withColumn("created_in_global_dw", current_timestamp())
            
            operational_daily_df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy("date_key") \
                .save(f"{global_dw_path}/global_fact_operational_daily")
            
            print(f"✅ global_fact_operational_daily created with {operational_daily_df.count():,} records")

def create_global_aggregates(spark, global_dw_path):
    """Create global aggregate tables for analytics."""
    print("\n📊 CREATING GLOBAL AGGREGATE TABLES...")
    
    # 1. Customer 360 View
    print("\n--- Creating global_agg_customer_360 ---")
    
    try:
        # Read global fact table
        customer_activity_df = spark.read.format("delta") \
            .load(f"{global_dw_path}/global_fact_customer_activity")
        
        customer_360_df = customer_activity_df.groupBy("customer_id") \
            .agg(
                spark_min("activity_timestamp").alias("first_activity_date"),
                spark_max("activity_timestamp").alias("last_activity_date"),
                count("*").alias("total_activities"),
                
                # Activity type counts
                spark_sum(when(col("activity_type") == "transaction", 1).otherwise(0)).alias("total_transactions"),
                spark_sum(when(col("activity_type") == "image_upload", 1).otherwise(0)).alias("total_image_uploads"), 
                spark_sum(when(col("activity_type") == "social_post", 1).otherwise(0)).alias("total_social_posts"),
                
                # Financial metrics (only from transactions)
                spark_sum(when(col("activity_type") == "transaction", col("amount")).otherwise(0)).alias("total_spent"),
                avg(when(col("activity_type") == "transaction", col("amount"))).alias("avg_transaction_value")
            ) \
            .withColumn("customer_score", 
                       (col("total_transactions") * 3 + 
                        col("total_image_uploads") * 1 + 
                        col("total_social_posts") * 2)) \
            .withColumn("created_in_global_dw", current_timestamp())
        
        customer_360_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(f"{global_dw_path}/global_agg_customer_360")
        
        print(f"✅ global_agg_customer_360 created with {customer_360_df.count():,} records")
        
    except Exception as e:
        print(f"⚠️ Could not create customer_360: {e}")
    
    # 2. Monthly Business Summary
    print("\n--- Creating global_agg_monthly_summary ---")
    
    try:
        operational_daily_df = spark.read.format("delta") \
            .load(f"{global_dw_path}/global_fact_operational_daily")
        
        dim_date_df = spark.read.format("delta") \
            .load(f"{global_dw_path}/global_dim_date")
        
        monthly_summary_df = operational_daily_df \
            .join(dim_date_df, "date_key") \
            .groupBy("year", "month", "month_name") \
            .agg(
                spark_sum("daily_revenue").alias("monthly_revenue"),
                spark_sum("daily_orders").alias("monthly_orders"),
                avg("avg_order_value").alias("avg_monthly_order_value"),
                spark_sum("daily_image_uploads").alias("monthly_image_uploads"),
                count("*").alias("active_days_in_month")
            ) \
            .withColumn("created_in_global_dw", current_timestamp()) \
            .orderBy("year", "month")
        
        # Add IoT metrics if they exist
        operational_daily_columns = operational_daily_df.columns
        if "daily_sensor_readings" in operational_daily_columns:
            agg_exprs = [
                spark_sum("daily_revenue").alias("monthly_revenue"),
                spark_sum("daily_orders").alias("monthly_orders"),
                avg("avg_order_value").alias("avg_monthly_order_value"),
                spark_sum("daily_image_uploads").alias("monthly_image_uploads"),
                spark_sum("daily_sensor_readings").alias("monthly_sensor_readings"),
                count("*").alias("active_days_in_month")
            ]
            
            # Add IoT sensor averages if they exist
            if "avg_temperature" in operational_daily_columns:
                agg_exprs.append(avg("avg_temperature").alias("avg_monthly_temperature"))
            if "avg_humidity" in operational_daily_columns:
                agg_exprs.append(avg("avg_humidity").alias("avg_monthly_humidity"))
            if "avg_battery_level" in operational_daily_columns:
                agg_exprs.append(avg("avg_battery_level").alias("avg_monthly_battery_level"))
            
            monthly_summary_df = operational_daily_df \
                .join(dim_date_df, "date_key") \
                .groupBy("year", "month", "month_name") \
                .agg(*agg_exprs) \
                .withColumn("created_in_global_dw", current_timestamp()) \
                .orderBy("year", "month")
        
        monthly_summary_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(f"{global_dw_path}/global_agg_monthly_summary")
        
        print(f"✅ global_agg_monthly_summary created with {monthly_summary_df.count():,} records")
        
    except Exception as e:
        print(f"⚠️ Could not create monthly_summary: {e}")

def generate_global_dw_catalog(spark, global_dw_path):
    """Generate a catalog of all global DW tables."""
    print("\n📋 GENERATING GLOBAL DW CATALOG...")
    
    # Define expected global tables
    global_tables = [
        "global_dim_date",
        "global_dim_customer", 
        "global_dim_product",
        "global_dim_location",
        "global_dim_sensor",
        "global_fact_customer_activity",
        "global_fact_operational_daily",
        "global_agg_customer_360",
        "global_agg_monthly_summary"
    ]
    
    catalog_data = []
    total_records = 0
    
    for table_name in global_tables:
        table_path = f"{global_dw_path}/{table_name}"
        
        if DeltaTable.isDeltaTable(spark, table_path):
            try:
                df = spark.read.format("delta").load(table_path)
                record_count = df.count()
                column_count = len(df.columns)
                
                # Get table type
                if "dim_" in table_name:
                    table_type = "Dimension"
                elif "fact_" in table_name:
                    table_type = "Fact"
                elif "agg_" in table_name:
                    table_type = "Aggregate"
                else:
                    table_type = "Other"
                
                catalog_data.append((
                    table_name,
                    table_type,
                    record_count,
                    column_count,
                    "Available",
                    datetime.now().isoformat()
                ))
                
                total_records += record_count
                print(f"✅ {table_name:<35} | {table_type:<10} | {record_count:>10,} records | {column_count:>2} columns")
                
            except Exception as e:
                catalog_data.append((
                    table_name,
                    "Unknown", 
                    0,
                    0,
                    f"Error: {str(e)[:50]}",
                    datetime.now().isoformat()
                ))
                print(f"❌ {table_name:<35} | Error reading table")
        else:
            catalog_data.append((
                table_name,
                "Missing",
                0, 
                0,
                "Table not found",
                datetime.now().isoformat()
            ))
            print(f"❌ {table_name:<35} | Not found")
    
    # Create catalog DataFrame
    catalog_df = spark.createDataFrame(
        catalog_data,
        ["table_name", "table_type", "record_count", "column_count", "status", "last_checked"]
    )
    
    # Save catalog
    catalog_df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{global_dw_path}/_catalog")
    
    print(f"\n📊 GLOBAL DW CATALOG SUMMARY:")
    print(f"   • Total Tables: {len(global_tables)}")
    print(f"   • Available Tables: {len([row for row in catalog_data if row[4] == 'Available'])}")
    print(f"   • Total Records: {total_records:,}")
    print(f"   • Catalog saved to: {global_dw_path}/_catalog")
    
    return catalog_df

def main():
    """Main function to create Global Data Warehouse."""
    print(f"""
    🏛️ GLOBAL DATA WAREHOUSE CONSOLIDATOR
    ====================================
    
    This script consolidates all domain gold tables into a unified Global DW:
    • Source: Domain-specific gold tables 
    • Target: s3a://global-dw/
    • Purpose: Cross-domain analytics and unified reporting
    
    ⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    global_dw_path = "s3a://global-dw"
    start_time = datetime.now()
    
    try:
        # Step 1: Check source table availability
        available_tables = check_source_tables_availability(spark)
        
        if not available_tables:
            print("❌ No source tables available. Please run domain gold scripts first.")
            return
        
        print(f"\n📊 Found {len(available_tables)} available source tables")
        
        # Step 2: Create global dimensions
        create_global_dimensions(spark, available_tables, global_dw_path)
        
        # Step 3: Create global facts
        create_global_facts(spark, available_tables, global_dw_path)
        
        # Step 4: Create global aggregates
        create_global_aggregates(spark, global_dw_path)
        
        # Step 5: Generate catalog
        catalog_df = generate_global_dw_catalog(spark, global_dw_path)
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n{'='*80}")
        print("🎯 GLOBAL DATA WAREHOUSE CREATION COMPLETED!")
        print('='*80)
        print(f"⏰ Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏁 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⌛ Duration: {duration}")
        print(f"📍 Location: {global_dw_path}")
        print(f"📊 Source Tables Processed: {len(available_tables)}")
        
        print(f"\n📋 GLOBAL DW STRUCTURE:")
        print(f"   🗃️ Dimensions: Consolidated customer, product, location, sensor, date")
        print(f"   📈 Facts: Customer activity timeline, operational daily metrics")
        print(f"   📊 Aggregates: Customer 360 view, monthly business summary")
        print(f"   📋 Catalog: Complete table inventory and metadata")
        
        print(f"\n🚀 NEXT STEPS:")
        print(f"   1. Connect your BI tools to s3a://global-dw/")
        print(f"   2. Schedule this script to run daily/weekly")
        print(f"   3. Set up monitoring on the _catalog table")
        print(f"   4. Create additional analytics marts as needed")
        
    except Exception as e:
        print(f"❌ Error during Global DW creation: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print(f"\n🎉 GLOBAL DW CONSOLIDATION FINISHED")
        spark.stop()

if __name__ == "__main__":
    main()
