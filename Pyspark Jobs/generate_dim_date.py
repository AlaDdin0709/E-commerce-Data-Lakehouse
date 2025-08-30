# generate_dim_date.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, year, month, dayofmonth, dayofweek, when, weekofyear, quarter, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType
from delta import DeltaTable
from datetime import datetime, timedelta

def create_spark_session():
    """Create Spark session with necessary configurations."""
    return SparkSession.builder \
        .appName("GenerateDimDate_GlobalDW") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-api.minio.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def extract_date_range_from_silver_tables(spark):
    """Extract min/max dates from all silver tables to determine date range."""
    print("🔍 Analyzing date ranges from silver tables...")
    
    # Define all silver table paths
    silver_paths = [
        "s3a://transactions/silver",
        "s3a://iot-sensors/silver", 
        "s3a://social-media/silver",
        "s3a://customer-images/silver"
    ]
    
    min_date = None
    max_date = None
    
    for path in silver_paths:
        try:
            if DeltaTable.isDeltaTable(spark, path):
                df = spark.read.format("delta").load(path)
                
                # Look for timestamp columns (different naming conventions)
                timestamp_cols = [col_name for col_name in df.columns 
                                if any(keyword in col_name.lower() 
                                      for keyword in ['timestamp', 'date', 'time'])]
                
                if timestamp_cols:
                    # Use the first timestamp column found
                    ts_col = timestamp_cols[0]
                    date_stats = df.select(
                        col(ts_col).cast("date").alias("date_col")
                    ).agg(
                        spark_min(col("date_col")).alias("min_date"),
                        spark_max(col("date_col")).alias("max_date")
                    ).collect()[0]
                    
                    table_min = date_stats["min_date"]
                    table_max = date_stats["max_date"]
                    
                    if table_min and table_max:
                        min_date = min(min_date, table_min) if min_date else table_min
                        max_date = max(max_date, table_max) if max_date else table_max
                        print(f"✅ {path}: {table_min} to {table_max}")
                    
        except Exception as e:
            print(f"⚠️ Could not process {path}: {e}")
            continue
    
    # Default range if no dates found
    if not min_date or not max_date:
        print("⚠️ Using default date range (2020-2030)")
        min_date = datetime(2020, 1, 1).date()
        max_date = datetime(2030, 12, 31).date()
    else:
        # Extend range by 1 year on each side for future/past data
        min_date = datetime(min_date.year - 1, 1, 1).date()
        max_date = datetime(max_date.year + 1, 12, 31).date()
    
    print(f"📅 Final date range: {min_date} to {max_date}")
    return min_date, max_date

def generate_date_dimension(spark, start_date, end_date):
    """Generate comprehensive date dimension."""
    print("🗓️ Generating date dimension...")
    
    # Create date range
    date_list = []
    current_date = start_date
    date_key = 1
    
    while current_date <= end_date:
        quarter_num = (current_date.month - 1) // 3 + 1
        
        date_list.append((
            date_key,                                    # date_key
            current_date,                               # full_date
            current_date.year,                          # year
            current_date.month,                         # month  
            current_date.day,                           # day
            current_date.strftime('%B'),                # month_name
            current_date.strftime('%A'),                # day_name
            current_date.strftime('%a'),                # day_name_short
            (current_date.weekday() + 1) % 7 + 1,      # day_of_week (1=Sunday)
            current_date.timetuple().tm_yday,           # day_of_year
            current_date.isocalendar()[1],              # week_of_year
            quarter_num,                                # quarter
            f"Q{quarter_num}",                          # quarter_name
            current_date.strftime('%Y-%m'),             # year_month
            f"{current_date.year}-Q{quarter_num}",      # year_quarter (fixed)
            current_date.weekday() >= 5,                # is_weekend
            current_date.month in [12, 1, 2],          # is_winter
            current_date.month in [3, 4, 5],           # is_spring  
            current_date.month in [6, 7, 8],           # is_summer
            current_date.month in [9, 10, 11],         # is_autumn
            current_date.day <= 15,                     # is_first_half_month
            current_date.day == 1,                      # is_month_start
            (current_date + timedelta(days=1)).day == 1, # is_month_end
            current_date.timetuple().tm_yday <= 15,     # is_year_start
            current_date.timetuple().tm_yday >= 350     # is_year_end
        ))
        
        current_date += timedelta(days=1)
        date_key += 1
    
    # Define schema
    schema = StructType([
        StructField("date_key", IntegerType(), False),
        StructField("full_date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("day_name", StringType(), False),
        StructField("day_name_short", StringType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_of_year", IntegerType(), False),
        StructField("week_of_year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("quarter_name", StringType(), False),
        StructField("year_month", StringType(), False),
        StructField("year_quarter", StringType(), False),
        StructField("is_weekend", BooleanType(), False),
        StructField("is_winter", BooleanType(), False),
        StructField("is_spring", BooleanType(), False),
        StructField("is_summer", BooleanType(), False),
        StructField("is_autumn", BooleanType(), False),
        StructField("is_first_half_month", BooleanType(), False),
        StructField("is_month_start", BooleanType(), False),
        StructField("is_month_end", BooleanType(), False),
        StructField("is_year_start", BooleanType(), False),
        StructField("is_year_end", BooleanType(), False)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(date_list, schema)
    print(f"✅ Generated {df.count()} date records")
    
    return df

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== GENERATING GLOBAL DATA WAREHOUSE DATE DIMENSION ===")
    
    dim_date_path = "s3a://common/gold/dim_date"
    
    try:
        # Extract date range from silver tables
        start_date, end_date = extract_date_range_from_silver_tables(spark)
        
        # Generate date dimension
        dim_date_df = generate_date_dimension(spark, start_date, end_date)
        
        # Save to Delta Lake
        print(f"💾 Saving dim_date to {dim_date_path}...")
        dim_date_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(dim_date_path)
        
        print("✅ Date dimension created successfully!")
        
        # Show sample data
        print("\n📊 Sample dim_date records:")
        spark.read.format("delta").load(dim_date_path).show(10, truncate=False)
        
        # Show statistics
        total_records = spark.read.format("delta").load(dim_date_path).count()
        print(f"\n📈 Total date records: {total_records}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🎉 DATE DIMENSION GENERATION COMPLETED")
        spark.stop()

if __name__ == "__main__":
    main()
