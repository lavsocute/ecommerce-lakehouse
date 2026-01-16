"""
Bronze Layer Extractor - Đọc CSV và ghi Parquet
================================================
CSV Files → PySpark → Parquet (Bronze Layer)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import os
from datetime import datetime


# === CONFIGURATION ===
RAW_PATH = "/data/raw/olist"
LAKEHOUSE_PATH = "/data/lakehouse"
BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"

# Mapping: tên bảng → file CSV
OLIST_FILES = {
    "raw_customers": "olist_customers_dataset.csv",
    "raw_orders": "olist_orders_dataset.csv",
    "raw_order_items": "olist_order_items_dataset.csv",
    "raw_order_payments": "olist_order_payments_dataset.csv",
    "raw_order_reviews": "olist_order_reviews_dataset.csv",
    "raw_products": "olist_products_dataset.csv",
    "raw_sellers": "olist_sellers_dataset.csv",
    "raw_geolocation": "olist_geolocation_dataset.csv",
    "raw_category_translation": "product_category_name_translation.csv",
}


def get_spark():
    """Tạo Spark session với config tối ưu cho Parquet."""
    return SparkSession.builder \
        .appName("LakehouseExtraction") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()


def extract_to_bronze(spark, table_name, filename):
    """
    Đọc CSV và ghi ra Parquet (Bronze layer).
    Thêm metadata: _extracted_at, _source_file
    """
    input_path = f"{RAW_PATH}/{filename}"
    output_path = f"{BRONZE_PATH}/{table_name}"
    
    print(f"Extracting {table_name}...")
    
    # Đọc CSV với Spark
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    
    # Thêm metadata columns
    df = df.withColumn("_extracted_at", current_timestamp()) \
           .withColumn("_source_file", lit(filename))
    
    # Ghi Parquet (Bronze)
    df.write.mode("overwrite").parquet(output_path)
    
    row_count = df.count()
    print(f"   ✓ {row_count:,} rows → {output_path}")
    return row_count


def run_extraction():
    """Chạy full extraction pipeline cho Bronze layer."""
    print("=" * 60)
    print("BRONZE LAYER EXTRACTION")
    print("=" * 60)
    
    spark = get_spark()
    os.makedirs(BRONZE_PATH, exist_ok=True)
    
    total_rows = 0
    for table_name, filename in OLIST_FILES.items():
        try:
            count = extract_to_bronze(spark, table_name, filename)
            total_rows += count
        except Exception as e:
            print(f"   ✗ ERROR {table_name}: {e}")
    
    print("=" * 60)
    print(f"Total: {total_rows:,} rows extracted to Bronze")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    run_extraction()
