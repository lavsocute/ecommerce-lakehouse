"""
E-commerce Lakehouse - Olist Dataset Extractor (PySpark)
=========================================================
Uses PySpark to load Olist CSV files into PostgreSQL.

Architecture: PySpark Extract → PostgreSQL → dbt-postgres Transform
Dataset: Brazilian E-Commerce Public Dataset by Olist
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import os
from datetime import datetime


# =============================================================================
# Configuration
# =============================================================================

RAW_PATH = "/data/raw/olist"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/lakehouse"
POSTGRES_PROPS = {
    "user": "lakehouse",
    "password": "lakehouse123",
    "driver": "org.postgresql.Driver"
}

# Olist dataset files mapping
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


# =============================================================================
# Spark Session
# =============================================================================

def get_spark():
    """Create Spark session with PostgreSQL JDBC driver."""
    return SparkSession.builder \
        .appName("OlistDataExtraction") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.extraClassPath", "/home/jovyan/.ivy2/jars/*") \
        .getOrCreate()


# =============================================================================
# Load Functions
# =============================================================================

def load_csv_to_postgres(spark, table_name, filename):
    """Read CSV with Spark and write to PostgreSQL."""
    input_path = f"{RAW_PATH}/{filename}"
    
    print(f"Loading {table_name}...")
    
    # Read CSV with Spark
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    
    # Add metadata columns
    df = df.withColumn("_extracted_at", current_timestamp()) \
           .withColumn("_source_file", lit(filename))
    
    # Write to PostgreSQL in bronze schema
    df.write \
        .mode("overwrite") \
        .jdbc(POSTGRES_URL, f"bronze.{table_name}", properties=POSTGRES_PROPS)
    
    row_count = df.count()
    print(f"   Loaded {row_count:,} rows to bronze.{table_name}")
    return row_count


def create_schemas(spark):
    """Create PostgreSQL schemas using JDBC."""
    # Use pandas for schema creation since Spark JDBC doesn't support DDL
    from sqlalchemy import create_engine, text
    
    engine = create_engine("postgresql://lakehouse:lakehouse123@postgres:5432/lakehouse")
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
    print("Created schemas: bronze, silver, gold")


def run_extraction():
    """Run the complete PySpark extraction pipeline."""
    print("=" * 70)
    print("E-commerce Lakehouse - Olist Dataset Extraction (PySpark)")
    print("=" * 70)
    print(f"Started at: {datetime.now()}")
    print(f"Target: PostgreSQL @ postgres:5432/lakehouse")
    print()
    
    # Create schemas first
    create_schemas(None)
    
    # Get Spark session
    print("Initializing Spark session...")
    spark = get_spark()
    print(f"Spark version: {spark.version}")
    print()
    
    # Process each table
    summary = {}
    for table_name, filename in OLIST_FILES.items():
        try:
            count = load_csv_to_postgres(spark, table_name, filename)
            summary[table_name] = count
        except Exception as e:
            print(f"   ERROR processing {table_name}: {e}")
            summary[table_name] = 0
    
    # Print summary
    print()
    print("=" * 70)
    print("Extraction Summary:")
    print("-" * 70)
    total_rows = 0
    for table, count in summary.items():
        print(f"  {table:30} : {count:>10,} rows")
        total_rows += count
    print("-" * 70)
    print(f"  {'TOTAL':30} : {total_rows:>10,} rows")
    print("=" * 70)
    print(f"Completed at: {datetime.now()}")
    print("=" * 70)
    
    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    run_extraction()
