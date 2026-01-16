"""
E-commerce Lakehouse - API Extractor
=====================================
Extracts data from FakeStore API and Exchange Rate API using PySpark.

APIs:
- FakeStore: https://fakestoreapi.com
- Exchange Rates: https://api.exchangerate-api.com
"""

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType


# =============================================================================
# Configuration
# =============================================================================

FAKESTORE_BASE_URL = "https://fakestoreapi.com"
EXCHANGE_RATE_URL = "https://api.exchangerate-api.com/v4/latest/USD"

OUTPUT_PATH = "/data/bronze"


# =============================================================================
# Spark Session
# =============================================================================

def get_spark_session():
    """Create Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("EcommerceLakehouse-Extract") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", "/data/iceberg") \
        .getOrCreate()


# =============================================================================
# API Extraction Functions
# =============================================================================

def extract_products():
    """Extract products from FakeStore API."""
    print("Extracting products...")
    response = requests.get(f"{FAKESTORE_BASE_URL}/products")
    products = response.json()
    
    # Add metadata
    for p in products:
        p['extracted_at'] = datetime.now().isoformat()
        p['source'] = 'fakestoreapi.com'
    
    print(f"   Found {len(products)} products")
    return products


def extract_categories():
    """Extract categories from FakeStore API."""
    print("Extracting categories...")
    response = requests.get(f"{FAKESTORE_BASE_URL}/products/categories")
    categories = response.json()
    
    result = [
        {
            'category_id': i + 1,
            'category_name': cat,
            'extracted_at': datetime.now().isoformat()
        }
        for i, cat in enumerate(categories)
    ]
    
    print(f"   Found {len(result)} categories")
    return result


def extract_carts():
    """Extract carts (orders) from FakeStore API."""
    print("Extracting carts...")
    response = requests.get(f"{FAKESTORE_BASE_URL}/carts")
    carts = response.json()
    
    for cart in carts:
        cart['extracted_at'] = datetime.now().isoformat()
    
    print(f"   Found {len(carts)} carts")
    return carts


def extract_users():
    """Extract users from FakeStore API."""
    print("Extracting users...")
    response = requests.get(f"{FAKESTORE_BASE_URL}/users")
    users = response.json()
    
    for user in users:
        user['extracted_at'] = datetime.now().isoformat()
    
    print(f"   Found {len(users)} users")
    return users


def extract_exchange_rates():
    """Extract exchange rates (USD as base)."""
    print("Extracting exchange rates...")
    response = requests.get(EXCHANGE_RATE_URL)
    data = response.json()
    
    rates = []
    for currency, rate in data.get('rates', {}).items():
        rates.append({
            'base_currency': 'USD',
            'target_currency': currency,
            'rate': rate,
            'extracted_at': datetime.now().isoformat()
        })
    
    print(f"   Found {len(rates)} exchange rates")
    return rates


# =============================================================================
# Save to Bronze Layer
# =============================================================================

def save_to_bronze(spark, data, table_name):
    """Save data to Bronze layer as Parquet."""
    df = spark.createDataFrame(data)
    
    output_path = f"{OUTPUT_PATH}/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"   Saved {table_name} to {output_path}")
    return df


# =============================================================================
# Main Extraction Pipeline
# =============================================================================

def run_extraction():
    """Run full extraction pipeline."""
    print("="*60)
    print("E-commerce Lakehouse - Data Extraction")
    print("="*60)
    print(f"Started at: {datetime.now()}")
    print()
    
    # Initialize Spark
    spark = get_spark_session()
    
    try:
        # Extract from APIs
        products = extract_products()
        categories = extract_categories()
        carts = extract_carts()
        users = extract_users()
        exchange_rates = extract_exchange_rates()
        
        print()
        print("Saving to Bronze layer...")
        
        # Save to Bronze
        save_to_bronze(spark, products, "raw_products")
        save_to_bronze(spark, categories, "raw_categories")
        save_to_bronze(spark, carts, "raw_carts")
        save_to_bronze(spark, users, "raw_users")
        save_to_bronze(spark, exchange_rates, "raw_exchange_rates")
        
        print()
        print("="*60)
        print("Extraction Complete!")
        print("="*60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    run_extraction()
