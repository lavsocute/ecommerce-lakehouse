"""
Load Gold to PostgreSQL - Đẩy data Gold vào PostgreSQL
=======================================================
Gold Parquet → Pandas/PyArrow → PostgreSQL

Mục đích: Load data từ Gold layer vào PostgreSQL
để Metabase có thể visualize.
"""

import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from datetime import datetime


# === CONFIGURATION ===
GOLD_PATH = "/data/lakehouse/gold"
POSTGRES_URL = "postgresql://lakehouse:lakehouse123@postgres:5432/lakehouse"


def load_table_to_postgres(engine, table_name):
    """Load 1 table từ Parquet vào PostgreSQL."""
    parquet_path = f"{GOLD_PATH}/{table_name}"
    
    if not os.path.exists(parquet_path):
        print(f"  ⏭ Bỏ qua {table_name} - không tìm thấy")
        return 0
    
    try:
        # Đọc Parquet với PyArrow
        df = pq.read_table(parquet_path).to_pandas()
        row_count = len(df)
        
        # Ghi vào PostgreSQL schema 'gold'
        df.to_sql(
            table_name, 
            engine, 
            schema='gold',
            if_exists='replace',
            index=False
        )
        
        print(f"  ✓ {table_name}: {row_count:,} rows")
        return row_count
    except Exception as e:
        print(f"  ✗ {table_name}: {e}")
        return 0


def main():
    """Chạy load tất cả tables từ Gold vào PostgreSQL."""
    print("=" * 60)
    print("LOAD GOLD TO POSTGRESQL")
    print(f"Started: {datetime.now()}")
    print("=" * 60)
    
    engine = create_engine(POSTGRES_URL)
    
    # Danh sách tables Gold cần load
    tables = [
        "dim_customers",
        "dim_products", 
        "dim_sellers",
        "dim_dates",
        "dim_geolocation",
        "dim_payment_types",
        "dim_categories",
        "dim_order_status",
        "fct_sales",
        "fct_order_reviews"
    ]
    
    total_rows = 0
    for table in tables:
        rows = load_table_to_postgres(engine, table)
        total_rows += rows
    
    print("=" * 60)
    print(f"Total: {total_rows:,} rows loaded")
    print(f"Completed: {datetime.now()}")
    print("=" * 60)
    
    engine.dispose()


if __name__ == "__main__":
    main()
