"""
Silver Layer Transform - Làm sạch và chuẩn hóa data
====================================================
Bronze Parquet → PySpark Transform → Silver Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime


# === CONFIGURATION ===
LAKEHOUSE_PATH = "/data/lakehouse"
BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"
SILVER_PATH = f"{LAKEHOUSE_PATH}/silver"


def get_spark():
    """Tạo Spark session."""
    return SparkSession.builder \
        .appName("LakehouseSilverTransform") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


# === TRANSFORM FUNCTIONS ===

def transform_orders(spark):
    """Transform orders - thêm delivery metrics."""
    df = spark.read.parquet(f"{BRONZE_PATH}/raw_orders")
    
    result = df.select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        to_timestamp("order_purchase_timestamp").alias("order_purchase_at"),
        to_timestamp("order_approved_at").alias("order_approved_at"),
        to_timestamp("order_delivered_carrier_date").alias("shipped_at"),
        to_timestamp("order_delivered_customer_date").alias("delivered_at"),
        to_timestamp("order_estimated_delivery_date").alias("estimated_delivery_at"),
        col("_extracted_at")
    ).withColumn(
        "delivery_days",  # Số ngày giao hàng
        datediff(col("delivered_at"), col("order_purchase_at"))
    ).withColumn(
        "is_on_time",  # Giao đúng hẹn?
        when(col("delivered_at") <= col("estimated_delivery_at"), True).otherwise(False)
    ).filter(col("order_id").isNotNull())
    
    result.write.mode("overwrite").parquet(f"{SILVER_PATH}/stg_orders")
    return result.count()


def transform_order_items(spark):
    """Transform order items - tính tổng tiền mỗi dòng."""
    df = spark.read.parquet(f"{BRONZE_PATH}/raw_order_items")
    
    result = df.select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("seller_id"),
        concat(col("order_id"), lit("_"), col("order_item_id")).alias("order_line_id"),
        col("price").cast("decimal(10,2)").alias("price"),
        col("freight_value").cast("decimal(10,2)").alias("freight_value"),
        (col("price") + col("freight_value")).cast("decimal(10,2)").alias("line_total"),
        to_timestamp("shipping_limit_date").alias("shipping_limit_at"),
        col("_extracted_at")
    ).filter(col("order_id").isNotNull() & col("product_id").isNotNull())
    
    result.write.mode("overwrite").parquet(f"{SILVER_PATH}/stg_order_items")
    return result.count()


def transform_order_payments(spark):
    """Transform payments - chuẩn hóa data type."""
    df = spark.read.parquet(f"{BRONZE_PATH}/raw_order_payments")
    
    result = df.select(
        col("order_id"),
        col("payment_sequential"),
        col("payment_type"),
        col("payment_installments"),
        col("payment_value").cast("decimal(10,2)").alias("payment_value"),
        col("_extracted_at")
    ).filter(col("order_id").isNotNull())
    
    result.write.mode("overwrite").parquet(f"{SILVER_PATH}/stg_order_payments")
    return result.count()


def transform_customers(spark):
    """Transform customers - chuẩn hóa tên thành phố."""
    df = spark.read.parquet(f"{BRONZE_PATH}/raw_customers")
    
    result = df.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix").alias("zip_code"),
        initcap("customer_city").alias("city"),  # Viết hoa chữ cái đầu
        col("customer_state").alias("state"),
        col("_extracted_at")
    ).filter(col("customer_id").isNotNull())
    
    result.write.mode("overwrite").parquet(f"{SILVER_PATH}/stg_customers")
    return result.count()


def transform_products(spark):
    """Transform products - join với bảng dịch category."""
    products = spark.read.parquet(f"{BRONZE_PATH}/raw_products")
    translations = spark.read.parquet(f"{BRONZE_PATH}/raw_category_translation")
    
    # LEFT JOIN để lấy tên category tiếng Anh
    result = products.join(
        translations, 
        products.product_category_name == translations.product_category_name,
        "left"
    ).select(
        products.product_id,
        products.product_category_name.alias("category_name_pt"),
        coalesce(translations.product_category_name_english, products.product_category_name).alias("category_name"),
        products.product_weight_g.cast("decimal(10,2)").alias("weight_grams"),
        (products.product_length_cm * products.product_height_cm * products.product_width_cm).alias("volume_cm3"),
        products._extracted_at
    ).filter(products.product_id.isNotNull())
    
    result.write.mode("overwrite").parquet(f"{SILVER_PATH}/stg_products")
    return result.count()


def transform_sellers(spark):
    """Transform sellers - chuẩn hóa địa chỉ."""
    df = spark.read.parquet(f"{BRONZE_PATH}/raw_sellers")
    
    result = df.select(
        col("seller_id"),
        col("seller_zip_code_prefix").alias("zip_code"),
        initcap("seller_city").alias("city"),
        col("seller_state").alias("state"),
        col("_extracted_at")
    ).filter(col("seller_id").isNotNull())
    
    result.write.mode("overwrite").parquet(f"{SILVER_PATH}/stg_sellers")
    return result.count()


def transform_reviews(spark):
    """Transform reviews - thêm sentiment analysis."""
    df = spark.read.parquet(f"{BRONZE_PATH}/raw_order_reviews")
    
    # Xử lý an toàn: chỉ cast nếu giá trị là số
    df = df.withColumn(
        "review_score_int",
        when(col("review_score").rlike("^[0-9]+$"), col("review_score").cast("int"))
        .otherwise(lit(None))
    )
    
    # Xử lý an toàn: chỉ parse timestamp nếu format đúng
    df = df.withColumn(
        "review_created_at_safe",
        when(col("review_creation_date").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}"),
             to_timestamp("review_creation_date"))
        .otherwise(lit(None))
    )
    
    # Phân loại sentiment dựa trên điểm review
    result = df.select(
        col("review_id"),
        col("order_id"),
        col("review_score_int").alias("review_score"),
        when(col("review_score_int") >= 4, "positive")
            .when(col("review_score_int") == 3, "neutral")
            .when(col("review_score_int").isNull(), "unknown")
            .otherwise("negative").alias("review_sentiment"),
        col("review_comment_title"),
        col("review_comment_message"),
        col("review_created_at_safe").alias("review_created_at"),
        col("_extracted_at")
    ).filter(col("review_id").isNotNull())
    
    result.write.mode("overwrite").parquet(f"{SILVER_PATH}/stg_order_reviews")
    return result.count()


# === MAIN ===

def run_silver_transforms():
    """Chạy tất cả transforms cho Silver layer."""
    print("=" * 60)
    print("SILVER LAYER TRANSFORM")
    print("=" * 60)
    
    spark = get_spark()
    
    transforms = [
        ("stg_orders", transform_orders),
        ("stg_order_items", transform_order_items),
        ("stg_order_payments", transform_order_payments),
        ("stg_customers", transform_customers),
        ("stg_products", transform_products),
        ("stg_sellers", transform_sellers),
        ("stg_order_reviews", transform_reviews),
    ]
    
    total_rows = 0
    for name, func in transforms:
        try:
            count = func(spark)
            total_rows += count
            print(f"  ✓ {name}: {count:,} rows")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
    
    print("=" * 60)
    print(f"Total: {total_rows:,} rows transformed to Silver")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    run_silver_transforms()
