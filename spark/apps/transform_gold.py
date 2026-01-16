"""
Gold Layer Transform - Tạo Dimensional Model
=============================================
Silver Parquet → PySpark Transform → Gold Parquet

Gold Layer = Data sẵn sàng cho analytics (Star Schema)
- Dimensions: dim_customers, dim_products, dim_sellers, dim_dates, etc.
- Facts: fct_sales, fct_order_reviews
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime


# === CONFIGURATION ===
LAKEHOUSE_PATH = "/data/lakehouse"
SILVER_PATH = f"{LAKEHOUSE_PATH}/silver"
GOLD_PATH = f"{LAKEHOUSE_PATH}/gold"


def get_spark():
    """Tạo Spark session."""
    return SparkSession.builder \
        .appName("LakehouseGoldTransform") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


# === DIMENSION TABLES ===

def create_dim_customers(spark):
    """Customer dimension with regions."""
    customers = spark.read.parquet(f"{SILVER_PATH}/stg_customers")
    orders = spark.read.parquet(f"{SILVER_PATH}/stg_orders")
    
    order_counts = orders.groupBy("customer_id").agg(count("*").alias("total_orders"))
    
    result = customers.join(order_counts, "customer_id", "left").select(
        customers.customer_id,
        customers.customer_unique_id,
        customers.zip_code,
        customers.city,
        customers.state,
        when(customers.state.isin("SP", "RJ", "MG", "ES"), "Southeast")
            .when(customers.state.isin("PR", "SC", "RS"), "South")
            .when(customers.state.isin("MT", "MS", "GO", "DF"), "Central-West")
            .when(customers.state.isin("BA", "PE", "CE", "MA", "PI", "RN", "PB", "SE", "AL"), "Northeast")
            .otherwise("North").alias("region"),
        coalesce(order_counts.total_orders, lit(0)).alias("total_orders")
    )
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_customers")
    return result.count()


def create_dim_products(spark):
    """Product dimension with sales stats."""
    products = spark.read.parquet(f"{SILVER_PATH}/stg_products")
    items = spark.read.parquet(f"{SILVER_PATH}/stg_order_items")
    
    sales_stats = items.groupBy("product_id").agg(
        count("*").alias("times_sold"),
        sum("price").alias("total_revenue"),
        avg("price").alias("avg_price")
    )
    
    result = products.join(sales_stats, "product_id", "left").select(
        products.product_id,
        products.category_name,
        products.category_name_pt,
        products.weight_grams,
        products.volume_cm3,
        when(products.weight_grams < 500, "Small")
            .when(products.weight_grams < 2000, "Medium")
            .when(products.weight_grams < 10000, "Large")
            .otherwise("Extra Large").alias("size_tier"),
        coalesce(sales_stats.times_sold, lit(0)).alias("times_sold"),
        coalesce(sales_stats.total_revenue, lit(0)).alias("total_revenue"),
        coalesce(sales_stats.avg_price, lit(0)).alias("avg_selling_price")
    )
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_products")
    return result.count()


def create_dim_sellers(spark):
    """Seller dimension with performance tiers."""
    sellers = spark.read.parquet(f"{SILVER_PATH}/stg_sellers")
    items = spark.read.parquet(f"{SILVER_PATH}/stg_order_items")
    
    seller_stats = items.groupBy("seller_id").agg(
        count_distinct("order_id").alias("total_orders"),
        count("*").alias("total_items_sold"),
        sum("price").alias("total_revenue")
    )
    
    result = sellers.join(seller_stats, "seller_id", "left").select(
        sellers.seller_id,
        sellers.zip_code,
        sellers.city,
        sellers.state,
        when(sellers.state.isin("SP", "RJ", "MG", "ES"), "Southeast")
            .when(sellers.state.isin("PR", "SC", "RS"), "South")
            .otherwise("Other").alias("region"),
        coalesce(seller_stats.total_orders, lit(0)).alias("total_orders"),
        coalesce(seller_stats.total_revenue, lit(0)).alias("total_revenue"),
        when(seller_stats.total_revenue >= 100000, "Platinum")
            .when(seller_stats.total_revenue >= 50000, "Gold")
            .when(seller_stats.total_revenue >= 10000, "Silver")
            .otherwise("Bronze").alias("seller_tier")
    )
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_sellers")
    return result.count()


def create_dim_dates(spark):
    """Date dimension."""
    orders = spark.read.parquet(f"{SILVER_PATH}/stg_orders")
    
    dates = orders.select(
        to_date("order_purchase_at").alias("full_date")
    ).distinct().filter(col("full_date").isNotNull())
    
    result = dates.select(
        date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date"),
        year("full_date").alias("year_number"),
        quarter("full_date").alias("quarter_number"),
        month("full_date").alias("month_number"),
        date_format("full_date", "MMMM").alias("month_name"),
        date_format("full_date", "yyyy-MM").alias("year_month"),
        weekofyear("full_date").alias("week_of_year"),
        dayofweek("full_date").alias("day_of_week_number"),
        date_format("full_date", "EEEE").alias("day_of_week_name"),
        when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend")
    ).orderBy("full_date")
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_dates")
    return result.count()


def create_dim_geolocation(spark):
    """Geolocation dimension with regions."""
    from pyspark.sql.functions import avg, first
    
    geo = spark.read.parquet(f"{LAKEHOUSE_PATH}/bronze/raw_geolocation")
    
    result = geo.groupBy("geolocation_zip_code_prefix").agg(
        avg("geolocation_lat").alias("latitude"),
        avg("geolocation_lng").alias("longitude"),
        first("geolocation_city").alias("city"),
        first("geolocation_state").alias("state"),
        count("*").alias("data_points")
    ).select(
        col("geolocation_zip_code_prefix").alias("zip_code"),
        initcap("city").alias("city"),
        col("state"),
        col("latitude"),
        col("longitude"),
        when(col("state").isin("SP", "RJ", "MG", "ES"), "Southeast")
            .when(col("state").isin("PR", "SC", "RS"), "South")
            .when(col("state").isin("MT", "MS", "GO", "DF"), "Central-West")
            .when(col("state").isin("BA", "PE", "CE", "MA", "PI", "RN", "PB", "SE", "AL"), "Northeast")
            .otherwise("North").alias("region"),
        col("data_points")
    )
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_geolocation")
    return result.count()


def create_dim_payment_types(spark):
    """Payment type dimension with market share."""
    payments = spark.read.parquet(f"{SILVER_PATH}/stg_order_payments")
    
    total_value = payments.agg(sum("payment_value")).collect()[0][0]
    
    result = payments.groupBy("payment_type").agg(
        count("*").alias("transaction_count"),
        count_distinct("order_id").alias("order_count"),
        sum("payment_value").alias("total_value"),
        avg("payment_value").alias("avg_value"),
        avg("payment_installments").alias("avg_installments")
    ).select(
        col("payment_type"),
        when(col("payment_type") == "credit_card", "Credit Card")
            .when(col("payment_type") == "boleto", "Boleto Bancario")
            .when(col("payment_type") == "voucher", "Voucher")
            .when(col("payment_type") == "debit_card", "Debit Card")
            .otherwise(initcap("payment_type")).alias("payment_type_name"),
        col("transaction_count"),
        col("order_count"),
        round("total_value", 2).alias("total_value"),
        round("avg_value", 2).alias("avg_transaction_value"),
        round("avg_installments", 1).alias("avg_installments"),
        round(col("total_value") / lit(total_value) * 100, 2).alias("value_share_pct")
    )
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_payment_types")
    return result.count()


def create_dim_categories(spark):
    """Product category dimension with tiers."""
    products = spark.read.parquet(f"{SILVER_PATH}/stg_products")
    items = spark.read.parquet(f"{SILVER_PATH}/stg_order_items")
    
    product_items = products.join(items, "product_id", "left")
    
    total_revenue = product_items.agg(sum("price")).collect()[0][0] or 1
    
    result = product_items.groupBy("category_name", "category_name_pt").agg(
        count_distinct("product_id").alias("product_count"),
        count_distinct("order_id").alias("order_count"),
        count("order_line_id").alias("items_sold"),
        sum("price").alias("total_revenue"),
        avg("price").alias("avg_price")
    ).select(
        monotonically_increasing_id().alias("category_id"),
        col("category_name"),
        col("category_name_pt"),
        col("product_count"),
        col("order_count"),
        col("items_sold"),
        round(coalesce("total_revenue", lit(0)), 2).alias("total_revenue"),
        round(coalesce("avg_price", lit(0)), 2).alias("avg_price"),
        when(col("total_revenue") >= 1000000, "Tier 1 (Top)")
            .when(col("total_revenue") >= 100000, "Tier 2 (High)")
            .when(col("total_revenue") >= 10000, "Tier 3 (Medium)")
            .otherwise("Tier 4 (Low)").alias("category_tier"),
        round(coalesce(col("total_revenue"), lit(0)) / lit(total_revenue) * 100, 2).alias("revenue_share_pct")
    ).filter(col("category_name").isNotNull())
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_categories")
    return result.count()


def create_dim_order_status(spark):
    """Order status dimension with metrics."""
    orders = spark.read.parquet(f"{SILVER_PATH}/stg_orders")
    
    total_orders = orders.count()
    
    result = orders.groupBy("order_status").agg(
        count("*").alias("order_count"),
        avg("delivery_days").alias("avg_delivery_days"),
        sum(when(col("is_on_time") == True, 1).otherwise(0)).alias("on_time_count")
    ).select(
        col("order_status").alias("status_code"),
        when(col("order_status") == "delivered", "Delivered")
            .when(col("order_status") == "shipped", "Shipped")
            .when(col("order_status") == "processing", "Processing")
            .when(col("order_status") == "canceled", "Canceled")
            .otherwise(initcap("order_status")).alias("status_name"),
        when(col("order_status") == "delivered", "Completed")
            .when(col("order_status").isin("shipped", "processing", "invoiced", "approved", "created"), "In Progress")
            .when(col("order_status").isin("canceled", "unavailable"), "Failed")
            .otherwise("Other").alias("status_category"),
        when(col("order_status").isin("delivered", "canceled", "unavailable"), True).otherwise(False).alias("is_terminal"),
        col("order_count"),
        round("avg_delivery_days", 1).alias("avg_delivery_days"),
        col("on_time_count"),
        round(col("on_time_count") / col("order_count") * 100, 1).alias("on_time_rate_pct"),
        round(col("order_count") / lit(total_orders) * 100, 2).alias("volume_share_pct")
    )
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_order_status")
    return result.count()


# =============================================================================
# Fact Transforms
# =============================================================================

def create_fct_sales(spark):
    """Sales fact table."""
    items = spark.read.parquet(f"{SILVER_PATH}/stg_order_items")
    orders = spark.read.parquet(f"{SILVER_PATH}/stg_orders")
    customers = spark.read.parquet(f"{SILVER_PATH}/stg_customers")
    products = spark.read.parquet(f"{SILVER_PATH}/stg_products")
    sellers = spark.read.parquet(f"{SILVER_PATH}/stg_sellers")
    
    result = items \
        .join(orders, "order_id") \
        .join(customers, "customer_id", "left") \
        .join(products, "product_id", "left") \
        .join(sellers, "seller_id", "left") \
        .select(
            items.order_line_id,
            items.order_id,
            items.product_id,
            items.seller_id,
            orders.customer_id,
            date_format(orders.order_purchase_at, "yyyyMMdd").cast("int").alias("date_key"),
            orders.order_status,
            orders.order_purchase_at,
            orders.delivered_at,
            orders.delivery_days,
            orders.is_on_time,
            items.price,
            items.freight_value,
            items.line_total,
            products.category_name,
            customers.state.alias("customer_state"),
            sellers.state.alias("seller_state"),
            when(customers.state == sellers.state, True).otherwise(False).alias("is_same_state")
        )
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/fct_sales")
    return result.count()


def create_fct_reviews(spark):
    """Reviews fact table."""
    reviews = spark.read.parquet(f"{SILVER_PATH}/stg_order_reviews")
    orders = spark.read.parquet(f"{SILVER_PATH}/stg_orders")
    
    result = reviews.join(orders, "order_id", "left").select(
        reviews.review_id,
        reviews.order_id,
        orders.customer_id,
        date_format(orders.order_purchase_at, "yyyyMMdd").cast("int").alias("order_date_key"),
        date_format(reviews.review_created_at, "yyyyMMdd").cast("int").alias("review_date_key"),
        reviews.review_score,
        reviews.review_sentiment,
        orders.order_status,
        orders.delivery_days,
        orders.is_on_time
    )
    
    result.write.mode("overwrite").parquet(f"{GOLD_PATH}/fct_order_reviews")
    return result.count()


def run_gold_transforms():
    """Run all Gold layer transforms."""
    print("=" * 70)
    print("E-commerce Lakehouse - Gold Layer Transform (PySpark)")
    print("=" * 70)
    print(f"Started at: {datetime.now()}")
    print()
    
    spark = get_spark()
    
    transforms = [
        ("dim_customers", create_dim_customers),
        ("dim_products", create_dim_products),
        ("dim_sellers", create_dim_sellers),
        ("dim_dates", create_dim_dates),
        ("dim_geolocation", create_dim_geolocation),
        ("dim_payment_types", create_dim_payment_types),
        ("dim_categories", create_dim_categories),
        ("dim_order_status", create_dim_order_status),
        ("fct_sales", create_fct_sales),
        ("fct_order_reviews", create_fct_reviews),
    ]
    
    summary = {}
    for name, func in transforms:
        try:
            print(f"Creating {name}...")
            count = func(spark)
            summary[name] = count
            print(f"   Created {count:,} rows")
        except Exception as e:
            print(f"   ERROR: {e}")
            import traceback
            traceback.print_exc()
            summary[name] = 0
    
    print()
    print("=" * 70)
    print("Gold Layer Summary:")
    print("-" * 70)
    for table, count in summary.items():
        print(f"  {table:30} : {count:>10,} rows")
    print("=" * 70)
    print(f"Completed at: {datetime.now()}")
    
    spark.stop()


if __name__ == "__main__":
    run_gold_transforms()
