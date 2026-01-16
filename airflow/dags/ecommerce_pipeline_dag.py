"""
E-commerce Lakehouse Pipeline DAG
==================================
Airflow DAG điều phối toàn bộ pipeline ETL.

Flow: Bronze (9 tasks) → Silver (7 tasks) → Gold (10 tasks) → PostgreSQL → Verify

Schedule: Hàng ngày lúc 6 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


# === CONFIGURATION ===
SPARK_APPS = '/opt/airflow/spark'
JAVA_HOME = '/usr/lib/jvm/java-17-openjdk-amd64'


def spark_cmd(script_cmd):
    """Wrap PySpark command với JAVA_HOME export."""
    return f'export JAVA_HOME={JAVA_HOME} && cd {SPARK_APPS} && python -c "{script_cmd}"'


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='ecommerce_lakehouse_pipeline',
    default_args=default_args,
    description='Lakehouse Pipeline: PySpark → Parquet → PostgreSQL → Metabase',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,   # Chỉ cho phép 1 DAG run cùng lúc
    concurrency=1,       # Chạy tuần tự để tránh conflict Spark
    tags=['lakehouse', 'pyspark', 'parquet', 'metabase'],
)


# === TASK DEFINITIONS ===

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)


# =============================================================================
# Bronze Layer - Extract CSV to Parquet (9 tasks - SEQUENTIAL to avoid Spark conflicts)
# =============================================================================
with TaskGroup(group_id='bronze_layer', dag=dag) as bronze_layer:
    
    bronze_customers = BashOperator(
        task_id='raw_customers',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_customers\\", \\"olist_customers_dataset.csv\\"); spark.stop()'),
    )
    
    bronze_orders = BashOperator(
        task_id='raw_orders',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_orders\\", \\"olist_orders_dataset.csv\\"); spark.stop()'),
    )
    
    bronze_order_items = BashOperator(
        task_id='raw_order_items',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_order_items\\", \\"olist_order_items_dataset.csv\\"); spark.stop()'),
    )
    
    bronze_order_payments = BashOperator(
        task_id='raw_order_payments',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_order_payments\\", \\"olist_order_payments_dataset.csv\\"); spark.stop()'),
    )
    
    bronze_order_reviews = BashOperator(
        task_id='raw_order_reviews',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_order_reviews\\", \\"olist_order_reviews_dataset.csv\\"); spark.stop()'),
    )
    
    bronze_products = BashOperator(
        task_id='raw_products',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_products\\", \\"olist_products_dataset.csv\\"); spark.stop()'),
    )
    
    bronze_sellers = BashOperator(
        task_id='raw_sellers',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_sellers\\", \\"olist_sellers_dataset.csv\\"); spark.stop()'),
    )
    
    bronze_geolocation = BashOperator(
        task_id='raw_geolocation',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_geolocation\\", \\"olist_geolocation_dataset.csv\\"); spark.stop()'),
    )
    
    bronze_category_translation = BashOperator(
        task_id='raw_category_translation',
        bash_command=spark_cmd('from extract_bronze import *; spark=get_spark(); extract_to_bronze(spark, \\"raw_category_translation\\", \\"product_category_name_translation.csv\\"); spark.stop()'),
    )


# =============================================================================
# Silver Layer - Transform to cleaned data (7 tasks)
# =============================================================================
with TaskGroup(group_id='silver_layer', dag=dag) as silver_layer:
    
    stg_orders = BashOperator(
        task_id='stg_orders',
        bash_command=spark_cmd('from transform_silver import *; spark=get_spark(); transform_orders(spark); spark.stop()'),
    )
    
    stg_order_items = BashOperator(
        task_id='stg_order_items',
        bash_command=spark_cmd('from transform_silver import *; spark=get_spark(); transform_order_items(spark); spark.stop()'),
    )
    
    stg_order_payments = BashOperator(
        task_id='stg_order_payments',
        bash_command=spark_cmd('from transform_silver import *; spark=get_spark(); transform_order_payments(spark); spark.stop()'),
    )
    
    stg_customers = BashOperator(
        task_id='stg_customers',
        bash_command=spark_cmd('from transform_silver import *; spark=get_spark(); transform_customers(spark); spark.stop()'),
    )
    
    stg_products = BashOperator(
        task_id='stg_products',
        bash_command=spark_cmd('from transform_silver import *; spark=get_spark(); transform_products(spark); spark.stop()'),
    )
    
    stg_sellers = BashOperator(
        task_id='stg_sellers',
        bash_command=spark_cmd('from transform_silver import *; spark=get_spark(); transform_sellers(spark); spark.stop()'),
    )
    
    stg_order_reviews = BashOperator(
        task_id='stg_order_reviews',
        bash_command=spark_cmd('from transform_silver import *; spark=get_spark(); transform_reviews(spark); spark.stop()'),
    )


# =============================================================================
# Gold Layer - Dimensions (8 tasks)
# =============================================================================
with TaskGroup(group_id='gold_dimensions', dag=dag) as gold_dimensions:
    
    dim_customers = BashOperator(
        task_id='dim_customers',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_dim_customers(spark); spark.stop()'),
    )
    
    dim_products = BashOperator(
        task_id='dim_products',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_dim_products(spark); spark.stop()'),
    )
    
    dim_sellers = BashOperator(
        task_id='dim_sellers',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_dim_sellers(spark); spark.stop()'),
    )
    
    dim_dates = BashOperator(
        task_id='dim_dates',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_dim_dates(spark); spark.stop()'),
    )
    
    dim_geolocation = BashOperator(
        task_id='dim_geolocation',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_dim_geolocation(spark); spark.stop()'),
    )
    
    dim_payment_types = BashOperator(
        task_id='dim_payment_types',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_dim_payment_types(spark); spark.stop()'),
    )
    
    dim_categories = BashOperator(
        task_id='dim_categories',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_dim_categories(spark); spark.stop()'),
    )
    
    dim_order_status = BashOperator(
        task_id='dim_order_status',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_dim_order_status(spark); spark.stop()'),
    )


# =============================================================================
# Gold Layer - Facts (2 tasks)
# =============================================================================
with TaskGroup(group_id='gold_facts', dag=dag) as gold_facts:
    
    fct_sales = BashOperator(
        task_id='fct_sales',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_fct_sales(spark); spark.stop()'),
    )
    
    fct_order_reviews = BashOperator(
        task_id='fct_order_reviews',
        bash_command=spark_cmd('from transform_gold import *; spark=get_spark(); create_fct_reviews(spark); spark.stop()'),
    )


# =============================================================================
# Load Gold to PostgreSQL (for Metabase visualization)
# =============================================================================
load_to_postgres = BashOperator(
    task_id='load_to_postgres',
    bash_command='cd /opt/airflow/spark && python load_gold_to_postgres.py',
    dag=dag,
)


# =============================================================================
# Verify data availability
# =============================================================================
verify_lakehouse = BashOperator(
    task_id='verify_lakehouse',
    bash_command='''
        echo "======================================="
        echo "Lakehouse Pipeline Complete!"
        echo "======================================="
        echo ""
        echo "Parquet Files:"
        echo "  Bronze: $(ls /data/lakehouse/bronze/ 2>/dev/null | wc -l) tables"
        echo "  Silver: $(ls /data/lakehouse/silver/ 2>/dev/null | wc -l) tables"
        echo "  Gold: $(ls /data/lakehouse/gold/ 2>/dev/null | wc -l) tables"
        echo ""
        echo "PostgreSQL Gold Schema: Ready for Metabase"
        echo "Visualize with Metabase: http://localhost:3000"
    ''',
    dag=dag,
)


# =============================================================================
# Task Dependencies
# =============================================================================

start >> bronze_layer >> silver_layer >> gold_dimensions >> gold_facts >> load_to_postgres >> verify_lakehouse >> end

