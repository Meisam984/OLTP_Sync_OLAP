from dotenv import load_dotenv
import os
import dateutil.relativedelta as relativedelta
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ingestion import *
from process import *
from aggregate import *

silver_path = '/user/jupyter/silver'
gold_path = '/user/jupyter/gold'
bronze_path = '/user/jupyter/bronze'


load_dotenv('../utils/.env')

host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')
database = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASS')

clickhouse_host = os.getenv('CLICKHOUSE_HOST')
clickhouse_port = os.getenv('CLICKHOUSE_PORT')
clickhouse_db = os.getenv('CLICKHOUSE_DB')
clickhouse_user = os.getenv('CLICKHOUSE_USER')
clickhouse_pass = os.getenv('CLICKHOUSE_PASS')

clickhouse_url = f'jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_db}'


spark = SparkSession. \
    builder. \
    config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0"). \
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"). \
    config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"). \
    config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true"). \
    appName('Aggregate silver data and load into Delta Lake gold layer'). \
    getOrCreate()


# Initiate SparkSession object
spark = SparkSession. \
    builder. \
    config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0"). \
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"). \
    config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"). \
    appName('Ingest raw data and load into Delta Lake bronze layer'). \
    getOrCreate()

# Database properties
url = f'jdbc:postgresql://{host}:{port}/{database}'
db_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

# Ingestion phase
load_raw_users_table()
load_raw_products_table()
load_raw_orders_table()

# Process phase
load_processed_users_table()
load_processed_products_table()
load_processed_orders_table()

# Aggregation phase
load_customer_lifetime_value()
load_sales_summary_by_product()
load_top_products_by_revenue()

spark.stop()

