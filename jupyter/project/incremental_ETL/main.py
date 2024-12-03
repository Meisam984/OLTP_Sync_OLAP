from pyspark.sql import SparkSession
from delta import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

import os
from dotenv import load_dotenv

load_dotenv('../utils/.env')

silver_path = '/user/jupyter/silver'
bronze_path = '/user/jupyter/bronze'
gold_path = '/user/jupyter/gold'

kafka_brokers = os.getenv('KAFKA_BROKERS')

clickhouse_host = os.getenv('CLICKHOUSE_HOST')
clickhouse_port = os.getenv('CLICKHOUSE_PORT')
clickhouse_db = os.getenv('CLICKHOUSE_DB')
clickhouse_user = os.getenv('CLICKHOUSE_USER')
clickhouse_pass = os.getenv('CLICKHOUSE_PASS')

clickhouse_url = f'jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_db}'


spark = SparkSession. \
    builder. \
    config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:2.1.0"). \
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"). \
    config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"). \
    config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true"). \
    appName('Incrementally load data into Delta Lake'). \
    getOrCreate()

import threading

from incremental_load import *

# Run all streams in parallel using threading
threads = []
threads.append(threading.Thread(target=incremental_load_new_users))
threads.append(threading.Thread(target=incremental_load_new_products))
threads.append(threading.Thread(target=incremental_load_new_orders))

for t in threads:
    t.start()

for t in threads:
    t.join()
