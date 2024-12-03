from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import dateutil.relativedelta as relativedelta

import os
from dotenv import load_dotenv

load_dotenv('../utils/.env')

host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')
database = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASS')

bronze_path = 'user/jupyter/bronze'

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


def load_raw_users_table():
    '''
    Load raw users table data from postgres into Delta Lake on HDFS.
    The rows are read in batches of batch_size within num_partitions of partitions, beginning from 
    lower_bound up to upper_bound, according to the 'id' column. 
    Then, the datagrame is written into Delta Lake, partitioned by derived column of 'birth_decade' 
    off 'date_of_birth' column.
    '''

    table_name = 'users'
    partition_column = "id"
    num_partitions = 10
    batch_size = 1000000

    for i in range(10):
        lower_bound = i * batch_size + 1
        upper_bound = lower_bound + batch_size - 1

        print(f'Reading data batch from id: {lower_bound} to id: {upper_bound} ...')
        users_df = spark. \
            read. \
            jdbc(
                url=url,
                table=table_name,
                column=partition_column,
                numPartitions=num_partitions,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                properties=db_properties
            )

        print(f'Writing data batch from id: {lower_bound} to id: {upper_bound} ...')
        users_df. \
            withColumn('birth_decade', (F.floor(F.year('date_of_birth') / 10) * 10).cast('int')). \
            coalesce(1). \
            write. \
            mode('append'). \
            format('delta'). \
            partitionBy('birth_decade'). \
            save(f'{bronze_path}/users')


def load_raw_products_table():
    '''
    Load raw products table data from postgres into Delta Lake on HDFS.
    The rows are read in batches of batch_size within num_partitions of partitions, beginning from 
    lower_bound up to upper_bound, according to the 'id' column. 
    Then, the datagrame is written into Delta Lake, partitioned by derived column of 'price_range'
    off 'price' column.
    '''

    table_name = 'products'
    partition_column = "id"
    num_partitions = 10
    batch_size = 1000000

    for i in range(2):
        lower_bound = i * batch_size + 1
        upper_bound = lower_bound + batch_size - 1

        print(f'Reading data batch from id: {lower_bound} to id: {upper_bound} ...')
        products_df = spark. \
            read. \
            jdbc(
                url=url,
                table=table_name,
                column=partition_column,
                numPartitions=num_partitions,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                properties=db_properties
            )

        print(f'Writing data batch from id: {lower_bound} to id: {upper_bound} ...')
        products_df. \
            withColumn('price_range',
                       F.when((F.col('price') >= 5) & (F.col('price') < 105), '5-105').
                       when((F.col('price') >= 105) & (F.col('price') < 205), '105-205').
                       when((F.col('price') >= 205) & (F.col('price') < 305), '205-305').
                       when((F.col('price') >= 305) & (F.col('price') < 405), '305-405').
                       when((F.col('price') >= 405) & (F.col('price') < 505), '405-505').
                       when((F.col('price') >= 505) & (F.col('price') < 605), '505-605').
                       when((F.col('price') >= 605) & (F.col('price') < 705), '605-705').
                       when((F.col('price') >= 705) & (F.col('price') < 805), '705-805').
                       when((F.col('price') >= 805) & (F.col('price') < 905), '805-905').
                       when((F.col('price') >= 905) & (F.col('price') <= 1000), '905-1000').
                       otherwise('out_of_range')
                       ). \
            coalesce(1). \
            write. \
            mode('append'). \
            format('delta'). \
            partitionBy('price_range'). \
            save(f'{bronze_path}/products')


def load_raw_orders_table():
    '''
    Load raw orders table data from postgres into Delta Lake on HDFS.
    The rows are read in monthly batches, from current_date up to 1 month later minus 1 second, 
    beginning from start_date up to end_date, according to the 'date' column. 
    Then, the datagrame is written into Delta Lake, partitioned by derived columns of 'year' and 'month'
    off 'date' column.
    '''

    table_name = 'orders'
    num_partitions = 10
    fetch_size = 10000

    start_date = datetime(2023, 10, 9, 0, 0, 0)
    end_date = datetime(2024, 10, 10, 0, 0, 0)
    current_date = start_date

    while current_date <= end_date:
        try:
            print(f'''
                Reading data batch from {current_date.strftime('%Y-%m-%d %H:%M:%S')} to
                {(current_date + relativedelta.relativedelta(months=1) - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')} ...
                ''')
            orders_df = spark. \
                read. \
                format('jdbc'). \
                option("url", url). \
                option("driver", db_properties['driver']). \
                option("user", db_properties['user']). \
                option("password", db_properties['password']). \
                option("query", f"""
                    SELECT *
                    FROM {table_name}
                    WHERE
                        date BETWEEN '{current_date.strftime('%Y-%m-%d %H:%M:%S')}' AND
                        '{(current_date + relativedelta.relativedelta(months=1) - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')}'
                    """). \
                option('numPartitions', num_partitions). \
                option('fetchsize', fetch_size). \
                load()
        except Exception as e:
            print(f'''
                Failed to Reading data batch from {current_date.strftime('%Y-%m-%d %H:%M:%S')} to
                {(current_date + relativedelta.relativedelta(months=1) - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')} ...
                  ''', e)
        try:
            print(f'''
                Writing data batch from {current_date.strftime('%Y-%m-%d %H:%M:%S')} to
                {(current_date + relativedelta.relativedelta(months=1) - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')} ...
                ''')
            orders_df. \
                withColumn('year', F.year('date')). \
                withColumn('month', F.month('date')). \
                coalesce(1). \
                write. \
                mode('append'). \
                format('delta'). \
                partitionBy('year', 'month'). \
                save(f'{bronze_path}/orders')

            current_date += relativedelta.relativedelta(months=1)
        except Exception as e:
            print(f'''
                Failed to Write data batch from {current_date.strftime('%Y-%m-%d %H:%M:%S')} to
                {(current_date + relativedelta.relativedelta(months=1) - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')} ...
                ''', e)


