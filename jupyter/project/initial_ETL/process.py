from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os
from dotenv import load_dotenv

load_dotenv('../utils/.env')

spark = SparkSession. \
    builder. \
    config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0"). \
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"). \
    config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"). \
    config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true"). \
    appName('Process raw data and load into Delta Lake silver layer'). \
    getOrCreate()


silver_path = '/user/jupyter/silver'
bronze_path = '/user/jupyter/bronze'

clickhouse_host = os.getenv('CLICKHOUSE_HOST')
clickhouse_port = os.getenv('CLICKHOUSE_PORT')
clickhouse_db = os.getenv('CLICKHOUSE_DB')
clickhouse_user = os.getenv('CLICKHOUSE_USER')
clickhouse_pass = os.getenv('CLICKHOUSE_PASS')

clickhouse_url = f'jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_db}'

def load_processed_orders_table():
    for year in range(2023, 2025):
        for month in range(1, 13):
            try:
                print(f"Reading data from bronze orders in year={year} and month={month} ...")
                monthly_orders_df = spark. \
                    read. \
                    format('delta'). \
                    load(f'{bronze_path}/orders'). \
                    filter(f"""
                            year = {year} AND 
                            month = {month}
                            """). \
                    dropDuplicates(['user_id', 'product_id', 'date'])
                print(f"Successfully loaded bronze orders in year={year} and month={month}.")
            except Exception as e:
                print(f"Error occured during loading bronze orders in year={year} and month={month} :", e)
            
            try:
                print(f"Writing data into silver orders in year={year} and month={month} partition ...")
                monthly_orders_df. \
                    coalesce(1). \
                    write. \
                    format('delta'). \
                    mode('append'). \
                    partitionBy('year', 'month'). \
                    save(f'{silver_path}/orders')
                print(f"Successfully wrote silver orders in year={year} and month={month} partition.")
            except Exception as e:
                print(f"Error occured during writing silver orders in year={year} and month={month} partition:", e)

            try:
                print(f"Writing orders data in year={year} and month={month} into Clickhouse ...")
                monthly_orders_df. \
                    write. \
                    format("jdbc"). \
                    option("batchsize", "10000"). \
                    option("url", f"{clickhouse_url}"). \
                    option("dbtable", "products"). \
                    option("user", f"{clickhouse_user}"). \
                    option("password", f"{clickhouse_pass}"). \
                    mode("append"). \
                    save()
                print(f'Successfully wrote orders data in year={year} and month={month} into Clickhouse.')
            except Exception as e:
                print(f'Error occurred during loading orders data in year={year} and month={month} into Clickhouse:', e)




def load_processed_products_table():
    for i in range(5, 1001, 100):
        rng = f'{i}-{i + 100}' if i != 905 else '905-1000'
        try:
            print(f"Reading data from bronze products in price_range={rng} ...")
            sliced_products_df = spark. \
                read. \
                format('delta'). \
                load(f'{bronze_path}/products'). \
                filter(f"""
                        price_range='{rng}'
                        """). \
                dropDuplicates(['id']). \
                withColumn('valid_from', F.current_date()). \
                withColumn('valid_to', F.lit(None).cast('date')). \
                withColumn('is_current', F.lit(True).cast('boolean'))
            print(f"Successfully loaded bronze products in price_range={rng}.")
        except Exception as e:
            print(f"Error occured during loading bronze products in price_range={rng} :", e)
        try:
            print(f"Writing data into silver products in partition_key partition ...")
            sliced_products_df. \
                withColumn('partition_key', (F.hash('id') % 20 + 20) % 20). \
                coalesce(1). \
                write. \
                format('delta'). \
                mode('append'). \
                partitionBy('partition_key'). \
                save(f'{silver_path}/products')
            print(f"Successfully wrote silver products in partition_key partition.")
        except Exception as e:
            print(f"Error occured during writing silver products in partition_key partition.:", e)
        
        try:
            print(f"Writing products data in price_range={rng} into Clickhouse ...")
            sliced_products_df. \
                write. \
                format("jdbc"). \
                option("batchsize", "10000"). \
                option("url", f"{clickhouse_url}"). \
                option("dbtable", "products"). \
                option("user", f"{clickhouse_user}"). \
                option("password", f"{clickhouse_pass}"). \
                mode("append"). \
                save()
            print(f'Successfully wrote products data in price_range={rng} into Clickhouse.')
        except Exception as e:
            print(f'Error occurred during loading products data in price_range={rng} into Clickhouse:', e)


def load_processed_users_table():
    for i in range(1900, 2021, 10):
        try:
            print(f"Reading data from bronze users in birth_decade={i} ...")
            sliced_users_df = spark. \
                read. \
                format('delta'). \
                load(f'{bronze_path}/users'). \
                filter(f"""
                                birth_decade={i}
                            """). \
                dropDuplicates(['id']). \
                withColumn('created_at', F.current_date())
            print(f"Successfully loaded bronze users in birth_decade={i}.")
        except Exception as e:
            print(f"Error occured during loading users in birth_decade={i} :", e)
        try:
            print(f"Writing data into users in partition_key partition ...")
            sliced_users_df. \
                withColumn('partition_key', (F.hash('id') % 20 + 20) % 20). \
                coalesce(1). \
                write. \
                format('delta'). \
                mode('append'). \
                partitionBy('partition_key'). \
                save(f'{silver_path}/users')
            print(f"Successfully wrote silver users in partition_key partition.")
        except Exception as e:
            print(f"Error occured during writing silver users in partition_key partition.:", e)

        try:
            print(f"Writing users data with birth_decade={i} into Clickhouse ...")
            sliced_users_df. \
                write. \
                format("jdbc"). \
                option("batchsize", "10000"). \
                option("url", f"{clickhouse_url}"). \
                option("dbtable", "products"). \
                option("user", f"{clickhouse_user}"). \
                option("password", f"{clickhouse_pass}"). \
                mode("append"). \
                save()
            print(f'Successfully wrote products data with birth_decade={i} into Clickhouse.')
        except Exception as e:
            print(
                f'Error occurred during loading products data with birth_decade={i} into Clickhouse:', e)


