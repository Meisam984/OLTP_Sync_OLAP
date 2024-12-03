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


def incremental_load_new_products():
    """
    Using Spark streaming, the data fetched by Kafka Debezium Connector is parsed.
    Then, according to the "op" value, in the kafka topic, the data in the silver
    Delta table is merged.
    Also, the raw data is appended to Bronze layer.
    """
    checkpoint_loaction = f'{bronze_path}/products/checkpoints'
    kafka_topic = 'debezium.public.products'

    product_schema = StructType(). \
        add('id', IntegerType()). \
        add('name', StringType()). \
        add('price', DoubleType())
    
    full_schema = StructType([
        # Schema for 'before' can be empty if you don't need it
        StructField("before", StructType([]), True),
        # 'after' contains the actual record data
        StructField("after", product_schema, True),
        # Schema for 'source' can be omitted if you don't need it
        StructField("source", StructType([]), True),
        # Operation type, e.g., 'c' for insert, 'u' for update
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)      # Timestamp for the event
    ])
    try:
        print('Reading streams of new data from products table ...')
        kafka_products_df = spark. \
            readStream. \
            format('kafka'). \
            option('kafka.bootstrap.servers', f'{kafka_brokers}'). \
            option('subscribe', f'{kafka_topic}'). \
            option("startingOffsets", "earliest"). \
            option("failOnDataLoss", "false"). \
            load()
        print('Successfully read streams of new data from products table.')
    except Exception as e:
        print('Failed to load streams of new data from products table:', e)
    
    def write_new_products_to_bronze_and_upsert_to_silver(micro_batch_df, batch_id):
        """
        This function writes streams of new data from products table into Bronze layer
        and Merges into Silver layer using Delta table, on top of silver data.        
        """
        silver_products = DeltaTable.forPath(spark, f'{silver_path}/products')


        products_raw_stream_df = micro_batch_df. \
            selectExpr('CAST(value AS STRING) AS value'). \
            withColumn('parsed_value', F.from_json(F.col('value'), schema=full_schema)). \
            withColumn('data', F.col('parsed_value.after')). \
            withColumn('operation', F.col('parsed_value.op')). \
            select(
                F.col('data.id').cast('int').alias('id'),
                F.col('data.name').cast('string').alias('name'),
                F.col('data.price').cast('double').alias('price'),
                F.col('operation')
            ). \
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
                    )

        products_stream_scd2_df = products_raw_stream_df. \
            withColumn('valid_from', F.current_date()). \
            withColumn('valid_to', F.lit(None).cast('date')). \
            withColumn('is_current', F.lit(True))


        products_raw_stream_df. \
            select('id', 'name', 'price', 'price_range'). \
            coalesce(1). \
            write. \
            format('delta'). \
            mode('append'). \
            partitionBy('price_range'). \
            save(f'{bronze_path}/products')
        
        silver_products.alias('tgt'). \
            merge(
                products_stream_scd2_df.alias('src'),
                "src.id = tgt.id AND tgt.is_current = True"
        ). \
            whenMatchedUpdate(
                condition="src.operation = 'u'",
                set={
                    'tgt.valid_to': "current_date()",
                    'tgt.is_current': 'False'
                }
        ). \
            whenNotMatchedInsert(
                values={
                    "id": "src.id",
                    "name": "src.name",
                    "price": "src.price",
                    "price_range": """
                                    CASE 
                                        WHEN src.price >= 5 AND src.price < 105 THEN '5-105'
                                        WHEN src.price >= 105 AND src.price < 205 THEN '105-205'
                                        WHEN src.price >= 205 AND src.price < 305 THEN '205-305'
                                        WHEN src.price >= 305 AND src.price < 405 THEN '305-405'
                                        WHEN src.price >= 405 AND src.price < 505 THEN '405-505'
                                        WHEN src.price >= 505 AND src.price < 605 THEN '505-605'
                                        WHEN src.price >= 605 AND src.price < 705 THEN '605-705'
                                        WHEN src.price >= 705 AND src.price < 805 THEN '705-805'
                                        WHEN src.price >= 805 AND src.price < 905 THEN '805-905'
                                        ELSE 'out_of_range'
                                    END
                                """,
                    "valid_from": "src.valid_from",
                    "valid_to": "src.valid_to",
                    "is_current": "src.is_current"
                }
        ).execute()

        # upsert new products data into Clickhouse
        products_stream_scd2_ids = [
            row['id'] for row in products_stream_scd2_df.select('id').distinct().collect()
        ]

        products_stream_scd2_ids_str = ','.join(
            [str(int(id)) for id in products_stream_scd2_ids])


        existing_products_query = f"""
            (SELECT * FROM products WHERE id IN ({products_stream_scd2_ids_str}) AND is_current = FALSE)
        """

        existing_filtered_products_df = spark. \
            read. \
            format("jdbc"). \
            option("url", f"{clickhouse_url}"). \
            option("driver", "com.clickhouse.jdbc.ClickHouseDriver"). \
            option("query", existing_products_query). \
            option("user", f"{clickhouse_user}"). \
            option("password", f"{clickhouse_pass}"). \
            load()
        
        records_to_update_df = existing_filtered_products_df. \
            withColumn("is_current", F.lit(False)). \
            withColumn("valid_to", F.current_time())
        
        records_to_update_df. \
            write. \
            format("jdbc"). \
            option("url", f"{clickhouse_url}"). \
            option("driver", "com.clickhouse.jdbc.ClickHouseDriver"). \
            option("dbtable", "products"). \
            option("user", f"{clickhouse_user}"). \
            option("password", f"{clickhouse_pass}"). \
            mode("append"). \
            save()
        
        new_records_to_insert_df = products_stream_scd2_df. \
            join(existing_filtered_products_df, 'id', 'left_anti')
        
        new_records_to_insert_df. \
            write. \
            format("jdbc"). \
            option("url", f"{clickhouse_url}"). \
            option("driver", "com.clickhouse.jdbc.ClickHouseDriver"). \
            option("dbtable", "products"). \
            option("user", f"{clickhouse_user}"). \
            option("password", f"{clickhouse_pass}"). \
            mode("append"). \
            save()
    
    product_streaming_query = kafka_products_df. \
        writeStream. \
        foreachBatch(write_new_products_to_bronze_and_upsert_to_silver). \
        outputMode('append'). \
        option('checkpointLocation', checkpoint_loaction). \
        trigger(processingTime='2 minutes'). \
        start()

    product_streaming_query.awaitTermination()


def incremental_load_new_users():
    checkpoint_loaction = f'{bronze_path}/users/checkpoints'
    kafka_topic = 'debezium.public.users'

    user_schema = StructType(). \
        add('id', IntegerType()). \
        add('name', StringType()). \
        add('phone', StringType()). \
        add('date_of_birth', TimestampType())
        

    full_schema = StructType([
        # Schema for 'before' can be empty if you don't need it
        StructField("before", StructType([]), True),
        # 'after' contains the actual record data
        StructField("after", user_schema, True),
        # Schema for 'source' can be omitted if you don't need it
        StructField("source", StructType([]), True),
        # Operation type, e.g., 'c' for insert, 'u' for update
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)      # Timestamp for the event
    ])

    kafka_users_df = spark. \
        readStream. \
        format('kafka'). \
        option('kafka.bootstrap.servers', f'{kafka_brokers}'). \
        option('subscribe', f'{kafka_topic}'). \
        option("startingOffsets", "earliest"). \
        option("failOnDataLoss", "false"). \
        load()
    

    def write_new_users_to_bronze_and_upsert_to_silver(micro_batch_df, batch_id):
        silver_users = DeltaTable.forPath(spark, f'{silver_path}/users')
        users_raw_stream_df = micro_batch_df. \
            selectExpr('CAST(value AS STRING) AS value'). \
            withColumn('parsed_value', F.from_json(F.col('value'), schema=full_schema)). \
            withColumn('data', F.col('parsed_value.after')). \
            withColumn('operation', F.col('parsed_value.op')). \
            select(
                F.col('data.id').cast('int').alias('id'),
                F.col('data.name').cast('string').alias('name'),
                F.col('data.phone').cast('string').alias('phone'),
                F.col('data.date_of_birth'). \
                    cast('timestamp').alias('date_of_birth'),
                F.col('operation')
            ). \
            withColumn('birth_decade', (F.floor(
                F.year('date_of_birth') / 10) * 10).cast('int'))

        users_silver_stream_df = users_raw_stream_df. \
            withColumn('created_at', F.current_date())

        users_raw_stream_df. \
            select('id', 'name', 'phone', 'date_of_birth', 'birth_decade'). \
            coalesce(1). \
            write. \
            format('delta'). \
            mode('append'). \
            partitionBy('birth_decade'). \
            save(f'{bronze_path}/users')

        silver_users.alias('tgt'). \
            merge(
                users_silver_stream_df.alias('src'),
                "src.id = tgt.id"
        ). \
        whenMatchedUpdateAll(). \
        whenNotMatchedInsertAll(). \
        execute()

        # insert new users data into Clickhouse
        users_silver_stream_df. \
            write. \
            format("jdbc"). \
            option("url", f"{clickhouse_url}"). \
            option("driver", "com.clickhouse.jdbc.ClickHouseDriver"). \
            option("dbtable", "users"). \
            option("user", f"{clickhouse_user}"). \
            option("password", f"{clickhouse_pass}"). \
            mode("append"). \
            save()


    user_streaming_query = kafka_users_df. \
        writeStream. \
        foreachBatch(write_new_users_to_bronze_and_upsert_to_silver). \
        outputMode('append'). \
        option('checkpointLocation', checkpoint_loaction). \
        trigger(processingTime='2 minutes'). \
        start()
    
    user_streaming_query.awaitTermination()
          

def incremental_load_new_orders():
    checkpoint_loaction = f'{bronze_path}/orders/checkpoints'
    kafka_topic = 'debezium.public.orders'

    order_schema = StructType(). \
        add('user_id', IntegerType()). \
        add('product_id', IntegerType()). \
        add('amount', IntegerType()). \
        add('discount', DoubleType()). \
        add('price', DoubleType()). \
        add('final_price', DoubleType()). \
        add('date', DateType())

    full_schema = StructType([
        # Schema for 'before' can be empty if you don't need it
        StructField("before", StructType([]), True),
        # 'after' contains the actual record data
        StructField("after", order_schema, True),
        # Schema for 'source' can be omitted if you don't need it
        StructField("source", StructType([]), True),
        # Operation type, e.g., 'c' for insert, 'u' for update
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)      # Timestamp for the event
    ])

    kafka_orders_df = spark. \
        readStream. \
        format('kafka'). \
        option('kafka.bootstrap.servers', f'{kafka_brokers}'). \
        option('subscribe', f'{kafka_topic}'). \
        option("startingOffsets", "earliest"). \
        option("failOnDataLoss", "false"). \
        load()
    
    def write_new_orders_to_bronze_and_upsert_to_silver(micro_batch_df, batch_id):
        silver_orders = DeltaTable.forPath(spark, f'{silver_path}/orders')
        orders_raw_stream_df = micro_batch_df. \
            selectExpr('CAST(value AS STRING) AS value'). \
            withColumn('parsed_value', F.from_json(F.col('value'), schema=full_schema)). \
            withColumn('data', F.col('parsed_value.after')). \
            withColumn('operation', F.col('parsed_value.op')). \
            select(
                F.col('data.user_id').cast('int').alias('user_id'),
                F.col('data.product_id').cast('int').alias('product_id'),
                F.col('data.amount').cast('int').alias('amount').
                F.col('data.discount').cast('double').alias('discount'),
                F.col('data.price').cast('double').alias('price'),
                F.col('data.final_price').cast('double').alias('final_price'),
                F.col('data.date').cast('timestamp').alias('date'),
                F.col('operation')
            )

        orders_silver_stream_df = orders_raw_stream_df. \
            withColumn('year', F.year(F.col('date'))). \
            withColumn('month', F.month(F.col('date')))

        orders_raw_stream_df. \
            select('user_id', 'product_id', 'amount', 'discount', 'price', 'final_price', 'date', 'year', 'month'). \
            coalesce(1). \
            write. \
            format('delta'). \
            mode('append'). \
            partitionBy('year', 'month'). \
            save(f'{bronze_path}/orders')

        silver_orders.alias('tgt'). \
            merge(
                orders_silver_stream_df.alias('src'),
                "src.user_id = tgt.user_id AND src.product = tgt.product_id AND src.date = tgt.date"
        ). \
            whenMatchedUpdateAll(). \
            whenNotMatchedInsertAll(). \
            execute()
        
        # insert new orders data into Clickhouse
        orders_silver_stream_df. \
            write. \
            format("jdbc"). \
            option("url", f"{clickhouse_url}"). \
            option("driver", "com.clickhouse.jdbc.ClickHouseDriver"). \
            option("dbtable", "orders"). \
            option("user", f"{clickhouse_user}"). \
            option("password", f"{clickhouse_pass}"). \
            mode("append"). \
            save()
        
    order_streaming_query = kafka_orders_df. \
        writeStream. \
        foreachBatch(write_new_orders_to_bronze_and_upsert_to_silver). \
        outputMode('append'). \
        option('checkpointLocation', checkpoint_loaction). \
        trigger(processingTime='2 minutes'). \
        start()
    
    order_streaming_query.awaitTermination() 




