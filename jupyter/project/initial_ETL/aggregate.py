from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession. \
    builder. \
    config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0"). \
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"). \
    config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"). \
    config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true"). \
    appName('Aggregate silver data and load into Delta Lake gold layer'). \
    getOrCreate()


silver_path = '/user/jupyter/silver'
gold_path = '/user/jupyter/gold'


def load_sales_summary_by_product():
    orders_df = spark. \
        read. \
        load(f'{silver_path}/orders')
    
    products_df = spark. \
        read. \
        load(f'{silver_path}/products')

    orders_trans_df = orders_df. \
        withColumn('total_revenue_after_discount', 
                   F.round(F.col('amount') * F.col('final_price'), 2)
                   ). \
        withColumn('total_revenue_before_discount', 
                   F.round(F.col('amount') * F.col('price'), 2)
                   )
    
    sales_summary_by_product_df = orders_trans_df. \
        join(products_df, products_df.id == orders_trans_df.product_id). \
        groupBy(
            orders_trans_df.year,
            orders_trans_df.month,
            products_df.id.alias('product_id'),
            products_df.name.alias('product_name')
        ). \
        agg(
            F.round(F.sum(orders_trans_df.amount), 2).alias('total_sale'),
            F.round(F.sum(orders_trans_df.total_revenue_before_discount),
                    2).alias('total_revenue_before_discount'),
            F.round(F.sum(orders_trans_df.total_revenue_after_discount),
                    2).alias('total_revenue_after_discount'),
            F.round(F.avg(orders_trans_df.discount),
                    2).alias('average_discount'),
            F.count(orders_trans_df.user_id).alias('total_orders')
        ). \
        orderBy(F.col('total_revenue_after_discount').desc(),
                F.col('total_orders').desc())
    
    sales_summary_by_product_df. \
        coalesce(1). \
        write. \
        format('delta'). \
        partitionBy('year', 'month'). \
        mode('overwrite'). \
        save(f'{gold_path}/sales_summary_by_product')


def load_customer_lifetime_value():
    users_df = spark. \
        read. \
        load(f'{silver_path}/users')
    
    orders_df = spark. \
        read. \
        load(f'{silver_path}/orders')
    
    orders_trans_df = orders_df. \
        withColumn('total_revenue_after_discount',
                   F.round(F.col('amount') * F.col('final_price'), 2)
                   ). \
        withColumn('total_revenue_before_discount',
                   F.round(F.col('amount') * F.col('price'), 2)
                   )
    
    customer_lifetime_value_df = orders_trans_df. \
        join(users_df, users_df.id == orders_trans_df.user_id). \
        groupBy(
            orders_trans_df.user_id,
            users_df.name.alias('user_name')
        ). \
        agg(
            F.round(F.sum(orders_trans_df.total_revenue_after_discount),
                    2).alias('total_spent'),
            F.count(orders_trans_df.user_id).alias('total_orders'),
            F.round(F.avg(orders_trans_df.total_revenue_after_discount),
                    2).alias('average_spent'),
            F.date_format(F.min(orders_trans_df.date),
                          'yyyy-MM-dd').cast('date').alias('first_order_date'),
            F.date_format(F.max(orders_trans_df.date),
                          'yyyy-MM-dd').cast('date').alias('last_order_date')
        ). \
        withColumn('segment',
                   F.when(F.col('total_spent') > 50000, 'Excellent buyer').
                   when(F.col('total_spent') > 25000, 'Good buyer').
                   when(F.col('total_spent') > 10000, 'Average buyer').
                   when(F.col('total_spent') > 5000, 'Occasional buyer').
                   otherwise('Passing buyer')
                   )
    
    customer_lifetime_value_df. \
        coalesce(4). \
        write. \
        format('delta'). \
        mode('overwrite'). \
        save(f'{gold_path}/customer_lifetime_value')


def load_top_products_by_revenue():
    window_spec = Window.orderBy(F.col('total_revenue_after_discount').desc())

    orders_df = spark. \
        read. \
        load(f'{silver_path}/orders')

    products_df = spark. \
        read. \
        load(f'{silver_path}/products')

    orders_trans_df = orders_df. \
        withColumn('total_revenue_after_discount',
                   F.round(F.col('amount') * F.col('final_price'), 2)
                   ). \
        withColumn('total_revenue_before_discount',
                   F.round(F.col('amount') * F.col('price'), 2)
                   )

    sales_summary_by_product_df = orders_trans_df. \
        join(products_df, products_df.id == orders_trans_df.product_id). \
        groupBy(
            orders_trans_df.year,
            orders_trans_df.month,
            products_df.id.alias('product_id'),
            products_df.name.alias('product_name')
        ). \
        agg(
            F.round(F.sum(orders_trans_df.amount), 2).alias('total_sale'),
            F.round(F.sum(orders_trans_df.total_revenue_before_discount),
                    2).alias('total_revenue_before_discount'),
            F.round(F.sum(orders_trans_df.total_revenue_after_discount),
                    2).alias('total_revenue_after_discount'),
            F.round(F.avg(orders_trans_df.discount),
                    2).alias('average_discount'),
            F.count(orders_trans_df.user_id).alias('total_orders')
        ). \
        orderBy(F.col('total_revenue_after_discount').desc(),
                F.col('total_orders').desc())
    
    top_products_df = sales_summary_by_product_df. \
        withColumn('rank', F.rank().over(window_spec)). \
        filter(F.col('rank') <= 10)
    
    top_products_df. \
        coalesce(1). \
        write. \
        format('delta'). \
        mode('overwrite'). \
        save(f'{gold_path}/top_products_by_revenue')


