import clickhouse_connect

import os
from dotenv import load_dotenv

load_dotenv('../.env')

clickhouse_host = os.getenv('CLICKHOUSE_HOST')
clickhouse_port = os.getenv('CLICKHOUSE_PORT')
clickhouse_db = os.getenv('CLICKHOUSE_DB')
clickhouse_user = os.getenv('CLICKHOUSE_USER')
clickhouse_pass = os.getenv('CLICKHOUSE_PASS')


# Connect to ClickHouse server
client = clickhouse_connect.get_client(
    host=f'{clickhouse_host}',
    port=clickhouse_port, 
    database=f'{clickhouse_db}',
    username=f'{clickhouse_user}', 
    password=f'{clickhouse_pass}'
    )

# SQL statements to create tables
create_users_table = """
CREATE TABLE IF NOT EXISTS users (
    id Int32,
    name String,
    phone String,
    date_of_birth DateTime,
    birth_decade Int32,
    created_at Date,
    PRIMARY KEY (id)
) ENGINE = MergeTree()
ORDER BY id;
"""

create_products_table = """
CREATE TABLE IF NOT EXISTS products (
    id Int32,
    name String,
    price Float32,
    price_range String,
    valid_from Date,
    valid_to Date,
    is_current Bool,
    PRIMARY KEY (id)
) ENGINE = MergeTree()
ORDER BY id;
"""

create_orders_table = """
CREATE TABLE IF NOT EXISTS orders (
    user_id Int32,
    product_id Int32,
    amount Int32,
    discount Float32 DEFAULT 0,
    price Float32,
    final_price Float32,
    date DateTime,
    year Int32,
    month Int32,
    PRIMARY KEY (user_id, product_id, date)
) ENGINE = MergeTree()
ORDER BY (user_id, product_id, date);
"""

# Execute the SQL statements
client.command(create_users_table)
client.command(create_products_table)
client.command(create_orders_table)

print("Tables created successfully!")
