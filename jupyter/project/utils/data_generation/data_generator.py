import psycopg2
from psycopg2 import extras
from faker import Faker
import faker_commerce
import random
import os
from dotenv import load_dotenv

from utils.data_generation import create_tables
from utils.db_connect import connect_to_db

# Connection to the database
conn = connect_to_db()
curr = conn.cursor()

# Initialize Faker to generate fake data
fake = Faker()
fake.add_provider(faker_commerce.Provider) # adding faker-commerce provider to generate random product names

# Generate Users data
def generate_users(num_users):
    users = []
    for _ in range(num_users):
        users.append((
            fake.name(),
            fake.phone_number(),
            fake.date_of_birth()
        ))

    return users


# Generate a set of unique random integers
def generate_unique_integers(num_values, min_value, max_value):
    unique_values = set()

    while len(unique_values) < num_values:
        unique_values.add(random.randint(min_value, max_value))

    return list(unique_values)


# Generate Products data
def generate_products(num_products, min_value, max_value):
    products = []
    for i in generate_unique_integers(num_products, min_value, max_value):
        products.append((
            i,
            fake.ecommerce_name(),
            round(random.uniform(5.0, 1000.0), 2)
        ))

    return products


# Generate Orders data
def generate_orders(num_orders, num_users, num_products):
    orders = []
    for _ in range(num_orders):
        user_id = random.randint(1, num_users)
        product_id = random.randint(1, num_products)
        amount = random.randint(1, 20)
        discount = round(random.uniform(0.0, 50.0), 2)  # Discount between 0% to 50%
        price = round(random.uniform(5.0, 1000.0), 2)  # Original price
        final_price = round(price * (100 - discount) / 100, 2)  # Discounted price
        date = fake.date_time_between(start_date='-1y', end_date='now')  # Random date within the last year
        orders.append((
            user_id,
            product_id,
            amount,
            discount,
            price,
            final_price,
            date
        ))

    return orders


# Insert data into the database
def insert_data(table, data, columns):
    query = f"INSERT INTO {table} ({','.join(columns)}) VALUES %s"
    try:
        print(f"Inserting data into {table} table ...")
        extras.execute_values(curr, query, data)
        print(f'Successfully inserted data into {table} table.')
    except Exception as e:
        print(f'Failed to insert data into {table} table:', e)
        
    conn.commit()


if __name__ == "__main__":
    # Create tables
    create_tables()


    # Set the number of rows to insert
    num_users = 10_000_000  # 10 million users
    num_products = 1_200_000  # 1.2 million products
    num_orders = 90_000_000  # 90 million orders


    # Generate and insert Users
    batch_size = 10000  # Insert in batches to avoid memory overload
    for i in range(1, num_users + 1, batch_size):
        print(f"Inserting users batch {i} - {i + batch_size - 1}")
        users_data = generate_users(batch_size)
        insert_data('Users', users_data, ['name', 'phone', 'date_of_birth'])


    # Generate and insert Products
    for i in range(1, num_products + 1, batch_size):
        print(f"Inserting products batch {i} - {i + batch_size - 1}")
        products_data = generate_products(batch_size, i, i + batch_size - 1)
        insert_data('Products', products_data, ['id', 'name', 'price'])


    # Generate and insert Orders
    for i in range(1, num_orders, batch_size):
        print(f"Inserting orders batch {i} - {i + batch_size - 1}")
        orders_data = generate_orders(batch_size, num_users, num_products)
        insert_data('Orders', orders_data, ['user_id', 'product_id', 'amount', 'discount', 'price', 'final_price', 'date'])


    # Close the connection
    curr.close()
    conn.close()
