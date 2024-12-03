import psycopg2
import os
from dotenv import load_dotenv

def connect_to_db():
    # Load .env file
    load_dotenv()

    # Establish a connection to PostgreSQL
    try:
        print(f'Connection to database initiated ...')
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASS')
        )
        print(f'Successfully established the connection to the database.')
    except Exception as e:
        print('Something went wrong, while connecting to the database:', e)

    return conn

