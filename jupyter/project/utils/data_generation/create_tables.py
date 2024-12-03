from utils.db_connect import connect_to_db

conn = connect_to_db()
cur = conn.cursor()

def create_tables():
    try:
        print('Creating users table ...')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                phone VARCHAR(255) NOT NULL,
                date_of_birth TIMESTAMP NOT NULL
            );
        ''')
        print('Successfully created users table.')
    except Exception as e:
        print('Failed to create users table:', e)
    
    try:
        print('Creating products table ...')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INT UNIQUE,
                name VARCHAR(255) NOT NULL,
                price FLOAT NOT NULL
            );
        ''')
        print('Successfully created products table.')
    except Exception as e:
        print('Failed to create products table:', e)

    try:
        print('Creating orders table ...')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                user_id INT REFERENCES users(id),
                product_id INT REFERENCES products(id),
                amount INT NOT NULL,
                discount FLOAT DEFAULT 0,
                price FLOAT NOT NULL,
                final_price FLOAT NOT NULL,
                date TIMESTAMP NOT NULL,
                PRIMARY KEY (user_id,product_id, date)
            );
        ''')
        print('Successfully created orders table.')
    except Exception as e:
        print('Failed to create orders table:', e)

    conn.commit()
