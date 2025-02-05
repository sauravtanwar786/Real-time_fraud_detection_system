

# 3 dictionary
# customer -> 10
# terminal -> 5
# transaction -> 50
# 
# 
# 
#  first dump customer and terminal data in the tables
# on gap on 10 sec transaction data


import psycopg2
from time import sleep
from datetime import datetime
import os

# Read environment variables
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_USER = os.getenv('POSTGRES_USER', 'postgres')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
DB_NAME = os.getenv('POSTGRES_DB', 'postgres')

# Database configuration
DB_CONFIG = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': DB_PORT
}

customer_data = {
    'customer_id': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    'x_location': [54.881350, 60.276338, 42.365480, 64.589411, 43.758721, 89.177300, 96.366276, 38.344152, 79.172504, 52.889492],
    'y_location': [71.518937, 54.488318, 64.589411, 43.758721, 89.177300, 96.366276, 38.344152, 79.172504, 52.889492, 56.804456]
}
terminal_data = {
    'terminal_id': [0, 1, 2, 3, 4],
    'x_location': [54.881350, 60.276338, 42.365480, 43.758721, 96.366276],
    'y_location': [71.518937, 54.488318, 64.589411, 89.177300, 38.344152]
}


transactions_data = {
    'tx_id': [0,1, 2, 3, 4, 5, 6, 7, 8, 9],
    'tx_datetime': [
        '2025-01-15 11:32:19', 
        '2025-01-28 09:12:45', 
        '2025-01-20 14:45:50', 
        '2025-01-18 18:32:10', 
        '2025-01-29 05:23:40', 
        '2025-01-27 10:45:00', 
        '2025-02-02 19:23:15', 
        '2025-01-24 23:50:30', 
        '2025-01-19 08:40:50', 
        '2025-01-25 14:15:00'
    ],
    'customer_id': [3, 7, 1, 0, 4, 9, 8, 2, 6, 5],
    'terminal_id': [2, 4, 1, 3, 0, 1, 2, 4, 3, 0],
    'tx_amount': [120.50, 897.34, 543.78, 67.89, 299.99, 876.21, 245.67, 489.34, 112.45, 639.50]
}

# Function to insert data
def insert_data():
    try:
        # Connect to PostgreSQL
        count = 0

        while count < 4:
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cursor = conn.cursor()
                break
            except Exception as e:
                print("Error:", e)
                count += 1
                sleep(5)
        
        # Insert customer data
        for i in range(len(customer_data['customer_id'])):
            cursor.execute("""
                INSERT INTO payment.customers (customer_id, x_location, y_location) 
                VALUES (%s, %s, %s)
                ON CONFLICT (customer_id) 
                DO UPDATE SET 
                x_location = EXCLUDED.x_location,
                y_location = EXCLUDED.y_location;
            """, (
                customer_data['customer_id'][i],
                customer_data['x_location'][i],
                customer_data['y_location'][i]
            ))
        
        # Insert terminal data
        for i in range(len(terminal_data['terminal_id'])):
            cursor.execute("""
                INSERT INTO payment.terminals (terminal_id, x_location, y_location) 
                VALUES (%s, %s, %s)
                ON CONFLICT (terminal_id) 
                DO UPDATE SET 
                x_location = EXCLUDED.x_location,
                y_location = EXCLUDED.y_location;
            """, (
                terminal_data['terminal_id'][i],
                terminal_data['x_location'][i],
                terminal_data['y_location'][i]
            ))

        conn.commit()
        print("Customer and Terminal data inserted successfully!")

        # Insert transaction data with 10 seconds delay
        for i in range(len(transactions_data['tx_id'])):
            cursor.execute("""
                INSERT INTO payment.transactions (tx_id,tx_datetime, customer_id, terminal_id, tx_amount) 
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tx_id) 
                DO UPDATE SET 
                tx_datetime = EXCLUDED.tx_datetime,
                customer_id = EXCLUDED.customer_id,
                terminal_id = EXCLUDED.terminal_id,
                tx_amount = EXCLUDED.tx_amount;
            """, (
                transactions_data['tx_id'][i],
                datetime.strptime(transactions_data['tx_datetime'][i], '%Y-%m-%d %H:%M:%S'),
                transactions_data['customer_id'][i],
                transactions_data['terminal_id'][i],
                transactions_data['tx_amount'][i]
            ))
            conn.commit()
            print(f"Transaction {i+1} inserted!")
            sleep(10)  # 10 seconds delay
        
        # Close the connection
        cursor.close()
        conn.close()
        print("Data insertion completed!")

    except Exception as e:
        print("Error:", e)

# Run the script
if __name__ == "__main__":
    insert_data()



# https://debezium.io/documentation/reference/stable/connectors/postgresql.html
# postgres config changes
# Kafka connect config + container
# Kafka Container 


