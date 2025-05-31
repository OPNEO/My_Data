import psycopg2
from kafka import KafkaProducer
from json import dumps
from datetime import date, datetime # Import date and datetime

# --- Connection Details for your PostgreSQL Server ---
db_host = "localhost"
db_user = "postgres"
db_password = "1234"      # IMPORTANT: The password must be a string
db_name = "dvdrental"      # A default database to connect to
db_port = "5432"

conn = None # Initialize conn to None
cur = None  # Initialize cur to None

try:
    # This single line will try to connect. If it fails, the program will crash.
    conn = psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        dbname=db_name,
        port=db_port
    )

    print("✅ Connection successful!")

    cur = conn.cursor()

    cur.execute("SELECT * FROM customer ")

    customers = cur.fetchall()

    column_names = [desc[0] for desc in cur.description]

    print(f"Fetched {len(customers)} customers from PostgreSQL.")

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    print("✅ Kafka Producer created.")

    topic_name = 'customer_data_topic' # Define your Kafka topic name

    for customer_row in customers:
        customer_data = dict(zip(column_names, customer_row))

        # --- Convert date/datetime objects to string ---
        for key, value in customer_data.items():
            if isinstance(value, (date, datetime)):
                customer_data[key] = value.isoformat()
        # --- End conversion ---

        producer.send(topic_name, value=customer_data)
        print(f"Sent customer_id {customer_data['customer_id']} to topic {topic_name}")

    producer.flush()
    print("All customer data sent to Kafka.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if cur:
        cur.close()
        print("Cursor closed.")
    if conn:
        conn.close()
        print("Connection closed.")