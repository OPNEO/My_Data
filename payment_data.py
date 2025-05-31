import psycopg2
from kafka import KafkaProducer
from json import dumps
from datetime import date, datetime
from decimal import Decimal  # <-- Import Decimal

# --- PostgreSQL Connection Details ---
db_host = "localhost"
db_user = "postgres"
db_password = "1234"
db_name = "dvdrental"
db_port = "5432"

conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        dbname=db_name,
        port=db_port
    )

    print("✅ Connection successful!")

    cur = conn.cursor()
    cur.execute("SELECT * FROM payment")
    payments = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]

    print(f"Fetched {len(payments)} rows from PostgreSQL.")

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    print("✅ Kafka Producer created.")
    topic_name = 'payment_data_topic'

    for row in payments:
        payment_data = dict(zip(column_names, row))

        # --- Convert datetime and Decimal types ---
        for key, value in payment_data.items():
            if isinstance(value, (date, datetime)):
                payment_data[key] = value.isoformat()
            elif isinstance(value, Decimal):
                payment_data[key] = float(value)
        # --- End conversion ---

        producer.send(topic_name, value=payment_data)
        print(f"Sent payment_id {payment_data['payment_id']} to topic {topic_name}")

    producer.flush()
    print("✅ All payment data sent to Kafka.")

except Exception as e:
    print(f"❌ An error occurred: {e}")

finally:
    if cur:
        cur.close()
        print("Cursor closed.")
    if conn:
        conn.close()
        print("Connection closed.")
