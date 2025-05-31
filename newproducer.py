import psycopg2
from kafka import KafkaProducer
from json import dumps
from datetime import date, datetime

# PostgreSQL connection configuration
DB_HOST = "localhost"
DB_USER = "postgres"
DB_PASSWORD = "1234"
DB_NAME = "dvdrental"
DB_PORT = "5432"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'customer_data'


def json_serializer(obj):
    """Custom serializer for date/datetime objects"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT
    )
    print("✅ PostgreSQL connection established")

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: dumps(x, default=json_serializer).encode('utf-8')
    )
    print("✅ Kafka producer created")

    with conn.cursor() as cur:
        # Fetch all rows from customer table
        cur.execute("SELECT * FROM customer")
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        total_rows = len(rows)
        print(f"📊 Fetched {total_rows} rows from database")

        # Process and send each row
        for i, row in enumerate(rows, 1):
            # Convert row to dictionary with column names
            record = dict(zip(column_names, row))

            # Send to Kafka
            producer.send(TOPIC_NAME, value=record)

            # Progress reporting
            if i % 100 == 0 or i == total_rows:
                print(f"🚀 Sent {i}/{total_rows} records (customer_id: {record['customer_id']})")

    # Ensure all messages are delivered
    producer.flush()
    print(f"✅ All {total_rows} records sent to Kafka topic '{TOPIC_NAME}'")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    # Clean up resources
    if 'conn' in locals() and conn:
        conn.close()
        print("🔌 PostgreSQL connection closed")