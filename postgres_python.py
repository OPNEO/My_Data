# #!/usr/bin/env python3
# """
# Kafka Consumer to PostgreSQL Writer
# Consumes messages from customer_data_topic and inserts them into PostgreSQL
# """
#
# import json
# import logging
# from kafka import KafkaConsumer
# import psycopg2
# from psycopg2.extras import RealDictCursor
# from datetime import datetime
# import sys
#
# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)
#
# # Configuration
# KAFKA_CONFIG = {
#     'bootstrap_servers': ['localhost:9092'],
#     'group_id': 'postgres-consumer-group4',
#     'auto_offset_reset': 'earliest',
#     'enable_auto_commit': True,
#     'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
# }
#
# POSTGRES_CONFIG = {
#     'host': 'localhost',
#     'database': 'kafka_data',
#     'user': 'postgres',  # Change this to your PostgreSQL username
#     'password': 1234,  # Change this to your PostgreSQL password
#     'port': 5432
# }
#
# TOPIC_NAME = 'customer_data_topic'
# TABLE_NAME = 'customer_data'
#
#
# class KafkaPostgresConsumer:
#     def __init__(self):
#         self.consumer = None
#         self.postgres_conn = None
#         self.postgres_cursor = None
#
#     def connect_to_kafka(self):
#         """Initialize Kafka consumer"""
#         try:
#             self.consumer = KafkaConsumer(TOPIC_NAME, **KAFKA_CONFIG)
#             logger.info(f"Connected to Kafka topic: {TOPIC_NAME}")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to connect to Kafka: {e}")
#             return False
#
#     def connect_to_postgres(self):
#         """Initialize PostgreSQL connection"""
#         try:
#             self.postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
#             self.postgres_cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
#             logger.info("Connected to PostgreSQL database")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to connect to PostgreSQL: {e}")
#             return False
#
#     def create_table_if_not_exists(self):
#         """Create the customer_data table if it doesn't exist"""
#         create_table_query = """
#         CREATE TABLE IF NOT EXISTS customer_data (
#             id SERIAL PRIMARY KEY,
#             customer_id VARCHAR(50),
#             name VARCHAR(255),
#             email VARCHAR(255),
#             phone VARCHAR(20),
#             address TEXT,
#             city VARCHAR(100),
#             state VARCHAR(100),
#             zip_code VARCHAR(20),
#             country VARCHAR(100),
#             registration_date DATE,
#             last_purchase_date DATE,
#             total_purchases DECIMAL(10,2),
#             customer_status VARCHAR(50),
#             kafka_offset BIGINT,
#             kafka_partition INTEGER,
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
#         """
#
#         try:
#             self.postgres_cursor.execute(create_table_query)
#             self.postgres_conn.commit()
#             logger.info(f"Table {TABLE_NAME} created or already exists")
#         except Exception as e:
#             logger.error(f"Error creating table: {e}")
#             self.postgres_conn.rollback()
#
#     def inspect_message_structure(self, data):
#         """Debug helper to inspect the actual message structure"""
#         logger.info("=== MESSAGE STRUCTURE INSPECTION ===")
#         logger.info(f"Message type: {type(data)}")
#         logger.info(f"Message content: {json.dumps(data, indent=2, default=str)}")
#
#         if isinstance(data, dict):
#             logger.info(f"Available keys: {list(data.keys())}")
#             for key, value in data.items():
#                 logger.info(f"  {key}: {type(value)} = {value}")
#         logger.info("=== END INSPECTION ===")
#
#     def extract_field_value(self, data, field_paths):
#         """
#         Extract field value from data using multiple possible paths
#         field_paths can be a string or list of possible field names/paths
#         """
#         if isinstance(field_paths, str):
#             field_paths = [field_paths]
#
#         for path in field_paths:
#             try:
#                 # Handle nested paths like 'customer.id' or 'customer_info.customer_id'
#                 if '.' in path:
#                     value = data
#                     for part in path.split('.'):
#                         value = value.get(part) if isinstance(value, dict) else None
#                         if value is None:
#                             break
#                     if value is not None:
#                         return value
#                 else:
#                     # Simple field access
#                     value = data.get(path)
#                     if value is not None:
#                         return value
#             except (AttributeError, TypeError):
#                 continue
#
#         return None
#
#     def insert_customer_data(self, data, offset, partition):
#         """Insert customer data into PostgreSQL with flexible field mapping"""
#
#         # Debug: inspect the message structure
#         self.inspect_message_structure(data)
#
#         insert_query = """
#         INSERT INTO customer_data (
#             customer_id, name, email, phone, address, city, state,
#             zip_code, country, registration_date, last_purchase_date,
#             total_purchases, customer_status, kafka_offset, kafka_partition
#         ) VALUES (
#             %(customer_id)s, %(name)s, %(email)s, %(phone)s, %(address)s,
#             %(city)s, %(state)s, %(zip_code)s, %(country)s, %(registration_date)s,
#             %(last_purchase_date)s, %(total_purchases)s, %(customer_status)s,
#             %(kafka_offset)s, %(kafka_partition)s
#         )
#         """
#
#         try:
#             # Map the actual Kafka message fields to PostgreSQL columns
#             # Combine first_name and last_name to create full name
#             first_name = self.extract_field_value(data, ['first_name'])
#             last_name = self.extract_field_value(data, ['last_name'])
#             full_name = None
#             if first_name and last_name:
#                 full_name = f"{first_name} {last_name}"
#             elif first_name:
#                 full_name = first_name
#             elif last_name:
#                 full_name = last_name
#
#             # Determine customer status based on active flag
#             active_flag = self.extract_field_value(data, ['active', 'activebool'])
#             customer_status = 'active' if active_flag else 'inactive'
#
#             insert_data = {
#                 'customer_id': str(self.extract_field_value(data, ['customer_id'])),  # Convert to string
#                 'name': full_name,
#                 'email': self.extract_field_value(data, ['email']),
#                 'phone': None,  # Not available in your Kafka data
#                 'address': None,  # Not available directly (only address_id)
#                 'city': None,  # Not available in your Kafka data
#                 'state': None,  # Not available in your Kafka data
#                 'zip_code': None,  # Not available in your Kafka data
#                 'country': None,  # Not available in your Kafka data
#                 'registration_date': self.extract_field_value(data, ['create_date']),
#                 'last_purchase_date': None,  # Not available in your Kafka data
#                 'total_purchases': None,  # Not available in your Kafka data
#                 'customer_status': customer_status,
#                 'kafka_offset': offset,
#                 'kafka_partition': partition
#             }
#
#             # Log what we extracted
#             logger.info("=== EXTRACTED DATA ===")
#             for key, value in insert_data.items():
#                 if key not in ['kafka_offset', 'kafka_partition']:
#                     logger.info(f"  {key}: {value}")
#             logger.info("=== END EXTRACTED DATA ===")
#
#             self.postgres_cursor.execute(insert_query, insert_data)
#             self.postgres_conn.commit()
#             logger.info(f"Inserted customer data for ID: {insert_data.get('customer_id')}")
#
#         except Exception as e:
#             logger.error(f"Error inserting data: {e}")
#             self.postgres_conn.rollback()
#             raise
#
#     def process_messages(self):
#         """Main message processing loop"""
#         logger.info("Starting to consume messages...")
#
#         try:
#             for message in self.consumer:
#                 try:
#                     # Extract message data
#                     customer_data = message.value
#                     offset = message.offset
#                     partition = message.partition
#
#                     if customer_data:
#                         logger.info(f"Processing message from partition {partition}, offset {offset}")
#                         self.insert_customer_data(customer_data, offset, partition)
#                     else:
#                         logger.warning("Received empty message")
#
#                 except json.JSONDecodeError as e:
#                     logger.error(f"JSON decode error: {e}")
#                     continue
#                 except Exception as e:
#                     logger.error(f"Error processing message: {e}")
#                     continue
#
#         except KeyboardInterrupt:
#             logger.info("Stopping consumer...")
#         except Exception as e:
#             logger.error(f"Unexpected error in message processing: {e}")
#
#     def close_connections(self):
#         """Clean up connections"""
#         if self.consumer:
#             self.consumer.close()
#             logger.info("Kafka consumer closed")
#
#         if self.postgres_cursor:
#             self.postgres_cursor.close()
#
#         if self.postgres_conn:
#             self.postgres_conn.close()
#             logger.info("PostgreSQL connection closed")
#
#
# def main():
#     consumer = KafkaPostgresConsumer()
#
#     try:
#         # Connect to services
#         if not consumer.connect_to_kafka():
#             sys.exit(1)
#
#         if not consumer.connect_to_postgres():
#             sys.exit(1)
#
#         # Create table
#         consumer.create_table_if_not_exists()
#
#         # Start processing
#         consumer.process_messages()
#
#     except Exception as e:
#         logger.error(f"Application error: {e}")
#     finally:
#         consumer.close_connections()
#
#
# if __name__ == "__main__":
#     main()
# !/usr/bin/env python3
"""
Kafka Consumer to PostgreSQL Writer
Consumes messages from customer_data_topic and inserts them into PostgreSQL
"""

import json
import logging
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'postgres-consumer-group',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
}

POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'kafka_data',
    'user': 'postgres',
    'password': "1234",  # Fixed: password must be string
    'port': 5432
}

TOPIC_NAME = 'customer_data_topic'
TABLE_NAME = 'customer_data'


class KafkaPostgresConsumer:
    def __init__(self):
        self.consumer = None
        self.postgres_conn = None
        self.postgres_cursor = None

    def connect_to_kafka(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(TOPIC_NAME, **KAFKA_CONFIG)
            logger.info(f"Connected to Kafka topic: {TOPIC_NAME}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def connect_to_postgres(self):
        """Initialize PostgreSQL connection"""
        try:
            self.postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
            self.postgres_cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def create_table_if_not_exists(self):
        """Create the customer_data table if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS customer_data (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(255) NOT NULL,
            store_id INTEGER NOT NULL,
            address_id INTEGER NOT NULL,
            activebool BOOLEAN NOT NULL,
            create_date DATE NOT NULL,
            last_update TIMESTAMP NOT NULL,
            active INTEGER NOT NULL,
            kafka_offset BIGINT,
            kafka_partition INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        try:
            self.postgres_cursor.execute(create_table_query)
            self.postgres_conn.commit()
            logger.info(f"Table {TABLE_NAME} created or already exists")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            self.postgres_conn.rollback()

    def extract_field_value(self, data, field_paths):
        """
        Extract field value from data using multiple possible paths
        field_paths can be a string or list of possible field names/paths
        """
        if isinstance(field_paths, str):
            field_paths = [field_paths]

        for path in field_paths:
            try:
                # Handle nested paths
                if '.' in path:
                    value = data
                    for part in path.split('.'):
                        value = value.get(part) if isinstance(value, dict) else None
                        if value is None:
                            break
                    if value is not None:
                        return value
                else:
                    # Simple field access
                    value = data.get(path)
                    if value is not None:
                        return value
            except (AttributeError, TypeError):
                continue

        return None

    def has_required_fields(self, data):
        """Check if all required fields are present and non-null"""
        required_fields = [
            'customer_id', 'store_id', 'first_name',
            'last_name', 'email', 'address_id',
            'activebool', 'create_date', 'last_update', 'active'
        ]

        for field in required_fields:
            value = self.extract_field_value(data, field)
            if value is None:
                logger.warning(f"Missing required field: {field}")
                return False
        return True

    def insert_customer_data(self, data, offset, partition):
        """Insert customer data into PostgreSQL if all required fields are present"""
        # Skip records with missing required fields
        if not self.has_required_fields(data):
            logger.warning("Skipping record due to missing required fields")
            return

        insert_query = f"""
        INSERT INTO {TABLE_NAME} (
            customer_id, first_name, last_name, email, store_id, address_id,
            activebool, create_date, last_update, active, kafka_offset, kafka_partition
        ) VALUES (
            %(customer_id)s, %(first_name)s, %(last_name)s, %(email)s, %(store_id)s, 
            %(address_id)s, %(activebool)s, %(create_date)s, %(last_update)s, 
            %(active)s, %(kafka_offset)s, %(kafka_partition)s
        )
        """

        try:
            # Map directly from Kafka message to PostgreSQL columns
            insert_data = {
                'customer_id': self.extract_field_value(data, 'customer_id'),
                'first_name': self.extract_field_value(data, 'first_name'),
                'last_name': self.extract_field_value(data, 'last_name'),
                'email': self.extract_field_value(data, 'email'),
                'store_id': self.extract_field_value(data, 'store_id'),
                'address_id': self.extract_field_value(data, 'address_id'),
                'activebool': self.extract_field_value(data, 'activebool'),
                'create_date': self.extract_field_value(data, 'create_date'),
                'last_update': self.extract_field_value(data, 'last_update'),
                'active': self.extract_field_value(data, 'active'),
                'kafka_offset': offset,
                'kafka_partition': partition
            }

            self.postgres_cursor.execute(insert_query, insert_data)
            self.postgres_conn.commit()
            logger.info(f"Inserted customer data for ID: {insert_data['customer_id']}")

        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            self.postgres_conn.rollback()

    def process_messages(self):
        """Main message processing loop"""
        logger.info("Starting to consume messages...")

        try:
            for message in self.consumer:
                try:
                    # Extract message data
                    customer_data = message.value
                    offset = message.offset
                    partition = message.partition

                    if customer_data:
                        logger.info(f"Processing message from partition {partition}, offset {offset}")
                        self.insert_customer_data(customer_data, offset, partition)
                    else:
                        logger.warning("Received empty message")

                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        except Exception as e:
            logger.error(f"Unexpected error in message processing: {e}")

    def close_connections(self):
        """Clean up connections"""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

        if self.postgres_cursor:
            self.postgres_cursor.close()

        if self.postgres_conn:
            self.postgres_conn.close()
            logger.info("PostgreSQL connection closed")


def main():
    consumer = KafkaPostgresConsumer()

    try:
        # Connect to services
        if not consumer.connect_to_kafka():
            sys.exit(1)

        if not consumer.connect_to_postgres():
            sys.exit(1)

        # Create table
        consumer.create_table_if_not_exists()

        # Start processing
        consumer.process_messages()

    except Exception as e:
        logger.error(f"Application error: {e}")
    finally:
        consumer.close_connections()


if __name__ == "__main__":
    main()