#!/usr/bin/env python3
"""
Kafka Message Inspector
Simple script to inspect the structure of messages in your Kafka topic
"""

import json
import logging
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'inspector-group1',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,  # Don't commit offsets
    'value_deserializer': lambda x: x.decode('utf-8') if x else None
}

TOPIC_NAME = 'customer_data_topic'


def inspect_messages(max_messages=5):
    """Inspect the first few messages from the topic"""
    try:
        consumer = KafkaConsumer(TOPIC_NAME, **KAFKA_CONFIG)
        logger.info(f"Connected to topic: {TOPIC_NAME}")
        logger.info(f"Will inspect first {max_messages} messages...")

        message_count = 0
        for message in consumer:
            message_count += 1

            print(f"\n{'=' * 50}")
            print(f"MESSAGE {message_count}")
            print(f"{'=' * 50}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Timestamp: {message.timestamp}")
            print(f"Key: {message.key}")

            # Try to parse as JSON
            raw_value = message.value
            print(f"Raw Value: {raw_value}")

            if raw_value:
                try:
                    json_data = json.loads(raw_value)
                    print(f"Parsed JSON:")
                    print(json.dumps(json_data, indent=2))

                    if isinstance(json_data, dict):
                        print(f"Available keys: {list(json_data.keys())}")
                        print(f"Field details:")
                        for key, value in json_data.items():
                            print(f"  {key}: {type(value).__name__} = {value}")

                except json.JSONDecodeError as e:
                    print(f"Failed to parse as JSON: {e}")
                    print(f"Treating as plain text: {raw_value}")
            else:
                print("Empty message value")

            if message_count >= max_messages:
                break

        consumer.close()
        print(f"\nInspected {message_count} messages.")

    except Exception as e:
        logger.error(f"Error inspecting messages: {e}")


if __name__ == "__main__":
    inspect_messages(max_messages=3)  # Inspect first 3 messages