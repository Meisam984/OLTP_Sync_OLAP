from kafka.admin import KafkaAdminClient, NewTopic

import os
from dotenv import load_dotenv

load_dotenv('../.env')

kafka_brokers = os.getenv('KAFKA_BROKERS')

def create_kafka_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=f"{kafka_brokers}",  # Kafka broker addresses
        client_id='topic_creation_script'
    )

    topic_list = [
        NewTopic(name="users", num_partitions=3, replication_factor=2),
        NewTopic(name="products", num_partitions=3, replication_factor=2),
        NewTopic(name="orders", num_partitions=3, replication_factor=2)
    ]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("Kafka topics created: users, products, orders")
    except Exception as e:
        print(f"Failed to create topics: {e}")
    finally:
        admin_client.close()
