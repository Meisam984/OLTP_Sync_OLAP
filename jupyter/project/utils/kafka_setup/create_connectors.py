import requests
import json

import os
from dotenv import load_dotenv

load_dotenv('../.env')

db_name = os.getenv('POSTGRES_DB')
db_password = os.getenv('POSTGRES_PASS')
db_user = os.getenv('POSTGRES_USER')
db_port = os.getenv('POSTGRES_PORT')
db_host = os.getenv('POSTGRES_HOST')
kafka_brokers = os.getenv('KAFKA_BROKERS')
kafka_connect = os.getenv('KAFKA_CONNECT')

def create_debezium_connector(table_name):
    url = f'{kafka_connect}'  # Debezium container REST API URL
    headers = {"Content-Type": "application/json"}

    # Debezium PostgreSQL connector configuration
    connector_config = {
        "name": f'{table_name}-connector',
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "plugin.name": "pgoutput",
            "tasks.max": "1",
            "database.hostname": f"{db_host}",
            "database.port": f"{db_port}",
            "database.user": f"{db_user}",
            "database.password": f"{db_password}",
            "database.dbname": f"{db_name}",
            "database.server.name": f"{db_host}",
            "table.include.list": f"public.{table_name}",
            "database.history.kafka.bootstrap.servers": f"{kafka_brokers}",
            "database.history.kafka.topic": f"{table_name}_history",
            "topic.prefix": "debezium",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            "topic.creation.enable": "true",
            "topic.creation.default.replication.factor": "2",
            "topic.creation.default.partitions": "3",
            "topic.creation.default.cleanup.policy": "delete",
            "topic.creation.default.retention.ms": "604800000"
        }
    }

    response = requests.post(url, headers=headers,
                             data=json.dumps(connector_config))

    if response.status_code == 201:
        print(f"Successfully created Debezium connector: {table_name}-connector")
    else:
        print(
            f"Failed to create Debezium connector: {response.status_code} - {response.text}")

# Define the Debezium connectors for the users, products, and orders tables


def setup_connectors():
    create_debezium_connector('orders')
    create_debezium_connector('products')
    create_debezium_connector('users')


