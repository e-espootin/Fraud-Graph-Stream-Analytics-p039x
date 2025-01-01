# kafka_config.py

CONFLUENT_CONFIG = {
    'bootstrap.servers': 'localhost:19092',  # Confluent Kafka bootstrap server
    'client.id': 'fin-trans-producer',
    'group.id': 'fin-consumer-group',
    'enable.auto.commit': False,
}

DEFAULT_TOPIC_CONFIG = {
    "num_partitions": 1,  # Default number of partitions
    "replication_factor": 1,  # Default replication factor
}

# Sensor-specific Kafka topics
KAFKA_TOPICS = {
    "fin-trans": "fin_trans_topic"
}
