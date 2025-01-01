# kafka_consumer.py

from confluent_kafka import Consumer, OFFSET_BEGINNING
from kafka_src.kafka_config import CONFLUENT_CONFIG, KAFKA_TOPICS, DEFAULT_TOPIC_CONFIG
import json


class KafkaSensorConsumer:
    def __init__(self, sensor_type: str):
        """
        Initialize a Kafka consumer for a specific sensor type.

        :param sensor_type: The type of sensor (e.g., 'temperature', 'humidity', 'gps').
        """
        if sensor_type not in KAFKA_TOPICS:
            raise ValueError(f"Unknown sensor type: {sensor_type}")

        self.topic = KAFKA_TOPICS[sensor_type]
        self.consumer = Consumer({
            **CONFLUENT_CONFIG,
            'auto.offset.reset': 'earliest',
        })
        self.consumer.subscribe([self.topic])

    def consume(self):
        """
        Continuously consume data from the Kafka topic.
        """
        print(f"Consuming messages from topic: {self.topic}")
        try:
            while True:
                message = self.consumer.poll(1.0)  # Poll with a timeout of 1 second
                if message is None:
                    continue
                if message.error():
                    print(f"Consumer error: {message.error()}")
                    continue

                data = json.loads(message.value().decode('utf-8'))
                print(f"Received from {self.topic}: {data}")
                self.consumer.commit()  # Commit offsets manually
        except KeyboardInterrupt:
            print("\nConsumer stopped.")
        finally:
            self.close()

    def close(self):
        self.consumer.close()