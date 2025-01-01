from confluent_kafka import Producer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from kafka_src.kafka_config import CONFLUENT_CONFIG, KAFKA_TOPICS, DEFAULT_TOPIC_CONFIG
import json


class KafkaSensorProducer:
    def __init__(self):
        self.producer = Producer(
            {'bootstrap.servers': CONFLUENT_CONFIG['bootstrap.servers']})
        self.admin_client = AdminClient(
            {'bootstrap.servers': CONFLUENT_CONFIG['bootstrap.servers']})

    # topic creation
    def create_topic(self, topic: str):
        """
        Create a Kafka topic if it doesn't exist.

        :param topic: Name of the Kafka topic.
        """
        existing_topics = set(
            self.admin_client.list_topics(timeout=5).topics.keys())
        if topic in existing_topics:
            print(f"Topic '{topic}' already exists.")
            return

        # Define the new topic
        new_topic = NewTopic(
            topic,
            num_partitions=DEFAULT_TOPIC_CONFIG["num_partitions"],
            replication_factor=DEFAULT_TOPIC_CONFIG["replication_factor"]
        )

        # Try to create the topic
        try:
            self.admin_client.create_topics([new_topic])
            print(f"Created topic: {topic}")
        except KafkaException as e:
            print(f"Failed to create topic {topic}: {e}")

    # send data
    def send(self, topic_name: str, data: dict):
        """
        Send data to the Kafka topic corresponding to the sensor type.

        :param sensor_type: The type of sensor (e.g., 'temperature', 'humidity', 'gps').
        :param data: The data to send to the Kafka topic.
        """
        try:
            if topic_name not in KAFKA_TOPICS:
                raise ValueError(f"Unknown sensor type: {topic_name}")

            topic = KAFKA_TOPICS[topic_name]
            payload = json.dumps(data).encode('utf-8')
            # todo add kafka key
            self.producer.produce(
                topic, payload, callback=self.delivery_report)
            self.producer.flush()  # Ensure all messages are sent
            # print(f"Sent to {topic_name}: {data}")
        except ValueError as ve:
            print(f"ValueError: {ve}")
            print(f"Topic '{topic_name}' does not exist. Creating topic...")
            self.create_topic(topic_name)

        except KafkaException as e:
            # Check for UNKNOWN_TOPIC_OR_PART error
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(
                    f"Topic '{topic_name}' does not exist. Creating topic...")
                self.create_topic(topic_name)
            # UNKNOWN_TOPIC_ID
            #
            else:
                print(f"Failed to send message to {topic}: {str(e)}")

    @staticmethod
    def delivery_report(err, msg):
        """Delivery callback to handle success or error after sending a message."""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def close(self):
        self.producer.flush()
