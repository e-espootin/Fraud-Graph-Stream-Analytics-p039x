from kafka_src import kafka_producer

class StreamHandler:
    def send(self, topic: str, data: dict):
        #  sending data to Kafka
        kafka_producer.KafkaSensorProducer().send(topic_name=topic, data=data)
        print(f"Streaming data: {data}")