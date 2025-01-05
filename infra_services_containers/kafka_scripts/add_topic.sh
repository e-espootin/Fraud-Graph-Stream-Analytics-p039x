#!/bin/bash

TOPIC_NAME="fin_trans_topic"
echo "creating topic ${TOPIC_NAME}"
# Kafka broker address
KAFKA_BROKER="kafka:9092"

# Create a Kafka topic
# Set default Kafka home path
KAFKA_HOME="/opt/kafka/bin/"
cd $KAFKA_HOME
kafka-topics.sh --create --bootstrap-server $KAFKA_BROKER --replication-factor 1 --partitions 1 --topic $TOPIC_NAME

# Verify the topic creation
kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER
echo "Topic ${TOPIC_NAME} created"