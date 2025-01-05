#!/bin/bash

# Kafka broker address
KAFKA_BROKER="kafka:9092"

# Topic name
TOPIC_NAME="test-topic"

# Create a Kafka topic
# Set default Kafka home path
KAFKA_HOME="/opt/kafka/bin/"
# export PATH=$PATH:$KAFKA_HOME/bin
cd $KAFKA_HOME
kafka-topics.sh --create --bootstrap-server $KAFKA_BROKER --replication-factor 1 --partitions 1 --topic $TOPIC_NAME

# Verify the topic creation
kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER