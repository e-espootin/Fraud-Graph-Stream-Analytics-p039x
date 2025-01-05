# Use Alpine as the base image
FROM alpine:3.18

# Install required dependencies and Java 11
RUN apk add --no-cache \
    openjdk11 \
    bash \
    curl \
    jq \
    git

# Set JAVA_HOME and PATH for Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Kafka client
RUN curl -O https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz && \
    tar -xzf kafka_2.13-3.8.0.tgz && \
    mv kafka_2.13-3.8.0 /opt/kafka && \
    rm kafka_2.13-3.8.0.tgz

# Add Kafka binaries to PATH
ENV PATH="/opt/kafka/bin:$PATH"

COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
# Set default command
CMD ["bash"]