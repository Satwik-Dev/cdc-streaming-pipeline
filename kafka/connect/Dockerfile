FROM confluentinc/cp-kafka-connect:7.3.0

# Install Debezium PostgreSQL connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.1.0

# Install HDFS connector for Kafka Connect
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs:10.1.0

# Copy custom configurations
COPY config/connect-distributed.properties /etc/kafka-connect/connect-distributed.properties

# Set working directory
WORKDIR /etc/kafka-connect

# Set user to run the container
USER root

# Entry point
CMD ["sh", "-c", "connect-distributed /etc/kafka-connect/connect-distributed.properties"]