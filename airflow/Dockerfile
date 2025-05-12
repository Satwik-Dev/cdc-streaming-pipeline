FROM apache/airflow:2.6.0-python3.9

# Switch to root user to install additional packages
USER root

# Install necessary system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    default-jdk \
    curl \
    wget \
    git \
    python3-dev \
    libsasl2-dev \
    libsasl2-modules \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop client
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.2.3/hadoop-3.2.3.tar.gz && \
    tar -xzf hadoop-3.2.3.tar.gz -C /opt && \
    rm hadoop-3.2.3.tar.gz && \
    ln -s /opt/hadoop-3.2.3 /opt/hadoop && \
    mkdir -p /opt/hadoop/logs

# Set Hadoop environment variables
ENV HADOOP_HOME="/opt/hadoop" \
    HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop" \
    PATH="/opt/hadoop/bin:${PATH}"

# Install PostgreSQL client
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Install Kafka client
RUN wget https://downloads.apache.org/kafka/3.3.2/kafka_2.13-3.3.2.tgz && \
    tar -xzf kafka_2.13-3.3.2.tgz -C /opt && \
    rm kafka_2.13-3.3.2.tgz && \
    ln -s /opt/kafka_2.13-3.3.2 /opt/kafka

# Set Kafka environment variables
ENV KAFKA_HOME="/opt/kafka" \
    PATH="/opt/kafka/bin:${PATH}"

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create directories for logs
RUN mkdir -p ${AIRFLOW_HOME}/logs

# Create empty .env file for environment variables
RUN touch ${AIRFLOW_HOME}/.env

WORKDIR ${AIRFLOW_HOME}