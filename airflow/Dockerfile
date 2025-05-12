FROM apache/airflow:2.6.0-python3.9

# Switch to root user to install system packages
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
    postgresql-client \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create Hadoop directory structure
RUN mkdir -p /opt/hadoop/logs

# Set Hadoop environment variables
ENV HADOOP_HOME="/opt/hadoop" \
    PATH="/opt/hadoop/bin:${PATH}"

# Switch back to airflow user for Python package installation
USER airflow

# Install Python dependencies
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create empty .env file for environment variables
RUN touch ${AIRFLOW_HOME}/.env

WORKDIR ${AIRFLOW_HOME}