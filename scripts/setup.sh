#!/bin/bash
# Setup script for the CDC Pipeline Environment

set -e

echo "Setting up CDC Pipeline Environment..."

# Create directories if they don't exist
mkdir -p postgres/init
mkdir -p kafka/connect/config
mkdir -p hadoop/config
mkdir -p hadoop/hive
mkdir -p test-generator
mkdir -p airflow/dags
mkdir -p airflow/plugins
mkdir -p scripts

# Wait for Airflow to be ready
wait_for_airflow() {
    echo "Waiting for Airflow webserver to be ready..."
    while ! curl -s "http://localhost:8080/health" | grep -q "healthy"; do
        echo "Airflow not ready yet, waiting..."
        sleep 5
    done
    echo "Airflow is ready!"
}

# Set up Airflow connections
setup_airflow_connections() {
    echo "Setting up Airflow connections..."
    
    # Wait for Airflow to be ready
    wait_for_airflow
    
    # Set up PostgreSQL connection
    docker exec airflow-webserver airflow connections add 'postgres_default' \
        --conn-type 'postgres' \
        --conn-host 'postgres' \
        --conn-login "${POSTGRES_USER}" \
        --conn-password "${POSTGRES_PASSWORD}" \
        --conn-port "${POSTGRES_PORT}" \
        --conn-schema "${POSTGRES_DB}"
    
    # Set up HDFS connection
    docker exec airflow-webserver airflow connections add 'hdfs_default' \
        --conn-type 'hdfs' \
        --conn-host 'namenode' \
        --conn-port '9000' \
        --conn-login 'root'
    
    # Set up Hive CLI connection
    docker exec airflow-webserver airflow connections add 'hive_cli_default' \
        --conn-type 'hive_cli' \
        --conn-host 'hive-server' \
        --conn-port '10000' \
        --conn-login 'hive' \
        --conn-schema 'default'
    
    # Set up Kafka connection
    docker exec airflow-webserver airflow connections add 'kafka_default' \
        --conn-type 'kafka' \
        --conn-host 'kafka' \
        --conn-port '9092'
    
    echo "Airflow connections have been set up successfully!"
}

# Initialize HDFS directories
init_hdfs() {
    echo "Initializing HDFS directories..."
    
    # Wait for HDFS to be ready
    echo "Waiting for HDFS namenode to be ready..."
    while ! docker exec namenode hdfs dfsadmin -report > /dev/null 2>&1; do
        echo "HDFS not ready yet, waiting..."
        sleep 5
    done
    
    # Create directories in HDFS
    docker exec namenode hdfs dfs -mkdir -p /data/cdc_sink
    docker exec namenode hdfs dfs -chmod -R 777 /data/cdc_sink
    
    echo "HDFS directories have been initialized!"
}

# Create Hive database and table
init_hive() {
    echo "Initializing Hive database and table..."
    
    # Wait for Hive to be ready
    echo "Waiting for Hive server to be ready..."
    while ! docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;" > /dev/null 2>&1; do
        echo "Hive not ready yet, waiting..."
        sleep 5
    done
    
    # Create CDC database and table
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "CREATE DATABASE IF NOT EXISTS cdc_db;"
    
    echo "Hive database has been initialized!"
}

# Main function
main() {
    echo "Starting setup process..."
    
    # Source environment variables
    source .env
    
    # Check if containers are running
    if ! docker ps | grep -q "airflow-webserver"; then
        echo "Error: Containers are not running. Please start the containers first."
        exit 1
    fi
    
    # Setup Airflow connections
    setup_airflow_connections
    
    # Initialize HDFS directories
    init_hdfs
    
    # Initialize Hive database
    init_hive
    
    echo "Setup completed successfully!"
}

# Execute main function
main