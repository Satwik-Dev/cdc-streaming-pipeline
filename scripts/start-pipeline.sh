#!/bin/bash
# Start the CDC Pipeline - Single Command Script

set -e

echo "Starting CDC Pipeline..."

# Source environment variables
source .env

# Start all containers if not already running
start_containers() {
    echo "Starting all containers..."
    
    # Check if containers are already running
    if docker ps | grep -q "airflow-webserver"; then
        echo "Containers are already running."
    else
        docker-compose up -d
        
        # Wait for containers to start
        echo "Waiting for containers to start..."
        sleep 30
    fi
}

# Run setup script to ensure all connections and configurations are ready
run_setup() {
    echo "Running setup script..."
    bash scripts/setup.sh
}

# Trigger the Airflow DAG
trigger_dag() {
    echo "Triggering Airflow DAG..."
    
    # Wait for Airflow to be ready
    echo "Waiting for Airflow webserver to be ready..."
    while ! curl -s "http://localhost:8080/health" | grep -q "healthy"; do
        echo "Airflow not ready yet, waiting..."
        sleep 5
    done
    
    # Unpause the DAG if it's paused
    docker exec airflow-webserver airflow dags unpause cdc_pipeline
    
    # Trigger the DAG
    docker exec airflow-webserver airflow dags trigger cdc_pipeline
    
    echo "Airflow DAG has been triggered successfully!"
}

# Monitor pipeline execution - this will display logs in separate panes using tmux
monitor_pipeline() {
    echo "Setting up monitoring for the pipeline..."
    
    # Install tmux if not already installed
    if ! command -v tmux &> /dev/null; then
        echo "tmux is required for monitoring. Please install it first."
        exit 1
    fi
    
    # Create a new tmux session
    tmux new-session -d -s cdc_pipeline
    
    # Split window into panes
    tmux split-window -h -t cdc_pipeline
    tmux split-window -v -t cdc_pipeline:0.0
    tmux split-window -v -t cdc_pipeline:0.1
    
    # Setup commands for each pane
    tmux send-keys -t cdc_pipeline:0.0 "docker logs -f postgres" C-m
    tmux send-keys -t cdc_pipeline:0.1 "docker logs -f kafka" C-m
    tmux send-keys -t cdc_pipeline:0.2 "docker logs -f kafka-connect" C-m
    tmux send-keys -t cdc_pipeline:0.3 "docker logs -f namenode" C-m
    
    # Attach to the session
    tmux attach-session -t cdc_pipeline
}

# Alternative monitoring using docker logs in sequence
monitor_simple() {
    echo "Monitoring pipeline execution..."
    
    # Monitor PostgreSQL
    echo "============== PostgreSQL Logs =============="
    docker logs postgres --tail 20
    
    # Monitor Kafka
    echo "============== Kafka Logs =============="
    docker logs kafka --tail 20
    
    # Monitor Kafka Connect
    echo "============== Kafka Connect Logs =============="
    docker logs kafka-connect --tail 20
    
    # Monitor HDFS Namenode
    echo "============== HDFS Namenode Logs =============="
    docker logs namenode --tail 20
    
    # Monitor Airflow
    echo "============== Airflow Logs =============="
    docker logs airflow-webserver --tail 20
    
    echo "Pipeline is running. Check Airflow UI at http://localhost:8080 for progress."
}

# Main function
main() {
    echo "Starting CDC Pipeline..."
    
    # Start containers
    start_containers
    
    # Run setup
    run_setup
    
    # Trigger DAG
    trigger_dag
    
    # Offer monitoring options
    echo "Pipeline has been started successfully!"
    echo "Choose monitoring option:"
    echo "1) Full monitoring with tmux (requires tmux)"
    echo "2) Simple monitoring (no additional dependencies)"
    read -p "Enter your choice (1/2): " choice
    
    if [ "$choice" = "1" ]; then
        monitor_pipeline
    else
        monitor_simple
    fi
}

# Execute main function
main