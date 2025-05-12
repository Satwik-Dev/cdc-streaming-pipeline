#!/bin/bash
# Monitor the CDC Pipeline - Display status and logs in split windows

set -e

# Source environment variables
source .env

# Verify HDFS file content
check_hdfs_file() {
    echo "Checking HDFS sink file content..."
    
    # Check if namenode container is running
    if ! docker ps | grep -q "namenode"; then
        echo "Error: HDFS namenode container is not running."
        return 1
    fi
    
    # Get the HDFS sink file path
    SINK_FILE="${HDFS_SINK_PATH}/${HDFS_SINK_FILE}"
    
    # Check if the file exists and is accessible
    if ! docker exec namenode hdfs dfs -test -e "$SINK_FILE"; then
        echo "Error: HDFS sink file $SINK_FILE does not exist."
        return 1
    fi
    
    # Check file size
    FILE_SIZE=$(docker exec namenode hdfs dfs -du -s "$SINK_FILE" | awk '{print $1}')
    echo "HDFS sink file size: $FILE_SIZE bytes"
    
    # Display file content (last few lines)
    echo "Last 10 lines of HDFS sink file content:"
    docker exec namenode hdfs dfs -cat "$SINK_FILE" | tail -n 10
    
    return 0
}

# Check Kafka topics and messages
check_kafka_topics() {
    echo "Checking Kafka topics and messages..."
    
    # Check if Kafka container is running
    if ! docker ps | grep -q "kafka"; then
        echo "Error: Kafka container is not running."
        return 1
    fi
    
    # List topics
    echo "Kafka topics:"
    docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
    
    # Get message counts for CDC topics
    echo "Message counts for CDC topics:"
    for TABLE in ${CDC_TABLES//,/ }; do
        TOPIC="cdc.${POSTGRES_DB}.test_schema.${TABLE}"
        COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic "$TOPIC" --time -1 | awk -F ':' '{sum += $3} END {print sum}')
        echo "$TOPIC: $COUNT messages"
    done
    
    return 0
}

# Check Debezium connector status
check_connectors() {
    echo "Checking Kafka Connect connectors status..."
    
    # Check if Kafka Connect container is running
    if ! docker ps | grep -q "kafka-connect"; then
        echo "Error: Kafka Connect container is not running."
        return 1
    fi
    
    # List connectors
    echo "Kafka Connect connectors:"
    curl -s http://localhost:8083/connectors | jq -r .
    
    # Check CDC connector status
    echo "CDC connector status:"
    curl -s http://localhost:8083/connectors/${CDC_CONNECTOR_NAME}/status | jq -r .
    
    # Check HDFS sink connector status
    echo "HDFS sink connector status:"
    curl -s http://localhost:8083/connectors/hdfs-sink-connector/status | jq -r .
    
    return 0
}

# Check Hive table
check_hive_table() {
    echo "Checking Hive table..."
    
    # Check if Hive server container is running
    if ! docker ps | grep -q "hive-server"; then
        echo "Error: Hive server container is not running."
        return 1
    fi
    
    # Check if the CDC database exists
    echo "Hive databases:"
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"
    
    # Check if the CDC table exists
    echo "Hive tables in cdc_db:"
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "USE cdc_db; SHOW TABLES;"
    
    # Count records in the CDC table
    echo "Record count in cdc_events table:"
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SELECT COUNT(*) FROM cdc_db.cdc_events;"
    
    # Sample records from the CDC table
    echo "Sample records from cdc_events table:"
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SELECT * FROM cdc_db.cdc_events LIMIT 5;"
    
    return 0
}

# Check Airflow DAG status
check_airflow_dag() {
    echo "Checking Airflow DAG status..."
    
    # Check if Airflow webserver container is running
    if ! docker ps | grep -q "airflow-webserver"; then
        echo "Error: Airflow webserver container is not running."
        return 1
    fi
    
    # Get DAG status
    echo "Airflow DAG status:"
    docker exec airflow-webserver airflow dags state cdc_pipeline
    
    # Get last DAG run
    echo "Last DAG run:"
    docker exec airflow-webserver airflow dags list-runs -d cdc_pipeline
    
    return 0
}

# Monitor all components using tmux
monitor_all() {
    echo "Setting up monitoring for all components..."
    
    # Install tmux if not already installed
    if ! command -v tmux &> /dev/null; then
        echo "tmux is required for monitoring. Please install it first."
        exit 1
    fi
    
    # Create a new tmux session
    tmux new-session -d -s cdc_monitor
    
    # Split window into panes
    tmux split-window -h -t cdc_monitor
    tmux split-window -v -t cdc_monitor:0.0
    tmux split-window -v -t cdc_monitor:0.1
    
    # Setup commands for each pane
    tmux send-keys -t cdc_monitor:0.0 "docker logs -f postgres | grep -i 'wal\|replication\|cdc'" C-m
    tmux send-keys -t cdc_monitor:0.1 "docker logs -f kafka-connect | grep -i 'debezium\|connector\|postgres'" C-m
    tmux send-keys -t cdc_monitor:0.2 "docker logs -f namenode | grep -i 'hdfs\|sink\|data'" C-m
    tmux send-keys -t cdc_monitor:0.3 "docker logs -f airflow-webserver | grep -i 'cdc_pipeline'" C-m
    
    # Attach to the session
    tmux attach-session -t cdc_monitor
}

# Main function
main() {
    echo "Monitoring CDC Pipeline..."
    
    # Offer menu options
    echo "Choose monitoring option:"
    echo "1) Check HDFS sink file"
    echo "2) Check Kafka topics and messages"
    echo "3) Check Kafka Connect connectors"
    echo "4) Check Hive table"
    echo "5) Check Airflow DAG status"
    echo "6) Monitor all components (requires tmux)"
    echo "7) Run all checks"
    echo "q) Quit"
    
    read -p "Enter your choice: " choice
    
    case $choice in
        1)
            check_hdfs_file
            ;;
        2)
            check_kafka_topics
            ;;
        3)
            check_connectors
            ;;
        4)
            check_hive_table
            ;;
        5)
            check_airflow_dag
            ;;
        6)
            monitor_all
            ;;
        7)
            check_hdfs_file
            check_kafka_topics
            check_connectors
            check_hive_table
            check_airflow_dag
            ;;
        q|Q)
            echo "Exiting..."
            exit 0
            ;;
        *)
            echo "Invalid choice."
            ;;
    esac
    
    # Return to menu
    echo ""
    read -p "Press Enter to return to menu..." dummy
    main
}

# Execute main function
main