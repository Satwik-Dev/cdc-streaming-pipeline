"""
CDC Pipeline DAG - Enterprise Grade Pipeline Project

This DAG orchestrates the CDC pipeline from PostgreSQL to HDFS via Kafka
with robust error handling, retries, and monitoring.
"""

import os
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.kafka.hooks.kafka import KafkaHook
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 1),
}

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
CDC_CONNECTOR_NAME = os.getenv('CDC_CONNECTOR_NAME', 'postgres-cdc-connector')
HDFS_SINK_PATH = os.getenv('HDFS_SINK_PATH', '/data/cdc_sink')
HDFS_SINK_FILE = os.getenv('HDFS_SINK_FILE', 'cdc_events.json')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'testdb')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
CDC_TABLES = os.getenv('CDC_TABLES', 'customer,order,product').split(',')

# Create DAG
dag = DAG(
    'cdc_pipeline',
    default_args=default_args,
    description='Enterprise Grade CDC Pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['cdc', 'kafka', 'hdfs', 'enterprise'],
    max_active_runs=1,
)

# Helper functions
def check_postgres_replication_slots():
    """Check if PostgreSQL replication slots are properly configured."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check logical replication is enabled
    result = pg_hook.get_records("SHOW wal_level;")
    wal_level = result[0][0]
    if wal_level != 'logical':
        raise ValueError(f"WAL level is '{wal_level}', expected 'logical'")
    
    # Check replication slot exists
    result = pg_hook.get_records(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'cdc_slot';"
    )
    if not result:
        raise ValueError("CDC replication slot 'cdc_slot' not found")
    
    # Check publication exists
    result = pg_hook.get_records(
        "SELECT pubname FROM pg_publication WHERE pubname = 'cdc_publication';"
    )
    if not result:
        raise ValueError("CDC publication 'cdc_publication' not found")
    
    return True


def create_kafka_topics():
    """Create Kafka topics for CDC data."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='airflow-cdc-admin'
    )
    
    topics = []
    for table in CDC_TABLES:
        topics.append(NewTopic(
            name=f"cdc.{POSTGRES_DB}.test_schema.{table}",
            num_partitions=1,
            replication_factor=1
        ))
    
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin_client.close()
    
    return True


def configure_debezium_connector():
    """Configure Debezium connector for PostgreSQL CDC."""
    # Check if connector exists
    connector_url = f"http://kafka-connect:8083/connectors/{CDC_CONNECTOR_NAME}"
    try:
        response = requests.get(connector_url)
        if response.status_code == 200:
            print(f"Connector {CDC_CONNECTOR_NAME} already exists.")
            return True
    except requests.exceptions.RequestException:
        pass
    
    # Create the connector
    connector_config = {
        "name": CDC_CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": POSTGRES_HOST,
            "database.port": POSTGRES_PORT,
            "database.user": "debezium",
            "database.password": "dbz",
            "database.dbname": POSTGRES_DB,
            "database.server.name": POSTGRES_DB,
            "table.include.list": "test_schema.(customer|order|product|order_item)",
            "plugin.name": "wal2json",
            "slot.name": "cdc_slot",
            "publication.name": "cdc_publication",
            "heartbeat.interval.ms": "5000",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.add.fields": "op,table,lsn",
            "tombstones.on.delete": "true",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false"
        }
    }
    
    response = requests.post(
        "http://kafka-connect:8083/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config)
    )
    
    if response.status_code not in (201, 200, 409):
        raise ValueError(f"Failed to create connector: {response.text}")
    
    return True


def create_hdfs_sink_directory():
    """Create HDFS directory for CDC sink."""
    hdfs_hook = HDFSHook(hdfs_conn_id='hdfs_default')
    hdfs = hdfs_hook.get_conn()
    
    # Create directory if it doesn't exist
    if not hdfs.exists(HDFS_SINK_PATH):
        hdfs.mkdir(HDFS_SINK_PATH)
        print(f"Created HDFS directory: {HDFS_SINK_PATH}")
    
    # Create empty file if it doesn't exist
    sink_file_path = f"{HDFS_SINK_PATH}/{HDFS_SINK_FILE}"
    if not hdfs.exists(sink_file_path):
        hdfs.write(sink_file_path, b'')
        print(f"Created HDFS sink file: {sink_file_path}")
    
    return True


def configure_hdfs_sink_connector():
    """Configure HDFS Sink connector for Kafka Connect."""
    # Check if connector exists
    connector_name = "hdfs-sink-connector"
    connector_url = f"http://kafka-connect:8083/connectors/{connector_name}"
    try:
        response = requests.get(connector_url)
        if response.status_code == 200:
            print(f"Connector {connector_name} already exists.")
            return True
    except requests.exceptions.RequestException:
        pass
    
    # Create the connector
    connector_config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
            "tasks.max": "1",
            "topics.regex": f"cdc\\.{POSTGRES_DB}\\.test_schema\\..*",
            "hdfs.url": "hdfs://namenode:9000",
            "hadoop.conf.dir": "/opt/hadoop/etc/hadoop",
            "hadoop.home": "/opt/hadoop",
            "format.class": "io.confluent.connect.hdfs.json.JsonFormat",
            "flush.size": "10",
            "rotate.interval.ms": "60000",
            "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "timestamp.extractor": "Record",
            "storage.class": "io.confluent.connect.hdfs.storage.HdfsStorage",
            "hdfs.compression.codec": "gzip",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "hive.integration": "false",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
            "hdfs.append.mode": "true",
            "hdfs.append.output.file": HDFS_SINK_FILE,
            "name": connector_name
        }
    }
    
    response = requests.post(
        "http://kafka-connect:8083/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config)
    )
    
    if response.status_code not in (201, 200, 409):
        raise ValueError(f"Failed to create connector: {response.text}")
    
    return True


def check_kafka_topics():
    """Check if Kafka topics are created and accessible."""
    from kafka.admin import KafkaAdminClient
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='airflow-cdc-admin'
    )
    
    try:
        topics = admin_client.list_topics()
        for table in CDC_TABLES:
            topic_name = f"cdc.{POSTGRES_DB}.test_schema.{table}"
            if topic_name not in topics:
                raise ValueError(f"Topic {topic_name} not found")
        
        return True
    finally:
        admin_client.close()


def check_connector_status():
    """Check the status of Kafka Connect connectors."""
    # Check CDC connector
    response = requests.get(f"http://kafka-connect:8083/connectors/{CDC_CONNECTOR_NAME}/status")
    if response.status_code != 200:
        raise ValueError(f"Could not get status for connector {CDC_CONNECTOR_NAME}")
    
    status = response.json()
    connector_state = status.get("connector", {}).get("state", "")
    
    if connector_state != "RUNNING":
        raise ValueError(f"Connector {CDC_CONNECTOR_NAME} is in state {connector_state}, expected RUNNING")
    
    # Check HDFS Sink connector
    response = requests.get("http://kafka-connect:8083/connectors/hdfs-sink-connector/status")
    if response.status_code != 200:
        raise ValueError("Could not get status for HDFS sink connector")
    
    status = response.json()
    connector_state = status.get("connector", {}).get("state", "")
    
    if connector_state != "RUNNING":
        raise ValueError(f"HDFS sink connector is in state {connector_state}, expected RUNNING")
    
    return True


def create_hive_table():
    """Create Hive table for CDC data."""
    hive_hook = HiveCliHook(hive_cli_conn_id='hive_cli_default')
    
    # Create database if it doesn't exist
    hive_hook.run_cli(f"CREATE DATABASE IF NOT EXISTS cdc_db")
    
    # Create table if it doesn't exist
    hive_hook.run_cli(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS cdc_db.cdc_events (
            id INT,
            name STRING,
            email STRING,
            address STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            description STRING,
            price DECIMAL(10,2),
            stock INT,
            category STRING,
            status STRING,
            customer_id INT,
            order_date TIMESTAMP,
            total_amount DECIMAL(12,2),
            shipping_address STRING,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10,2),
            order_id INT,
            op STRING,
            table_name STRING,
            lsn STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
        STORED AS TEXTFILE
        LOCATION 'hdfs://namenode:9000{HDFS_SINK_PATH}'
        TBLPROPERTIES ('serialization.format'='1');
    """)
    
    return True


def check_hdfs_sink_file():
    """Check if the HDFS sink file exists and is accessible."""
    hdfs_hook = HDFSHook(hdfs_conn_id='hdfs_default')
    hdfs = hdfs_hook.get_conn()
    
    sink_file_path = f"{HDFS_SINK_PATH}/{HDFS_SINK_FILE}"
    if not hdfs.exists(sink_file_path):
        raise ValueError(f"HDFS sink file {sink_file_path} not found")
    
    return True


def run_test_data_generator():
    """Run the test data generator to produce CDC events."""
    # This function will kick off the test data generator container
    return True


def monitor_pipeline():
    """Monitor the CDC pipeline and log status."""
    # Monitor Kafka topics
    from kafka import KafkaConsumer
    
    # Create consumers for all topics
    consumers = {}
    for table in CDC_TABLES:
        topic_name = f"cdc.{POSTGRES_DB}.test_schema.{table}"
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'airflow-monitor-{table}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        consumers[topic_name] = consumer
    
    # Check if topics have messages
    for topic_name, consumer in consumers.items():
        partitions = consumer.partitions_for_topic(topic_name)
        if not partitions:
            print(f"Warning: No partitions found for topic {topic_name}")
            continue
        
        # Get topic info
        print(f"Monitoring topic: {topic_name}")
        
        # Close consumer
        consumer.close()
    
    # Check HDFS file
    hdfs_hook = HDFSHook(hdfs_conn_id='hdfs_default')
    hdfs = hdfs_hook.get_conn()
    
    sink_file_path = f"{HDFS_SINK_PATH}/{HDFS_SINK_FILE}"
    if hdfs.exists(sink_file_path):
        file_info = hdfs.info(sink_file_path)
        file_size = file_info.get('size', 0)
        print(f"HDFS sink file size: {file_size} bytes")
    else:
        print(f"Warning: HDFS sink file {sink_file_path} not found")
    
    return True


# Task definitions
check_postgres = PythonOperator(
    task_id='check_postgres_replication',
    python_callable=check_postgres_replication_slots,
    dag=dag,
)

create_topics = PythonOperator(
    task_id='create_kafka_topics',
    python_callable=create_kafka_topics,
    dag=dag,
)

create_hdfs_dir = PythonOperator(
    task_id='create_hdfs_directory',
    python_callable=create_hdfs_sink_directory,
    dag=dag,
)

configure_cdc = PythonOperator(
    task_id='configure_debezium_connector',
    python_callable=configure_debezium_connector,
    dag=dag,
)

configure_hdfs_sink = PythonOperator(
    task_id='configure_hdfs_sink',
    python_callable=configure_hdfs_sink_connector,
    dag=dag,
)

check_kafka = PythonOperator(
    task_id='check_kafka_topics',
    python_callable=check_kafka_topics,
    dag=dag,
)

check_connectors = PythonOperator(
    task_id='check_connector_status',
    python_callable=check_connector_status,
    dag=dag,
)

setup_hive = PythonOperator(
    task_id='create_hive_table',
    python_callable=create_hive_table,
    dag=dag,
)

check_hdfs = PythonOperator(
    task_id='check_hdfs_sink_file',
    python_callable=check_hdfs_sink_file,
    dag=dag,
)

run_test_generator = PythonOperator(
    task_id='run_test_data_generator',
    python_callable=run_test_data_generator,
    dag=dag,
)

monitor = PythonOperator(
    task_id='monitor_pipeline',
    python_callable=monitor_pipeline,
    dag=dag,
)

# Task dependencies
check_postgres >> create_topics >> configure_cdc
check_postgres >> create_hdfs_dir >> configure_hdfs_sink
configure_cdc >> check_kafka
configure_hdfs_sink >> check_connectors
[check_kafka, check_connectors] >> setup_hive >> check_hdfs >> run_test_generator >> monitor