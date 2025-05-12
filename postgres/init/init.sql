-- Create schema for test data
CREATE SCHEMA IF NOT EXISTS test_schema;

-- Create tables for CDC testing
CREATE TABLE test_schema.customer (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_schema.product (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_schema.order (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES test_schema.customer(id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending',
    total_amount DECIMAL(12, 2) NOT NULL,
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_schema.order_item (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES test_schema.order(id),
    product_id INT REFERENCES test_schema.product(id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create publication for CDC
CREATE PUBLICATION cdc_publication FOR TABLE test_schema.customer, test_schema.product, test_schema.order, test_schema.order_item;

-- Create a replication slot for CDC
SELECT pg_create_logical_replication_slot('cdc_slot', 'wal2json');

-- Create triggers for updating the updated_at timestamp
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = CURRENT_TIMESTAMP;
   RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_customer_timestamp
BEFORE UPDATE ON test_schema.customer
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER update_product_timestamp
BEFORE UPDATE ON test_schema.product
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER update_order_timestamp
BEFORE UPDATE ON test_schema.order
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER update_order_item_timestamp
BEFORE UPDATE ON test_schema.order_item
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

-- Create a dedicated user for CDC replication
CREATE USER debezium WITH REPLICATION PASSWORD 'dbz';
ALTER USER debezium WITH SUPERUSER;
GRANT ALL PRIVILEGES ON DATABASE testdb TO debezium;
GRANT ALL PRIVILEGES ON SCHEMA test_schema TO debezium;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA test_schema TO debezium;
GRANT USAGE ON SCHEMA test_schema TO debezium;
ALTER PUBLICATION cdc_publication OWNER TO debezium;

-- Grant replication privileges
ALTER ROLE debezium REPLICATION LOGIN;