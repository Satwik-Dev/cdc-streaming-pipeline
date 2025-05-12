#!/usr/bin/env python3
"""
Test data generator for CDC pipeline.
This script generates test data for the PostgreSQL database, 
performing inserts, updates, and deletes to test the CDC pipeline.
"""

import os
import time
import random
import logging
from typing import List, Dict, Any
import psycopg2
from faker import Faker
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# PostgreSQL connection parameters
PG_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'testdb'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
}

# Test configuration
TEST_SIZE = int(os.getenv('TEST_SIZE', '100'))
TEST_OPERATIONS = os.getenv('TEST_OPERATIONS', 'insert,update,delete').split(',')
TEST_INTERVAL_SEC = int(os.getenv('TEST_INTERVAL_SEC', '10'))

# Initialize Faker
fake = Faker()


def get_connection():
    """Get a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = True
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise


def create_customer() -> Dict[str, Any]:
    """Generate fake customer data."""
    return {
        'name': fake.name(),
        'email': fake.email(),
        'address': fake.address(),
    }


def create_product() -> Dict[str, Any]:
    """Generate fake product data."""
    return {
        'name': fake.word() + ' ' + fake.word(),
        'description': fake.text(max_nb_chars=200),
        'price': round(random.uniform(10.0, 1000.0), 2),
        'stock': random.randint(0, 100),
        'category': random.choice(['Electronics', 'Clothing', 'Food', 'Books', 'Toys']),
    }


def create_order(customer_ids: List[int]) -> Dict[str, Any]:
    """Generate fake order data."""
    if not customer_ids:
        logger.warning("No customers found to create order")
        return None
    
    return {
        'customer_id': random.choice(customer_ids),
        'status': random.choice(['pending', 'processing', 'shipped', 'delivered']),
        'total_amount': round(random.uniform(50.0, 5000.0), 2),
        'shipping_address': fake.address(),
    }


def create_order_item(order_id: int, product_ids: List[int]) -> Dict[str, Any]:
    """Generate fake order item data."""
    if not product_ids:
        logger.warning("No products found to create order item")
        return None
    
    return {
        'order_id': order_id,
        'product_id': random.choice(product_ids),
        'quantity': random.randint(1, 10),
        'unit_price': round(random.uniform(10.0, 500.0), 2),
    }


def insert_data(conn) -> Dict[str, List[int]]:
    """Insert initial test data into the database."""
    cursor = conn.cursor()
    ids = {'customers': [], 'products': [], 'orders': [], 'order_items': []}
    
    logger.info(f"Generating {TEST_SIZE} records for each table...")
    
    # Insert customers
    for _ in range(TEST_SIZE):
        customer = create_customer()
        cursor.execute(
            """
            INSERT INTO test_schema.customer (name, email, address)
            VALUES (%(name)s, %(email)s, %(address)s)
            RETURNING id
            """,
            customer
        )
        ids['customers'].append(cursor.fetchone()[0])
    
    # Insert products
    for _ in range(TEST_SIZE):
        product = create_product()
        cursor.execute(
            """
            INSERT INTO test_schema.product (name, description, price, stock, category)
            VALUES (%(name)s, %(description)s, %(price)s, %(stock)s, %(category)s)
            RETURNING id
            """,
            product
        )
        ids['products'].append(cursor.fetchone()[0])
    
    # Insert orders
    for _ in range(TEST_SIZE):
        order = create_order(ids['customers'])
        if order:
            cursor.execute(
                """
                INSERT INTO test_schema.order (customer_id, status, total_amount, shipping_address)
                VALUES (%(customer_id)s, %(status)s, %(total_amount)s, %(shipping_address)s)
                RETURNING id
                """,
                order
            )
            order_id = cursor.fetchone()[0]
            ids['orders'].append(order_id)
            
            # Insert 1-3 order items per order
            for _ in range(random.randint(1, 3)):
                order_item = create_order_item(order_id, ids['products'])
                if order_item:
                    cursor.execute(
                        """
                        INSERT INTO test_schema.order_item (order_id, product_id, quantity, unit_price)
                        VALUES (%(order_id)s, %(product_id)s, %(quantity)s, %(unit_price)s)
                        RETURNING id
                        """,
                        order_item
                    )
                    ids['order_items'].append(cursor.fetchone()[0])
    
    logger.info(f"Inserted {len(ids['customers'])} customers, {len(ids['products'])} products, "
                f"{len(ids['orders'])} orders, and {len(ids['order_items'])} order items.")
    return ids


def update_random_records(conn, ids: Dict[str, List[int]]):
    """Update random records in the database."""
    if 'update' not in TEST_OPERATIONS:
        return
    
    cursor = conn.cursor()
    update_count = 0
    
    # Update some customers
    for _ in range(min(10, len(ids['customers']))):
        customer_id = random.choice(ids['customers'])
        customer = create_customer()
        cursor.execute(
            """
            UPDATE test_schema.customer
            SET name = %(name)s, email = %(email)s, address = %(address)s
            WHERE id = %(id)s
            """,
            {**customer, 'id': customer_id}
        )
        update_count += cursor.rowcount
    
    # Update some products
    for _ in range(min(10, len(ids['products']))):
        product_id = random.choice(ids['products'])
        product = create_product()
        cursor.execute(
            """
            UPDATE test_schema.product
            SET name = %(name)s, description = %(description)s, 
                price = %(price)s, stock = %(stock)s, category = %(category)s
            WHERE id = %(id)s
            """,
            {**product, 'id': product_id}
        )
        update_count += cursor.rowcount
    
    # Update some orders
    for _ in range(min(10, len(ids['orders']))):
        order_id = random.choice(ids['orders'])
        cursor.execute(
            """
            UPDATE test_schema.order
            SET status = %(status)s
            WHERE id = %(id)s
            """,
            {'status': random.choice(['pending', 'processing', 'shipped', 'delivered']), 'id': order_id}
        )
        update_count += cursor.rowcount
    
    logger.info(f"Updated {update_count} records")


def delete_random_records(conn, ids: Dict[str, List[int]]):
    """Delete random records from the database."""
    if 'delete' not in TEST_OPERATIONS:
        return
    
    cursor = conn.cursor()
    delete_count = 0
    
    # Delete some order items first (to avoid foreign key constraints)
    if ids['order_items']:
        # Choose order items to delete (max 5% of total)
        items_to_delete = random.sample(
            ids['order_items'], 
            min(int(len(ids['order_items']) * 0.05) + 1, len(ids['order_items']))
        )
        for item_id in items_to_delete:
            cursor.execute("DELETE FROM test_schema.order_item WHERE id = %s", (item_id,))
            delete_count += cursor.rowcount
            ids['order_items'].remove(item_id)
    
    # Delete some orders (if they don't have order items)
    if ids['orders']:
        # Find orders without order items
        cursor.execute("""
            SELECT o.id FROM test_schema.order o
            LEFT JOIN test_schema.order_item oi ON o.id = oi.order_id
            WHERE oi.id IS NULL
        """)
        eligible_orders = [row[0] for row in cursor.fetchall()]
        
        # Choose orders to delete (max 5% of eligible)
        if eligible_orders:
            orders_to_delete = random.sample(
                eligible_orders,
                min(int(len(eligible_orders) * 0.05) + 1, len(eligible_orders))
            )
            for order_id in orders_to_delete:
                cursor.execute("DELETE FROM test_schema.order WHERE id = %s", (order_id,))
                delete_count += cursor.rowcount
                if order_id in ids['orders']:
                    ids['orders'].remove(order_id)
    
    logger.info(f"Deleted {delete_count} records")


def run_continuous_test(conn, ids: Dict[str, List[int]]):
    """Run continuous test operations."""
    logger.info("Starting continuous test operations...")
    
    try:
        while True:
            # Perform random operation
            operation = random.choice(TEST_OPERATIONS)
            
            if operation == 'insert':
                # Insert new data
                new_customer = create_customer()
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO test_schema.customer (name, email, address)
                    VALUES (%(name)s, %(email)s, %(address)s)
                    RETURNING id
                    """,
                    new_customer
                )
                customer_id = cursor.fetchone()[0]
                ids['customers'].append(customer_id)
                logger.info(f"Inserted new customer with ID {customer_id}")
            
            elif operation == 'update':
                update_random_records(conn, ids)
            
            elif operation == 'delete':
                delete_random_records(conn, ids)
            
            # Wait for the next interval
            time.sleep(TEST_INTERVAL_SEC)
    
    except KeyboardInterrupt:
        logger.info("Test generator stopped by user")
    except Exception as e:
        logger.error(f"Error in continuous test: {e}")


def main():
    """Main function."""
    logger.info("Starting test data generator...")
    
    # Wait for PostgreSQL to be ready
    max_retries = 10
    retry_interval = 5
    
    for attempt in range(max_retries):
        try:
            conn = get_connection()
            logger.info("Successfully connected to PostgreSQL")
            break
        except Exception as e:
            logger.warning(f"Connection attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Max retries reached. Could not connect to PostgreSQL.")
                return
    
    try:
        # Insert initial test data
        ids = insert_data(conn)
        
        # Run continuous test
        run_continuous_test(conn, ids)
    
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()