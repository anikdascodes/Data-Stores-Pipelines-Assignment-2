#!/usr/bin/env python3
"""
Simplified Food Delivery Orders CDC Producer
Assignment 2 - Data Stores & Pipelines
Student: Anik Das (2025EM1100026)

This simplified version works with local PySpark and Docker containers.
"""

import argparse
import json
import time
import os
from datetime import datetime
import yaml
import psycopg2
from kafka import KafkaProducer

def load_config(config_path):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def get_last_processed_timestamp(timestamp_file):
    """Get the last processed timestamp from file"""
    if os.path.exists(timestamp_file):
        with open(timestamp_file, 'r') as f:
            timestamp = f.read().strip()
            if timestamp:
                return timestamp
    return "1970-01-01 00:00:00"

def save_last_processed_timestamp(timestamp_file, timestamp):
    """Save the last processed timestamp to file"""
    os.makedirs(os.path.dirname(timestamp_file), exist_ok=True)
    with open(timestamp_file, 'w') as f:
        f.write(str(timestamp))

def create_json_event(row):
    """Convert database row to JSON format as specified in assignment"""
    return {
        "order_id": int(row[0]),
        "customer_name": str(row[1]),
        "restaurant_name": str(row[2]),
        "item": str(row[3]),
        "amount": float(row[4]),
        "order_status": str(row[5]),
        "created_at": row[6].strftime("%Y-%m-%dT%H:%M:%SZ") if row[6] else None
    }

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Food Orders CDC Producer')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Setup paths
    timestamp_file = os.path.join(config['streaming']['last_processed_timestamp_location'], "last_timestamp.txt")
    
    print(f"Starting Simplified CDC Producer for table: orders_2025EM1100026")
    print(f"Kafka topic: {config['kafka']['topic']}")
    print(f"Polling interval: {config['streaming']['batch_interval']} seconds")
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=config['postgres']['host'],
            port=config['postgres']['port'],
            database=config['postgres']['db'],
            user=config['postgres']['user'],
            password=config['postgres']['password']
        )
        print("✅ Connected to PostgreSQL")
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        return
    
    # Connect to Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=config['kafka']['brokers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connected to Kafka")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        conn.close()
        return
    
    try:
        while True:
            try:
                # Get last processed timestamp
                last_timestamp = get_last_processed_timestamp(timestamp_file)
                print(f"Polling for records created after: {last_timestamp}")
                
                # Query for new records
                cursor = conn.cursor()
                query = """
                    SELECT order_id, customer_name, restaurant_name, item, amount, order_status, created_at 
                    FROM orders_2025EM1100026 
                    WHERE created_at > %s 
                    ORDER BY created_at ASC
                """
                cursor.execute(query, (last_timestamp,))
                rows = cursor.fetchall()
                
                record_count = len(rows)
                
                if record_count > 0:
                    print(f"Found {record_count} new orders")
                    
                    # Convert to JSON and publish to Kafka
                    for row in rows:
                        json_event = create_json_event(row)
                        producer.send(config['kafka']['topic'], json_event)
                        print(f"Published order {json_event['order_id']} to Kafka")
                    
                    producer.flush()
                    
                    # Update last processed timestamp
                    max_timestamp = max(row[6] for row in rows)
                    save_last_processed_timestamp(timestamp_file, str(max_timestamp))
                    
                    print(f"✅ Successfully published {record_count} orders to Kafka topic: {config['kafka']['topic']}")
                    print(f"Updated last processed timestamp to: {max_timestamp}")
                    
                else:
                    print("No new orders found")
                
                cursor.close()
                
                # Wait for next polling interval
                print(f"Waiting {config['streaming']['batch_interval']} seconds for next poll...")
                time.sleep(config['streaming']['batch_interval'])
                
            except Exception as e:
                print(f"Error during polling: {e}")
                print(f"Retrying in {config['streaming']['batch_interval']} seconds...")
                time.sleep(config['streaming']['batch_interval'])
                
    except KeyboardInterrupt:
        print("CDC Producer stopped by user")
    finally:
        conn.close()
        producer.close()
        print("CDC Producer shutdown complete")

if __name__ == "__main__":
    main()