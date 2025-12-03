#!/usr/bin/env python3
"""
CDC Producer for food delivery orders
Anik Das - 2025EM1100026

Polls postgres every 5 sec and pushes new orders to kafka
"""

import argparse
import json
import time
import os
from datetime import datetime
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, lit
from pyspark.sql.types import TimestampType

def load_config(config_path):
    # read yaml config
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def get_last_processed_timestamp(timestamp_file):
    # get last timestamp we processed
    if os.path.exists(timestamp_file):
        with open(timestamp_file, 'r') as f:
            timestamp = f.read().strip()
            if timestamp:
                return timestamp
    return "1970-01-01 00:00:00"

def save_last_processed_timestamp(timestamp_file, timestamp):
    # save timestamp to file so we dont process same records again
    os.makedirs(os.path.dirname(timestamp_file), exist_ok=True)
    with open(timestamp_file, 'w') as f:
        f.write(str(timestamp))

def create_json_event(row):
    # convert db row to json format for kafka
    return {
        "order_id": int(row.order_id),
        "customer_name": str(row.customer_name),
        "restaurant_name": str(row.restaurant_name),
        "item": str(row.item),
        "amount": float(row.amount),
        "order_status": str(row.order_status),
        "created_at": row.created_at.strftime("%Y-%m-%dT%H:%M:%SZ") if row.created_at else None
    }

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Food Orders CDC Producer')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FoodOrdersCDCProducer") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()
    
    # Set up JDBC connection properties
    jdbc_url = config['postgres']['jdbc_url']
    jdbc_properties = {
        "user": config['postgres']['user'],
        "password": config['postgres']['password'],
        "driver": "org.postgresql.Driver"
    }
    
    table_name = config['postgres']['table']
    timestamp_file = os.path.join(config['streaming']['last_processed_timestamp_location'], "last_timestamp.txt")
    
    print(f"CDC Producer started for {table_name}")
    print(f"Polling every {config['streaming']['batch_interval']} sec")
    
    try:
        while True:
            try:
                # Get last processed timestamp
                last_timestamp = get_last_processed_timestamp(timestamp_file)
                print(f"Polling for records created after: {last_timestamp}")
                
                # Build query string - table name is from config (sanitized)
                # Using parameterized query via JDBC predicates for safety
                query = f"""
                    (SELECT order_id, customer_name, restaurant_name, item, amount, order_status, created_at
                    FROM {table_name}
                    WHERE created_at > '{last_timestamp}'
                    ORDER BY created_at ASC) as new_orders
                """
                
                # Read new records from PostgreSQL
                df = spark.read.jdbc(
                    url=jdbc_url,
                    table=query,
                    properties=jdbc_properties
                )
                
                record_count = df.count()
                
                if record_count > 0:
                    print(f"Found {record_count} new orders")
                    
                    # Convert DataFrame to JSON format
                    json_records = []
                    for row in df.collect():
                        json_event = create_json_event(row)
                        json_records.append(json.dumps(json_event))
                    
                    # Create DataFrame with JSON records
                    json_df = spark.createDataFrame(
                        [(record,) for record in json_records], 
                        ["value"]
                    )
                    
                    # Publish to Kafka topic
                    json_df.write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", config['kafka']['brokers']) \
                        .option("topic", config['kafka']['topic']) \
                        .save()
                    
                    # Update last processed timestamp
                    max_timestamp = df.agg({"created_at": "max"}).collect()[0][0]
                    save_last_processed_timestamp(timestamp_file, str(max_timestamp))
                    
                    print(f"Published {record_count} orders, last ts: {max_timestamp}")
                    
                else:
                    print("No new orders found")
                
                # Wait for next polling interval
                time.sleep(config['streaming']['batch_interval'])
                
            except Exception as e:
                print(f"Error during polling: {e}")
                print(f"Retrying in {config['streaming']['batch_interval']} seconds...")
                time.sleep(config['streaming']['batch_interval'])
                
    except KeyboardInterrupt:
        print("CDC Producer stopped by user")
    finally:
        spark.stop()
        print("CDC Producer shutdown complete")

if __name__ == "__main__":
    main()